from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from result import Err, Ok

from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller
from src.analysis.mesh_analysis.analyzers.waku.waku_analyzer import WakuAnalyzer
from src.analysis.mesh_analysis.readers.builders.victoria_reader_builder import VictoriaQueryBuilder
from src.analysis.mesh_analysis.readers.victoria_reader import VictoriaReader
from src.analysis.metrics import scrape_utils
from src.analysis.post_run.metrics import plot_run_metrics, scrape_run_metrics
from src.deployments.core.event_window_bridge import EventNotFound

if TYPE_CHECKING:
    from src.deployments.experiments.logos_messaging import LogosMessagingExperiment

logger = logging.getLogger(__name__)

VICTORIA_LOGS_URL = "https://vlselect.lab.vac.dev/select/logsql/query"
METRICS_URL = "https://metrics.lab.vac.dev/select/0/prometheus/api/v1/"
RECEIVED_CSV = Path("summary") / "received.csv"
SETTINGS_LINE = "Messaging API settings"
RECEIVED_LINE = "Message received"
PROPAGATED_LINE = "Message successfully propagated"
FAILED_LINE = "Failed to send message"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
LINE_TIME = r"^\w{3} (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+[+-]\d\d:\d\d) "
RECEIVED_LINE_RE = re.compile(LINE_TIME + r"Message received.*?msg_hash=(0x[0-9a-f]+)")
SEND_LINE_RE = re.compile(
    LINE_TIME + r"(Message successfully propagated|Failed to send message)"
    r".*?requestId=(\S+).*?msgHash=(0x[0-9a-f]+)"
)

ReceivedLine = Tuple[str, str, int]
"""(node, message hash, line time ns)."""
SendLine = Tuple[str, str, str, str, int]
"""(node, `propagated` or `failed`, requestId, message hash, line time ns)."""


def distribution_ms(values_ns: List[int]) -> dict:
    if not values_ns:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    ms = np.array(values_ns, dtype=float) / 1e6
    return {
        "count": len(ms),
        "p50": round(float(np.percentile(ms, 50)), 1),
        "p95": round(float(np.percentile(ms, 95)), 1),
        "max": round(float(ms.max()), 1),
    }


def line_ns(timestamp: str) -> int:
    since_epoch = datetime.fromisoformat(timestamp) - EPOCH
    seconds = since_epoch.days * 86_400 + since_epoch.seconds
    return seconds * 1_000_000_000 + since_epoch.microseconds * 1_000


def parse_received_line(line: str) -> Optional[Tuple[str, int]]:
    """Message hash and node-clock time (ns) of a `Message received` line."""
    match = RECEIVED_LINE_RE.match(line)
    if not match:
        return None
    return match.group(2), line_ns(match.group(1))


def parse_send_line(line: str) -> Optional[Tuple[str, str, str, int]]:
    """Outcome, requestId, message hash and time (ns) of a propagated or failed send line."""
    match = SEND_LINE_RE.match(line)
    if not match:
        return None
    outcome = "propagated" if match.group(2) == PROPAGATED_LINE else "failed"
    return outcome, match.group(3), match.group(4), line_ns(match.group(1))


def load_sends(events_log: Path) -> List[dict]:
    """The experiment's `send` events, one per message it tried to send."""
    events = (json.loads(line) for line in events_log.read_text().splitlines() if line.strip())
    return [event for event in events if event.get("event") == "send"]


def send_outcomes(sends: List[dict], send_lines: List[SendLine]) -> dict:
    """Sends matched to their node log line by `requestId`.

    A send without a reply has no `requestId` to match, so those are counted per node next to
    the outcome lines whose `requestId` the experiment never received.
    """
    known = {s["requestId"] for s in sends if s.get("requestId")}
    outcome_of = {request_id: outcome for _, outcome, request_id, _, _ in send_lines}
    counts = {"propagated": 0, "failed": 0, "no_log_line": 0}
    for request_id in known:
        counts[outcome_of.get(request_id, "no_log_line")] += 1
    without_reply = Counter(s["node"] for s in sends if not s.get("requestId"))
    unmatched = Counter(node for node, _, request_id, _, _ in send_lines if request_id not in known)
    messages = {
        msg_hash: node
        for node, outcome, request_id, msg_hash, _ in send_lines
        if outcome == "propagated" and (request_id in known or node in without_reply)
    }
    return {
        "sent": len(sends),
        "accepted": len(known),
        **counts,
        "without_reply": dict(sorted(without_reply.items())),
        "unmatched_lines": dict(sorted(unmatched.items())),
        "messages": messages,
    }


def first_lines(received_lines: List[ReceivedLine]) -> Dict[Tuple[str, str], int]:
    """(message hash, node) -> time of its first `Message received` line."""
    first: Dict[Tuple[str, str], int] = {}
    for node, msg_hash, ns in received_lines:
        first[(msg_hash, node)] = min(ns, first.get((msg_hash, node), ns))
    return first


def receivers_by_hash(received_lines: List[ReceivedLine]) -> Dict[str, Set[str]]:
    receivers: Dict[str, Set[str]] = defaultdict(set)
    for node, msg_hash, _ in received_lines:
        receivers[msg_hash].add(node)
    return receivers


def delivery(
    messages: Dict[str, str], received_lines: List[ReceivedLine], nodes: List[str]
) -> dict:
    """Receivers of each propagated message other than its sender, against `len(nodes) - 1`."""
    receivers = receivers_by_hash(received_lines)
    missing_by_node: Dict[str, int] = defaultdict(int)
    complete = 0
    for msg_hash, sender in messages.items():
        missing = set(nodes) - receivers[msg_hash] - {sender}
        for node in missing:
            missing_by_node[node] += 1
        complete += not missing
    expected = (len(nodes) - 1) * len(messages)
    observed = sum(len(receivers[h] - {s}) for h, s in messages.items())
    return {
        "expected_per_message": len(nodes) - 1,
        "expected": expected,
        "observed": observed,
        "residual": expected - observed,
        "messages_complete": complete,
        "self_receipts": sum(s in receivers[h] for h, s in messages.items()),
        "missing_by_node": dict(sorted(missing_by_node.items())),
    }


def delivery_latency(messages: Dict[str, str], received_lines: List[ReceivedLine]) -> List[int]:
    """Each receiver's first line minus the sender's own `Message received` line (ns)."""
    first = first_lines(received_lines)
    latencies = []
    for (msg_hash, node), ns in first.items():
        sender = messages.get(msg_hash)
        if sender and node != sender and (msg_hash, sender) in first:
            latencies.append(ns - first[(msg_hash, sender)])
    return latencies


def propagation_time(
    messages: Dict[str, str], send_lines: List[SendLine], received_lines: List[ReceivedLine]
) -> List[int]:
    """`propagated` line minus the sender's own `Message received` line (ns)."""
    first = first_lines(received_lines)
    return [
        ns - first[(msg_hash, node)]
        for node, outcome, _, msg_hash, ns in send_lines
        if outcome == "propagated" and messages.get(msg_hash) == node and (msg_hash, node) in first
    ]


def relay_cross_check(
    messages: Dict[str, str],
    received_lines: List[ReceivedLine],
    relay_receipts: Dict[str, Set[str]],
) -> dict:
    """`Message received` receivers against `Received relay message` receivers, sender excluded."""
    receivers = receivers_by_hash(received_lines)
    mismatches = []
    for msg_hash, sender in sorted(messages.items()):
        messaging = receivers[msg_hash] - {sender}
        relay = relay_receipts.get(msg_hash, set()) - {sender}
        if messaging != relay:
            mismatches.append(
                {
                    "hash": msg_hash,
                    "messaging_only": sorted(messaging - relay),
                    "relay_only": sorted(relay - messaging),
                }
            )
    messaging_total = sum(len(receivers[h] - {s}) for h, s in messages.items())
    relay_total = sum(len(relay_receipts.get(h, set()) - {s}) for h, s in messages.items())
    return {
        "messaging_lines": messaging_total,
        "relay_lines": relay_total,
        "residual": messaging_total - relay_total,
        "mismatches": mismatches,
    }


def settings_reliability(line: str) -> Optional[str]:
    """`reliability` value in a node's startup settings line."""
    match = re.search(r"\breliability=(\w+)", line)
    return match.group(1) if match else None


def relay_statefulsets(metadata: dict) -> List[Tuple[str, int]]:
    """Statefulsets of the messaging nodes and their sizes. The bootstrap only does discovery."""
    stack = metadata["stack"]
    return [
        (name, count)
        for name, count in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
        if "bootstrap" not in name
    ]


def _query_relay_lines(
    experiment: "LogosMessagingExperiment", text: str, window: str
) -> List[Tuple[str, str]]:
    """(pod, line) for every relay log line containing `text` in a metadata window."""
    bounds = experiment.metadata["results"][window]
    kwargs = {
        "namespace": experiment.namespace or experiment.metadata["stack"].get("namespace"),
        "start_time": bounds["start"],
        "end_time": bounds["end"],
    }
    found = []
    for name, _ in relay_statefulsets(experiment.metadata):
        config = (
            VictoriaQueryBuilder()
            .with_url(VICTORIA_LOGS_URL)
            .with_filters(kwargs)
            .with_stateful_set(name, None)
            .with_query(f'"{text}"')
            .build_query_config()
        )
        lines = VictoriaReader(None, config).multiline_query_info()
        if lines.is_err():
            raise RuntimeError(f"Query for `{text}` failed: {lines.err_value}")
        entries = (json.loads(raw) for raw in lines.ok_value)
        found.extend((e.get("kubernetes.pod_name"), e.get("_msg", "")) for e in entries)
    return found


def query_settings(experiment: "LogosMessagingExperiment") -> List[dict]:
    """Each relay's `Messaging API settings` line over the whole run."""
    return [
        {"node": pod, "reliability": settings_reliability(line)}
        for pod, line in _query_relay_lines(experiment, SETTINGS_LINE, "complete")
    ]


def query_received_lines(experiment: "LogosMessagingExperiment") -> List[ReceivedLine]:
    parsed = (
        (pod, parse_received_line(line))
        for pod, line in _query_relay_lines(experiment, RECEIVED_LINE, "messages")
    )
    return [(pod, *fields) for pod, fields in parsed if fields]


def query_send_lines(experiment: "LogosMessagingExperiment") -> List[SendLine]:
    lines = _query_relay_lines(experiment, PROPAGATED_LINE, "messages") + _query_relay_lines(
        experiment, FAILED_LINE, "messages"
    )
    parsed = ((pod, parse_send_line(line)) for pod, line in lines)
    return [(pod, *fields) for pod, fields in parsed if fields]


def query_counter_total(experiment: "LogosMessagingExperiment") -> Optional[int]:
    """Live deliveries the nodes counted themselves, summed over nodes, from VictoriaMetrics."""
    bounds = experiment.metadata["results"]["messages"]
    namespace = experiment.namespace or experiment.metadata["stack"].get("namespace")
    query = (
        "sum(max_over_time(logos_delivery_recv_messages_total"
        f"{{namespace='{namespace}',source='live'}}[60s]))"
    )
    start, end = (datetime.fromisoformat(bounds[key]) for key in ("start", "end"))
    match scrape_utils.get_query_data(
        scrape_utils.create_promql(METRICS_URL, query, start, end, 60)
    ):
        case Ok(data):
            values = [float(v) for s in data["data"]["result"] for _, v in s.get("values", [])]
            return int(max(values)) if values else None
        case Err(error):
            logger.warning(f"Could not read the delivery counter: {error}")
            return None


def read_relay_receipts(csv_path: Path) -> Dict[str, Set[str]]:
    """Message hash -> pods with a `Received relay message` line, from the analyzer dump."""
    df = pd.read_csv(csv_path, usecols=["msg_hash", "kubernetes.pod_name"])
    receipts: Dict[str, Set[str]] = defaultdict(set)
    for msg_hash, pod in df.itertuples(index=False):
        receipts[msg_hash].add(pod)
    return receipts


def _pull_relay_lines(experiment: "LogosMessagingExperiment", dump_dir: Path) -> Optional[Path]:
    stack = dict(experiment.metadata["stack"])
    if {stack.get("start_time"), stack.get("end_time")} & {None, EventNotFound}:
        logger.warning("No bounded messages window; skipping the relay log check")
        return None
    stack.update(
        {
            "type": "vaclab",
            "url": VICTORIA_LOGS_URL,
            "reader": "victoria",
            "namespace": experiment.namespace or stack.get("namespace"),
        }
    )
    relays = relay_statefulsets(experiment.metadata)
    # Its check counts each sender as missing its own messages; `relay_cross_check` does not.
    WakuAnalyzer(dump_analysis_dir=str(dump_dir)).with_data_puller(
        DataPuller().with_kwargs(stack)
    ).with_reliability_check(
        stateful_sets=[name for name, _ in relays],
        nodes_per_ss=[count for _, count in relays],
        expected_num_peers=sum(count for _, count in relays),
        expected_num_messages=experiment.config.num_messages,
    ).run()
    csv_path = dump_dir / RECEIVED_CSV
    return csv_path if csv_path.exists() else None


def summarize(
    nodes: List[str],
    sends: List[dict],
    send_lines: List[SendLine],
    received_lines: List[ReceivedLine],
    settings: List[dict],
    counter_total: Optional[int],
    relay_receipts: Optional[Dict[str, Set[str]]],
) -> dict:
    outcomes = send_outcomes(sends, send_lines)
    messages = outcomes.pop("messages")
    summary = {
        "nodes": len(nodes),
        "sends": outcomes,
        "delivery": delivery(messages, received_lines, nodes),
        "counter_check": {"counter_total": counter_total, "log_lines": len(received_lines)},
        "reliability_off": {
            "nodes_checked": len(settings),
            "not_false": sorted(s["node"] for s in settings if s.get("reliability") != "false"),
        },
        "latency_ms": {
            "delivery": distribution_ms(delivery_latency(messages, received_lines)),
            "propagation": distribution_ms(propagation_time(messages, send_lines, received_lines)),
        },
    }
    if relay_receipts is not None:
        summary["relay_check"] = relay_cross_check(messages, received_lines, relay_receipts)
    return summary


def report(summary: dict) -> None:
    s, d, c = summary["sends"], summary["delivery"], summary["counter_check"]
    lines = [
        f"Sends: {s['accepted']}/{s['sent']} accepted, {s['propagated']} propagated, "
        f"{s['failed']} failed, {s['no_log_line']} without a log line, "
        f"without a reply by node {s['without_reply']}, "
        f"unmatched outcome lines by node {s['unmatched_lines']}",
        f"Delivery: {d['observed']}/{d['expected']} (residual {d['residual']}), "
        f"{d['messages_complete']} messages at all {d['expected_per_message']} other nodes, "
        f"self receipts {d['self_receipts']}",
        f"Counter check: nodes counted {c['counter_total']}, "
        f"`Message received` lines {c['log_lines']}",
        f"Reliability off: {summary['reliability_off']['nodes_checked']} nodes checked, "
        f"not false: {summary['reliability_off']['not_false']}",
    ]
    if "relay_check" in summary:
        r = summary["relay_check"]
        lines.append(
            f"Relay check: messaging lines {r['messaging_lines']} vs relay lines "
            f"{r['relay_lines']} (residual {r['residual']}), {len(r['mismatches'])} messages differ"
        )
    for name, dist in summary["latency_ms"].items():
        lines.append(f"Latency {name} (ms): {dist}")
    for line in lines:
        logger.info(line)


def scrape_and_plot(experiment: "LogosMessagingExperiment") -> None:
    """Bandwidth, memory and mesh-health figures over the sending window."""
    if not experiment.metadata.get("results", {}).get("stable"):
        logger.warning("Run has no sending window; skipping the metrics scrape")
        return

    out = Path(experiment.output_folder)
    label = f"{experiment.config.num_relay_nodes} nodes"
    dump = scrape_run_metrics(experiment.metadata, out / "metrics", name=label)
    plot_run_metrics({label: dump}, out / "plots", xlabel=experiment.namespace)


def _safe(label: str, query, default):
    try:
        return query()
    except Exception:
        logger.exception(f"{label} failed")
        return default


def run_logos_messaging_analysis(experiment: "LogosMessagingExperiment") -> dict:
    """Send outcomes, delivery and latency from the node logs, cross-checked against counters."""
    if experiment.output_folder is None or experiment.metadata is None:
        raise ValueError("Messaging post-run analysis needs output_folder and metadata")

    dump_dir = Path(experiment.output_folder) / "analysis_data"
    csv_path = _safe("Relay line pull", lambda: _pull_relay_lines(experiment, dump_dir), None)
    summary = summarize(
        nodes=[
            f"{name}-{index}"
            for name, count in relay_statefulsets(experiment.metadata)
            for index in range(count)
        ],
        sends=load_sends(Path(experiment.events_log_path)),
        send_lines=_safe("Send line query", lambda: query_send_lines(experiment), []),
        received_lines=_safe(
            "`Message received` query", lambda: query_received_lines(experiment), []
        ),
        settings=_safe("Settings query", lambda: query_settings(experiment), []),
        counter_total=_safe("Counter query", lambda: query_counter_total(experiment), None),
        relay_receipts=read_relay_receipts(csv_path) if csv_path else None,
    )
    dump_dir.mkdir(parents=True, exist_ok=True)
    (dump_dir / "messaging_summary.json").write_text(json.dumps(summary, indent=2))
    report(summary)
    _safe("Metrics scrape", lambda: scrape_and_plot(experiment), None)
    return summary

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller
from src.analysis.mesh_analysis.analyzers.waku.waku_analyzer import WakuAnalyzer
from src.analysis.mesh_analysis.readers.builders.victoria_reader_builder import VictoriaQueryBuilder
from src.analysis.mesh_analysis.readers.victoria_reader import VictoriaReader
from src.deployments.core.event_window_bridge import EventNotFound
from src.deployments.experiments.logos_messaging import API_DIR, NODE_METRICS_DIR

if TYPE_CHECKING:
    from src.deployments.experiments.logos_messaging import LogosMessagingExperiment

logger = logging.getLogger(__name__)

VICTORIA_LOGS_URL = "https://vlselect.lab.vac.dev/select/logsql/query"
RECEIVED_CSV = Path("summary") / "received.csv"
SETTINGS_LINE = "Messaging API settings"
RECEIVED_LINE = "Message received"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RECEIVED_LINE_RE = re.compile(
    r"^\w{3} (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+[+-]\d\d:\d\d) Message received.*?msg_hash=(0x[0-9a-f]+)"
)
METRIC_LINE = re.compile(r"^(logos_delivery_\w+(?:\{[^}]*\})?)\s+([-+\deE.]+)$")

Receipts = Dict[str, Set[str]]
"""msg_id -> nodes that read it with `source` `live`."""


def load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def parse_metrics(text: str) -> Dict[str, float]:
    """`logos_delivery_*` series, keyed by name with labels."""
    series = {}
    for line in text.splitlines():
        match = METRIC_LINE.match(line.strip())
        if match:
            series[match.group(1)] = float(match.group(2))
    return series


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


def accepted_sends(sends: List[dict]) -> Dict[str, dict]:
    """msg_id -> send record, for sends the node accepted."""
    return {s["msg_id"]: s for s in sends if s.get("status") == 200 and s.get("requestId")}


def live_receipts(received: List[dict]) -> Tuple[Receipts, int]:
    """Live receipts per message, and how many records repeated a (message, node) pair."""
    receipts: Receipts = defaultdict(set)
    duplicates = 0
    for record in received:
        if record.get("source") != "live" or record.get("msg_id") is None:
            continue
        if record["node"] in receipts[record["msg_id"]]:
            duplicates += 1
        receipts[record["msg_id"]].add(record["node"])
    return receipts, duplicates


def delivery(sends: Dict[str, dict], receipts: Receipts, nodes: List[str]) -> dict:
    """Receipts at nodes other than the sender, against `len(nodes) - 1` per message."""
    expected_per_message = len(nodes) - 1
    missing_by_node: Dict[str, int] = defaultdict(int)
    per_message = []
    for msg_id, send in sorted(sends.items()):
        readers = receipts.get(msg_id, set())
        missing = sorted(set(nodes) - readers - {send["node"]})
        for node in missing:
            missing_by_node[node] += 1
        per_message.append(
            {
                "msg_id": msg_id,
                "sender": send["node"],
                "receivers": len(readers - {send["node"]}),
                "self_receipt": send["node"] in readers,
                "missing": missing,
            }
        )
    expected = expected_per_message * len(sends)
    observed = sum(row["receivers"] for row in per_message)
    return {
        "expected_per_message": expected_per_message,
        "expected": expected,
        "observed": observed,
        "residual": expected - observed,
        "messages_complete": sum(1 for row in per_message if not row["missing"]),
        "self_receipts": sum(1 for row in per_message if row["self_receipt"]),
        "unknown_messages": sorted(set(receipts) - set(sends)),
        "missing_by_node": dict(sorted(missing_by_node.items())),
        "per_message": per_message,
    }


def seq_gaps(received: List[dict]) -> Dict[str, int]:
    """Records each node skipped between reads: the node dropped them or another client read them."""
    seqs: Dict[str, List[int]] = defaultdict(list)
    for record in received:
        if record.get("seq") is not None:
            seqs[record["node"]].append(record["seq"])
    gaps = {}
    for node, values in seqs.items():
        values = sorted(set(values))
        gaps[node] = (values[0] - 1) + sum(b - a - 1 for a, b in zip(values, values[1:]))
    return gaps


def unexplained_missing(missing_by_node: Dict[str, int], gaps: Dict[str, int]) -> Dict[str, int]:
    """Per node, missing receipts minus `seq` gaps. Nonzero means the gaps do not account for it."""
    nodes = set(missing_by_node) | {n for n, g in gaps.items() if g}
    diffs = {n: missing_by_node.get(n, 0) - gaps.get(n, 0) for n in sorted(nodes)}
    return {n: d for n, d in diffs.items() if d}


def metrics_check(receipts: Receipts, metrics: Dict[str, Dict[str, float]]) -> dict:
    """API reads per node, own messages included, against the node's live delivery counter."""
    api_counts: Dict[str, int] = defaultdict(int)
    for readers in receipts.values():
        for node in readers:
            api_counts[node] += 1
    mismatches = {}
    for node, series in sorted(metrics.items()):
        counter = int(series.get('logos_delivery_recv_messages_total{source="live"}', 0))
        if counter != api_counts.get(node, 0):
            mismatches[node] = {"api": api_counts.get(node, 0), "counter": counter}
    return {
        "nodes_checked": len(metrics),
        "mismatches": mismatches,
        "history_counter": sum(
            s.get('logos_delivery_recv_messages_total{source="history"}', 0)
            for s in metrics.values()
        ),
        "received_dropped": sum(
            s.get("logos_delivery_rest_received_dropped_total", 0) for s in metrics.values()
        ),
        "send_dropped": sum(
            s.get("logos_delivery_rest_send_dropped_total", 0) for s in metrics.values()
        ),
    }


def reply_lost_sends(
    sends: List[dict], send_events: List[dict], hashes: Dict[str, str]
) -> Dict[str, dict]:
    """Sends without a reply whose node still reported events for that message.

    Matched by message hash on the sending node; the record gets the node's `requestId`.
    """
    request_ids = {
        (event["node"], event["messageHash"]): event["requestId"]
        for event in send_events
        if event.get("messageHash")
    }
    lost = {}
    for send in sends:
        if send.get("requestId") or send["msg_id"] not in hashes:
            continue
        request_id = request_ids.get((send["node"], hashes[send["msg_id"]]))
        if request_id:
            lost[send["msg_id"]] = {**send, "requestId": request_id}
    return lost


def send_outcomes(sends: List[dict], taken: Dict[str, dict], send_events: List[dict]) -> dict:
    """Every send the node took should end with exactly one `propagated` event."""
    kinds: Dict[str, List[str]] = defaultdict(list)
    for event in send_events:
        kinds[event["requestId"]].append(event["kind"])
    outcomes = {"propagated_only": 0, "error": 0, "no_events": 0, "other": []}
    for send in taken.values():
        events = kinds.get(send["requestId"], [])
        if events == ["propagated"]:
            outcomes["propagated_only"] += 1
        elif "error" in events:
            outcomes["error"] += 1
        elif not events:
            outcomes["no_events"] += 1
        else:
            outcomes["other"].append({"msg_id": send["msg_id"], "events": events})
    http_failures = [
        {"msg_id": s["msg_id"], "status": s.get("status"), "error": s.get("error")}
        for s in sends
        if s["msg_id"] not in taken
    ]
    return {
        "sent": len(sends),
        "accepted": len(taken),
        "reply_lost": sorted(m for m, s in taken.items() if s.get("status") != 200),
        "http_failures": http_failures,
        **outcomes,
    }


def message_timestamps(received: List[dict]) -> Dict[str, int]:
    """msg_id -> message `timestamp`, which the sending node stamps when it accepts the POST."""
    return {r["msg_id"]: r["timestamp"] for r in received if r.get("msg_id") and r.get("timestamp")}


def propagation_time(
    sends: Dict[str, dict], send_events: List[dict], timestamps: Dict[str, int]
) -> List[int]:
    """`propagated` minus the message timestamp, both on the sending node's clock (ns)."""
    msg_by_request = {send["requestId"]: msg_id for msg_id, send in sends.items()}
    return [
        event["timestamp"] - timestamps[msg_by_request[event["requestId"]]]
        for event in send_events
        if event["kind"] == "propagated"
        and event["requestId"] in msg_by_request
        and msg_by_request[event["requestId"]] in timestamps
    ]


def log_check(
    sends: Dict[str, dict], receipts: Receipts, hashes: Dict[str, str], log_receipts: Receipts
) -> dict:
    """API receipts against `Received relay message` lines, per message, sender excluded.

    `log_receipts` is keyed by message hash; `hashes` maps msg_id to it.
    """
    mismatches = []
    for msg_id, send in sorted(sends.items()):
        api = receipts.get(msg_id, set()) - {send["node"]}
        logs = log_receipts.get(hashes.get(msg_id, ""), set()) - {send["node"]}
        if api != logs:
            mismatches.append(
                {"msg_id": msg_id, "api_only": sorted(api - logs), "log_only": sorted(logs - api)}
            )
    api_total = sum(len(receipts.get(m, set()) - {s["node"]}) for m, s in sends.items())
    log_total = sum(
        len(log_receipts.get(hashes.get(m, ""), set()) - {s["node"]}) for m, s in sends.items()
    )
    return {
        "api": api_total,
        "logs": log_total,
        "residual": api_total - log_total,
        "mismatches": mismatches,
    }


def relay_latency(
    hashes: Dict[str, str], timestamps: Dict[str, int], log_received_ns: Dict[Tuple[str, str], int]
) -> List[int]:
    """`Received relay message` time minus the message timestamp (ns), across node clocks."""
    by_hash = {h: timestamps[m] for m, h in hashes.items() if m in timestamps}
    return [ns - by_hash[h] for (h, _node), ns in log_received_ns.items() if h in by_hash]


def parse_received_line(line: str) -> Optional[Tuple[str, int]]:
    """Message hash and node-clock time (ns) of a `Message received` line."""
    match = RECEIVED_LINE_RE.match(line)
    if not match:
        return None
    since_epoch = datetime.fromisoformat(match.group(1)) - EPOCH
    seconds = since_epoch.days * 86_400 + since_epoch.seconds
    return match.group(2), seconds * 1_000_000_000 + since_epoch.microseconds * 1_000


def delivery_latency(
    sends: Dict[str, dict],
    hashes: Dict[str, str],
    timestamps: Dict[str, int],
    lines: List[Tuple[str, str, int]],
) -> List[int]:
    """First `Message received` line per (message, node) minus the message timestamp (ns).

    `lines` holds (node, message hash, line time). Senders are left out.
    """
    by_hash = {hashes[m]: m for m in sends if m in hashes and m in timestamps}
    first: Dict[Tuple[str, str], int] = {}
    for node, msg_hash, line_ns in lines:
        msg_id = by_hash.get(msg_hash)
        if msg_id is None or node == sends[msg_id]["node"]:
            continue
        first[(msg_id, node)] = min(line_ns, first.get((msg_id, node), line_ns))
    return [line_ns - timestamps[msg_id] for (msg_id, _), line_ns in first.items()]


def read_log_receipts(csv_path: Path) -> Tuple[Receipts, Dict[Tuple[str, str], int]]:
    """Receipts and receive times per (hash, pod) from the analyzer's received dump."""
    df = pd.read_csv(csv_path, usecols=["msg_hash", "kubernetes.pod_name", "timestamp"])
    df["ns"] = pd.to_datetime(df["timestamp"], utc=True).astype("int64")
    receipts: Receipts = defaultdict(set)
    times: Dict[Tuple[str, str], int] = {}
    for msg_hash, pod, ns in df[["msg_hash", "kubernetes.pod_name", "ns"]].itertuples(index=False):
        receipts[msg_hash].add(pod)
        times[(msg_hash, pod)] = min(ns, times.get((msg_hash, pod), ns))
    return receipts, times


def settings_reliability(line: str) -> Optional[str]:
    """`reliability` value in a node's startup settings line."""
    match = re.search(r"\breliability=(\w+)", line)
    return match.group(1) if match else None


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
    config = (
        VictoriaQueryBuilder()
        .with_url(VICTORIA_LOGS_URL)
        .with_filters(kwargs)
        .with_stateful_set("relay-0", None)
        .with_query(f'"{text}"')
        .build_query_config()
    )
    lines = VictoriaReader(None, config).multiline_query_info()
    if lines.is_err():
        raise RuntimeError(f"Query for `{text}` failed: {lines.err_value}")
    entries = (json.loads(raw) for raw in lines.ok_value)
    return [(e.get("kubernetes.pod_name"), e.get("_msg", "")) for e in entries]


def query_settings(experiment: "LogosMessagingExperiment") -> List[dict]:
    """Each relay's `Messaging API settings` line over the whole run."""
    return [
        {"node": pod, "reliability": settings_reliability(line)}
        for pod, line in _query_relay_lines(experiment, SETTINGS_LINE, "complete")
    ]


def query_received_lines(experiment: "LogosMessagingExperiment") -> List[Tuple[str, str, int]]:
    """(node, message hash, line time) of every `Message received` line while messages flow."""
    parsed = (
        (pod, parse_received_line(line))
        for pod, line in _query_relay_lines(experiment, RECEIVED_LINE, "messages")
    )
    return [(pod, *fields) for pod, fields in parsed if fields]


def _pull_logs(experiment: "LogosMessagingExperiment", dump_dir: Path) -> Optional[Path]:
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
    relays = [
        (name, count)
        for name, count in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
        if name.startswith("relay")
    ]
    # Its check counts each sender as missing its own messages; `log_check` excludes senders.
    WakuAnalyzer(dump_analysis_dir=str(dump_dir)).with_data_puller(
        DataPuller().with_kwargs(stack)
    ).with_reliability_check(
        stateful_sets=[name for name, _ in relays],
        nodes_per_ss=[count for _, count in relays],
        expected_num_peers=experiment.config.num_relay_nodes,
        expected_num_messages=experiment.config.num_messages,
    ).run()
    csv_path = dump_dir / RECEIVED_CSV
    return csv_path if csv_path.exists() else None


def summarize(
    nodes: List[str],
    sends: List[dict],
    received: List[dict],
    send_events: List[dict],
    settings: List[dict],
    poll_errors: List[dict],
    metrics: Dict[str, Dict[str, float]],
    log_data: Optional[Tuple[Receipts, Dict[Tuple[str, str], int]]],
    received_lines: List[Tuple[str, str, int]],
) -> dict:
    receipts, duplicates = live_receipts(received)
    timestamps = message_timestamps(received)
    hashes = {r["msg_id"]: r["messageHash"] for r in received if r.get("msg_id")}
    taken = {**accepted_sends(sends), **reply_lost_sends(sends, send_events, hashes)}
    gaps = seq_gaps(received)
    delivered = delivery(taken, receipts, nodes)
    summary = {
        "nodes": len(nodes),
        "delivery": delivered,
        "missing_not_in_seq_gaps": unexplained_missing(delivered["missing_by_node"], gaps),
        "duplicate_reads": duplicates,
        "history_reads": sum(1 for r in received if r.get("source") != "live"),
        "seq_gaps": {"total": sum(gaps.values()), "by_node": {n: g for n, g in gaps.items() if g}},
        "metrics": metrics_check(receipts, metrics),
        "sends": send_outcomes(sends, taken, send_events),
        "poll_errors": len(poll_errors),
        "reliability_off": {
            "nodes_checked": len(settings),
            "not_false": sorted(s["node"] for s in settings if s.get("reliability") != "false"),
        },
        "latency_ms": {
            "delivery": distribution_ms(
                delivery_latency(taken, hashes, timestamps, received_lines)
            ),
            "propagation": distribution_ms(propagation_time(taken, send_events, timestamps)),
        },
    }
    if log_data is not None:
        log_receipts, log_times = log_data
        summary["logs"] = log_check(taken, receipts, hashes, log_receipts)
        summary["latency_ms"]["relay"] = distribution_ms(
            relay_latency(hashes, timestamps, log_times)
        )
    return summary


def report(summary: dict) -> None:
    d = summary["delivery"]
    s = summary["sends"]
    m = summary["metrics"]
    lines = [
        f"Delivery: {d['observed']}/{d['expected']} receipts (residual {d['residual']}), "
        f"{d['messages_complete']}/{len(d['per_message'])} messages at all "
        f"{d['expected_per_message']} other nodes, self receipts {d['self_receipts']}",
        f"Sends: {s['accepted']}/{s['sent']} accepted, {len(s['reply_lost'])} with the reply lost, "
        f"{s['propagated_only']} propagated only, "
        f"{s['error']} error, {s['no_events']} no events, {len(s['other'])} other",
        f"Client losses: seq gaps {summary['seq_gaps']['total']}, "
        f"received dropped {m['received_dropped']}, send dropped {m['send_dropped']}, "
        f"missing receipts not in seq gaps {summary['missing_not_in_seq_gaps']}",
        f"Counter check: {len(m['mismatches'])} of {m['nodes_checked']} nodes differ, "
        f"history reads {summary['history_reads']}, history counter {m['history_counter']}",
        f"Reliability off: {summary['reliability_off']['nodes_checked']} nodes checked, "
        f"not false: {summary['reliability_off']['not_false']}",
        f"Poll errors: {summary['poll_errors']}, duplicate reads: {summary['duplicate_reads']}",
    ]
    if "logs" in summary:
        lines.append(
            f"Log check: API {summary['logs']['api']} vs logs {summary['logs']['logs']} "
            f"(residual {summary['logs']['residual']}), "
            f"{len(summary['logs']['mismatches'])} messages differ"
        )
    for name, dist in summary["latency_ms"].items():
        lines.append(f"Latency {name} (ms): {dist}")
    for line in lines:
        logger.info(line)


def run_logos_messaging_analysis(experiment: "LogosMessagingExperiment") -> dict:
    """API-level delivery, send outcomes and latency, cross-checked against counters and logs."""
    if experiment.output_folder is None or experiment.metadata is None:
        raise ValueError("Messaging post-run analysis needs output_folder and metadata")

    out = Path(experiment.output_folder)
    api = out / API_DIR
    nodes = [f"relay-0-{index}" for index in range(experiment.config.num_relay_nodes)]
    metrics = {
        path.stem: parse_metrics(path.read_text())
        for path in sorted((out / NODE_METRICS_DIR).glob("*.txt"))
    }
    dump_dir = out / "analysis_data"
    settings: List[dict] = []
    received_lines: List[Tuple[str, str, int]] = []
    try:
        settings = query_settings(experiment)
    except Exception:
        logger.exception("Settings query failed")
    try:
        received_lines = query_received_lines(experiment)
    except Exception:
        logger.exception("`Message received` query failed")
    log_data = None
    try:
        csv_path = _pull_logs(experiment, dump_dir)
        if csv_path is not None:
            log_data = read_log_receipts(csv_path)
    except Exception:
        logger.exception("Relay log check failed")

    summary = summarize(
        nodes=nodes,
        sends=load_jsonl(api / "sends.jsonl"),
        received=load_jsonl(api / "received.jsonl"),
        send_events=load_jsonl(api / "send_events.jsonl"),
        settings=settings,
        poll_errors=load_jsonl(api / "poll_errors.jsonl"),
        metrics=metrics,
        log_data=log_data,
        received_lines=received_lines,
    )
    dump_dir.mkdir(parents=True, exist_ok=True)
    (dump_dir / "messaging_summary.json").write_text(json.dumps(summary, indent=2))
    report(summary)
    return summary

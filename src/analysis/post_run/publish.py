"""Publish a run's data to the DST dashboard, which keeps it past the lab's retention."""

import csv
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from src.analysis.metrics.config import MetricToScrape, ScrapeConfig
from src.analysis.post_run.metrics import run_scrape_config

logger = logging.getLogger(__name__)

DASHBOARD_URL = "https://api.dashboard.lab.vac.dev"
TOKEN_ENV = "DST_DASHBOARD_TOKEN"
DATASOURCE = "victoria-metrics"
LOGS_DATASOURCE = "victoria-logs"
TIMEOUT_S = 600
# The name the run's scraped CSVs share, kept so a fetch writes the same files.
SCRAPE_LABEL = "scrape_label"
SCRAPE_INTERVAL = "30s"
# Labels every series carries; the others tell a pod's series apart.
TARGET_LABELS = {
    "__name__",
    "cluster",
    "container",
    "endpoint",
    "instance",
    "job",
    "metrics_path",
    "namespace",
    "node",
    "pod",
    "pod-node",
    "prometheus",
    "service",
    "uid",
}
CONTAINER_METRICS = [
    "container_cpu_usage_seconds_total",
    "container_cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_memory_working_set_bytes",
    "container_memory_rss",
]
POD_METRICS = ["container_network_receive_bytes_total", "container_network_transmit_bytes_total"]
KUBE_METRICS = {"kube_pod_info": ["node"], "kube_pod_start_time": []}
# The dashboard parses only the nimlibp2p tracer's log lines.
LOG_EXPERIMENTS = "nimlibp2p"
LOG_FIELDS = {
    "sent": {"timestamp": "datetime", "msgId": "integer", "pod_name": "string"},
    "received": {
        "timestamp": "datetime",
        "msgId": "integer",
        "sentAt": "datetime",
        "delayMs": "float",
        "pod_name": "string",
    },
}
SUMMARY = Path("analysis_data/summary")
UNCHANGED = "unchanged"
UNCHANGED_RULE = (
    "A whole-run metric that every pod of the run reports with the same labels and value for the "
    "whole run is not stored as a dataset; `metrics` gives those labels and that value."
)


class PublishError(Exception):
    pass


def _utc(dt: datetime) -> str:
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).isoformat()


def _schema(fields: Dict[str, str]) -> List[dict]:
    return [{"name": name, "type": kind} for name, kind in fields.items()]


def metric_dataset(metric: MetricToScrape, config: ScrapeConfig) -> dict:
    labels = metric.extract_field.split("-")
    labels += [label for label in ("container", "metrics_path") if getattr(metric, label)]
    return {
        "name": metric.folder_name.strip("/"),
        "datasource": DATASOURCE,
        "timeRange": {"start": _utc(config.start), "end": _utc(config.end)},
        "query": {
            "expr": metric.query.replace("$__rate_interval", config.rate_interval),
            "step": config.step,
        },
        "schema": _schema(
            {"timestamp": "datetime", "value": "float"} | dict.fromkeys(labels, "string")
        ),
    }


def run_title(run_name: str, config: ScrapeConfig) -> str:
    return f"{run_name} {config.start:%Y-%m-%d %H:%M}"


def scrape_label(run: Path) -> Optional[str]:
    """The name the run's scraped CSVs share (`metrics/<metric>/<label>.csv`), if one."""
    labels = {path.stem for path in (run / "metrics").glob("*/*.csv")}
    return labels.pop() if len(labels) == 1 else None


def run_pods(stack: dict) -> List[str]:
    return [
        f"{name}-{i}"
        for name, count in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
        for i in range(count)
    ]


def _window(exp: dict, name: str) -> Dict[str, str]:
    window = exp["results"][name]
    return {key: _utc(datetime.fromisoformat(window[key])) for key in ("start", "end")}


def _pods(exp: dict, ordinal: str = "[0-9]+") -> str:
    stack = exp["stack"]
    sets = "|".join(stack["stateful_sets"])
    return f'namespace="{stack["namespace"]}", pod=~"({sets})-{ordinal}"'


def _raw(name: str, selector: str, labels: List[str], window: dict) -> dict:
    by = ", ".join(["pod", *labels])
    return {
        "name": name,
        "datasource": DATASOURCE,
        "timeRange": window,
        "query": {"expr": f"max by ({by}) ({name}{{{selector}}})", "step": SCRAPE_INTERVAL},
        "schema": _schema(
            {"timestamp": "datetime", "value": "float", "pod": "string"}
            | dict.fromkeys(labels, "string")
        ),
    }


def _instant(expr: str, at: float, vm_url: str) -> List[dict]:
    for attempt in range(2):
        response = requests.get(
            f"{vm_url.rstrip('/')}/query", params={"query": expr, "time": at}, timeout=TIMEOUT_S
        )
        if response.ok:
            break
        # Cold data can run past VM's query limit once, then answer.
        logger.warning(f"VM answered {response.status_code} to `{expr}`: {response.text[:200]}")
    response.raise_for_status()
    return response.json()["data"]["result"]


def node_metrics(exp: dict, vm_url: str) -> Dict[str, List[str]]:
    """The metrics the nodes' own scrape job reported, from each statefulset's first pod."""
    end = datetime.fromisoformat(exp["results"]["stable"]["end"]).replace(tzinfo=timezone.utc)
    first = _pods(exp, "0")
    found = _instant(f"last_over_time({{{first}}}[300s])", end.timestamp(), vm_url)
    series = [s["metric"] for s in found]
    jobs = {s.get("job") for s in series if s["__name__"].startswith("libp2p_")}
    labels: Dict[str, set] = {}
    for s in series:
        name = s["__name__"]
        if (
            s.get("job") in jobs
            and not name.endswith("_created")
            and name != "up"
            and not name.startswith("scrape_")
        ):
            labels.setdefault(name, set()).update(
                l for l in set(s) - TARGET_LABELS if l.isidentifier()
            )
    if not labels:
        raise PublishError(f"No node metrics on {first} in the 5 min before the stable end")
    return {name: sorted(labels[name]) for name in sorted(labels)}


def _unchanged(
    selector: str, labels: List[str], window: dict, pods: List[str], vm_url: str
) -> Optional[dict]:
    """The labels and value every pod reports for `selector` over `window`, else None."""
    start, end = (datetime.fromisoformat(window[key]).timestamp() for key in ("start", "end"))
    # A range query can take samples from a scrape before `start` and a step after `end`.
    at, span = end + 30, f"{int(end - start) + 90}s"
    over = lambda agg: f"{agg}_over_time({selector}[{span}])"
    low, high = (_instant(f"{agg}({over(agg)})", at, vm_url) for agg in ("min", "max"))
    if not low or float(low[0]["value"][1]) != float(high[0]["value"][1]):
        return None
    by = ", ".join(["pod", *labels])
    sets = [s["metric"] for s in _instant(f"count by ({by}) ({over('last')})", at, vm_url)]
    reported = {s.pop("pod", None) for s in sets}
    if reported != set(pods) or len({tuple(sorted(s.items())) for s in sets}) != 1:
        return None
    return {"value": float(low[0]["value"][1]), "labels": sets[0]}


def run_datasets(
    exp: dict, metrics: Dict[str, List[str]], vm_url: str
) -> Tuple[List[dict], Dict[str, dict]]:
    """Whole-run node and pod datasets, and the metrics UNCHANGED_RULE leaves out."""
    window, pods, names = _window(exp, "complete"), _pods(exp), run_pods(exp["stack"])
    cadvisor = f'{pods}, metrics_path="/metrics/cadvisor"'
    container = f'{cadvisor}, container="{exp["stack"]["container_name"]}"'
    series = (
        [(name, pods, labels) for name, labels in (metrics | KUBE_METRICS).items()]
        + [(name, container, []) for name in CONTAINER_METRICS]
        + [(name, cadvisor, []) for name in POD_METRICS]
    )

    def check(entry: tuple) -> Optional[dict]:
        name, selector, labels = entry
        return _unchanged(f"{name}{{{selector}}}", labels, window, names, vm_url)

    with ThreadPoolExecutor(4) as pool:
        found = list(pool.map(check, series))
    datasets, unchanged = [], {}
    for (name, selector, labels), same in zip(series, found):
        if same is None:
            datasets.append(_raw(name, selector, labels, window))
        else:
            unchanged[name] = same
    return datasets, unchanged


def log_datasets(exp: dict) -> List[dict]:
    """A nim-libp2p run's sends and deliveries, the rows of its summary/{sent,received}.csv."""
    if not exp["experiment"]["name"].startswith(LOG_EXPERIMENTS):
        return []
    query = {"namespace": exp["stack"]["namespace"], "tracer": "nimlibp2p"}
    return [
        {
            "name": pattern,
            "datasource": LOGS_DATASOURCE,
            "timeRange": _window(exp, "complete"),
            "query": query | {"pattern": pattern},
            "schema": _schema(fields),
        }
        for pattern, fields in LOG_FIELDS.items()
    ]


def run_files(run: Path) -> dict:
    """The run's events, without experiment dumps and per-message sends, and its deployed specs."""
    events = run / "events.log"
    lines = events.read_text().splitlines() if events.exists() else []
    kept = [
        e
        for e in map(json.loads, filter(None, lines))
        if e.get("event") not in ("metadata", "publish", "send")
    ]
    specs = {
        path.name: path.read_text() for path in sorted((run / "deployment_yamls").glob("*.yaml"))
    }
    return {"events": kept, "deployment": specs}


def experiment_body(
    exp: dict,
    config: ScrapeConfig,
    run_name: str,
    label: Optional[str] = None,
    extra: List[dict] = (),
    metadata: Optional[dict] = None,
) -> dict:
    """The scraped metrics over the stable window, as the local CSVs, then `extra` datasets."""
    stack = exp.get("stack", {})
    return {
        "title": run_title(run_name, config),
        "family": exp["experiment"]["name"],
        "date": f"{config.start:%Y-%m-%d}",
        "docker_image": exp.get("params", {}).get("image"),
        # The dashboard's log fetch reads statefulsets, nodes, namespace and container_name.
        "metadata": {"run": run_name, "experiment": exp["experiment"]["name"]}
        | {key: exp[key] for key in ("params", "code", "stack", "results") if key in exp}
        | ({SCRAPE_LABEL: label} if label else {})
        | {
            "statefulsets": stack.get("stateful_sets"),
            "nodes": stack.get("nodes_per_statefulset"),
            "namespace": stack.get("namespace"),
            "container_name": stack.get("container_name"),
        }
        | (metadata or {}),
        "datasets": [metric_dataset(metric, config) for metric in config.metrics_to_scrape]
        + list(extra),
        "panels": [],
        "publish": False,
    }


def run_body(exp: dict, run: Path) -> dict:
    """Everything published for one run folder."""
    config = run_scrape_config(exp, name=run.name)
    datasets, unchanged = run_datasets(exp, node_metrics(exp, config.url), config.url)
    metadata = run_files(run) | {UNCHANGED: {"rule": UNCHANGED_RULE, "metrics": unchanged}}
    extra = datasets + log_datasets(exp)
    return experiment_body(exp, config, run.name, scrape_label(run), extra, metadata)


def expected_rows(body: dict, run: Path) -> Dict[str, int]:
    """Row counts the log datasets must match: the run's own summary CSVs."""
    counts = {}
    for dataset in body["datasets"]:
        if dataset["datasource"] != LOGS_DATASOURCE:
            continue
        path = run / SUMMARY / f"{dataset['name']}.csv"
        if not path.exists():
            logger.warning(f"`{run.name}` has no {path.name}; only checking it got rows")
            continue
        with open(path, newline="") as f:
            counts[dataset["name"]] = sum(1 for _ in csv.reader(f)) - 1
    return counts


def _existing(title: str, url: str) -> dict:
    response = requests.get(f"{url}/experiments", timeout=TIMEOUT_S)
    response.raise_for_status()
    found = [e for e in response.json() if e["title"] == title]
    if not found:
        raise PublishError(f"`{title}` got a 409 but is not on the dashboard")
    return found[0]


def _bad_datasets(
    experiment_id: str, names: List[str], url: str, expected: Dict[str, int]
) -> List[str]:
    """Datasets stored empty, or with other row counts than the run's CSVs."""
    bad = []
    for name in names:
        response = requests.get(
            f"{url}/experiments/{experiment_id}/datasets/{name}/data", timeout=TIMEOUT_S
        )
        if response.status_code == 404:
            bad.append(f"{name} (not stored)")
            continue
        response.raise_for_status()
        rows = len(response.json()["data"])
        if not rows or rows != expected.get(name, rows):
            bad.append(f"{name} ({rows} rows, expected {expected.get(name, 'some')})")
    return bad


def publish(
    body: dict, token: str, url: str = DASHBOARD_URL, expected: Dict[str, int] = None
) -> str:
    """Create the experiment and check every dataset's rows, or check the one already there."""
    headers, title, expected = {"Authorization": f"Bearer {token}"}, body["title"], expected or {}
    response = requests.post(f"{url}/experiments", json=body, headers=headers, timeout=TIMEOUT_S)
    if response.status_code == 409:
        existing = _existing(title, url)
        names = [d["name"] for d in existing["datasets"]]
        missing = [d["name"] for d in body["datasets"] if d["name"] not in names]
        if missing:
            logger.warning(f"`{title}` is already on the dashboard without {missing}; not added")
        # Never deleted here: it may still be processing, or hold data VM no longer has.
        bad = _bad_datasets(existing["id"], names, url, expected)
        if bad:
            raise PublishError(f"`{title}` is already on the dashboard with {bad}")
        logger.info(f"`{title}` is already on the dashboard as {existing['id']}")
        return existing["id"]
    if not response.ok:
        raise PublishError(f"`{title}` got {response.status_code}: {response.text[:500]}")

    experiment_id = response.json()["id"]
    try:
        bad = _bad_datasets(experiment_id, [d["name"] for d in body["datasets"]], url, expected)
        if bad:
            raise PublishError(f"`{title}` got {bad}; removed it")
    except Exception:
        requests.delete(
            f"{url}/experiments/{experiment_id}", headers=headers, timeout=TIMEOUT_S
        ).raise_for_status()
        raise

    logger.info(f"`{title}` is on the dashboard as {experiment_id}")
    return experiment_id


def unpublishable(exp: dict) -> Optional[str]:
    """Why a run stays off the dashboard, or None if it can go there."""
    # The bridge leaves out `duration` when the window is inverted.
    if not exp.get("results", {}).get("stable", {}).get("duration"):
        return "it has no stable window"
    code = exp.get("code")
    if not code:
        return "no 10ksim commit was recorded"
    if not code.get("clean"):
        return f"10ksim had uncommitted changes on top of {code['commit'][:7]}"
    if not code.get("pushed"):
        return f"10ksim {code['commit'][:7]} was not pushed"
    return None


def publish_run_metrics(exp: dict, run: Path) -> Optional[str]:
    """Publish this run's metrics, logs and specs. Logs failures, never raises."""
    reason = unpublishable(exp)
    if reason:
        logger.warning(f"`{run.name}` is not on the dashboard: {reason}")
        return None
    token = os.environ.get(TOKEN_ENV)
    if not token:
        logger.warning(f"{TOKEN_ENV} is not set; `{run.name}` is not on the dashboard")
        return None
    try:
        body = run_body(exp, run)
        return publish(body, token, expected=expected_rows(body, run))
    except Exception:
        logger.exception(f"Publishing `{run.name}` to the dashboard failed")
        return None

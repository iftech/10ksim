"""Read a published run's metrics back from the DST dashboard into the local scrape layout."""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import requests

from src.analysis.data.data_request_handler import DataRequestHandler
from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import (
    DASHBOARD_URL,
    SCRAPE_LABEL,
    TIMEOUT_S,
    UNCHANGED,
    metric_dataset,
    run_pods,
    run_title,
)

logger = logging.getLogger(__name__)


def run_metrics(run: Path, url: str = DASHBOARD_URL) -> Path:
    """The run's metric dump: `<run>/metrics` as scraped, else fetched there from the dashboard."""
    dump = run / "metrics"
    if any(dump.glob("*/*.csv")):
        return dump
    if dump.exists():
        raise FileExistsError(f"`{dump}` holds no .csv files; rename or move it to fetch the run")
    title = None
    if (run / "metadata.json").exists():
        exp = json.loads((run / "metadata.json").read_text())
        title = run_title(run.name, run_scrape_config(exp, name=run.name))
    return fetch_run_metrics(run.name, dump, url, title)


def find_experiment(run_name: str, url: str = DASHBOARD_URL, title: Optional[str] = None) -> dict:
    """The experiment titled `title`, else the only one published from a folder named `run_name`."""
    response = requests.get(f"{url}/experiments", timeout=TIMEOUT_S)
    response.raise_for_status()
    if title:
        found = [e for e in response.json() if e["title"] == title]
    else:
        found = [e for e in response.json() if (e.get("metadata") or {}).get("run") == run_name]
    if len(found) != 1:
        titles = [e["title"] for e in found]
        raise LookupError(
            f"Expected one dashboard experiment for `{title or run_name}`, found {titles}"
        )
    return found[0]


def _utc_seconds(timestamp: str) -> float:
    dt = datetime.fromisoformat(timestamp)
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def _query_response(rows: List[dict]) -> dict:
    """The stored rows as the VM query_range response they came from."""
    series = {}
    for row in rows:
        labels = {k: v for k, v in row.items() if k not in ("timestamp", "value")}
        key = tuple(sorted(labels.items()))
        series.setdefault(key, (labels, []))[1].append(
            [_utc_seconds(row["timestamp"]), row["value"]]
        )
    return {"data": {"result": [{"metric": m, "values": v} for m, v in series.values()]}}


def run_dataset(experiment: dict, name: str, url: str = DASHBOARD_URL) -> List[dict]:
    """A dataset's rows; a metric under UNCHANGED_RULE is one row per pod at the end of the run."""
    metadata = experiment["metadata"]
    unchanged = metadata.get(UNCHANGED, {}).get("metrics", {})
    if name in unchanged:
        end, same = metadata["results"]["complete"]["end"], unchanged[name]
        return [
            {"timestamp": end, "value": same["value"], "pod": pod, **same["labels"]}
            for pod in run_pods(metadata["stack"])
        ]
    if name not in {d["name"] for d in experiment.get("datasets", [])}:
        raise LookupError(f"`{experiment['title']}` has no `{name}` dataset")
    response = requests.get(
        f"{url}/experiments/{experiment['id']}/datasets/{name}/data", timeout=TIMEOUT_S
    )
    if response.status_code == 404:
        raise LookupError(f"`{experiment['title']}` has no stored `{name}` data")
    response.raise_for_status()
    return response.json()["data"]


def fetch_run_metrics(
    run_name: str, dump: Path, url: str = DASHBOARD_URL, title: Optional[str] = None
) -> Path:
    """Write the run's stored datasets to `dump` with the scraper's own CSV writer."""
    experiment = find_experiment(run_name, url, title)
    queries = {d["name"]: d["query"] for d in experiment.get("datasets", [])}

    partial = dump.with_name(f"{dump.name}.partial")
    shutil.rmtree(partial, ignore_errors=True)
    config = run_scrape_config(experiment["metadata"], partial, run_name)
    label = experiment["metadata"].get(SCRAPE_LABEL, run_name)
    for metric in config.metrics_to_scrape:
        name = metric.folder_name.strip("/")
        if name not in queries:
            logger.warning(f"`{run_name}` was published without {metric.name}")
            continue
        query, current = queries[name], metric_dataset(metric, config)["query"]
        if (query.get("expr"), query.get("step")) != (current["expr"], current["step"]):
            raise ValueError(f"`{run_name}` stored {name} as {query}, not as today's {current}")
        rows = run_dataset(experiment, name, url)
        if not rows:
            raise LookupError(f"`{run_name}` has no stored rows for {name}")
        handler = DataRequestHandler(_query_response(rows))
        handler.create_dataframe_from_request(
            metric.extract_field, metric.container, metric.metrics_path
        )
        handler.dump_dataframe((partial / metric.folder_name / f"{label}.csv").as_posix())

    if not partial.exists():
        raise LookupError(f"`{run_name}` has no stored metrics on the dashboard")
    partial.rename(dump)
    logger.info(f"Fetched `{run_name}` from the dashboard ({experiment['id']}) into `{dump}`")
    return dump

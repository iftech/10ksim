"""Read a published run's metrics back from the DST dashboard into the local scrape layout."""

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests

from src.analysis.data.data_request_handler import DataRequestHandler
from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import DASHBOARD_URL, TIMEOUT_S

logger = logging.getLogger(__name__)


def run_metrics(run: Path, url: str = DASHBOARD_URL) -> Path:
    """The run's metric dump: `<run>/metrics` as scraped, else fetched there from the dashboard."""
    dump = run / "metrics"
    if any(dump.glob("*/*.csv")):
        return dump
    if dump.exists():
        raise FileExistsError(f"`{dump}` holds no .csv files; rename or move it to fetch the run")
    return fetch_run_metrics(run.name, dump, url)


def find_experiment(run_name: str, url: str = DASHBOARD_URL) -> dict:
    response = requests.get(f"{url}/experiments", timeout=TIMEOUT_S)
    response.raise_for_status()
    found = [e for e in response.json() if (e.get("metadata") or {}).get("run") == run_name]
    if len(found) != 1:
        titles = [e["title"] for e in found]
        raise LookupError(f"Expected one dashboard experiment for `{run_name}`, found {titles}")
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


def fetch_run_metrics(run_name: str, dump: Path, url: str = DASHBOARD_URL) -> Path:
    """Write the run's stored datasets to `dump` with the scraper's own CSV writer."""
    experiment = find_experiment(run_name, url)
    response = requests.get(f"{url}/experiments/{experiment['id']}/datasets", timeout=TIMEOUT_S)
    response.raise_for_status()
    stored = {d["name"]: d["data"] for d in response.json()["datasets"]}

    partial = dump.with_name(f"{dump.name}.partial")
    shutil.rmtree(partial, ignore_errors=True)
    config = run_scrape_config(experiment["metadata"], partial, run_name)
    unknown = set(stored) - {m.folder_name.strip("/") for m in config.metrics_to_scrape}
    if unknown:
        logger.warning(f"`{run_name}` has stored datasets no current metric matches: {unknown}")
    for metric in config.metrics_to_scrape:
        rows = stored.get(metric.folder_name.strip("/"))
        if not rows:
            logger.warning(f"`{run_name}` has no stored {metric.name} data")
            continue
        handler = DataRequestHandler(_query_response(rows))
        handler.create_dataframe_from_request(
            metric.extract_field, metric.container, metric.metrics_path
        )
        handler.dump_dataframe((partial / metric.folder_name / f"{run_name}.csv").as_posix())

    if not partial.exists():
        raise LookupError(f"`{run_name}` has no stored metrics on the dashboard")
    partial.rename(dump)
    logger.info(f"Fetched `{run_name}` from the dashboard ({experiment['id']}) into `{dump}`")
    return dump

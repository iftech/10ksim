"""Publish a run's metrics to the DST dashboard, which keeps them past the lab's retention."""

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

import requests

from src.analysis.metrics.config import MetricToScrape, ScrapeConfig
from src.analysis.post_run.metrics import run_scrape_config

logger = logging.getLogger(__name__)

DASHBOARD_URL = "https://api.dashboard.lab.vac.dev"
TOKEN_ENV = "DST_DASHBOARD_TOKEN"
DATASOURCE = "victoria-metrics"
TIMEOUT_S = 600


class PublishError(Exception):
    pass


def _utc(dt: datetime) -> str:
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).isoformat()


def _dataset(metric: MetricToScrape, config: ScrapeConfig) -> dict:
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
        "schema": [{"name": "timestamp", "type": "datetime"}, {"name": "value", "type": "float"}]
        + [{"name": label, "type": "string"} for label in labels],
    }


def experiment_body(exp: dict, config: ScrapeConfig, run_name: str) -> dict:
    """One dataset per scraped metric, over the same window and step as the local CSVs."""
    return {
        "title": f"{run_name} {config.start:%Y-%m-%d %H:%M}",
        "family": exp["experiment"]["name"],
        "date": f"{config.start:%Y-%m-%d}",
        "docker_image": exp.get("params", {}).get("image"),
        "metadata": {"run": run_name, "experiment": exp["experiment"]["name"]}
        | {key: exp[key] for key in ("params", "code", "stack", "results") if key in exp},
        "datasets": [_dataset(metric, config) for metric in config.metrics_to_scrape],
        "panels": [],
        "publish": False,
    }


def _existing_id(title: str, url: str) -> str:
    response = requests.get(f"{url}/experiments", timeout=TIMEOUT_S)
    response.raise_for_status()
    ids = [e["id"] for e in response.json() if e["title"] == title]
    if not ids:
        raise PublishError(f"`{title}` got a 409 but is not on the dashboard")
    return ids[0]


def _empty_datasets(experiment_id: str, url: str) -> List[str]:
    response = requests.get(f"{url}/experiments/{experiment_id}/datasets", timeout=TIMEOUT_S)
    response.raise_for_status()
    return [d["name"] for d in response.json()["datasets"] if not d.get("rowCount")]


def publish(body: dict, token: str, url: str = DASHBOARD_URL) -> str:
    """Create the experiment, or find the one already there, and check every dataset has rows."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(f"{url}/experiments", json=body, headers=headers, timeout=TIMEOUT_S)
    if response.status_code == 409:
        experiment_id = _existing_id(body["title"], url)
    else:
        response.raise_for_status()
        experiment_id = response.json()["id"]

    empty = _empty_datasets(experiment_id, url)
    if empty:
        requests.delete(
            f"{url}/experiments/{experiment_id}", headers=headers, timeout=TIMEOUT_S
        ).raise_for_status()
        raise PublishError(f"`{body['title']}` got no rows for {empty}; removed it")

    logger.info(f"`{body['title']}` is on the dashboard as {experiment_id}")
    return experiment_id


def unpublishable(exp: dict) -> Optional[str]:
    """Why a run stays off the dashboard, or None if its 10ksim code was clean and pushed."""
    code = exp.get("code")
    if not code:
        return "no 10ksim commit was recorded"
    if not code.get("clean"):
        return f"10ksim had uncommitted changes on top of {code['commit'][:7]}"
    if not code.get("pushed"):
        return f"10ksim {code['commit'][:7]} was not pushed"
    return None


def publish_run_metrics(exp: dict, run_name: str) -> Optional[str]:
    """Publish what `scrape_run_metrics` scraped for this run. Logs failures, never raises."""
    reason = unpublishable(exp)
    if reason:
        logger.warning(f"`{run_name}` is not on the dashboard: {reason}")
        return None
    token = os.environ.get(TOKEN_ENV)
    if not token:
        logger.warning(f"{TOKEN_ENV} is not set; `{run_name}` is not on the dashboard")
        return None
    try:
        return publish(experiment_body(exp, run_scrape_config(exp, name=run_name), run_name), token)
    except Exception:
        logger.exception(f"Publishing `{run_name}` to the dashboard failed")
        return None

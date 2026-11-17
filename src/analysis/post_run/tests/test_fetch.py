import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.analysis.data.data_request_handler import DataRequestHandler
from src.analysis.post_run import fetch
from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import SCRAPE_LABEL, metric_dataset

EXP = {
    "experiment": "nimlibp2p",
    "stack": {"namespace": "zerotesting"},
    "results": {"stable": {"start": "2026-09-16T10:10:00", "end": "2026-09-16T10:11:00"}},
}
STREAMS = "libp2p-open-streams"
T0 = datetime(2026, 9, 16, 10, 10, tzinfo=timezone.utc).timestamp()
VM_RESPONSE = {
    "data": {
        "result": [
            {
                "metric": {"instance": "10.0.0.2:8008", "type": "Yamux", "dir": "in", "job": "j"},
                "values": [[T0, "3"], [T0 + 60, "4"]],
            },
            {
                "metric": {"instance": "10.0.0.1:8008", "type": "Yamux", "dir": "out", "job": "j"},
                "values": [[T0, "5"], [T0 + 60, "6.5"]],
            },
        ]
    }
}


class _Response:
    status_code = 200

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


def _stored_rows(response):
    """What the dashboard keeps of a VM response: one row per sample, schema labels only."""
    return [
        {
            "timestamp": pd.to_datetime(ts, unit="s").isoformat(),
            "value": float(value),
            **{k: series["metric"][k] for k in ("instance", "type", "dir")},
        }
        for series in response["data"]["result"]
        for ts, value in series["values"]
    ]


def _published(monkeypatch, run, query=None, empty=()):
    """A dashboard holding `run` twice, from two windows; the right one has `empty` datasets too."""
    (run).mkdir()
    (run / "metadata.json").write_text(json.dumps(EXP))
    config = run_scrape_config(EXP, name=run.name)
    queries = {
        m.folder_name.strip("/"): metric_dataset(m, config)["query"]
        for m in config.metrics_to_scrape
    }
    stored = [{"name": STREAMS, "query": query or queries[STREAMS]}]
    stored += [{"name": name, "query": queries[name]} for name in empty]
    experiments = [
        {"id": "e0", "title": "run 2026-01-01 00:00", "metadata": {"run": "run"}},
        {"id": "e1", "title": "run 2026-09-16 10:10", "datasets": stored}
        | {"metadata": {**EXP, "run": "run", SCRAPE_LABEL: "yamux"}},
    ]

    def get(url, **kwargs):
        if url.endswith("/experiments"):
            return _Response(experiments)
        rows = {STREAMS: _stored_rows(VM_RESPONSE)} | dict.fromkeys(empty, [])
        name = url.removeprefix("http://dash/experiments/e1/datasets/").removesuffix("/data")
        assert name in rows, url
        return _Response({"data": rows[name]})

    monkeypatch.setattr(fetch.requests, "get", get)


def test_a_fetched_run_matches_its_local_scrape(tmp_path, monkeypatch):
    _published(monkeypatch, tmp_path / "run")
    dump = fetch.run_metrics(tmp_path / "run", url="http://dash")

    scraped = DataRequestHandler(VM_RESPONSE)
    scraped.create_dataframe_from_request("instance-type-dir", None, None)
    scraped.dump_dataframe(str(tmp_path / "scraped.csv"))
    read = lambda path: pd.read_csv(path, index_col="Time", parse_dates=["Time"])
    pd.testing.assert_frame_equal(
        read(dump / STREAMS / "yamux.csv"), read(tmp_path / "scraped.csv"), check_dtype=False
    )


@pytest.mark.parametrize(
    "query, empty, error, match",
    [
        (None, ["mesh-peers"], LookupError, "no stored rows for mesh-peers"),
        ({"expr": "other", "step": "60s"}, (), ValueError, "not as today's"),
    ],
)
def test_a_run_stored_empty_or_under_another_query_is_not_fetched(
    tmp_path, monkeypatch, query, empty, error, match
):
    _published(monkeypatch, tmp_path / "run", query, empty)
    with pytest.raises(error, match=match):
        fetch.run_metrics(tmp_path / "run", url="http://dash")
    assert not (tmp_path / "run" / "metrics").exists()


def test_a_local_scrape_is_used_without_the_dashboard(tmp_path, monkeypatch):
    local = tmp_path / "run" / "metrics" / "libp2p-in"
    local.mkdir(parents=True)
    (local / "quic.csv").write_text("Time,pod-0\n")

    def get(*args, **kwargs):
        raise AssertionError("contacted the dashboard")

    monkeypatch.setattr(fetch.requests, "get", get)
    assert fetch.run_metrics(tmp_path / "run") == tmp_path / "run" / "metrics"


def test_a_metrics_folder_without_csvs_is_not_overwritten(tmp_path, monkeypatch):
    local = tmp_path / "run" / "metrics" / "libp2p-in"
    local.mkdir(parents=True)
    (local / "quic").write_text("Time,pod-0\n")

    def get(*args, **kwargs):
        raise AssertionError("contacted the dashboard")

    monkeypatch.setattr(fetch.requests, "get", get)
    with pytest.raises(FileExistsError):
        fetch.run_metrics(tmp_path / "run")

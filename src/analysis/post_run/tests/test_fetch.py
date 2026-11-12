from datetime import datetime, timezone

import pandas as pd
import pytest

from src.analysis.data.data_request_handler import DataRequestHandler
from src.analysis.post_run import fetch

EXP = {
    "experiment": "nimlibp2p",
    "stack": {"namespace": "zerotesting"},
    "results": {"stable": {"start": "2026-09-16T10:10:00", "end": "2026-09-16T10:11:00"}},
}
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


def test_a_fetched_run_matches_its_local_scrape(tmp_path, monkeypatch):
    def get(url, **kwargs):
        if url.endswith("/experiments"):
            return _Response([{"id": "e1", "title": "run", "metadata": {**EXP, "run": "run"}}])
        rows = _stored_rows(VM_RESPONSE)
        return _Response({"datasets": [{"name": "libp2p-open-streams", "data": rows}]})

    monkeypatch.setattr(fetch.requests, "get", get)
    dump = fetch.run_metrics(tmp_path / "run", url="http://dash")

    scraped = DataRequestHandler(VM_RESPONSE)
    scraped.create_dataframe_from_request("instance-type-dir", None, None)
    scraped.dump_dataframe(str(tmp_path / "scraped.csv"))
    read = lambda path: pd.read_csv(path, index_col="Time", parse_dates=["Time"])
    pd.testing.assert_frame_equal(
        read(dump / "libp2p-open-streams" / "run.csv"),
        read(tmp_path / "scraped.csv"),
        check_dtype=False,
    )


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

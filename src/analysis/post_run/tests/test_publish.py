import pytest

from src.analysis.post_run import publish as publish_module
from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import PublishError, experiment_body, publish

EXP = {
    "experiment": {"name": "nimlibp2p"},
    "params": {"muxer": "yamux", "image": "regression-node:v2.4.0"},
    "stack": {"namespace": "zerotesting"},
    "results": {
        "complete": {"start": "2026-09-16T10:03:45", "end": "2026-09-16T10:26:32"},
        "stable": {"start": "2026-09-16T10:10:00", "end": "2026-09-16T10:16:30"},
    },
}


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_each_scraped_metric_is_a_dataset_over_the_stable_window():
    config = run_scrape_config(EXP, name="run")
    body = experiment_body(EXP, config, "run")

    datasets = {d["name"]: d for d in body["datasets"]}
    assert set(datasets) == {m.folder_name.strip("/") for m in config.metrics_to_scrape}
    for dataset in datasets.values():
        assert dataset["timeRange"] == {
            "start": "2026-09-16T10:10:00+00:00",
            "end": "2026-09-16T10:16:30+00:00",
        }
        assert "$__rate_interval" not in dataset["query"]["expr"]
    assert "[121s]" in datasets["libp2p-in"]["query"]["expr"]
    # The scrape names CSV columns after these labels, so they must be kept.
    open_streams = [field["name"] for field in datasets["libp2p-open-streams"]["schema"]]
    assert {"instance", "type", "dir"} <= set(open_streams)


def test_a_dataset_without_rows_fails_the_publish_and_is_removed(monkeypatch):
    deleted = []
    monkeypatch.setattr(
        publish_module.requests, "post", lambda *a, **k: _Response(201, {"id": "e1"})
    )
    monkeypatch.setattr(
        publish_module.requests,
        "get",
        lambda *a, **k: _Response(
            200,
            {
                "datasets": [
                    {"name": "libp2p-in", "rowCount": 7},
                    {"name": "mesh-peers", "rowCount": 0},
                ]
            },
        ),
    )
    monkeypatch.setattr(
        publish_module.requests, "delete", lambda url, **k: deleted.append(url) or _Response(204)
    )

    with pytest.raises(PublishError, match="mesh-peers"):
        publish({"title": "run"}, token="t", url="http://dash")
    assert deleted == ["http://dash/experiments/e1"]


@pytest.mark.parametrize(
    "code, published",
    [
        (None, False),
        ({"commit": "abc1234", "clean": False, "pushed": True}, False),
        ({"commit": "abc1234", "clean": True, "pushed": False}, False),
        ({"commit": "abc1234", "clean": True, "pushed": True}, True),
    ],
)
def test_only_runs_from_clean_pushed_code_are_published(monkeypatch, code, published):
    posted = []
    monkeypatch.setenv(publish_module.TOKEN_ENV, "t")
    monkeypatch.setattr(publish_module, "publish", lambda body, token: posted.append(body))

    publish_module.publish_run_metrics({**EXP, "code": code}, "run")

    assert bool(posted) is published

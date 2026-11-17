import re
from pathlib import Path

import pytest

from src.analysis.post_run import fetch
from src.analysis.post_run import publish as publish_module
from src.analysis.post_run.metrics import run_scrape_config
from src.analysis.post_run.publish import PublishError, experiment_body, publish

EXP = {
    "experiment": {"name": "nimlibp2p"},
    "params": {"muxer": "yamux", "image": "regression-node:v2.4.0"},
    "stack": {"namespace": "zerotesting"},
    "results": {
        "complete": {"start": "2026-09-16T10:03:45", "end": "2026-09-16T10:26:32"},
        "stable": {
            "start": "2026-09-16T10:10:00",
            "end": "2026-09-16T10:16:30",
            "duration": "0h 6m 30s",
        },
    },
}
BODY = {"title": "run", "datasets": [{"name": "mesh-peers"}, {"name": "received"}]}


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = ""
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(self.status_code)


def _dashboard(monkeypatch, post, existing, rows):
    """A dashboard answering a POST with `post`, holding `existing`, its datasets with `rows`."""
    deleted = []

    def get(url, **kwargs):
        if url == "http://dash/experiments":
            return _Response(200, [{"id": "e0", "title": "other"}, existing])
        name = url.removeprefix("http://dash/experiments/e1/datasets/").removesuffix("/data")
        if isinstance(rows.get(name), Exception):
            raise rows[name]
        if name not in rows:
            return _Response(404)
        return _Response(200, {"data": [{}] * rows[name]})

    monkeypatch.setattr(publish_module.requests, "post", lambda *a, **k: post)
    monkeypatch.setattr(publish_module.requests, "get", get)
    monkeypatch.setattr(
        publish_module.requests, "delete", lambda url, **k: deleted.append(url) or _Response(204)
    )
    return deleted


def _vm(monkeypatch, discovered, metrics):
    """VM with `discovered` series on the first pods; `metrics`: name -> (min, max, series)."""

    def get(url, params=None, **kwargs):
        assert url.endswith("/query"), f"contacted {url}"
        query = params["query"]
        if query.startswith("last_over_time({"):
            return _Response(200, {"data": {"result": [{"metric": s} for s in discovered]}})
        name = re.search(r"_over_time\((\w+)\{", query)[1]
        low, high, series = metrics.get(name, (0.0, 5.0, []))
        if query.startswith("count by"):
            return _Response(200, {"data": {"result": [{"metric": dict(s)} for s in series]}})
        value = low if query.startswith("min") else high
        return _Response(200, {"data": {"result": [{"value": [0, str(value)]}]}})

    monkeypatch.setattr(publish_module.requests, "get", get)


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


@pytest.mark.parametrize(
    "rows, bad",
    [
        ({"mesh-peers": 0, "received": 5}, "mesh-peers"),
        ({"mesh-peers": 3, "received": 4}, "received"),
        ({"mesh-peers": 3, "received": TimeoutError("read timed out")}, "read timed out"),
        ({"mesh-peers": 3, "received": 5}, None),
    ],
)
def test_a_created_experiment_that_fails_its_check_is_removed(monkeypatch, rows, bad):
    deleted = _dashboard(monkeypatch, _Response(201, {"id": "e1"}), {"id": "e1"}, rows)

    if bad is None:
        assert publish(BODY, token="t", url="http://dash", expected={"received": 5}) == "e1"
        assert deleted == []
        return
    with pytest.raises((PublishError, TimeoutError), match=bad):
        publish(BODY, token="t", url="http://dash", expected={"received": 5})
    assert deleted == ["http://dash/experiments/e1"]


@pytest.mark.parametrize(
    "datasets, rows, bad",
    [
        (["mesh-peers", "received"], {"mesh-peers": 3, "received": 4}, "received"),
        (["mesh-peers", "received"], {"mesh-peers": 3}, "not stored"),
        (["mesh-peers"], {"mesh-peers": 3}, None),
    ],
)
def test_an_experiment_already_there_is_checked_but_never_removed(monkeypatch, datasets, rows, bad):
    existing = {"id": "e1", "title": "run", "datasets": [{"name": n} for n in datasets]}
    deleted = _dashboard(monkeypatch, _Response(409), existing, rows)

    if bad is None:
        assert publish(BODY, token="t", url="http://dash", expected={"received": 5}) == "e1"
    else:
        with pytest.raises(PublishError, match=bad):
            publish(BODY, token="t", url="http://dash", expected={"received": 5})
    assert deleted == []


def test_node_metrics_are_the_node_jobs_own_series_on_every_statefulset(monkeypatch):
    discovered = [
        {"__name__": "libp2p_peers", "job": "nodes", "pod": "relay-0", "instance": "i"},
        {"__name__": "libp2p_network_bytes_total", "job": "nodes", "direction": "in"},
        {"__name__": "libp2p_network_bytes_total", "job": "nodes", "direction": "out"},
        {"__name__": "logos_archive_messages", "job": "nodes", "pod": "store-0"},
        {"__name__": "libp2p_pubsub_peers_created", "job": "nodes"},
        {"__name__": "scrape_duration_seconds", "job": "nodes"},
        {"__name__": "up", "job": "nodes"},
        {"__name__": "container_cpu_usage_seconds_total", "job": "kubelet", "cpu": "total"},
    ]
    stack = {"namespace": "z", "stateful_sets": ["relay", "store"], "nodes_per_statefulset": [9, 2]}
    exp = {**EXP, "stack": stack}

    _vm(monkeypatch, discovered, {})
    assert publish_module.node_metrics(exp, "http://vm") == {
        "libp2p_network_bytes_total": ["direction"],
        "libp2p_peers": [],
        "logos_archive_messages": [],
    }
    _vm(monkeypatch, [], {})
    with pytest.raises(PublishError, match="No node metrics"):
        publish_module.node_metrics(exp, "http://vm")


def test_a_metric_that_never_changes_is_published_as_its_value_and_read_back_as_it(
    monkeypatch, tmp_path
):
    pods = ["pod-0", "pod-1", "pod-2"]
    every = lambda labels: [{"pod": pod} | labels for pod in pods]
    # name: (min, max, series over the run), and the labels discovery finds on it
    vm = {
        "libp2p_peers": (3.0, 9.0, every({}), {}),
        "libp2p_pubsub_topics": (1.0, 1.0, every({}), {}),
        "logos_delivery_version": (1.0, 1.0, every({"version": "v1"}), {"version": "v1"}),
        "libp2p_gossipsub_received_iwants": (
            0.0,
            0.0,
            every({"kind": "a"}) + every({"kind": "b"}),
            {"kind": "a"},
        ),
        "libp2p_gossipsub_low_peers_topics": (0.0, 0.0, every({})[:2], {}),
    }
    discovered = [{"__name__": n, "job": "nodes"} | v[3] for n, v in vm.items()]
    _vm(monkeypatch, discovered, {n: v[:3] for n, v in vm.items()})
    stack = {"namespace": "z", "stateful_sets": ["pod"], "nodes_per_statefulset": [3]}
    exp = {**EXP, "stack": stack | {"container_name": "pod-0"}}
    body = publish_module.run_body(exp, tmp_path / "run")

    names = {d["name"] for d in body["datasets"]}
    assert {"libp2p_peers", "libp2p_gossipsub_received_iwants"} <= names
    assert "libp2p_gossipsub_low_peers_topics" in names
    assert body["metadata"]["unchanged"]["metrics"] == {
        "libp2p_pubsub_topics": {"value": 1.0, "labels": {}},
        "logos_delivery_version": {"value": 1.0, "labels": {"version": "v1"}},
    }
    end = EXP["results"]["complete"]["end"]
    assert fetch.run_dataset({"id": "e1"} | body, "logos_delivery_version") == [
        {"timestamp": end, "value": 1.0, "pod": pod, "version": "v1"} for pod in pods
    ]


@pytest.mark.parametrize(
    "stable, code, published",
    [
        (True, None, False),
        (True, {"commit": "abc1234", "clean": False, "pushed": True}, False),
        (True, {"commit": "abc1234", "clean": True, "pushed": False}, False),
        (True, {"commit": "abc1234", "clean": True, "pushed": True}, True),
        (False, {"commit": "abc1234", "clean": True, "pushed": True}, False),
    ],
)
def test_only_runs_with_a_stable_window_from_clean_pushed_code_are_published(
    monkeypatch, stable, code, published
):
    posted = []
    monkeypatch.setenv(publish_module.TOKEN_ENV, "t")
    monkeypatch.setattr(publish_module, "run_body", lambda exp, run: {"datasets": []})
    monkeypatch.setattr(publish_module, "publish", lambda body, token, **k: posted.append(body))
    # The bridge leaves out `duration` when the stable window is inverted.
    results = EXP["results"] if stable else {"stable": {"start": "10:02:25", "end": "10:01:51"}}

    publish_module.publish_run_metrics({**EXP, "results": results, "code": code}, Path("run"))

    assert bool(posted) is published

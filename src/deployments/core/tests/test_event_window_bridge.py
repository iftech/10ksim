# Python Import
import json
import logging
from datetime import timedelta

# Project Import
from src.deployments.core.event_window_bridge import (
    EventBound,
    EventNotFound,
    EventWindow,
    EventWindowBridge,
)


def write_events_log(tmp_path, events):
    log_path = tmp_path / "events.log"
    log_path.write_text("\n".join(json.dumps(event) for event in events) + "\n")
    return log_path


class ExampleWindowBridge(EventWindowBridge):
    interval: str = "stable"
    container_name: str = "example-container"

    def event_windows(self):
        return [
            EventWindow(
                key="complete",
                start=EventBound("experiment_started"),
                end=EventBound("experiment_finished", timedelta(seconds=30)),
            ),
            EventWindow(
                key="stable",
                start=EventBound(
                    {"event": "messages_started", "role": "publisher"}, timedelta(minutes=3)
                ),
                end=EventBound("messages_finished", timedelta(seconds=-30)),
            ),
        ]


def test_event_bound_builds_key_from_string_event():
    bound = EventBound("experiment_started", timedelta(seconds=5))

    assert bound == EventBound("experiment_started", timedelta(seconds=5))
    assert bound.key == {"event": "experiment_started"}
    assert bound.time_shift == timedelta(seconds=5)


def test_event_bound_uses_dict_event_as_key():
    bound = EventBound({"event": "messages_started", "role": "publisher"})

    assert bound.key == {"event": "messages_started", "role": "publisher"}
    assert bound.time_shift == timedelta(0)


def test_event_window_bridge_extracts_results_and_selected_interval(tmp_path):
    log_path = write_events_log(
        tmp_path,
        [
            {
                "event": "deployment",
                "phase": "start",
                "kind": "StatefulSet",
                "name": "publisher",
                "replicas": 2,
                "namespace": "test",
            },
            {
                "event": "metadata",
                "experiment_name": "window-demo",
                "experiment_class": "WindowDemo",
                "command": "run",
                "kube_config": "kind",
            },
            {"event": "experiment_started", "timestamp": "2026-01-01 12:00:00"},
            {"event": "experiment_finished", "timestamp": "2026-01-01 12:30:00"},
            {
                "event": "messages_started",
                "role": "publisher",
                "timestamp": "2026-01-01 12:05:00",
            },
            {"event": "messages_finished", "timestamp": "2026-01-01 12:20:00"},
        ],
    )

    metadata = ExampleWindowBridge().get_metadata(log_path)

    # Check complete interval
    complete = metadata["results"]["complete"]
    assert complete["start"] == "2026-01-01T12:00:00"
    assert complete["end"] == "2026-01-01T12:30:30"
    assert complete["duration"] == "0h 30m 30s"
    assert complete["grafana"].startswith("https://grafana.lab.vac.dev/d/jIrqsZTIz/nwaku?")
    assert "var-namespace=test" in complete["grafana"]
    assert complete["victoria_logs"].startswith("https://vlselect.lab.vac.dev/select/vmui/#/?")
    assert "kubernetes.pod_namespace%3Atest" in complete["victoria_logs"]

    # Check stable interval
    stable = metadata["results"]["stable"]
    assert stable["start"] == "2026-01-01T12:08:00"
    assert stable["end"] == "2026-01-01T12:19:30"
    assert stable["duration"] == "0h 11m 30s"
    assert stable["grafana"].startswith("https://grafana.lab.vac.dev/d/jIrqsZTIz/nwaku?")
    assert stable["victoria_logs"].startswith("https://vlselect.lab.vac.dev/select/vmui/#/?")

    # Check selected interval promoted to stack
    assert metadata["stack"]["start_time"] == "2026-01-01T12:08:00"
    assert metadata["stack"]["end_time"] == "2026-01-01T12:19:30"
    assert metadata["stack"]["container_name"] == "example-container"
    assert metadata["stack"]["stateful_sets"] == ["publisher"]
    assert metadata["experiment"]["name"] == "window-demo"
    assert "event_windows" not in metadata["experiment"]["bridge_class"]


def test_event_window_bridge_warns_when_selected_interval_is_missing(tmp_path, caplog):
    log_path = write_events_log(
        tmp_path,
        [
            {
                "event": "deployment",
                "phase": "start",
                "kind": "StatefulSet",
                "name": "publisher",
                "replicas": 2,
                "namespace": "test",
            },
            {
                "event": "metadata",
                "experiment_name": "window-demo",
                "experiment_class": "WindowDemo",
            },
            {"event": "experiment_started", "timestamp": "2026-01-01 12:00:00"},
            {"event": "experiment_finished", "timestamp": "2026-01-01 12:30:00"},
        ],
    )

    with caplog.at_level(logging.WARNING):
        metadata = ExampleWindowBridge().get_metadata(log_path)

    # Should warn about missing interval
    assert "Analysis window `stable` not found" in caplog.text

    # Should still create metadata with EventNotFound for times
    assert metadata["stack"]["start_time"] == EventNotFound
    assert metadata["stack"]["end_time"] == EventNotFound
    assert metadata["stack"]["container_name"] == "example-container"
    assert metadata["experiment"]["name"] == "window-demo"

    # Should have results with only the intervals that exist
    assert "complete" in metadata["results"]
    assert "stable" not in metadata["results"]

import pandas as pd

from src.analysis.mesh_analysis.analyzers.waku.delivery_latency import (
    delivery_latency,
    relay_receipts,
    write_delivery_latency,
)
from src.analysis.plotting.latency_plotter import load_delays

BASE = pd.Timestamp("2026-09-15 10:00:00")


def _rows(rows):
    return pd.DataFrame(
        [
            {
                "msg_hash": msg,
                "timestamp": BASE + pd.Timedelta(milliseconds=ms),
                "kubernetes.pod_name": pod,
            }
            for msg, pod, ms in rows
        ]
    )


def _network():
    # Relay message R: its publish target fserver-0-0 receives it at 0ms.
    # Lightpush message L: lpserver-0-0 handles it at 100ms and so logs no relay receipt.
    received = _rows(
        [
            ("R", "fserver-0-0", 0),
            ("R", "fserver-0-1", 5),
            ("R", "lpserver-0-0", 10),
            ("L", "lpserver-0-0", 100),  # the handling row reliability also collects
            ("L", "fserver-0-0", 103),
            ("L", "fserver-0-1", 107),
        ]
    )
    lightpush = _rows([("L", "lpserver-0-0", 100)])
    filter_received = _rows(
        [
            ("R", "fclient-0-0", 20),
            ("L", "fclient-0-0", 110),
            ("U", "fclient-0-0", 50),  # never entered the network through a path we saw
        ]
    )
    return received, lightpush, filter_received


def _delays(frame):
    return sorted(round(value, 3) for value in frame["delayMs"])


def test_relay_receipts_drop_only_the_handling_row():
    received, lightpush, _ = _network()

    relay = relay_receipts(received, lightpush)

    assert len(relay) == 5
    assert not ((relay["msg_hash"] == "L") & (relay["kubernetes.pod_name"] == "lpserver-0-0")).any()


def test_each_path_measures_from_its_entry_point():
    paths = delivery_latency(*_network())

    assert _delays(paths["relay"]) == [5.0, 10.0]
    assert _delays(paths["lightpush"]) == [3.0, 7.0]
    assert _delays(paths["filter"]) == [10.0, 20.0]


def test_relay_only_run_has_no_lightpush_or_filter_path():
    received = _rows([("R", "relay-0-0", 0), ("R", "relay-0-1", 4)])
    empty = _rows([])

    paths = delivery_latency(
        received, empty.reindex(columns=received.columns), empty.reindex(columns=received.columns)
    )

    assert list(paths) == ["relay"]
    assert _delays(paths["relay"]) == [4.0]


def test_written_csvs_load_in_the_latency_plotter(tmp_path):
    written = write_delivery_latency(delivery_latency(*_network()), tmp_path)

    assert sorted(load_delays(written["filter"]).tolist()) == [10.0, 20.0]

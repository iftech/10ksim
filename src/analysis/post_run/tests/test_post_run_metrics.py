import logging

import pandas as pd

from src.analysis.plotting.config import PlotConfig
from src.analysis.post_run.metrics import STANDARD_PLOTS, plot_run_metrics


def _scrape_dump(root, muxer, metrics, rows=6):
    """Write a dump in the shape Scrapper produces: <dump>/<metric>/<run name>.csv."""
    dump = root / muxer
    times = pd.to_datetime([f"2026-08-10 01:{m:02d}:00" for m in range(rows)])
    for metric in metrics:
        (dump / metric).mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"pod-0": [1.0] * rows, "pod-1": [2.0] * rows}, index=times).rename_axis(
            "Time"
        ).to_csv(dump / metric / f"{muxer}.csv")
    return dump


def test_plots_a_scrape_dump_without_staging_it_first(tmp_path):
    metrics = ["libp2p-in", "libp2p-out"]
    dumps = {m: _scrape_dump(tmp_path / "metrics", m, metrics) for m in ("yamux", "quic")}
    spec = [PlotConfig(name="bandwidth", metrics=metrics, ylabel_name="KBytes/s")]

    written = plot_run_metrics(dumps, tmp_path / "plots", xlabel="test", plots=spec)

    assert [p.name for p in written] == ["bandwidth.jpg"]
    assert written[0].exists()


def test_a_plot_with_no_data_is_skipped_and_said_so(tmp_path, caplog):
    """A silently missing figure reads as "we did not measure that"."""
    dumps = {"yamux": _scrape_dump(tmp_path / "metrics", "yamux", ["libp2p-in"])}
    spec = [PlotConfig(name="memory", metrics=["nim-gc-memory"], scale_x=1_000_000)]

    with caplog.at_level(logging.WARNING):
        assert plot_run_metrics(dumps, tmp_path / "plots", xlabel="test", plots=spec) == []
    assert "No data for the memory plot" in caplog.text


def test_a_muxer_missing_one_metric_does_not_drop_the_others(tmp_path):
    root = tmp_path / "metrics"
    dumps = {
        "yamux": _scrape_dump(root, "yamux", ["connections"]),
        "quic": _scrape_dump(root, "quic", ["libp2p-in"]),
    }
    spec = [PlotConfig(name="connections", metrics=["connections"], scale_x=1, fig_size=[9, 6])]

    written = plot_run_metrics(dumps, tmp_path / "plots", xlabel="test", plots=spec)

    assert written[0].exists()


def test_a_dump_missing_part_of_a_plot_is_left_out_of_it(tmp_path, caplog):
    root = tmp_path / "metrics"
    dumps = {
        "yamux": _scrape_dump(root, "yamux", ["libp2p-in", "libp2p-out"]),
        "quic": _scrape_dump(root, "quic", ["libp2p-in"]),
    }
    spec = [PlotConfig(name="bandwidth", metrics=["libp2p-in", "libp2p-out"], scale_x=1000)]

    with caplog.at_level(logging.ERROR):
        written = plot_run_metrics(dumps, tmp_path / "plots", xlabel="test", plots=spec)

    assert written[0].exists()
    assert "`quic` has no ['libp2p-out'] data" in caplog.text


def test_the_standard_set_covers_the_report_figures():
    names = {plot.name for plot in STANDARD_PLOTS}
    assert names == {"bandwidth", "memory", "connections"}

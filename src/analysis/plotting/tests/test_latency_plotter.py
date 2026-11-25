from pathlib import Path

import pandas as pd

from src.analysis.plotting.latency_plotter import latency_percentiles, latency_table, load_delays


def _write_received(run: Path, delays, pods=None) -> Path:
    d = run / "analysis_data" / "summary"
    d.mkdir(parents=True, exist_ok=True)
    pods = pods or [f"pod-{i}" for i in range(len(delays))]
    pd.DataFrame(
        {
            "msgId": range(len(delays)),
            "timestamp": ["2026-07-18 04:40:00"] * len(delays),
            "sentAt": ["2026-07-18 04:40:00"] * len(delays),
            "delayMs": delays,
            "kubernetes.pod_name": pods,
        }
    ).to_csv(d / "received.csv", index=False)
    return run


class TestLoadDelays:
    def test_reads_the_analysis_dump(self, tmp_path):
        _write_received(tmp_path, [10, 20, 30])
        assert list(load_delays(tmp_path)) == [10, 20, 30]

    def test_accepts_the_csv_directly(self, tmp_path):
        _write_received(tmp_path, [5, 15])
        csv = tmp_path / "analysis_data" / "summary" / "received.csv"
        assert list(load_delays(csv)) == [5, 15]

    def test_missing_run_is_empty_not_an_error(self, tmp_path):
        assert load_delays(tmp_path / "nope").empty

    def test_unparseable_delays_are_dropped(self, tmp_path):
        _write_received(tmp_path, [10, "n/a", 30])
        assert list(load_delays(tmp_path)) == [10.0, 30.0]


class TestPercentiles:
    def test_percentiles_and_count(self, tmp_path):
        _write_received(tmp_path, list(range(1, 101)))  # 1..100
        summary = latency_percentiles(tmp_path)
        assert summary["p50"] == 50.5
        assert summary["p99"] == 99.0  # 99.01, reported to one decimal
        assert summary["max"] == 100.0
        assert summary["deliveries"] == 100

    def test_empty_run_gives_empty_summary(self, tmp_path):
        assert latency_percentiles(tmp_path / "nope") == {}

    def test_table_puts_runs_in_columns(self, tmp_path):
        fast = _write_received(tmp_path / "fast", [1, 2, 3])
        slow = _write_received(tmp_path / "slow", [100, 200, 300])
        table = latency_table({"v2.1.0": fast, "v2.2.0": slow})
        assert list(table.columns) == ["v2.1.0", "v2.2.0"]
        assert table.loc["p50", "v2.1.0"] == 2.0
        assert table.loc["p50", "v2.2.0"] == 200.0


class TestBoxPlot:
    def test_writes_one_box_per_run(self, tmp_path):
        from src.analysis.plotting.latency_plotter import LatencyPlotConfig, LatencyPlotter

        fast = _write_received(tmp_path / "fast", [1, 2, 3, 400])
        slow = _write_received(tmp_path / "slow", [10, 20, 30])
        config = LatencyPlotConfig(
            name="box", kind="box", runs={"fast": fast, "slow": slow}, out_dir=tmp_path
        )

        out = LatencyPlotter(configs=[config])._create_plot(config)

        assert out == tmp_path / "box.jpg"
        assert out.exists()

    def test_no_data_writes_nothing(self, tmp_path):
        from src.analysis.plotting.latency_plotter import LatencyPlotConfig, LatencyPlotter

        config = LatencyPlotConfig(
            name="box", kind="box", runs={"none": tmp_path / "nope"}, out_dir=tmp_path
        )

        assert LatencyPlotter(configs=[config])._create_plot(config) is None
        assert not (tmp_path / "box.jpg").exists()

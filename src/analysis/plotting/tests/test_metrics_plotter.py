from src.analysis.data.data_file_handler import DataPath
from src.analysis.plotting.config import PlotConfig
from src.analysis.plotting.metrics_plotter import MetricsPlotter


def test_named_files_for_metric_expands_include_files_to_metric_files(tmp_path):
    run_0 = tmp_path / "asd_run_0"
    run_1 = tmp_path / "asd_run_1"
    (run_0 / "container-memory").mkdir(parents=True)
    (run_1 / "container-memory").mkdir(parents=True)
    (run_0 / "container-memory" / "mplex").touch()
    (run_1 / "container-memory" / "quic").touch()

    plot_config = PlotConfig(name="memory", include_files=["mplex", "quic", "yamux"])
    named_files = MetricsPlotter(configs=[plot_config])._named_files_for_metric(
        plot_config,
        [
            DataPath(name="mplex", path=run_0),
            DataPath(name="quic", path=run_1),
        ],
        "container-memory",
    )

    assert [(file.name, file.path) for file in named_files] == [
        ("mplex", run_0 / "container-memory" / "mplex"),
        ("quic", run_1 / "container-memory" / "quic"),
    ]


def test_named_files_for_metric_uses_display_name_with_explicit_file_name(tmp_path):
    run_0 = tmp_path / "asd_run_0"
    run_1 = tmp_path / "asd_run_1"
    (run_0 / "container-memory").mkdir(parents=True)
    (run_1 / "container-memory").mkdir(parents=True)
    (run_0 / "container-memory" / "mplex").touch()
    (run_1 / "container-memory" / "mplex").touch()

    plot_config = PlotConfig(name="memory", include_files=["mplex", "quic", "yamux"])
    named_files = MetricsPlotter(configs=[plot_config])._named_files_for_metric(
        plot_config,
        [
            DataPath(name="2.3.0/mplex", path=run_0, file_name="mplex"),
            DataPath(name="2.2.0/mplex", path=run_1, file_name="mplex"),
        ],
        "container-memory",
    )

    assert [(file.name, file.path) for file in named_files] == [
        ("2.3.0/mplex", run_0 / "container-memory" / "mplex"),
        ("2.2.0/mplex", run_1 / "container-memory" / "mplex"),
    ]


def test_named_files_for_metric_disambiguates_multiple_included_files(tmp_path):
    run_0 = tmp_path / "asd_run_0"
    (run_0 / "container-memory").mkdir(parents=True)
    (run_0 / "container-memory" / "mplex").touch()
    (run_0 / "container-memory" / "quic").touch()

    plot_config = PlotConfig(name="memory", include_files=["mplex", "quic"])
    named_files = MetricsPlotter(configs=[plot_config])._named_files_for_metric(
        plot_config,
        [DataPath(name="2.3.0", path=run_0)],
        "container-memory",
    )

    assert [(file.name, file.path) for file in named_files] == [
        ("2.3.0/mplex", run_0 / "container-memory" / "mplex"),
        ("2.3.0/quic", run_0 / "container-memory" / "quic"),
    ]


def test_named_files_for_metric_keeps_legacy_metric_file_paths_without_include_files(tmp_path):
    plot_config = PlotConfig(name="memory")
    named_files = MetricsPlotter(configs=[plot_config])._named_files_for_metric(
        plot_config,
        [DataPath(name="v1", path=tmp_path / "v1")],
        "container-memory",
    )

    assert [(file.name, file.path) for file in named_files] == [
        ("v1", tmp_path / "v1" / "container-memory")
    ]


def test_named_files_for_metric_finds_csv_suffixed_scrapes(tmp_path):
    run = tmp_path / "run"
    (run / "container-recv").mkdir(parents=True)
    (run / "container-recv" / "fserver-0.csv").touch()
    (run / "container-recv" / "fclient-0.csv").touch()

    plot_config = PlotConfig(name="bandwidth", include_files=["fserver-0", "fclient-0"])
    named_files = MetricsPlotter(configs=[plot_config])._named_files_for_metric(
        plot_config,
        [
            DataPath(name="filter server", path=run, file_name="fserver-0"),
            DataPath(name="filter client", path=run, file_name="fclient-0"),
        ],
        "container-recv",
    )

    assert [(file.name, file.path) for file in named_files] == [
        ("filter server", run / "container-recv" / "fserver-0.csv"),
        ("filter client", run / "container-recv" / "fclient-0.csv"),
    ]

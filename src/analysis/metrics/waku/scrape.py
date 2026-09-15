import logging
from pathlib import Path
from typing import List

from src.analysis.data.data_file_handler import DataPath
from src.analysis.metrics.config import ScrapeConfig
from src.analysis.metrics.libp2p.scrape import Nimlibp2pScrapeBuilder
from src.analysis.metrics.scrapper import Scrapper
from src.analysis.metrics.waku.metrics import archive_insert_micros, role_metrics
from src.analysis.plotting.config import DataGroup, PlotConfig, PlotConfigBuilder
from src.analysis.plotting.metrics_plotter import MetricsPlotter

logger = logging.getLogger(__name__)

ROLE_LABELS = {
    "fserver-0": "filter server",
    "lpserver-0": "lightpush server",
    "store-0": "store",
    "fclient-0": "filter client",
    "lpclient-0": "lightpush client",
}
"""Roles plotted, in plot order. Bootstrap is left out: the plotter drops its columns."""


def role_scrape_configs(exp: dict, dump_location: Path) -> List[ScrapeConfig]:
    """One scrape per role, named after its stateful set, so each metric file is one role."""
    namespace = exp["stack"]["namespace"]
    configs = []
    for stateful_set in exp["stack"]["stateful_sets"]:
        if stateful_set not in ROLE_LABELS:
            continue
        builder = (
            Nimlibp2pScrapeBuilder()
            .with_exp(exp, extract_name=False)
            .with_dump_location(dump_location)
        )
        builder.name = stateful_set
        builder.metrics_to_scrape = list(role_metrics(namespace, stateful_set))
        if stateful_set.startswith("store"):
            builder.metrics_to_scrape.append(archive_insert_micros(namespace, stateful_set))
        configs.append(builder.build())
    return configs


def role_plots(configs: List[ScrapeConfig], label: str, out_dir: Path) -> List[PlotConfig]:
    """Box plots with node role on the x axis."""
    paths = [
        DataPath(name=ROLE_LABELS[config.name], path=config.dump_location, file_name=config.name)
        for config in configs
    ]
    group = DataGroup(name=label, data_paths=paths)
    order = [path.name for path in paths]

    def plot(name: str, metrics: List[str], ylabel: str, scale: int, roles=None) -> PlotConfig:
        # The plotter only reads a data path's own file when include_files is set;
        # otherwise it reads every role's file for every role.
        builder = (
            PlotConfigBuilder(name=str(out_dir / name))
            .with_groups(group)
            .with_include_files([config.name for config in configs])
        )
        for metric in metrics:
            builder = builder.with_metric(metric)
        config = builder.with_x_order(roles or order).build()
        config.xlabel_name = "Node type"
        config.ylabel_name = ylabel
        config.scale_x = scale
        config.fig_size = [16, 9]
        return config

    store = [ROLE_LABELS["store-0"]] if "store-0" in [c.name for c in configs] else None
    plots = [
        plot("bandwidth", ["container-recv", "container-sent"], "KBytes/s", 1000),
        plot("memory", ["container-memory"], "MBytes", 1_000_000),
        plot("cpu", ["container-cpu"], "millicores", 1),
    ]
    if store:
        store_group = DataGroup(name=label, data_paths=[p for p in paths if p.name == store[0]])
        insert = plot("store_insert", ["archive-insert"], "microseconds", 1, roles=store)
        insert.groups = [store_group]
        plots.append(insert)
    return plots


def scrape_and_plot_roles(k8s_config: str, exp: dict, dump_location: Path, label: str) -> Path:
    """Scrape every role's resources for one run and box plot them next to the data."""
    configs = role_scrape_configs(exp, dump_location)
    for config in configs:
        Scrapper(k8s_config, config).query_and_dump_metrics()
    out_dir = Path(dump_location) / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    MetricsPlotter(configs=role_plots(configs, label, out_dir)).create_plots()
    return out_dir

"""Scrape a finished run's metrics and draw the standard plots.

The reliability and latency analysis already runs itself, but the resource and mesh-health
half of a regression report did not: every campaign grew its own scrape-and-plot script.
These are the same figures, drawn the same way, off the run's own stable window.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from src.analysis.data.data_file_handler import DataPath
from src.analysis.metrics.libp2p.scrape import Nimlibp2pScrapeBuilder
from src.analysis.metrics.scrapper import Scrapper
from src.analysis.plotting.config import DataGroup, PlotConfig
from src.analysis.plotting.metrics_plotter import MetricsPlotter

logger = logging.getLogger(__name__)


STANDARD_PLOTS = [
    PlotConfig(
        name="bandwidth",
        metrics=["libp2p-in", "libp2p-out"],
        ylabel_name="KBytes/s",
        scale_x=1000,
        fig_size=[14, 5],
        outliers=False,
    ),
    PlotConfig(
        name="memory",
        metrics=["container-memory", "nim-gc-memory"],
        ylabel_name="MBytes",
        scale_x=1_000_000,
        fig_size=[14, 5],
        outliers=False,
    ),
    PlotConfig(
        name="connections",
        metrics=["connections", "mesh-peers"],
        ylabel_name="peers per node",
        scale_x=1,
        fig_size=[14, 5],
        outliers=False,
    ),
]
"""Templates: `name` is the figure's file stem, the run fills in groups and output path."""


def scrape_run_metrics(
    exp: dict, dump_location: Path, kube_config: Optional[str] = None, name: Optional[str] = None
) -> Path:
    """Dump the per-metric CSVs for one run's stable window."""
    builder = Nimlibp2pScrapeBuilder().with_exp(exp, extract_name=name is None)
    if name is not None:
        builder.name = name
    config = builder.with_dump_location(str(dump_location)).with_libp2p_metrics().build()
    logger.info(f"Scraping `{config.name}` metrics: {config.start} -> {config.end}")
    Scrapper(kube_config, config).query_and_dump_metrics()
    return dump_location


def plot_run_metrics(
    dumps: Dict[str, Path], out_dir: Path, xlabel: str, plots: Optional[List[PlotConfig]] = None
) -> List[Path]:
    """Box-plot each standard figure across `dumps`, keyed by series label (eg. muxer).

    Reads the scrape dump as it lies: `<dump>/<metric>/<run name>.csv`.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    configs = []
    for template in plots or STANDARD_PLOTS:
        available = {}
        for label, dump in dumps.items():
            missing = _missing_metrics(dump, template.metrics)
            if missing:
                logger.error(
                    f"`{label}` has no {missing} data; leaving it out of the {template.name} plot"
                )
            else:
                available[label] = dump
        if not available:
            logger.warning(f"No data for the {template.name} plot; skipping it")
            continue
        target = out_dir / template.name
        configs.append(
            template.model_copy(
                deep=True,
                update={
                    "name": str(target),
                    "groups": [
                        DataGroup(name=label, data_paths=[DataPath(name=label, path=dump)])
                        for label, dump in available.items()
                    ],
                    "legend_order": list(available),
                    "xlabel_name": xlabel,
                },
            )
        )
        written.append(target.with_suffix(".jpg"))

    if configs:
        MetricsPlotter(configs=configs).create_plots()
        logger.info(f"Wrote {len(configs)} plots to `{out_dir}/`")
    return written


def _missing_metrics(dump: Path, metrics: List[str]) -> List[str]:
    return [metric for metric in metrics if not (dump / metric).exists()]

"""Plot message delivery latency as a CDF, one curve per run.

Latency is not a scraped metric: it comes from the delay stamped on each
`Received message` log line, which the reliability analysis dumps to
`analysis_data/summary/received.csv`. The tail is the interesting part (mesh push
versus gossip pull sit orders of magnitude apart), so a CDF reads better than a box
plot and this does not fit MetricsPlotter.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pydantic import BaseModel, Field, PositiveInt

logger = logging.getLogger(__name__)
sns.set_theme()

DELAY_COLUMN = "delayMs"
SUMMARY_RECEIVED = Path("analysis_data") / "summary" / "received.csv"
DEFAULT_PERCENTILES = (50, 95, 99)


class LatencyPlotConfig(BaseModel):
    name: str = "latency"
    kind: Literal["cdf", "box"] = "cdf"
    """A CDF with one curve per run, or a box per run with outliers shown."""
    runs: Dict[str, Path] = Field(default_factory=dict)
    """Curve label -> run folder (or the received.csv itself)."""
    percentiles: List[PositiveInt] = Field(default_factory=lambda: list(DEFAULT_PERCENTILES))
    log_x: bool = True
    """Log the latency axis (x on a CDF, y on a box): it spans ms to seconds in a degraded mesh."""
    xlabel_name: str = "Delivery latency (ms)"
    ylabel_name: str = "Share of deliveries"
    fig_size: List[PositiveInt] = Field(default_factory=lambda: [10, 6])
    out_dir: Optional[Path] = None
    """Where to write the plot. Defaults to the working directory."""


def _received_csv(run: Path) -> Path:
    return run if run.suffix == ".csv" else run / SUMMARY_RECEIVED


def load_delays(run: Union[str, Path]) -> pd.Series:
    """Delivery delays (ms) for one run, from the reliability analysis dump."""
    csv = _received_csv(Path(run))
    if not csv.exists():
        logger.warning(f"latency: missing {csv}")
        return pd.Series(dtype="float64")
    delays = pd.read_csv(csv, usecols=[DELAY_COLUMN])[DELAY_COLUMN]
    return pd.to_numeric(delays, errors="coerce").dropna()


def latency_percentiles(run: Union[str, Path], percentiles=DEFAULT_PERCENTILES) -> Dict[str, float]:
    """Percentile table for the report, plus the delivery count behind it."""
    delays = load_delays(run)
    if delays.empty:
        return {}
    summary = {f"p{p}": round(float(delays.quantile(p / 100)), 1) for p in percentiles}
    summary["max"] = round(float(delays.max()), 1)
    summary["deliveries"] = int(delays.size)
    return summary


class LatencyPlotter(BaseModel):
    configs: List[LatencyPlotConfig]

    def create_plots(self) -> None:
        for config in self.configs:
            logger.info(f'Plotting "{config.name}"')
            self._create_plot(config)
            logger.info(f'Plot "{config.name}" finished')

    def _create_plot(self, config: LatencyPlotConfig) -> Optional[Path]:
        if config.kind == "box":
            return self._create_box_plot(config)
        plt.figure(figsize=tuple(config.fig_size))
        plotted = False
        for label, run in config.runs.items():
            delays = load_delays(run)
            if delays.empty:
                continue
            ordered = delays.sort_values()
            # step: a CDF is a step function, and interpolating hides heartbeat plateaus
            plt.step(
                ordered,
                (ordered.rank(method="first") / ordered.size),
                where="post",
                label=f"{label} (n={ordered.size})",
            )
            plotted = True

        if not plotted:
            logger.warning(f'No latency data for "{config.name}"')
            plt.close()
            return None

        if config.log_x:
            plt.xscale("log")
        plt.ylim(0, 1)
        plt.xlabel(config.xlabel_name)
        plt.ylabel(config.ylabel_name)
        plt.legend()
        plt.tight_layout()
        out = Path(config.out_dir or ".") / f"{config.name}.jpg"
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out)
        plt.close()
        return out

    def _create_box_plot(self, config: LatencyPlotConfig) -> Optional[Path]:
        frames = [
            pd.DataFrame({"run": f"{label} (n={delays.size})", DELAY_COLUMN: delays})
            for label, delays in ((label, load_delays(run)) for label, run in config.runs.items())
            if not delays.empty
        ]
        if not frames:
            logger.warning(f'No latency data for "{config.name}"')
            return None

        plt.figure(figsize=tuple(config.fig_size))
        sns.boxplot(
            data=pd.concat(frames, ignore_index=True),
            x="run",
            y=DELAY_COLUMN,
            flierprops={"marker": ".", "markersize": 3, "alpha": 0.3},
        )
        if config.log_x:
            plt.yscale("log")
        plt.xlabel("")
        plt.ylabel(config.xlabel_name)
        plt.tight_layout()
        out = Path(config.out_dir or ".") / f"{config.name}.jpg"
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out)
        plt.close()
        return out


def latency_table(runs: Dict[str, Union[str, Path]], percentiles=DEFAULT_PERCENTILES):
    """Run-by-percentile table, the small latency table the report carries."""
    return pd.DataFrame(
        {label: latency_percentiles(run, percentiles) for label, run in runs.items()}
    )


def plot_dump_latency(dump_dir: Path, *, label: str = "run") -> Optional[Path]:
    """CDF and percentile table for one reliability dump, written beside it.

    `dump_dir` is the analysis dump folder holding `summary/received.csv`, which is
    what both platforms produce: Shadow dumps under the run folder, cluster runs
    under their own analysis folder. Returns None when there is no delivery data,
    the normal case for a run whose reliability analysis produced nothing.
    """
    csv = dump_dir / "summary" / "received.csv"
    if load_delays(csv).empty:
        logger.warning(f"latency: no delivery data in {csv}")
        return None

    LatencyPlotter(
        configs=[LatencyPlotConfig(name="latency", runs={label: csv}, out_dir=dump_dir)]
    ).create_plots()
    logger.info(f"Delivery latency (ms) for {label}:\n{latency_table({label: csv}).to_string()}")
    return dump_dir / "latency.jpg"


def main() -> None:
    """CDF + percentile table for any set of run folders, on either platform."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Plot delivery latency as a CDF across runs and print the percentile table."
    )
    parser.add_argument(
        "runs",
        nargs="+",
        metavar="LABEL=RUN_DIR",
        help="Curve label and run folder, e.g. mplex=out/n1000-mplex-kad__rand_195553",
    )
    parser.add_argument("--name", default="latency", help="Output file stem.")
    parser.add_argument("--out-dir", type=Path, default=None, help="Where to write the plot.")
    args = parser.parse_args()

    runs: Dict[str, Path] = {}
    for item in args.runs:
        label, sep, path = item.partition("=")
        if not sep or not path:
            parser.error(f"expected LABEL=RUN_DIR, got `{item}`")
        if label in runs:
            parser.error(f"duplicate label `{label}`")
        runs[label] = Path(path)

    LatencyPlotter(
        configs=[LatencyPlotConfig(name=args.name, runs=runs, out_dir=args.out_dir)]
    ).create_plots()
    print(latency_table(runs).to_string())


if __name__ == "__main__":
    main()

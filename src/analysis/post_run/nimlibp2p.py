from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller
from src.analysis.mesh_analysis.analyzers.nimlibp2p_analyzer import Nimlibp2pAnalyzer
from src.analysis.plotting.latency_plotter import plot_dump_latency
from src.analysis.post_run.delivery_cross_check import counter_deliveries, cross_check, report
from src.analysis.post_run.metrics import plot_run_metrics, scrape_run_metrics

if TYPE_CHECKING:
    from src.deployments.experiments.libp2p.nimlibp2p import NimLibp2pExperiment

logger = logging.getLogger(__name__)

VICTORIA_LOGS_URL = "https://vlselect.lab.vac.dev/select/logsql/query"
METRICS_URL = "https://metrics.lab.vac.dev/select/0/prometheus/api/v1/"
REQUIRED_TIME_WINDOW_KEYS = ("start_time", "end_time")


def _require_bounded_query(stack: dict) -> None:
    missing = [key for key in REQUIRED_TIME_WINDOW_KEYS if not stack.get(key)]
    if missing:
        raise ValueError(
            "Nimlibp2p post-run analysis requires a bounded metadata stack; "
            f"missing: {missing}. Refusing to run an unbounded VictoriaLogs query."
        )


def _log_derived_deliveries(reliability: dict) -> int:
    """(node, message) pairs present in the collected logs."""
    observed = reliability["num_peers"] * reliability["num_unique_messages"]
    missing = sum(
        node.get("missing") or 0
        for entry in reliability.get("missing_messages", [])
        for node in entry.get("nodes", [])
    )
    return observed - missing


def _cross_check_delivery(results, stack: dict, dump_dir: Path) -> None:
    reliability = next(
        (r.intermediates for r in results if r.name == "reliability" and r.status != "error"), None
    )
    if reliability is None:
        logger.info("No reliability result to cross-check")
        return

    from_logs = _log_derived_deliveries(reliability)
    from_counters = counter_deliveries(
        url=METRICS_URL,
        namespace=stack["namespace"],
        start=stack["start_time"],
        end=stack["end_time"],
    )
    result = cross_check(from_logs, from_counters)
    report(result)

    dump_dir.mkdir(parents=True, exist_ok=True)
    (dump_dir / "delivery_cross_check.json").write_text(result.model_dump_json(indent=2))


def run_nimlibp2p_analysis(experiment: "NimLibp2pExperiment") -> None:
    """Message reliability and delivery latency for a finished cluster run."""
    if experiment.output_folder is None:
        raise ValueError("Nimlibp2p post-run analysis requires experiment.output_folder")
    if experiment.metadata is None:
        raise ValueError("Nimlibp2p post-run analysis requires experiment.metadata")

    cfg = experiment.config
    stack = dict(experiment.metadata["stack"])
    stack.update(
        {
            "type": "vaclab",
            "url": VICTORIA_LOGS_URL,
            "reader": "victoria",
            "namespace": experiment.namespace or stack.get("namespace"),
        }
    )
    _require_bounded_query(stack)

    # Bootstrap nodes do not relay, so they are excluded from the delivery counts.
    relays = [
        pair
        for pair in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
        if "bootstrap" not in pair[0]
    ]
    dump_dir = experiment.output_folder / "analysis_data"

    try:
        results = (
            Nimlibp2pAnalyzer(dump_analysis_dir=str(dump_dir))
            .with_data_puller(DataPuller().with_kwargs(stack))
            .with_ss_check(stack["stateful_sets"], stack["nodes_per_statefulset"])
            .with_reliability_check(
                stateful_sets=[name for name, _ in relays],
                nodes_per_ss=[count for _, count in relays],
                expected_num_peers=cfg.num_relay_nodes,
                expected_num_messages=cfg.num_messages,
            )
            .run()
        )
    except Exception:
        logger.exception("Nimlibp2p message analysis failed")
    else:
        try:
            _cross_check_delivery(results, stack, dump_dir)
        except Exception:
            logger.exception("Delivery cross-check failed")
    try:
        plot_dump_latency(dump_dir, label=cfg.muxer)
    except Exception:
        logger.exception("Nimlibp2p latency plot failed")
    try:
        _scrape_and_plot(experiment)
    except Exception:
        logger.exception("Nimlibp2p metrics scrape failed")


def _scrape_and_plot(experiment: "NimLibp2pExperiment") -> None:
    """Resource and mesh-health figures, off the run's own stable window."""
    exp = experiment.metadata
    if not exp.get("results", {}).get("stable"):
        logger.warning("Run has no stable window; skipping the metrics scrape")
        return

    label = experiment.config.muxer
    dump = scrape_run_metrics(exp, experiment.output_folder / "metrics", name=label)
    plot_run_metrics(
        {label: dump},
        experiment.output_folder / "plots",
        xlabel=f"{experiment.namespace}, {experiment.config.num_relay_nodes} nodes",
    )

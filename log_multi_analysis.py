import argparse
import asyncio
import json
import logging
import os
import sys
import traceback
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator, Optional, Self

from pydantic import BaseModel

from src.analysis.mesh_analysis.analyzers.connmanager_analyzer import ConnManagerAnalyzer
from src.analysis.mesh_analysis.analyzers.waku.waku_analyzer import WakuAnalyzer
from src.analysis.utils.file_utils import extract_exps, get_folders
from src.analysis.utils.log_utils import init_logger, log_to_path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "deployments")))

from src.analysis.mesh_analysis.analyzers.analyzer import Analyzer
from src.analysis.mesh_analysis.analyzers.data_puller import DataPuller

logger = logging.getLogger(__name__)


def get_experiments(*, experiment_class: Optional[str] = None) -> Iterator[dict]:
    start_dir = Path("../deployments/")
    subdirs = []

    # Relative paths to parent of experiments.
    start_subs = [
        # TODO: Put your paths here.
    ]

    for start_sub in start_subs:
        subdirs += [
            start_sub / path for path in get_folders(start_dir / start_sub, "metadata.json")
        ]
    for d in subdirs:
        logger.info(d)

    logger.info(f"found {len(subdirs)} paths")
    for path in subdirs:
        logger.info(f"path: {path}")

    def filter_by_class(exp) -> bool:
        if exp["experiment"]["class"] != experiment_class:
            return False
        return True

    filters = []
    if experiment_class:
        filters.append(filter_by_class)
    for exp in extract_exps(subdirs, filters):
        try:
            yield exp
        except Exception as e:
            full_trace = traceback.format_exc()
            logger.error(f"exception: {full_trace}")
            raise


class StackGen:
    _args: dict = {}

    def vaclab(self) -> Self:
        self._args.update(
            {
                "url": "https://vlselect.lab.vac.dev/select/logsql/query",
                "type": "vaclab",
                "reader": "victoria",
            }
        )
        return self

    def local(self, folder) -> Self:
        self._args.update({"local_folder": folder})
        return self

    def with_experiment(self, exp: dict) -> Self:
        self._args.update(exp["stack"])
        return self

    def build(self) -> dict:
        required_fields = [
            "type",
            "url",
            "start_time",
            "end_time",
            "stateful_sets",
            "nodes_per_statefulset",
            "container_name",
            "extra_fields",
        ]
        for key in required_fields:
            assert key in self._args, f"Missing key in stack. key: `{key}` stack: `{self._args}`"

        return self._args


def get_analyzer(metadata) -> Analyzer:
    experiment_name = metadata.get("experiment", {}).get("name", "")
    if experiment_name.startswith("connmanager"):
        return get_connmanager_analyzer(metadata)
    if experiment_name == "full-logos-delivery":
        return get_full_logos_delivery_analyzer(metadata)
    return get_analyzer_for_dev_testing(metadata)


async def process_experiment(exp: dict) -> dict:
    exp_name = exp["stack"]["name"]
    base_data_path = Path("local_data/simulations_data/")
    log_path = base_data_path / exp_name / "out.log"

    results_dict = {"exp": exp}
    with log_to_path(log_path):
        logger.info(f"log_path: {log_path}")
        logger.info(f"Processing experiment: {exp_name}\n")
        exp["stack"] = StackGen().vaclab().with_experiment(exp).build()
        new_analyzer = get_analyzer(exp)
        results_dict["results"] = new_analyzer.run()

    return results_dict


def get_analyzer_for_dev_testing(metadata) -> Analyzer:
    stack = metadata["stack"]
    params = metadata["params"]
    data_puller = DataPuller().with_kwargs(stack)
    stateful_sets = stack["stateful_sets"]
    nodes_per_statefulset = stack["nodes_per_statefulset"]
    out_folder = metadata["experiment"]["dump"]["output_folder"]

    # TODO: We search for all StatefulSets in our exp["stack"],
    # but the bootstrap nodes are relay=False, so they are filtered here.
    reliability = [
        ss
        for ss in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
        if "bootstrap" not in ss[0]
    ]

    return (
        WakuAnalyzer()
        .with_data_puller(data_puller)
        .with_ss_check(stateful_sets, nodes_per_statefulset)
        .with_reliability_check(
            stateful_sets=[ss[0] for ss in reliability],
            nodes_per_ss=[ss[1] for ss in reliability],
            expected_num_peers=params["num_relay_nodes"],
            expected_num_messages=params["num_messages"],
        )
        .with_dump_analysis_dir(f"{out_folder}/analysis/")
    )


RELAYING_SETS = ("fserver", "lpserver", "store")
"""Roles that join the mesh. Bootstrap and the edge clients never log a received message."""


def get_full_logos_delivery_analyzer(metadata) -> Analyzer:
    stack = metadata["stack"]
    params = metadata["params"]
    data_puller = DataPuller().with_kwargs(stack)
    stateful_sets = stack["stateful_sets"]
    nodes_per_statefulset = stack["nodes_per_statefulset"]
    out_folder = metadata["experiment"]["dump"]["output_folder"]

    relaying = [
        (name, num)
        for name, num in zip(stateful_sets, nodes_per_statefulset)
        if name.startswith(RELAYING_SETS)
    ]

    return (
        WakuAnalyzer()
        .with_data_puller(data_puller)
        .with_ss_check(stateful_sets, nodes_per_statefulset)
        .with_reliability_check(
            stateful_sets=[name for name, _ in relaying],
            nodes_per_ss=[num for _, num in relaying],
            expected_num_peers=sum(num for _, num in relaying),
            expected_num_messages=params["num_messages"],
        )
        .with_store_archive_check(folder=Path(out_folder) / "store_messages")
        .with_delivery_latency_check(
            lightpush_sets=[
                pair
                for pair in zip(stateful_sets, nodes_per_statefulset)
                if pair[0].startswith("lpserver")
            ],
            filter_sets=[
                pair
                for pair in zip(stateful_sets, nodes_per_statefulset)
                if pair[0].startswith("fclient")
            ],
        )
        .with_dump_analysis_dir(f"{out_folder}/analysis/")
    )


def get_connmanager_analyzer(metadata) -> Analyzer:
    stack = metadata["stack"]
    params = metadata.get("params", {})
    data_puller = DataPuller().with_kwargs(stack)

    wave_sets = ["wave1", "wave2"] if params.get("run", "").upper() == "B" else None

    return (
        ConnManagerAnalyzer(
            dump_analysis_dir=f"local_data/simulations_data/{stack['name']}/",
        )
        .with_data_puller(data_puller)
        .with_hub_analysis(
            hub_pod="hub-0",
            grace_period_s=params.get("grace_period_s", 0),
            protected_peer_ids=params.get("protected_peer_ids") or None,
            wave_sets=wave_sets,
        )
    )


def unravel(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return unravel(obj.model_dump())
    elif isinstance(obj, dict):
        return {key: unravel(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return type(obj)(unravel(item) for item in obj)
    else:
        return obj


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A tool to run analysis after experiments have been run."
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbosity",
        default=0,
        help="Set the log level: -v (warnings), -vv (info), -vvv (debug) -vvvv (most verbose)",
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    verbosity = args.verbosity or 2
    init_logger(logging.getLogger(), verbosity, None)

    all_statuses = defaultdict(int)
    summary = defaultdict(int)

    all_results = []
    experiments = [
        exp
        for exp_class in ("WakuExperiment", "FullLogosDeliveryExperiment")
        for exp in get_experiments(experiment_class=exp_class)
    ]
    for exp in experiments:
        try:
            results = await process_experiment(exp)
            all_results.append(results)
            passed = True
            for item in results["results"]:
                all_statuses[item.status] += 1
                if item.status != "passed":
                    passed = False

            if passed:
                summary["passed"] += 1
        except Exception as e:
            # Catch all exceptions so we can still print results table
            # even if an experiment had a problem.
            logger.error(f"exception: {e}")
            full_trace = traceback.format_exc()
            logger.error(f"exception: {full_trace}")

    logger.info(f"=== All Results ===\n{json.dumps(unravel(all_results), indent=2, default=str)}")
    not_passed = len(all_results) - summary["passed"]
    logger.info(f"Passed: {summary['passed']}\nNot Passed: {not_passed}\nTotal: {len(all_results)}")
    status_str = "\n".join([f"{key}: {value}" for key, value in all_statuses.items()])
    logger.info(f"=== Statuses === \n{status_str}")

    if not_passed:
        logger.error("At least one check failed!")


if __name__ == "__main__":
    asyncio.run(main())

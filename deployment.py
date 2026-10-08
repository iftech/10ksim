# Python Imports
import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from kubernetes import config
from kubernetes.client import ApiClient
from ruamel import yaml

# Project Imports
from src.analysis.utils.log_utils import init_logger
from src.deployments.core.k8s_kubeconfig import set_config_file
from src.deployments.registry import registry as experiment_registry
from src.deployments.utils.parser import ARG_NOT_SET, with_experiment_from_values

logger = logging.getLogger(__name__)


async def run_experiment(
    name: str,
    args: argparse.Namespace,
    values_path: Optional[str],
    kube_config: Path,
):
    logger.debug(f"params: {args}")
    config.load_kube_config(config_file=kube_config)
    set_config_file(kube_config)
    api_client = ApiClient()

    try:
        with open(values_path, "r") as values:
            values_yaml = yaml.safe_load(values.read())  # todo: change to get_YAML().load()...
    except TypeError:
        # values_path is None.
        values_yaml = None
    if values_yaml is None:
        values_yaml = {}

    cli_args = {key: value for key, value in vars(args).items() if value is not ARG_NOT_SET}
    info = experiment_registry[name]
    experiment = info.cls(
        api_client=api_client,
        config={**values_yaml, **cli_args},
        namespace=args.namespace,
        output_folder=args.out_folder,
        skip_check=args.skip_check,
        dry_run=args.dry_run,
    )
    logger.info(f"Running experiment. name `{info.name}` file: `{info.metadata['module_path']}`")
    await experiment.run()


def add_top_level_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--values",
        default=None,
        help="Path to values.yaml. Its `experiment` key picks the experiment when none is given.",
        dest="values_path",
    )
    parser.add_argument(
        "--config",
        required=False,
        help="Config passed to --kubeconfig in kubernetes commands.",
        default="~/.kube/config",
        dest="kube_config",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbosity",
        default=0,
        help="Set the log level: -v (warnings), -vv (info), -vvv (debug) -vvvv (most verbose)",
    )
    parser.add_argument(
        "-l",
        "--out-folder",
        type=Path,
        dest="out_folder",
        default=None,
        required=False,
        help="Output folder to contain all experiment output. "
        "Path is relative to `__file__/out/` unless given as absolute path. "
        "Default path uses `__file__/out/{{datetime}}`.",
    )


async def main():
    parser = argparse.ArgumentParser(
        description="A tool to run experiments. Generates deployment yaml with helm and deploys using the given kubeconfig. Dependencies: helm must be installed and in $PATH."
    )

    subparsers = parser.add_subparsers(dest="experiment", required=True)
    add_top_level_args(parser)

    # Scan for experiments.
    experiment_registry.scan(Path(__file__) / ".." / "src" / "deployments", mode="skip")

    # Add subparsers for all experiments.
    for info in experiment_registry.items():
        try:
            info.cls.add_parser(subparsers)
        except AttributeError as e:
            raise AttributeError(f"{info}") from e

    top_level = argparse.ArgumentParser(add_help=False)
    add_top_level_args(top_level)
    names = [info.name for info in experiment_registry.items()]
    args = parser.parse_args(with_experiment_from_values(sys.argv[1:], top_level, names))
    verbosity = args.verbosity or 2
    init_logger(logging.getLogger(), verbosity)

    try:
        await run_experiment(
            name=args.experiment,
            args=args,
            values_path=args.values_path,
            kube_config=args.kube_config,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())

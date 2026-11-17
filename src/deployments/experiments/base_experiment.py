# Python Imports
import asyncio
import json
import logging
import os
import random
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from collections import defaultdict
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Dict, Generic, List, Literal, Optional, TypeVar, Union

from kubernetes.client import (
    ApiClient,
    V1ConfigMap,
    V1CronJob,
    V1DaemonSet,
    V1Deployment,
    V1Job,
    V1NetworkPolicy,
    V1Pod,
    V1PodTemplateSpec,
    V1Role,
    V1RoleBinding,
    V1Service,
    V1ServiceAccount,
    V1StatefulSet,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator
from ruamel import yaml

# Project Imports
from src.analysis.post_run_analysis import run_post_analysis
from src.analysis.utils.log_utils import log_to_path
from src.deployments.core.base_bridge import BaseBridge
from src.deployments.core.code_version import code_version
from src.deployments.core.k8s_cleanup import (
    get_cleanup,
    poll_namespace_has_objects,
    wait_for_no_objs_in_namespace,
)
from src.deployments.core.k8s_deploy import kubectl_apply
from src.deployments.core.k8s_object import k8s_obj_to_dict
from src.deployments.core.k8s_rollout import wait_for_rollout
from src.deployments.registry import registry as experiment_registry
from src.deployments.utils.flatten import flatten
from src.deployments.utils.parser import _config_model_fields_to_args
from src.utils.cli_utils import flag_exists
from src.utils.yaml_utils import get_YAML

V1Deployable = Union[
    V1PodTemplateSpec,
    V1Pod,
    V1Deployment,
    V1Service,
    V1StatefulSet,
    V1DaemonSet,
    V1Job,
    V1CronJob,
    V1Role,
    V1RoleBinding,
    V1ConfigMap,
    V1ServiceAccount,
    V1NetworkPolicy,
]


logger = logging.getLogger(__name__)


class ExperimentFailed(Exception):
    """The run completed but its result is not usable."""


TCfg = TypeVar("TCfg", bound=BaseModel)


def dispatch_post_analysis(experiment: "BaseExperiment") -> Any:
    return run_post_analysis(experiment)


def kind_of(dep):
    return getattr(dep, "kind", None) or getattr(dep, "__class__", type(dep)).__name__


class BaseExperiment(ABC, BaseModel, Generic[TCfg]):
    """Base experiment that add an ExitStack with `workdir` to `run` and uses an internal `_run`.

    How to use:
        - Inherit from this class.
        - Implement `_run` in the child class.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    post_run_analysis: ClassVar[Optional[str]] = None

    _type: str = PrivateAttr()

    out_log_path: Path = Field(default=Path("out.log"))
    events_log_path: Path = Field(default=Path("events.log"))
    metadata_log_path: Path = Field(default=Path("metadata.json"))

    api_client: ApiClient = Field(exclude=True)
    dry_run: bool = False
    """If True, does not actually deploy kubernetes configs but runs kubectl apply --dry-run instead."""
    skip_check: bool = False
    """If present, does not wait until the namespace is empty before running the test."""
    output_folder: Optional[Path] = None
    """Base output folder for experiment."""
    namespace: Optional[str] = None

    config: TCfg

    metadata: Optional[dict] = None

    _deployed: dict[str, list] = defaultdict(list)
    """Dict of [namespace : yamls] for every yaml deployed with self.deploy.

    Used to determine whether or not to call `_wait_until_clear`."""

    _workdir: Optional[Path] = None
    """Path to deployment output folder. Based off of self.output_folder"""
    _stack: Optional[ExitStack]

    _failures: List[str] = PrivateAttr(default_factory=list)
    """Reasons the run is invalid, raised once it has finished. See `fail_run`."""

    @model_validator(mode="after")
    def set_type(self):
        self._type = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        return self

    def serialize(self) -> Dict[str, Any]:
        """Serialize this class to a dict, excluding metadata"""
        return {
            "_type": self._type,
            **self.model_dump(exclude={"metadata"}),
        }

    def __init_subclass__(cls, **kwargs):
        """Ensure add_parser is not overridden."""
        super().__init_subclass__(**kwargs)
        if cls.add_parser.__func__ is not BaseExperiment.add_parser.__func__:
            raise TypeError("Cannot override add_parser method")

    @classmethod
    def add_parser(cls, subparsers) -> None:
        subparser = subparsers.add_parser(cls.name, help=cls.__doc__)
        cls.add_args(subparser)
        cls.add_base_args(subparser)
        cls.add_config_args(subparser)

    @classmethod
    def add_args(cls, subparser) -> None:
        pass

    @staticmethod
    def add_base_args(subparser: ArgumentParser):
        subparser.add_argument(
            "--skip-check",
            action="store_true",
            required=False,
            help="If present, does not wait until the namespace is empty before running the test.",
        )
        subparser.add_argument(
            "--dry-run",
            action="store_true",
            required=False,
            default=False,
            help="If True, does not actually deploy kubernetes configs but run kubectl apply --dry-run.",
        )
        subparser.add_argument(
            "--namespace",
            type=str,
            required=True,
            metavar="(str)",
            help="The namespace for deployments.",
        )

    @classmethod
    def add_config_args(cls, subparser: ArgumentParser) -> None:
        config_model = cls.model_fields["config"].annotation
        for flag, kwargs in _config_model_fields_to_args(config_model):
            if flag_exists(subparser, flag):
                # Do not generate config flag if it already exists.
                continue
            subparser.add_argument(flag, **kwargs)

    async def deploy(
        self,
        deployment: V1Deployable | list | dict | set | yaml.YAMLObject,
        *,
        wait_for_ready: bool = True,
        exist_ok: bool = False,
        timeout=3600,
        strategy: Literal["parallel", "serial"] = "parallel",
    ):
        async def deploy_one(dep):
            await self._deploy(
                deployment=dep, wait_for_ready=wait_for_ready, exist_ok=exist_ok, timeout=timeout
            )

        async def deploy_batch(items):
            if strategy == "parallel":
                await asyncio.gather(*(deploy_one(dep) for dep in items))
            else:
                for item in items:
                    await deploy_one(item)

        items = list(flatten(deployment))
        base_deployments = {"ServiceAccount", "Role", "ConfigMap", "Service", "RoleBinding"}
        phase_1 = [dep for dep in items if kind_of(dep) in base_deployments]
        phase_2 = [dep for dep in items if kind_of(dep) not in base_deployments]

        await deploy_batch(phase_1)
        await deploy_batch(phase_2)

    async def _deploy(
        self,
        deployment: Optional[yaml.YAMLObject | V1Deployable] = None,
        *,
        wait_for_ready: bool = True,
        exist_ok: bool = False,
        timeout=3600,
    ):
        if isinstance(deployment, V1Deployable):
            deployment = self.api_client.sanitize_for_serialization(deployment)

        await self.deploy_yaml(
            deployment_yaml=deployment,
            wait_for_ready=wait_for_ready,
            exist_ok=exist_ok,
            timeout=timeout,
        )

    async def deploy_yaml(
        self,
        deployment_yaml: Optional[yaml.YAMLObject],
        *,
        wait_for_ready: bool = True,
        exist_ok: bool = False,
        timeout=3600,
    ):
        self.dump_yaml(deployment_yaml)

        namespace = deployment_yaml["metadata"]["namespace"]
        if len(self._deployed[namespace]) == 0:
            self._wait_until_clear(
                namespace=namespace,
                skip_check=self.skip_check,
            )

        if not self.dry_run:
            cleanup = get_cleanup(
                api_client=self.api_client,
                namespace=namespace,
                deployments=[deployment_yaml],
            )
            self._stack.callback(cleanup)

        deployment_metadata = {
            "event": "deployment",
            "namespace": namespace,
            "kind": deployment_yaml["kind"],
            "name": deployment_yaml["metadata"]["name"],
        }
        if deployment_yaml["kind"] == "StatefulSet":
            deployment_metadata["replicas"] = deployment_yaml["spec"]["replicas"]

        self.log_event({"phase": "start", **deployment_metadata})
        self._deployed[namespace].append(deployment_yaml)
        kubectl_apply(deployment_yaml, namespace=namespace, dry_run=self.dry_run, exist_ok=exist_ok)

        if not self.dry_run:
            if wait_for_ready:
                await wait_for_rollout(deployment_yaml, self.api_client, timeout=timeout)
        self.log_event({"phase": "finished", **deployment_metadata})

        return deployment_yaml

    def _get_out_path(self, path: Path, out_dir: Optional[str]) -> Path:
        if path.is_absolute():
            return path
        if not path.is_absolute():
            if out_dir is None:
                raise ValueError(
                    f"Out paths are required to be absolute paths or have a non-None out path. Path: `{path}` out_dir: `{out_dir}` experiment type: `{type(self)}`"
                )
            return Path(out_dir) / path

    def dump_yaml(self, obj: V1Deployable | dict, name: Optional[str] = None):
        if not isinstance(obj, dict):
            obj = k8s_obj_to_dict(obj)
        name = name or obj["metadata"]["name"]
        out_path = Path(self._workdir) / f"{name}.yaml"
        logger.info(f"Dumping deployment. name: `{name}` path: `{out_path}`")
        if out_path.exists():
            logger.warning(f"File already exists. Overwriting {out_path}")
        os.makedirs(out_path.parent, exist_ok=True)
        with open(out_path, "w") as out_file:
            yaml = get_YAML()
            yaml.dump(k8s_obj_to_dict(obj), out_file)

    def _get_metadata(self) -> dict:
        return BaseBridge().get_metadata(self.events_log_path)

    def log_metadata(self, metadata: dict):
        self.log_event({**{"event": "metadata"}, **metadata})

    def _dump_metadata(self):
        self.metadata = self._get_metadata()
        self.log_metadata(self.metadata)
        full_metadata = defaultdict(dict, deepcopy(self.metadata))
        full_metadata["experiment"]["dump"] = self.serialize()
        out_path = Path(self.metadata_log_path)
        with open(out_path, "a") as out_file:
            out_file.write(json.dumps(full_metadata, default=str))

    def _dump_initial_metadata(self):
        self.log_metadata(
            {
                "experiment_name": self.__class__.name,
                "experiment_class": self.__class__.__name__,
                "dump": self.serialize(),
            }
        )

    def _setup_log_paths(self):
        base_out_dir = Path(__file__).parent / "out"
        if self.output_folder is None:
            # Adding a random number helps distinguish experiments.
            random_number = random.randint(1000, 9999)
            datetime_str = datetime.now().strftime("%Y.%m.%d_%H.%M.%f")[:-3]
            self.output_folder = base_out_dir / f"{datetime_str}_{random_number}"
        elif not self.output_folder.is_absolute():
            self.output_folder = base_out_dir / self.output_folder

        self._workdir = self.output_folder / "deployment_yamls"
        self.out_log_path = self._get_out_path(self.out_log_path, self.output_folder)
        self.events_log_path = self._get_out_path(self.events_log_path, self.output_folder)
        self.metadata_log_path = self._get_out_path(self.metadata_log_path, self.output_folder)

        for path in [self.output_folder, self._workdir]:
            path.mkdir(parents=True, exist_ok=True)
        for path in [self.out_log_path, self.events_log_path, self.metadata_log_path]:
            path.parent.mkdir(parents=True, exist_ok=True)

    async def run(self, *, run_post_analysis: bool = True):
        self._deployed.clear()
        self._setup_log_paths()
        self._dump_initial_metadata()
        self.log_metadata({"code": code_version()})

        with log_to_path(self.out_log_path):
            with ExitStack() as self._stack:
                self._stack.callback(lambda: self.log_event("cleanup_finished"))
                self.log_metadata({"params": vars(self.config)})
                await self._run()
                self._stack.callback(lambda: self.log_event("cleanup_start"))
            self._stack = None

        self.log_event("run_finished")
        self._dump_metadata()
        if run_post_analysis:
            dispatch_post_analysis(self)

        if self._failures:
            raise ExperimentFailed(f"`{self._type}`: " + "; ".join(self._failures))

    def fail_run(self, reason: str) -> None:
        """Mark the run invalid without cutting it short.

        Raising from `_run` would skip metadata and post-run analysis while cleanup still
        deletes the pods, so the data is lost as well as the result. Recording the reason
        lets the run finish and collect its data, then exit non-zero.
        """
        logger.error(reason)
        self.log_event({"event": "run_invalid", "reason": reason})
        self._failures.append(reason)

    @abstractmethod
    async def _run(self):
        pass

    def _wait_until_clear(self, namespace: str, skip_check: bool):
        # Wait for namespace to be clear unless --skip-check flag was used.
        if not skip_check:
            self.log_event("wait_for_clear_start")
            wait_for_no_objs_in_namespace(namespace=namespace, api_client=self.api_client)
        else:
            namepace_is_empty = poll_namespace_has_objects(
                namespace=namespace, api_client=self.api_client
            )
            if not namepace_is_empty:
                logger.warning(f"Namespace is not empty! Namespace: `{namespace}`")
        self.log_event("wait_for_clear_finished")

    def _preprocess_event(self, event: Any) -> Any:
        if isinstance(event, str):
            event = {"event": event}

        if isinstance(event, dict):
            event["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            return json.dumps(event, default=str)
        else:
            return event

    def log_event(self, event: Any):
        logger.info(event)
        out_path = Path(self.events_log_path)
        with open(out_path, "a") as out_file:
            out_file.write(self._preprocess_event(event))
            out_file.write("\n")


def experiment_from_metadata(api_client: ApiClient, metadata: dict) -> BaseExperiment:
    """
    Deserialize an experiment instance from its metadata by looking up the correct
    experiment class via the registry and validating the data.

    :param metadata: Full metadata, including metadata["experiment"]["dump"] containing
    the serialized experiment.

    :rtype: Derived class based on the metadata _type.
    :returns: An instance of the appropriate BaseExperiment subclass.
    """

    dump = metadata["experiment"]["dump"]
    experiment_infos = experiment_registry.get_by_metadata({"type": dump["_type"]})
    if len(experiment_infos) != 1:
        raise ValueError(f"Ambiguous experiment type. {experiment_infos}")
    exp_cls = experiment_infos[0].cls
    exp = exp_cls.model_validate({"api_client": api_client, **dump})
    exp.metadata = metadata
    return exp


def read_experiment(api_client: ApiClient, arg: Path | str) -> BaseExperiment:
    path = Path(arg)
    with open(path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return experiment_from_metadata(api_client, metadata)

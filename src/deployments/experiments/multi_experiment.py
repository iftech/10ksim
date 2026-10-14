import asyncio
import logging
import random
import traceback
from abc import abstractmethod
from copy import deepcopy
from datetime import datetime
from datetime import timezone as dt_timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from src.analysis.post_run_analysis import run_post_analysis
from src.deployments.experiments.base_experiment import BaseExperiment, ExperimentFailed
from src.deployments.registry import experiment
from src.deployments.registry import registry as experiment_registry
from src.deployments.utils.parser import ARG_NOT_SET
from src.utils.dict_utils import dict_set

logger = logging.getLogger(__name__)


def dict_to_namespace(dictionary: dict):
    if isinstance(dictionary, dict):
        return SimpleNamespace(
            **{key: dict_to_namespace(value) for key, value in dictionary.items()}
        )
    return dictionary


class Config(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: Optional[str] = None
    """Name of experiment to run."""
    delay: float = 120
    """Delay (in seconds) between each experiment run."""

    def __init__(self, **data: Any):
        super().__init__(**data)
        object.__setattr__(self, "_raw_input", data)

    def get_raw_input(self) -> Dict[str, Any]:
        return self._raw_input

    def get_extra_fields(self) -> Dict[str, Any]:
        return self.model_extra or {}


@experiment(name="multi")
class Multiple(BaseExperiment[Config]):
    """Run an experiment multiple times with different parameters."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def add_args(cls, subparser) -> None:
        subparser.add_argument(
            "--name", type=str, required=False, help="Name of the experiment to run."
        )
        subparser.add_argument(
            "--delay",
            type=float,
            required=False,
            default=ARG_NOT_SET,
            help="Delay (in seconds) between each experiment run.",
        )

    def log_event(self, event):
        logger.info(event)
        return super().log_event(event)

    async def _run(self):
        logger.info("Multiple experiments")
        param_list = self.get_params_list()
        assert (
            self.config.delay
        ), "Delay between experiments must be specified either in the subclass or the cli args (--delay)"
        completed_experiments = []
        invalid: list[str] = []
        for params in param_list:
            this_time = datetime.now(dt_timezone.utc)
            logger.info(f"UTC time: {this_time.hour:02d}:{this_time.minute:02d}")

            params_str = self.get_name_from_params(params)

            # Adding a random number helps distinguish experiments.
            random_number = random.randint(1, 200000)
            exp_outpath = Path(self.output_folder) / f"{params_str}__rand_{random_number}"

            # Build experiment params using original input.
            exp_values_yaml = deepcopy(self.config.get_raw_input())
            for key, value in params.items():
                dict_set(exp_values_yaml, key, value, sep=".", replace_leaf=True)

            exp_name = self.config.name
            if not exp_name:
                raise ValueError("Missing name of experiment to run.")

            info = experiment_registry[exp_name]
            experiment = info.cls(
                api_client=self.api_client,
                config=exp_values_yaml,
                namespace=self.namespace,
                output_folder=exp_outpath,
                skip_check=self.skip_check,
                dry_run=self.dry_run,
            )
            logger.info(
                f"Running experiment. name `{info.name}` file: `{info.metadata['module_path']}`"
            )
            try:
                await experiment.run(run_post_analysis=False)
            except ExperimentFailed as e:
                # The run itself finished, so its data is worth analysing; only the result
                # is not usable. Dropping it here would lose the analysis of the run that
                # most needs looking at.
                logger.error(f"Experiment result is not usable. {e}")
                invalid.append(str(e))
                completed_experiments.append(experiment)
            except Exception as e:
                logger.error(f"Experiment failed. Exception: {e} {traceback.format_exc()}")
                invalid.append(f"{params_str}: {e!r}")
            else:
                completed_experiments.append(experiment)

            logger.info(f"sleeping {self.config.delay} between experiments")
            await asyncio.sleep(self.config.delay)

        for experiment in completed_experiments:
            run_post_analysis(experiment)

        if invalid:
            self.fail_run(f"{len(invalid)} of the sweep's runs are not usable: {invalid}")

    @abstractmethod
    def get_params_list(self) -> List[dict]:
        """Return list of param sets.
        Each param set is a dict with `key` : `value`,
        where `key` is mapped to the values.yaml using the `get_params_paths()`."""
        raise NotImplementedError("Implement in derived class.")

    def get_name_from_params(self, params: dict) -> str:
        param_list = [f"{key}_{value}" for key, value in params.items()]
        return "__".join(param_list)

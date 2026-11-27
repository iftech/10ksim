from __future__ import annotations

from pathlib import Path
from typing import Any, List, Tuple
from unittest.mock import Mock

import pytest
import yaml

from src.deployments.core.k8s_object import k8s_obj_to_dict
from src.deployments.logos_core.experiment import ExpConfig, LogosDeliveryExperiment
from src.deployments.utils.flatten import flatten

TEST_DATA_DIR = Path(__file__).parent / "data"


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_expected_case(case_dir: Path) -> List[Tuple[str, Any]]:
    expected_dir = case_dir / "expected"
    result: dict[str, Any] = {}
    for path in sorted(expected_dir.glob("*.yaml")):
        dict_obj = load_yaml(path)
        result[path.name] = dict_obj
    return sorted(result.items())


def collect_actual_resources(experiment: LogosDeliveryExperiment) -> List[Tuple[str, Any]]:
    deployments = flatten(
        [
            experiment.build_publisher(),
            experiment.build_node_deployments("bootstrap"),
            experiment.build_node_deployments("relay"),
        ]
    )
    result: dict[str, Any] = {}
    for dep in deployments:
        name = dep.metadata.name
        result[f"{name}.yaml"] = k8s_obj_to_dict(dep)
    return sorted(result.items())


@pytest.mark.parametrize("case_name", ["default"])
def test_experiment_cases(case_name: str):
    case_dir = TEST_DATA_DIR / "cases" / case_name
    input_data = load_yaml(case_dir / "input.yaml")
    exp_config = ExpConfig(**input_data)

    experiment = LogosDeliveryExperiment.model_construct(
        config=exp_config,
        api_client=Mock(),
        namespace="ns",
    )

    actual = collect_actual_resources(experiment)
    expected = load_expected_case(case_dir)

    assert expected == actual

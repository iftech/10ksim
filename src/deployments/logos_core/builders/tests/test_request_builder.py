from __future__ import annotations

import random
from pathlib import Path
from typing import Any, Callable, List, Tuple

import pytest
import yaml

from src.deployments.core.configs.container import Image
from src.deployments.core.k8s_object import k8s_obj_to_dict
from src.deployments.logos_core.builders.request_builder import LogoscorePodApiRequester
from src.deployments.utils.flatten import flatten

TEST_DATA_DIR = Path(__file__).parent / "requester_data"


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_expected_case(case_dir: Path) -> List[Tuple[str, Any]]:
    result: dict[str, Any] = {}
    for path in sorted(case_dir.glob("*.yaml")):
        dict_obj = load_yaml(path)
        result[path.name] = dict_obj
    return sorted(result.items())


def get_deployments(builder: LogoscorePodApiRequester) -> List[Tuple[str, Any]]:
    """Executes funcs, builds, and returns sorted list of (filename, dict)."""
    deployments = flatten({"pod": builder.build(), **builder.build_dependencies()})
    result: dict[str, Any] = {}
    for dep in deployments:
        name = dep.metadata.name
        result[f"{name}.yaml"] = k8s_obj_to_dict(dep)
    return sorted(result.items())


def check_deployments_outputs(case_name: str, builder: LogoscorePodApiRequester):
    """
    Core logic: Reads expected YAML for case/scenario, executes func_list,
    and asserts they match.
    """
    expected_yaml_data = load_expected_case(TEST_DATA_DIR / case_name)
    actual_yaml_data = get_deployments(builder)
    assert expected_yaml_data == actual_yaml_data


def get_scenarios(funcs: List[Callable]) -> List[pytest.ParamSpec]:
    scenarios = []
    scenarios.append(pytest.param(funcs, id="base_order"))
    scenarios.append(pytest.param(list(reversed(funcs)), id="reversed_order"))
    rng = random.Random()
    for seed in [7, 41, 227]:
        shuffled_case = funcs.copy()
        rng.seed(seed)
        rng.shuffle(shuffled_case)
        scenarios.append(pytest.param(shuffled_case, id=f"shuffled_seed_{seed}"))
    return scenarios


@pytest.mark.parametrize(
    "func_list",
    get_scenarios(
        [
            lambda builder: builder.with_namespace("ns"),
            lambda builder: builder.with_name("logoscore-requester"),
            lambda builder: builder.with_image(Image(repo="repo", tag="tag")),
            lambda builder: builder.with_mode("server"),
            lambda builder: builder.with_service_account_name("service-account-name"),
            lambda builder: builder.with_service_name("service-name"),
            lambda builder: builder.with_app("app"),
            lambda builder: builder.with_logoscore(),
        ]
    ),
)
def test_order_independences(func_list):
    builder = LogoscorePodApiRequester()
    for fun in func_list:
        fun(builder)

    check_deployments_outputs("base_case", builder)


class TestDnsConfig:
    """Test that custom dns_config searches are retained,
    while the feature manages its own dns_config.

    The order should be most recently called last.
    """

    search_1 = "bootstrap-service.ns.svc.cluster.local"
    search_2 = "relay_service.ns.svc.cluster.local"
    feature_search = "service-name.ns.svc.cluster.local"

    def test_feature_first(self, base_builder):
        pod = (
            base_builder.with_logoscore()
            .with_dns_search(self.search_1)
            .with_dns_search(self.search_2)
        ).build()
        assert pod.spec.dns_config.searches == [self.feature_search, self.search_1, self.search_2]

    def test_feature_last(self, base_builder):
        pod = (
            base_builder.with_dns_search(self.search_1)
            .with_dns_search(self.search_2)
            .with_logoscore()
        ).build()
        assert pod.spec.dns_config.searches == [self.search_1, self.search_2, self.feature_search]

    def test_feature_both(self, base_builder):
        pod = (
            base_builder.with_logoscore()
            .with_dns_search(self.search_1)
            .with_dns_search(self.search_2, overwrite=True)
            .with_logoscore()
        ).build()
        assert pod.spec.dns_config.searches == [self.search_1, self.search_2, self.feature_search]

    def test_feature_multiple(self, base_builder):
        pod = (
            base_builder.with_dns_search(self.search_1, overwrite=True)
            .with_dns_search(self.search_1, overwrite=True)
            .with_logoscore()
            .with_dns_search(self.search_2, overwrite=True)
            .with_logoscore()
            .with_dns_search(self.search_1, overwrite=True)
        ).build()
        assert pod.spec.dns_config.searches == [self.search_2, self.feature_search, self.search_1]


class TestErrors:
    """Test required fields are set before building the deployment."""

    def test_no_namespace(self):
        with pytest.raises(ValueError):
            _pod = (
                LogoscorePodApiRequester()
                .with_name("logoscore-requester")
                .with_logoscore()
                .with_mode("server")
                .build()
            )


@pytest.fixture()
def base_builder() -> LogoscorePodApiRequester:
    return (
        LogoscorePodApiRequester()
        .with_namespace("ns")
        .with_name("logoscore-requester")
        .with_image(Image(repo="repo", tag="tag"))
        .with_mode("server")
        .with_service_name("service-name")
        .with_app("app")
    )


class TestServiceAccountName:
    def test_default(self, base_builder):
        """Test that we have a default service account name."""
        pod = base_builder.with_app("app").with_logoscore().build()
        assert pod.spec.service_account_name == "secret-creator2"

    def test_after_feature(self, base_builder):
        """Test that we can override default service account name."""
        pod = (
            base_builder.with_app("app")
            .with_logoscore()
            .with_service_account_name("custom-service-account")
            .build()
        )
        assert pod.spec.service_account_name == "custom-service-account"

    def test_before_feature(self, base_builder):
        """Test that we can override default service account name."""
        pod = (
            base_builder.with_app("app")
            .with_service_account_name("custom-service-account")
            .with_logoscore()
            .build()
        )
        assert pod.spec.service_account_name == "custom-service-account"

    def test_multiple_calls(self, base_builder):
        """Test that we can override default service account name."""
        pod = (
            base_builder.with_app("app")
            .with_logoscore()
            .with_service_account_name("custom-service-account")
            .with_service_account_name("custom-service-account2")
            .build()
        )
        assert pod.spec.service_account_name == "custom-service-account2"

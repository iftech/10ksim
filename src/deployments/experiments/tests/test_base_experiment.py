import json
import sys
from types import ModuleType
from typing import ClassVar
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes.client import ApiClient
from pydantic import BaseModel

from src.deployments.experiments.base_experiment import BaseExperiment


class DummyCfg(BaseModel):
    foo: str = "bar"


class DummyExperiment(BaseExperiment[DummyCfg]):
    config: DummyCfg

    async def _run(self):
        pass


class DeploymentObj:
    def __init__(self, kind, name):
        self.kind = kind
        self.metadata = {"name": name, "namespace": "ns"}


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["serial", "parallel"])
async def test_deploy_orders_base_resources_before_workloads(strategy):
    exp = DummyExperiment.model_construct(
        api_client=Mock(),
        config=DummyCfg(),
        namespace="ns",
    )

    exp._deploy = AsyncMock(return_value=None)

    items = [
        DeploymentObj("Pod", "pod1"),
        DeploymentObj("RoleBinding", "rb1"),
        DeploymentObj("StatefulSet", "sts1"),
        DeploymentObj("ServiceAccount", "sa1"),
        DeploymentObj("Service", "s"),
        DeploymentObj("DaemonSet", "ds"),
        DeploymentObj("Job", "j"),
        DeploymentObj("CronJob", "cj"),
        DeploymentObj("Role", "role1"),
        DeploymentObj("ConfigMap", "cm1"),
    ]

    await exp.deploy(items, strategy=strategy, timeout=1)

    kinds = [c.kwargs["deployment"].kind for c in exp._deploy.await_args_list]

    foundational = {"ServiceAccount", "Role", "ConfigMap", "Service", "RoleBinding"}
    workloads = {
        "Pod",
        "StatefulSet",
        "DaemonSet",
        "Deployment",
        "Job",
        "CronJob",
        "ReplicaSet",
        "ReplicationController",
    }

    foundation_positions = [i for i, k in enumerate(kinds) if k in foundational]
    workload_positions = [i for i, k in enumerate(kinds) if k in workloads]

    assert foundation_positions
    assert workload_positions
    assert max(foundation_positions) < min(workload_positions)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "deployments, expected",
    [
        pytest.param(
            [DeploymentObj("ServiceAccount", "sa1"), DeploymentObj("Role", "role1")],
            ["ServiceAccount", "Role"],
            id="list",
        ),
        pytest.param(
            {"sa": DeploymentObj("ServiceAccount", "sa1"), "role": DeploymentObj("Role", "role1")},
            ["ServiceAccount", "Role"],
            id="dict",
        ),
        pytest.param(DeploymentObj("Deployment", "dep1"), ["Deployment"], id="single"),
        pytest.param(
            {
                "outer": [
                    DeploymentObj("ConfigMap", "cm1"),
                    {"inner": DeploymentObj("Service", "svc1")},
                ]
            },
            ["ConfigMap", "Service"],
            id="nested",
        ),
        pytest.param(
            {
                "a": [
                    DeploymentObj("ServiceAccount", "sa1"),
                    {"b": DeploymentObj("RoleBinding", "rb1")},
                ],
                "c": DeploymentObj("Pod", "pod1"),
            },
            ["ServiceAccount", "RoleBinding", "Pod"],
            id="nested_dict_list",
        ),
    ],
)
async def test_deploy_flattens_input_shapes(deployments, expected):
    exp = DummyExperiment.model_construct(
        api_client=Mock(),
        config=DummyCfg(),
        namespace="ns",
    )
    exp._deploy = AsyncMock(return_value=None)

    await exp.deploy(deployments, strategy="serial", timeout=1)

    kinds = [item.kwargs["deployment"].kind for item in exp._deploy.await_args_list]
    assert kinds == expected


@pytest.mark.asyncio
async def test_run_writes_final_metadata_before_configured_post_analysis(tmp_path, monkeypatch):
    observed = {}
    module_name = "tests_base_experiment_post_run_analysis"
    module = ModuleType(module_name)

    def analysis(experiment):
        observed["experiment"] = experiment
        observed["metadata"] = experiment.metadata
        observed["metadata_file_exists"] = experiment.metadata_log_path.exists()
        observed["metadata_file"] = json.loads(experiment.metadata_log_path.read_text())

    module.analysis = analysis
    monkeypatch.setitem(sys.modules, module_name, module)

    class AnalysisExperiment(BaseExperiment[DummyCfg]):
        name: ClassVar[str] = "analysis-test"
        config: DummyCfg
        post_run_analysis: ClassVar[str] = f"{module_name}:analysis"

        async def _run(self):
            self.log_event("run_start")

        def _get_metadata(self) -> dict:
            return {
                "stack": {"name": "analysis-dummy"},
                "experiment": {"name": "analysis-dummy"},
                "metadata": {"namespace": self.namespace},
            }

    exp = AnalysisExperiment(
        api_client=ApiClient(),
        config=DummyCfg(),
        namespace="ns",
        output_folder=tmp_path / "run",
    )

    await exp.run()

    assert observed["experiment"] is exp
    assert observed["metadata"] == exp.metadata
    assert observed["metadata_file_exists"] is True
    assert observed["metadata_file"]["stack"]["name"] == "analysis-dummy"
    assert observed["metadata_file"]["experiment"]["dump"]["_type"] == exp._type


@pytest.mark.asyncio
async def test_run_preserves_completed_experiment_when_post_analysis_fails(
    tmp_path, monkeypatch, caplog
):
    module_name = "tests_base_experiment_failing_post_run_analysis"
    module = ModuleType(module_name)

    def analysis(_experiment):
        raise RuntimeError("analysis failed")

    module.analysis = analysis
    monkeypatch.setitem(sys.modules, module_name, module)

    class FailingPostAnalysisExperiment(BaseExperiment[DummyCfg]):
        name: ClassVar[str] = "failing-analysis-test"
        config: DummyCfg
        post_run_analysis: ClassVar[str] = f"{module_name}:analysis"

        async def _run(self):
            self.log_event("run_start")

        def _get_metadata(self) -> dict:
            return {
                "stack": {"name": "analysis-dummy"},
                "experiment": {"name": "analysis-dummy"},
            }

    exp = FailingPostAnalysisExperiment(
        api_client=ApiClient(),
        config=DummyCfg(),
        namespace="ns",
        output_folder=tmp_path / "run",
    )

    with caplog.at_level("ERROR", logger="src.analysis.post_run_analysis"):
        await exp.run()

    assert exp.metadata_log_path.exists()
    assert json.loads(exp.metadata_log_path.read_text())["stack"]["name"] == "analysis-dummy"
    assert "Post-run analysis failed" in caplog.text
    assert "analysis failed" in caplog.text


@pytest.mark.asyncio
async def test_run_can_skip_configured_post_analysis(tmp_path, monkeypatch):
    calls = []
    module_name = "tests_base_experiment_skipped_post_run_analysis"
    module = ModuleType(module_name)

    def analysis(experiment):
        calls.append(experiment)

    module.analysis = analysis
    monkeypatch.setitem(sys.modules, module_name, module)

    class SkippedPostAnalysisExperiment(BaseExperiment[DummyCfg]):
        name: ClassVar[str] = "skipped-analysis-test"
        config: DummyCfg
        post_run_analysis: ClassVar[str] = f"{module_name}:analysis"

        async def _run(self):
            self.log_event("run_start")

        def _get_metadata(self) -> dict:
            return {
                "stack": {"name": "analysis-dummy"},
                "experiment": {"name": "analysis-dummy"},
            }

    exp = SkippedPostAnalysisExperiment(
        api_client=ApiClient(),
        config=DummyCfg(),
        namespace="ns",
        output_folder=tmp_path / "run",
    )

    await exp.run(run_post_analysis=False)

    assert exp.metadata_log_path.exists()
    assert exp.metadata is not None
    assert calls == []

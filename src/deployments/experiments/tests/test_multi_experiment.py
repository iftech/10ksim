from types import SimpleNamespace
from typing import ClassVar, List

import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments.base_experiment import ExperimentFailed
from src.deployments.experiments.multi_experiment import Config, Multiple


class FakeRegistry:
    def __init__(self, experiment_cls):
        self.info = SimpleNamespace(
            name="child",
            cls=experiment_cls,
            metadata={"module_path": "tests/child.py"},
        )

    def __getitem__(self, name):
        assert name == "child"
        return self.info


@pytest.mark.asyncio
async def test_multiple_runs_child_analysis_after_all_experiments(monkeypatch, tmp_path):
    events = []

    class ChildExperiment:
        def __init__(self, **kwargs):
            self.config = kwargs["config"]
            self.output_folder = kwargs["output_folder"]

        async def run(self, *, run_post_analysis: bool = True):
            events.append(("run", self.config["case"], run_post_analysis))

    class MultiTestExperiment(Multiple):
        name: ClassVar[str] = "multi-test"
        config: Config

        def get_params_list(self) -> List[dict]:
            return [{"case": "a"}, {"case": "b"}]

    async def sleep(_delay):
        events.append(("sleep",))

    def run_post_analysis(experiment):
        events.append(("analysis", experiment.config["case"]))

    monkeypatch.setattr(
        "src.deployments.experiments.multi_experiment.experiment_registry",
        FakeRegistry(ChildExperiment),
    )
    monkeypatch.setattr("src.deployments.experiments.multi_experiment.asyncio.sleep", sleep)
    monkeypatch.setattr(
        "src.deployments.experiments.multi_experiment.run_post_analysis",
        run_post_analysis,
    )

    experiment = MultiTestExperiment(
        api_client=ApiClient(),
        config=Config(name="child", delay=1),
        namespace="ns",
        output_folder=tmp_path,
    )

    await experiment._run()

    assert events == [
        ("run", "a", False),
        ("sleep",),
        ("run", "b", False),
        ("sleep",),
        ("analysis", "a"),
        ("analysis", "b"),
    ]


@pytest.mark.asyncio
async def test_a_crashed_child_is_not_analysed_and_fails_the_sweep(monkeypatch, tmp_path):
    events = []

    class ChildExperiment:
        def __init__(self, **kwargs):
            self.config = kwargs["config"]

        async def run(self, *, run_post_analysis: bool = True):
            events.append(("run", self.config["case"], run_post_analysis))
            if self.config["case"] == "failed":
                raise RuntimeError("child failed")

    class MultiTestExperiment(Multiple):
        name: ClassVar[str] = "multi-test"
        config: Config

        def get_params_list(self) -> List[dict]:
            return [{"case": "failed"}, {"case": "passed"}]

    async def sleep(_delay):
        events.append(("sleep",))

    def run_post_analysis(experiment):
        events.append(("analysis", experiment.config["case"]))

    monkeypatch.setattr(
        "src.deployments.experiments.multi_experiment.experiment_registry",
        FakeRegistry(ChildExperiment),
    )
    monkeypatch.setattr("src.deployments.experiments.multi_experiment.asyncio.sleep", sleep)
    monkeypatch.setattr(
        "src.deployments.experiments.multi_experiment.run_post_analysis",
        run_post_analysis,
    )

    experiment = MultiTestExperiment(
        api_client=ApiClient(),
        config=Config(name="child", delay=1),
        namespace="ns",
        output_folder=tmp_path,
    )

    with pytest.raises(ExperimentFailed, match="child failed"):
        await experiment.run(run_post_analysis=False)

    assert events == [
        ("run", "failed", False),
        ("sleep",),
        ("run", "passed", False),
        ("sleep",),
        ("analysis", "passed"),
    ]

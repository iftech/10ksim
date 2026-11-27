import json
from typing import ClassVar

import pytest
from kubernetes.client import ApiClient
from pydantic import BaseModel

from src.deployments.experiments.base_experiment import BaseExperiment, ExperimentFailed


class DummyCfg(BaseModel):
    foo: str = "bar"


def _experiment(tmp_path, body):
    class Exp(BaseExperiment[DummyCfg]):
        name: ClassVar[str] = "fail-run-test"
        config: DummyCfg

        async def _run(self):
            body(self)

        def _get_metadata(self) -> dict:
            return {"stack": {"name": "dummy"}, "experiment": {"name": "dummy"}}

    return Exp(
        api_client=ApiClient(),
        config=DummyCfg(),
        namespace="ns",
        output_folder=tmp_path / "run",
    )


@pytest.mark.asyncio
async def test_a_clean_run_does_not_raise(tmp_path):
    await _experiment(tmp_path, lambda exp: None).run()


@pytest.mark.asyncio
async def test_a_failed_run_raises_once_it_has_finished(tmp_path):
    exp = _experiment(tmp_path, lambda e: e.fail_run("3 of 600 messages never published"))
    with pytest.raises(ExperimentFailed, match="never published"):
        await exp.run()


@pytest.mark.asyncio
async def test_the_data_survives_a_failed_run(tmp_path):
    """Raising from _run would skip metadata while cleanup still deletes the pods."""
    exp = _experiment(tmp_path, lambda e: e.fail_run("bad run"))
    with pytest.raises(ExperimentFailed):
        await exp.run()

    assert exp.metadata_log_path.exists()
    assert json.loads(exp.metadata_log_path.read_text())["stack"]["name"] == "dummy"


@pytest.mark.asyncio
async def test_the_reason_is_written_to_the_event_log(tmp_path):
    exp = _experiment(tmp_path, lambda e: e.fail_run("bad run"))
    with pytest.raises(ExperimentFailed):
        await exp.run()

    events = [json.loads(line) for line in exp.events_log_path.read_text().splitlines() if line]
    invalid = [e for e in events if e.get("event") == "run_invalid"]
    assert invalid and invalid[0]["reason"] == "bad run"


@pytest.mark.asyncio
async def test_every_reason_is_reported_not_just_the_first(tmp_path):
    def two(exp):
        exp.fail_run("first")
        exp.fail_run("second")

    with pytest.raises(ExperimentFailed, match="first; second"):
        await _experiment(tmp_path, two).run()

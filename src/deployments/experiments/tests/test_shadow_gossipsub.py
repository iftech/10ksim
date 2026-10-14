import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments.base_experiment import ExperimentFailed
from src.deployments.experiments.libp2p import shadow_gossipsub
from src.deployments.experiments.libp2p.shadow_gossipsub import (
    ExpConfig,
    ShadowGossipsubExperiment,
)


@pytest.mark.asyncio
async def test_a_failed_shadow_job_fails_the_run(tmp_path, mocker):
    """Must be ExperimentFailed: Multiple swallows other exceptions and exits 0."""
    mocker.patch.object(ShadowGossipsubExperiment, "deploy_yaml", mocker.AsyncMock())
    mocker.patch.object(
        shadow_gossipsub, "wait_for_job_complete", mocker.AsyncMock(return_value="failed")
    )
    mocker.patch.object(shadow_gossipsub, "pull_shadow_logs")
    exp = ShadowGossipsubExperiment(
        api_client=ApiClient(), config=ExpConfig(), namespace="ns", output_folder=tmp_path / "run"
    )

    with pytest.raises(ExperimentFailed, match="Shadow Job .* failed"):
        await exp.run(run_post_analysis=False)

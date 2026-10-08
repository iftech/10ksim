import asyncio

import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments.libp2p import nimlibp2p
from src.deployments.experiments.libp2p.nimlibp2p import ExpConfig, NimLibp2pExperiment, publish
from src.deployments.pod_api_requester.pod_api_requester import (
    PodApiApplicationError,
    PodApiClientError,
)

REQUESTER = ("10.0.0.1", 30000)


@pytest.mark.asyncio
async def test_a_delivered_publish_reports_success(mocker):
    mocker.patch.object(nimlibp2p, "libp2p_dst_node_publish", return_value=None)
    assert await publish(ExpConfig(), "ns", "pod-0", REQUESTER) is True


@pytest.mark.parametrize(
    "error",
    [PodApiClientError("timed out"), PodApiApplicationError("node said no"), RuntimeError("boom")],
)
@pytest.mark.asyncio
async def test_a_lost_publish_is_reported_not_swallowed(mocker, error):
    """Every failure used to be logged and discarded, so the count came out right anyway."""
    mocker.patch.object(nimlibp2p, "libp2p_dst_node_publish", side_effect=error)
    assert await publish(ExpConfig(), "ns", "pod-0", REQUESTER) is False


def test_no_publish_may_fail_by_default():
    assert ExpConfig().max_failed_publishes == 0


def test_the_tolerance_can_be_raised_for_a_run_that_expects_losses():
    assert ExpConfig(max_failed_publishes=10).max_failed_publishes == 10


@pytest.mark.asyncio
async def test_the_requester_is_looked_up_once_per_run_not_per_message(tmp_path, mocker):
    lookup = mocker.patch.object(nimlibp2p, "requester_address", return_value=REQUESTER)
    send = mocker.patch.object(nimlibp2p, "libp2p_dst_node_publish", return_value=None)
    mocker.patch.object(nimlibp2p, "resolved_images", return_value={})
    mocker.patch.object(NimLibp2pExperiment, "deploy", mocker.AsyncMock())
    mocker.patch.object(NimLibp2pExperiment, "dump_yaml")
    mocker.patch.object(NimLibp2pExperiment, "_dwell_and_capture", mocker.AsyncMock())
    mocker.patch.object(asyncio, "sleep", mocker.AsyncMock())
    exp = NimLibp2pExperiment(
        api_client=ApiClient(),
        config=ExpConfig(num_messages=5),
        namespace="ns",
        output_folder=tmp_path / "run",
    )
    exp.events_log_path = tmp_path / "events.log"

    await exp._run()

    lookup.assert_called_once_with("ns")
    assert [call.kwargs["requester"] for call in send.call_args_list] == [REQUESTER] * 5

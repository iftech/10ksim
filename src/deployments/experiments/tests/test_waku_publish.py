import asyncio

import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments import waku
from src.deployments.experiments.waku import ExpConfig, WakuExperiment

REQUESTER = ("10.0.0.1", 30000)


@pytest.mark.asyncio
async def test_the_requester_is_looked_up_once_per_run_not_per_message(tmp_path, mocker):
    lookup = mocker.patch.object(waku, "requester_address", return_value=REQUESTER)
    send = mocker.patch.object(waku, "waku_publish", return_value=None)
    mocker.patch.object(WakuExperiment, "deploy", mocker.AsyncMock())
    mocker.patch.object(asyncio, "sleep", mocker.AsyncMock())
    exp = WakuExperiment(
        api_client=ApiClient(),
        config=ExpConfig(num_messages=4),
        namespace="ns",
        output_folder=tmp_path / "run",
    )
    exp.events_log_path = tmp_path / "events.log"

    await exp._run()

    lookup.assert_called_once_with("ns")
    assert [call.kwargs["requester"] for call in send.call_args_list] == [REQUESTER] * 4

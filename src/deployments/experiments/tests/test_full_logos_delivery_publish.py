import json

import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments import full_logos_delivery
from src.deployments.experiments.full_logos_delivery import (
    ExpConfig,
    FullLogosDeliveryExperiment,
    publish,
)
from src.deployments.pod_api_requester.pod_api_requester import (
    PodApiApplicationError,
    PodApiClientError,
)

PROTOCOLS = ["relay", "lightpush"]


def _publish(protocol):
    return publish(protocol, "ns", "pod-0", "svc", 1, 0, "/my-app/1/dst/proto")


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.asyncio
async def test_a_delivered_publish_reports_success(mocker, protocol):
    mocker.patch.object(full_logos_delivery, "waku_publish", return_value=None)
    mocker.patch.object(full_logos_delivery, "waku_lightpush_publish", return_value=None)
    assert await _publish(protocol) is True


@pytest.mark.parametrize("protocol", PROTOCOLS)
@pytest.mark.parametrize(
    "error",
    [PodApiClientError("timed out"), PodApiApplicationError("node said no"), RuntimeError("boom")],
)
@pytest.mark.asyncio
async def test_a_lost_publish_is_reported_not_swallowed(mocker, protocol, error):
    mocker.patch.object(full_logos_delivery, "waku_publish", side_effect=error)
    mocker.patch.object(full_logos_delivery, "waku_lightpush_publish", side_effect=error)
    assert await _publish(protocol) is False


def _experiment(tmp_path, **config):
    config = ExpConfig(num_messages=4, delay_after_publish=0, **config)
    return FullLogosDeliveryExperiment(
        api_client=ApiClient(),
        config=config,
        namespace="ns",
        output_folder=tmp_path,
        events_log_path=tmp_path / "events.log",
    )


def _events(exp):
    return [json.loads(line) for line in exp.events_log_path.read_text().splitlines()]


@pytest.mark.asyncio
async def test_lost_publishes_mark_the_run_invalid(tmp_path, mocker):
    mocker.patch.object(full_logos_delivery, "publish", side_effect=[True, False, True, False])
    exp = _experiment(tmp_path)

    await exp._publish_loop(cluster_id=0)

    summary = next(e for e in _events(exp) if e["event"] == "publish_summary")
    assert (summary["attempted"], summary["failed"]) == (4, 2)
    assert any(e["event"] == "run_invalid" for e in _events(exp))
    assert exp._failures == [
        "2 of 4 messages were never published, so delivery is measured against a "
        "denominator the run did not send"
    ]


@pytest.mark.asyncio
async def test_losses_within_the_tolerance_keep_the_run_valid(tmp_path, mocker):
    mocker.patch.object(full_logos_delivery, "publish", side_effect=[True, False, True, True])
    exp = _experiment(tmp_path, max_failed_publishes=1)

    await exp._publish_loop(cluster_id=0)

    assert exp._failures == []

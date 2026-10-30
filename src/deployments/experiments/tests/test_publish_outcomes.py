import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from kubernetes.client import ApiClient

from src.deployments.experiments import base_experiment
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


class StallingClock:
    """Run clock that only the run's sleeps move, each by `stall` more than asked."""

    def __init__(self, stall: float):
        # Mid-second, so a check that rounds to whole seconds is off.
        self.time = datetime(2026, 8, 15, 8, 17, 14, 600000, tzinfo=timezone.utc)
        self.stall = stall

    def now(self, tz=None):
        return self.time

    def monotonic(self):
        return self.time.timestamp()

    async def sleep(self, seconds):
        self.time += timedelta(seconds=seconds + self.stall)


async def run_with_stall(tmp_path, mocker, *, stall: float, messages: int, delay: float) -> list:
    clock = StallingClock(stall)
    mocker.patch.object(base_experiment, "datetime", clock)
    mocker.patch.object(nimlibp2p, "time", clock)
    mocker.patch.object(asyncio, "sleep", clock.sleep)
    mocker.patch.object(nimlibp2p, "requester_address", return_value=REQUESTER)
    mocker.patch.object(nimlibp2p, "libp2p_dst_node_publish", return_value=None)
    mocker.patch.object(nimlibp2p, "resolved_images", return_value={})
    mocker.patch.object(NimLibp2pExperiment, "deploy", mocker.AsyncMock())
    mocker.patch.object(NimLibp2pExperiment, "dump_yaml")
    mocker.patch.object(NimLibp2pExperiment, "_dwell_and_capture", mocker.AsyncMock())
    exp = NimLibp2pExperiment(
        api_client=ApiClient(),
        config=ExpConfig(num_messages=messages, delay_after_publish=delay),
        namespace="ns",
        output_folder=tmp_path / "run",
    )
    exp.events_log_path = tmp_path / "events.log"

    await exp._run()

    return exp._failures


# Long, short, sub-second and slow-cadence runs.
RUNS = [(600, 1), (20, 1), (10, 0.25), (8, 60)]


@pytest.mark.parametrize("messages, delay", RUNS)
@pytest.mark.asyncio
async def test_a_run_that_published_slower_than_configured_is_invalid(
    tmp_path, mocker, messages, delay
):
    """The v2.3.0 matrix took 2.08 s per 1 s publish and ran at 0.48 msg/s."""
    failures = await run_with_stall(
        tmp_path, mocker, stall=1.08 * delay, messages=messages, delay=delay
    )
    assert any("msg/s" in reason for reason in failures)


@pytest.mark.parametrize("messages, delay", RUNS)
@pytest.mark.asyncio
async def test_a_run_at_its_configured_rate_is_valid(tmp_path, mocker, messages, delay):
    assert await run_with_stall(tmp_path, mocker, stall=0, messages=messages, delay=delay) == []

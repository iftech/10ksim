import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from src.deployments.pod_api_requester import pod_api_requester
from src.deployments.pod_api_requester.pod_api_requester import (
    REQUEST_TIMEOUT_S,
    PodApiClientError,
    PodResponse,
    post_async,
)


class _Session:
    """Captures the timeout aiohttp.ClientSession was built with."""

    built_with = None

    def __init__(self, *_, timeout=None, **__):
        type(self).built_with = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def post(self, *_, **__):
        response = MagicMock(status=200, reason="OK", headers={})
        response.text = AsyncMock(return_value="{}")
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        return context


@pytest.mark.asyncio
async def test_a_request_is_bounded_rather_than_using_the_300s_default(mocker):
    mocker.patch.object(aiohttp, "ClientSession", _Session)
    await post_async("http://node/process", {})
    assert _Session.built_with.total == REQUEST_TIMEOUT_S


@pytest.mark.asyncio
async def test_the_bound_is_short_enough_to_free_the_connection_pool():
    """aiohttp holds 100 sockets; at 300s each, publishes to dead pods starve the loop."""
    assert REQUEST_TIMEOUT_S < 300


@pytest.mark.asyncio
async def test_a_timeout_surfaces_as_a_typed_error(mocker):
    mocker.patch.object(
        pod_api_requester, "_get_api_requester_info", return_value=("10.0.0.1", 30000)
    )
    mocker.patch.object(pod_api_requester, "post_async", side_effect=asyncio.TimeoutError)

    with pytest.raises(PodApiClientError, match="did not answer within"):
        await pod_api_requester.pod_api_request(
            namespace="ns",
            service_name="svc",
            app="app",
            url_template="http://{target_ip}:{node_port}/process",
            data={},
        )


@pytest.mark.asyncio
async def test_a_known_requester_is_not_looked_up_again(mocker):
    lookup = mocker.patch.object(pod_api_requester, "_get_api_requester_info")
    post = mocker.patch.object(
        pod_api_requester,
        "post_async",
        return_value=PodResponse(
            status_code=200, reason="OK", text='{"response": {"status_code": 200, "text": "ok"}}'
        ),
    )

    await pod_api_requester.pod_api_request(
        namespace="ns",
        service_name="svc",
        app="app",
        url_template="http://{target_ip}:{node_port}/process",
        data={},
        requester=("10.0.0.1", 30000),
    )

    lookup.assert_not_called()
    assert post.call_args.args[0] == "http://10.0.0.1:30000/process"


@pytest.mark.asyncio
async def test_a_slow_lookup_does_not_stall_other_tasks(mocker):
    finished = {}

    def slow_lookup(**_):
        time.sleep(0.3)
        finished["lookup"] = time.monotonic()
        return "10.0.0.1", 30000

    async def ticker():
        for _ in range(10):
            await asyncio.sleep(0.01)
        finished["ticker"] = time.monotonic()

    mocker.patch.object(pod_api_requester, "_get_api_requester_info", side_effect=slow_lookup)
    mocker.patch.object(
        pod_api_requester,
        "post_async",
        return_value=PodResponse(
            status_code=200, reason="OK", text='{"response": {"status_code": 200, "text": "ok"}}'
        ),
    )

    await asyncio.gather(
        pod_api_requester.pod_api_request(
            namespace="ns",
            service_name="svc",
            app="app",
            url_template="http://{target_ip}:{node_port}/process",
            data={},
        ),
        ticker(),
    )

    assert finished["ticker"] < finished["lookup"]

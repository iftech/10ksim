import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.deployments.logos_storage import transfer
from src.deployments.pod_api_requester.storage import StorageRequestError

pytestmark = pytest.mark.asyncio
ADDRESS = ("10.0.0.1", 30000)


def receipts(**download_overrides):
    upload = {"cid": "zTestCid", "sha256": "a" * 64, "file_size_bytes": 1048576, "seconds": 0.2}
    download = {**upload, "seconds": 0.4, "verified": True}
    return upload, {**download, **download_overrides}


async def run_transfer(**overrides):
    return await transfer.run_file_transfer(
        **{
            "namespace": "storage-test",
            "source_pod": "storage-nodes-0",
            "target_pods": ["storage-nodes-1"],
            "file_size_bytes": 1048576,
            "timeout": 60,
            "api_port": 8080,
            **overrides,
        }
    )


@pytest.fixture
def operations(monkeypatch):
    lookup = Mock(return_value=ADDRESS)
    upload, download = receipts()
    upload_op = AsyncMock(return_value=upload)
    download_op = AsyncMock(return_value=download)
    monkeypatch.setattr(transfer, "requester_address", lookup)
    monkeypatch.setattr(transfer, "storage_upload", upload_op)
    monkeypatch.setattr(transfer, "storage_download", download_op)
    return lookup, upload_op, download_op


async def test_transfer_uploads_to_one_node_and_downloads_through_the_other(operations):
    lookup, upload, download = operations
    result = await run_transfer()

    assert result["status"] == "passed"
    assert result["cid"] == "zTestCid"
    assert result["download_attempts"] == 1
    assert result["upload"]["seconds"] == 0.2
    assert result["downloads"][0]["download"]["seconds"] == 0.4
    assert result["downloads"][0]["target_pod"] == "storage-nodes-1"
    lookup.assert_called_once_with("storage-test")
    upload.assert_awaited_once_with(
        namespace="storage-test",
        pod="storage-nodes-0",
        api_port=8080,
        file_size_bytes=1048576,
        timeout=60,
        requester=ADDRESS,
    )
    args = download.await_args.kwargs
    assert args["pod"] == "storage-nodes-1"
    assert args["cid"] == "zTestCid"
    assert args["expected_sha256"] == "a" * 64
    assert args["expected_size_bytes"] == 1048576
    assert args["requester"] == ADDRESS
    assert 0 < args["timeout"] <= 60


@pytest.mark.parametrize(
    ("overrides", "error"),
    [({"sha256": "b" * 64}, "SHA-256"), ({"file_size_bytes": 42}, "Downloaded 42 bytes")],
)
async def test_inconsistent_receipts_fail_without_retrying(operations, overrides, error):
    _, _, download = operations
    download.return_value = receipts(**overrides)[1]
    result = await run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert result["downloads"][0]["stage"] == "verify"
    assert error in result["error"]
    download.assert_awaited_once()
    assert "upload" in result and "download" in result["downloads"][0]


async def test_newly_advertised_content_can_be_retried_with_one_deadline(monkeypatch, operations):
    _, _, download = operations
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(transfer, "time", SimpleNamespace(monotonic=lambda: clock.now))
    download.side_effect = [StorageRequestError("upstream HTTP 404", retryable=True), receipts()[1]]

    async def sleep(seconds):
        clock.now += seconds

    pause = AsyncMock(side_effect=sleep)
    monkeypatch.setattr(transfer.asyncio, "sleep", pause)
    result = await run_transfer()

    assert result["status"] == "passed"
    assert result["download_attempts"] == 2
    assert [call.kwargs["timeout"] for call in download.await_args_list] == [60, 58]
    pause.assert_awaited_once_with(2)


async def test_retry_deadline_preserves_the_last_download_error(monkeypatch, operations):
    _, _, download = operations
    clock = SimpleNamespace(now=0)

    async def unavailable(**_kwargs):
        clock.now = 60
        raise StorageRequestError("provider unavailable", retryable=True)

    download.side_effect = unavailable
    pause = AsyncMock()
    monkeypatch.setattr(transfer, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(transfer.asyncio, "sleep", pause)
    result = await run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert "provider unavailable" in result["error"]
    assert result["download_attempts"] == 1
    assert result["elapsed_seconds"] == 60
    pause.assert_not_awaited()


async def test_upload_failure_does_not_attempt_a_download(operations):
    _, upload, download = operations
    upload.side_effect = StorageRequestError("upload refused", retryable=True)
    result = await run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "upload"
    assert result["error"] == "upload refused"
    upload.assert_awaited_once()
    download.assert_not_awaited()


async def test_integrity_failure_is_not_retried(operations):
    _, _, download = operations
    download.side_effect = StorageRequestError("Downloaded data failed verification")
    result = await run_transfer()
    assert result["stage"] == "download"
    assert result["status"] == "failed"
    download.assert_awaited_once()
    json.dumps(result)


async def test_download_cannot_exceed_its_overall_deadline(operations):
    _, _, download = operations

    async def stalled(**_kwargs):
        await asyncio.sleep(1)

    download.side_effect = stalled
    result = await run_transfer(timeout=0.01)
    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert "Download exceeded" in result["error"]
    download.assert_awaited_once()


async def test_one_upload_reaches_three_receivers_with_bounded_concurrency(operations):
    _, upload, download = operations
    targets = [f"storage-nodes-{index}" for index in range(1, 4)]
    entered = {pod: asyncio.Event() for pod in targets}
    release = asyncio.Event()
    active = 0
    peak = 0

    async def receive(**kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        entered[kwargs["pod"]].set()
        try:
            await release.wait()
            return receipts()[1]
        finally:
            active -= 1

    download.side_effect = receive
    task = asyncio.create_task(run_transfer(target_pods=targets, concurrency=2))
    try:
        await asyncio.wait_for(
            asyncio.gather(entered[targets[0]].wait(), entered[targets[1]].wait()), timeout=2
        )
        assert not entered[targets[2]].is_set()
        assert download.await_count == 2
    finally:
        release.set()
        result = await asyncio.wait_for(task, timeout=2)

    assert result["status"] == "passed"
    assert peak == 2
    assert result["download_attempts"] == 3
    assert [entry["target_pod"] for entry in result["downloads"]] == targets
    assert all(entry["status"] == "passed" for entry in result["downloads"])
    upload.assert_awaited_once()
    assert download.await_count == 3
    for call in download.await_args_list:
        assert call.kwargs["cid"] == "zTestCid"
        assert call.kwargs["expected_sha256"] == "a" * 64
        assert call.kwargs["expected_size_bytes"] == 1048576


async def test_one_receiver_failure_preserves_the_other_results(operations):
    _, upload, download = operations
    targets = [f"storage-nodes-{index}" for index in range(1, 4)]

    async def receive(**kwargs):
        if kwargs["pod"] == "storage-nodes-2":
            raise StorageRequestError("SHA-256 verification failed")
        await asyncio.sleep(0)
        return receipts()[1]

    download.side_effect = receive
    result = await run_transfer(target_pods=targets)
    assert result["status"] == "failed"
    assert "1 of 3 downloads failed" in result["error"]
    assert "storage-nodes-2" in result["error"]
    assert [entry["status"] for entry in result["downloads"]] == ["passed", "failed", "passed"]
    assert "SHA-256" in result["downloads"][1]["error"]
    assert download.await_count == 3
    upload.assert_awaited_once()
    json.dumps(result)


async def test_only_the_transiently_failing_receiver_is_retried(monkeypatch, operations):
    _, _, download = operations
    attempts = {f"storage-nodes-{index}": 0 for index in range(1, 4)}

    async def receive(**kwargs):
        pod = kwargs["pod"]
        attempts[pod] += 1
        if pod == "storage-nodes-2" and attempts[pod] == 1:
            raise StorageRequestError("Provider not found", retryable=True)
        return receipts()[1]

    download.side_effect = receive
    monkeypatch.setattr(transfer.asyncio, "sleep", AsyncMock())
    result = await run_transfer(target_pods=list(attempts))
    assert result["status"] == "passed"
    assert list(attempts.values()) == [1, 2, 1]
    assert result["download_attempts"] == 4
    assert [entry["download_attempts"] for entry in result["downloads"]] == [1, 2, 1]


async def test_queued_receivers_get_a_fresh_deadline(monkeypatch, operations):
    _, _, download = operations
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(transfer, "time", SimpleNamespace(monotonic=lambda: clock.now))

    async def receive(**_kwargs):
        clock.now += 40
        return receipts()[1]

    download.side_effect = receive
    result = await run_transfer(
        target_pods=[f"storage-nodes-{index}" for index in range(1, 4)], concurrency=1
    )
    assert result["status"] == "passed"
    assert [call.kwargs["timeout"] for call in download.await_args_list] == [60, 60, 60]
    assert result["elapsed_seconds"] == 120


@pytest.mark.parametrize(
    "overrides",
    [
        {"target_pods": []},
        {"target_pods": ["storage-nodes-0"]},
        {"target_pods": ["storage-nodes-1", "storage-nodes-1"]},
        {"concurrency": 0},
    ],
)
async def test_invalid_receiver_settings_do_not_upload(operations, overrides):
    _, upload, download = operations
    result = await run_transfer(**overrides)
    assert result["status"] == "failed"
    upload.assert_not_awaited()
    download.assert_not_awaited()

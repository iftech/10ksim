import json
from unittest.mock import AsyncMock

import pytest

from src.deployments.pod_api_requester import storage
from src.deployments.pod_api_requester.pod_api_requester import PodApiRequesterError

pytestmark = pytest.mark.asyncio
ADDRESS = ("10.0.0.1", 30000)
RECEIPT = {"cid": "zTestCid", "sha256": "a" * 64, "file_size_bytes": 1048576, "seconds": 0.2}


@pytest.fixture
def api_request(monkeypatch):
    call = AsyncMock(return_value={"response": {"status_code": 200, "text": json.dumps(RECEIPT)}})
    monkeypatch.setattr(storage, "pod_api_request", call)
    return call


async def upload():
    return await storage.storage_upload(
        namespace="storage-test",
        pod="storage.nodes-0",
        api_port=8080,
        file_size_bytes=1048576,
        timeout=60,
        requester=ADDRESS,
    )


async def test_upload_targets_one_pod_and_allows_the_operation_to_finish(api_request):
    assert await upload() == RECEIPT
    args = api_request.await_args.kwargs
    assert args["url_template"].endswith("/storage/upload")
    assert args["data"]["target"]["value"]["name_template"] == r"^storage\.nodes\-0$"
    assert args["data"]["target"]["value"]["port"] == 8080
    assert args["data"]["file_size_bytes"] == 1048576
    assert args["data"]["timeout_seconds"] == 60
    assert args["timeout_s"] == 65
    assert args["requester"] == ADDRESS


async def test_download_passes_the_upload_receipt_and_requires_verification(api_request):
    receipt = {**RECEIPT, "verified": True}
    api_request.return_value["response"]["text"] = json.dumps(receipt)
    assert (
        await storage.storage_download(
            namespace="storage-test",
            pod="storage-nodes-1",
            api_port=8080,
            cid=RECEIPT["cid"],
            expected_sha256=RECEIPT["sha256"],
            expected_size_bytes=1048576,
            timeout=17,
            requester=ADDRESS,
        )
        == receipt
    )
    args = api_request.await_args.kwargs
    assert args["url_template"].endswith("/storage/download")
    assert args["data"]["transport"] == "direct"
    assert args["data"]["expected_sha256"] == RECEIPT["sha256"]
    assert args["data"]["expected_size_bytes"] == 1048576
    assert args["timeout_s"] == 22


@pytest.mark.parametrize(
    ("status", "detail", "retryable"),
    [
        (502, {"upstream_status_code": 404}, True),
        (502, {"upstream_status_code": 503}, True),
        (502, "connection failed", True),
        (504, "timed out", True),
        (409, "integrity failure", False),
        (404, "no matching pod or missing route", False),
        (422, "invalid input", False),
        (502, {"upstream_status_code": 400}, False),
    ],
)
async def test_only_transient_operation_errors_are_retryable(
    api_request, status, detail, retryable
):
    api_request.side_effect = PodApiRequesterError("node error", status_code=status, detail=detail)
    with pytest.raises(storage.StorageRequestError) as error:
        await upload()
    assert error.value.retryable is retryable


@pytest.mark.parametrize("receipt", ["not JSON", "null", "{}", '{"cid": "x"}'])
async def test_malformed_receipts_fail_without_retrying(api_request, receipt):
    api_request.return_value["response"]["text"] = receipt
    with pytest.raises(storage.StorageRequestError, match="invalid upload receipt") as error:
        await upload()
    assert not error.value.retryable


async def test_a_download_receipt_without_verification_is_rejected(api_request):
    with pytest.raises(storage.StorageRequestError, match="invalid download receipt"):
        await storage.storage_download(
            namespace="storage-test",
            pod="storage-nodes-1",
            api_port=8080,
            cid=RECEIPT["cid"],
            expected_sha256=RECEIPT["sha256"],
            expected_size_bytes=1048576,
            timeout=60,
            requester=ADDRESS,
        )

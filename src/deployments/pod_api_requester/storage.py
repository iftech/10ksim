"""Client operations for the requester's streaming Logos Storage API."""

import json
import re
from typing import Literal

from pydantic import BaseModel, Field, PositiveInt, ValidationError

from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import (
    _DEFAULTS,
    PodApiClientError,
    PodApiError,
    PodApiRequesterError,
    pod_api_request,
    wrap_arg,
)

RESPONSE_GRACE_SECONDS = 5


class StorageRequestError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class UploadReceipt(BaseModel):
    cid: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size_bytes: PositiveInt
    seconds: float = Field(ge=0, allow_inf_nan=False)


class DownloadReceipt(UploadReceipt):
    verified: Literal[True]


def _retryable(error: PodApiRequesterError) -> bool:
    if error.status_code in {429, 503, 504}:
        return True
    if error.status_code != 502:
        return False
    upstream_status = (
        error.detail.get("upstream_status_code") if isinstance(error.detail, dict) else None
    )
    return upstream_status is None or upstream_status in {404, 408, 429, 500, 502, 503, 504}


async def _storage_request(
    operation: str,
    *,
    namespace: str,
    pod: str,
    api_port: int,
    timeout: float,
    requester: tuple[str, int],
    **params,
) -> dict:
    target = Target(name=pod, name_template=f"^{re.escape(pod)}$", port=api_port)
    try:
        result = await pod_api_request(
            namespace=namespace,
            service_name=_DEFAULTS["service_name"],
            app=_DEFAULTS["app"],
            url_template=f"http://{{target_ip}}:{{node_port}}/storage/{operation}",
            data={"target": wrap_arg(target), "timeout_seconds": timeout, **params},
            requester=requester,
            timeout_s=timeout + RESPONSE_GRACE_SECONDS,
        )
    except PodApiRequesterError as exc:
        raise StorageRequestError(str(exc), retryable=_retryable(exc)) from exc
    except PodApiClientError as exc:
        raise StorageRequestError(str(exc), retryable=True) from exc
    except PodApiError as exc:
        raise StorageRequestError(str(exc)) from exc

    receipt_type = UploadReceipt if operation == "upload" else DownloadReceipt
    try:
        return receipt_type.model_validate(json.loads(result["response"]["text"])).model_dump()
    except (KeyError, TypeError, ValueError, ValidationError) as exc:
        raise StorageRequestError(f"{pod}: invalid {operation} receipt: {exc}") from exc


async def storage_upload(
    *,
    namespace: str,
    pod: str,
    api_port: int,
    file_size_bytes: int,
    timeout: float,
    requester: tuple[str, int],
) -> dict:
    return await _storage_request(
        "upload",
        namespace=namespace,
        pod=pod,
        api_port=api_port,
        file_size_bytes=file_size_bytes,
        timeout=timeout,
        requester=requester,
    )


async def storage_download(
    *,
    namespace: str,
    pod: str,
    api_port: int,
    cid: str,
    expected_sha256: str,
    expected_size_bytes: int,
    timeout: float,
    requester: tuple[str, int],
) -> dict:
    return await _storage_request(
        "download",
        namespace=namespace,
        pod=pod,
        api_port=api_port,
        cid=cid,
        expected_sha256=expected_sha256,
        expected_size_bytes=expected_size_bytes,
        transport="direct",
        timeout=timeout,
        requester=requester,
    )

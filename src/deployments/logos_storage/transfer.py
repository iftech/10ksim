"""Upload once and verify streamed downloads inside the other Storage pods."""

import asyncio
import logging
import time
from typing import Literal

from src.deployments.pod_api_requester.pod_api_requester import requester_address
from src.deployments.pod_api_requester.storage import (
    StorageRequestError,
    storage_download,
    storage_upload,
)

logger = logging.getLogger(__name__)


class FileTransferError(RuntimeError):
    pass


async def _download_target(
    *,
    namespace: str,
    target_pod: str,
    cid: str,
    file_size_bytes: int,
    expected_sha256: str,
    timeout: float,
    api_port: int,
    requester: tuple[str, int],
    transport: Literal["direct", "mix"],
) -> dict:
    """Give one receiver a deadline, including retries, once its slot is available."""
    started = time.monotonic()
    report = {
        "status": "failed",
        "stage": "download",
        "target_pod": target_pod,
        "download_attempts": 0,
        "verification": "pending",
    }
    try:
        deadline = time.monotonic() + timeout
        last_error = None
        logger.info("Downloading CID %s through %s", cid, target_pod)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FileTransferError(f"Download exceeded {timeout}s: {last_error}")
            report["download_attempts"] += 1
            try:
                async with asyncio.timeout(remaining):
                    download = await storage_download(
                        namespace=namespace,
                        pod=target_pod,
                        api_port=api_port,
                        cid=cid,
                        expected_size_bytes=file_size_bytes,
                        expected_sha256=expected_sha256,
                        timeout=remaining,
                        requester=requester,
                        transport=transport,
                    )
                break
            except TimeoutError as exc:
                raise FileTransferError(
                    f"Download exceeded {timeout}s: {last_error or 'request timed out'}"
                ) from exc
            except StorageRequestError as exc:
                if not exc.retryable:
                    raise
                last_error = exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FileTransferError(f"Download exceeded {timeout}s: {exc}") from exc
                logger.warning("Download attempt %s failed: %s", report["download_attempts"], exc)
                await asyncio.sleep(min(2, remaining))

        report["download"] = download
        report["stage"] = "confirm"
        if download["cid"] != cid:
            raise FileTransferError("Download receipt does not match the requested CID")
        if download["file_size_bytes"] != file_size_bytes:
            raise FileTransferError(
                f"Downloaded {download['file_size_bytes']} bytes; expected {file_size_bytes}"
            )
        if download["sha256"] != expected_sha256:
            raise FileTransferError("Downloaded file SHA-256 does not match the upload")
        if (
            download["verified"] is not True
            or download["verification_location"] != "receiver_pod"
            or download["transport"] != transport
        ):
            raise FileTransferError("Download receipt does not confirm verification in the receiver pod")
        report.update(status="passed", stage="complete", verification="sha256")
    except Exception as exc:
        report["error"] = str(exc) or type(exc).__name__
    report["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return report


async def run_file_transfer(
    *,
    namespace: str,
    source_pod: str,
    target_pods: list[str],
    file_size_bytes: int,
    timeout: float,
    api_port: int,
    concurrency: int = 3,
    transport: Literal["direct", "mix"] = "direct",
) -> dict:
    """Upload once, then hash each receiver's network stream in a co-located verifier.

    Only control and verified receipts reach the central requester and experiment runner.
    A failed receiver does not cancel the others; all results are saved in the report.
    """
    started = time.monotonic()
    report = {
        "status": "failed",
        "stage": "upload",
        "source_pod": source_pod,
        "target_pods": target_pods,
        "file_size_bytes": file_size_bytes,
        "transport": transport,
        "download_mode": "stream",
        "verification": "sha256",
        "verification_location": "receiver_pod",
        "download_concurrency": concurrency,
        "download_attempts": 0,
        "downloads": [],
    }
    try:
        if (
            not target_pods
            or source_pod in target_pods
            or len(set(target_pods)) != len(target_pods)
        ):
            raise FileTransferError(
                "Receivers must be a nonempty list of unique pods excluding the source"
            )
        if concurrency < 1:
            raise FileTransferError("Download concurrency must be positive")
        if transport not in {"direct", "mix"}:
            raise FileTransferError("Download transport must be direct or mix")
        async with asyncio.timeout(timeout):
            requester = await asyncio.to_thread(requester_address, namespace)
        logger.info("Uploading %s bytes to %s", file_size_bytes, source_pod)
        upload = await storage_upload(
            namespace=namespace,
            pod=source_pod,
            api_port=api_port,
            file_size_bytes=file_size_bytes,
            timeout=timeout,
            requester=requester,
        )
        report["upload"] = upload
        cid = upload["cid"].strip()
        if not cid or upload["file_size_bytes"] != file_size_bytes:
            raise FileTransferError("Upload receipt has an empty CID or incorrect file size")
        report["cid"] = cid
        report["stage"] = "download"
        slots = asyncio.Semaphore(concurrency)

        async def receive(pod):
            async with slots:
                return await _download_target(
                    namespace=namespace,
                    target_pod=pod,
                    cid=cid,
                    file_size_bytes=file_size_bytes,
                    expected_sha256=upload["sha256"],
                    timeout=timeout,
                    api_port=api_port,
                    requester=requester,
                    transport=transport,
                )

        downloads = await asyncio.gather(*(receive(pod) for pod in target_pods))
        report["downloads"] = downloads
        report["download_attempts"] = sum(result["download_attempts"] for result in downloads)
        failed = [result for result in downloads if result["status"] != "passed"]
        if failed:
            errors = "; ".join(
                f"{result['target_pod']} ({result['stage']}): {result['error']}"
                for result in failed
            )
            raise FileTransferError(f"{len(failed)} of {len(downloads)} downloads failed: {errors}")
        report.update(status="passed", stage="complete")
    except Exception as exc:
        report["error"] = str(exc) or type(exc).__name__
    report["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return report

"""Run a small REST file-transfer check inside the existing Storage pods."""

import json
import logging
import shlex
import subprocess
import time
from urllib.parse import quote

from src.deployments.core.k8s_kubeconfig import get_config_file

logger = logging.getLogger(__name__)

TEMP_FILE = """set -eu
workdir=$(mktemp -d /tmp/logos-storage-transfer.XXXXXX)
trap 'rm -rf "$workdir"' EXIT
"""

FILE_INFO = """sha256=$(sha256sum "$workdir/payload")
sha256=${sha256%% *}
size=$(wc -c < "$workdir/payload")
jq -n --arg sha256 "$sha256" --argjson size "$size" --argjson seconds "$seconds" \\
    '{sha256: $sha256, file_size_bytes: $size, seconds: $seconds}'
"""


class FileTransferError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


def _upload_script(file_size_bytes: int, timeout: float, api_port: int) -> str:
    return (
        TEMP_FILE
        + f"""head -c {file_size_bytes} /dev/urandom > "$workdir/payload"
seconds=$(curl --silent --show-error --fail --connect-timeout 5 \\
    --max-time {max(timeout, 0.001):.3f} --request POST \\
    --upload-file "$workdir/payload" --output "$workdir/cid" \\
    --write-out '%{{time_total}}' \\
    'http://127.0.0.1:{api_port}/api/storage/v1/data')
cid=$(cat "$workdir/cid")
test -n "$cid" || {{ echo 'Upload returned an empty CID' >&2; exit 1; }}
info=$(
"""
        + FILE_INFO
        + """\n)
printf '%s\\n' "$info" | jq --arg cid "$cid" '. + {cid: $cid}'
"""
    )


def _download_script(cid: str, timeout: float, api_port: int) -> str:
    url = (
        f"http://127.0.0.1:{api_port}/api/storage/v1/data/{quote(cid, safe='')}"
        "/network/stream?transport=direct"
    )
    return TEMP_FILE + f"""seconds=$(curl --silent --show-error --fail --connect-timeout 5 \\
    --max-time {max(timeout, 0.001):.3f} --output "$workdir/payload" \\
    --write-out '%{{time_total}}' {shlex.quote(url)})
""" + FILE_INFO


def _pod_json(namespace: str, pod: str, script: str, timeout: float) -> dict:
    command = ["kubectl"]
    if kube_config := get_config_file():
        command.extend(["--kubeconfig", kube_config])
    command.extend(
        [
            "exec",
            "--namespace",
            namespace,
            pod,
            "--container",
            "storage",
            "--",
            "/bin/sh",
            "-c",
            script,
        ]
    )
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        detail = exc.stderr or exc.stdout or ""
        if isinstance(detail, bytes):
            detail = detail.decode(errors="replace")
        raise FileTransferError(
            f"{pod}: command exceeded {timeout:.1f}s: {detail.strip()}", retryable=True
        ) from exc
    if result.returncode:
        # Curl's HTTP, connection, partial-transfer and timeout failures can be transient.
        retryable = result.returncode in {7, 18, 22, 28, 52, 56}
        raise FileTransferError(
            f"{pod}: exit {result.returncode}: {result.stderr.strip() or result.stdout.strip()}",
            retryable=retryable,
        )
    try:
        return json.loads(result.stdout)
    except ValueError as exc:
        raise FileTransferError(f"{pod}: invalid transfer receipt: {result.stdout!r}") from exc


def run_file_transfer(
    *,
    namespace: str,
    source_pod: str,
    target_pod: str,
    file_size_bytes: int,
    timeout: float,
    api_port: int,
) -> dict:
    """Upload fresh content to source, retrieve through target, and compare size and SHA-256.

    Each stage has a timeout, including kubectl execution. Download retries share one
    deadline so a newly advertised CID can become discoverable. Receipts retain timings
    reported by curl; elapsed_seconds includes generation, hashing, exec and retries.
    """
    started = time.monotonic()
    report = {
        "status": "failed",
        "stage": "upload",
        "source_pod": source_pod,
        "target_pod": target_pod,
        "file_size_bytes": file_size_bytes,
        "transport": "direct",
        "download_attempts": 0,
    }
    try:
        logger.info("Uploading %s bytes to %s", file_size_bytes, source_pod)
        upload = _pod_json(
            namespace, source_pod, _upload_script(file_size_bytes, timeout, api_port), timeout
        )
        report["upload"] = upload
        cid = upload["cid"].strip()
        if not cid or upload["file_size_bytes"] != file_size_bytes:
            raise FileTransferError("Upload receipt has an empty CID or incorrect file size")
        report["cid"] = cid
        report["stage"] = "download"
        deadline = time.monotonic() + timeout
        logger.info("Downloading CID %s through %s", cid, target_pod)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FileTransferError(f"Download did not complete within {timeout}s")
            report["download_attempts"] += 1
            try:
                download = _pod_json(
                    namespace, target_pod, _download_script(cid, remaining, api_port), remaining
                )
                break
            except FileTransferError as exc:
                if not exc.retryable:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FileTransferError(f"Download exceeded {timeout}s: {exc}") from exc
                logger.warning("Download attempt %s failed: %s", report["download_attempts"], exc)
                time.sleep(min(2, remaining))

        report["download"] = download
        report["stage"] = "verify"
        if download["file_size_bytes"] != file_size_bytes:
            raise FileTransferError(
                f"Downloaded {download['file_size_bytes']} bytes; expected {file_size_bytes}"
            )
        if download["sha256"] != upload["sha256"]:
            raise FileTransferError("Downloaded SHA-256 does not match the uploaded file")
        report.update(status="passed", stage="complete")
    except Exception as exc:
        report["error"] = str(exc)
    report["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return report

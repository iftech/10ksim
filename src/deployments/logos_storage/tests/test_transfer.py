import json
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.deployments.logos_storage import transfer
from src.deployments.logos_storage.transfer import FileTransferError, run_file_transfer


def receipts(**download_overrides):
    upload = {"cid": "zTestCid", "sha256": "a" * 64, "file_size_bytes": 1048576, "seconds": 0.2}
    download = {"sha256": "a" * 64, "file_size_bytes": 1048576, "seconds": 0.4}
    return upload, {**download, **download_overrides}


def run_transfer():
    return run_file_transfer(
        namespace="storage-test",
        source_pod="storage-nodes-0",
        target_pod="storage-nodes-1",
        file_size_bytes=1048576,
        timeout=60,
        api_port=8080,
    )


def test_transfer_uploads_to_one_node_and_downloads_through_the_other(monkeypatch):
    execute = Mock(side_effect=receipts())
    monkeypatch.setattr(transfer, "_pod_json", execute)

    result = run_transfer()

    assert result["status"] == "passed"
    assert result["cid"] == "zTestCid"
    assert result["download_attempts"] == 1
    assert result["upload"]["seconds"] == 0.2
    assert result["download"]["seconds"] == 0.4
    upload, download = execute.call_args_list
    assert upload.args[:2] == ("storage-test", "storage-nodes-0")
    assert download.args[:2] == ("storage-test", "storage-nodes-1")
    assert "/dev/urandom" in upload.args[2]
    assert "/zTestCid/network/stream?transport=direct" in download.args[2]
    assert 0 < download.args[3] <= 60


@pytest.mark.parametrize(
    ("overrides", "error"),
    [({"sha256": "b" * 64}, "SHA-256"), ({"file_size_bytes": 42}, "Downloaded 42 bytes")],
)
def test_corrupt_download_fails_without_retrying(monkeypatch, overrides, error):
    execute = Mock(side_effect=receipts(**overrides))
    monkeypatch.setattr(transfer, "_pod_json", execute)

    result = run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "verify"
    assert error in result["error"]
    assert execute.call_count == 2
    assert "upload" in result and "download" in result


def test_newly_advertised_content_can_be_retried(monkeypatch):
    upload, download = receipts()
    execute = Mock(side_effect=[upload, FileTransferError("HTTP 404", retryable=True), download])
    sleep = Mock()
    monkeypatch.setattr(transfer, "_pod_json", execute)
    monkeypatch.setattr(transfer.time, "sleep", sleep)

    result = run_transfer()

    assert result["status"] == "passed"
    assert result["download_attempts"] == 2
    assert execute.call_count == 3
    sleep.assert_called_once()


def test_retry_deadline_preserves_the_last_download_error(monkeypatch):
    clock = SimpleNamespace(now=0)
    upload, _ = receipts()

    def execute(_namespace, pod, _script, _timeout):
        if pod.endswith("-0"):
            return upload
        clock.now = 60
        raise FileTransferError("provider unavailable", retryable=True)

    sleep = Mock()
    monkeypatch.setattr(transfer, "_pod_json", execute)
    monkeypatch.setattr(transfer.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(transfer.time, "sleep", sleep)

    result = run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert "provider unavailable" in result["error"]
    assert result["download_attempts"] == 1
    assert result["elapsed_seconds"] == 60
    sleep.assert_not_called()


def test_upload_failure_does_not_attempt_a_download(monkeypatch):
    execute = Mock(side_effect=FileTransferError("upload refused"))
    monkeypatch.setattr(transfer, "_pod_json", execute)

    result = run_transfer()

    assert result["status"] == "failed"
    assert result["stage"] == "upload"
    assert result["error"] == "upload refused"
    execute.assert_called_once()


def test_exec_uses_runner_kubeconfig_and_enforces_a_process_timeout(monkeypatch):
    execute = Mock(return_value=SimpleNamespace(returncode=0, stdout='{"ok": true}', stderr=""))
    monkeypatch.setattr(transfer.subprocess, "run", execute)
    monkeypatch.setattr(transfer, "get_config_file", lambda: "/tmp/kube config")

    assert transfer._pod_json("storage-test", "storage-nodes-0", "printf receipt", 17) == {
        "ok": True
    }

    command = execute.call_args.args[0]
    assert command[:3] == ["kubectl", "--kubeconfig", "/tmp/kube config"]
    assert command[3:] == [
        "exec",
        "--namespace",
        "storage-test",
        "storage-nodes-0",
        "--container",
        "storage",
        "--",
        "/bin/sh",
        "-c",
        "printf receipt",
    ]
    assert execute.call_args.kwargs["timeout"] == 17

    execute.side_effect = subprocess.TimeoutExpired(command, 17, stderr=b"curl: request timed out")
    with pytest.raises(FileTransferError, match="exceeded 17.0s: curl: request timed out"):
        transfer._pod_json("storage-test", "storage-nodes-0", "printf receipt", 17)


@pytest.mark.parametrize(("code", "retryable"), [(22, True), (28, True), (127, False), (1, False)])
def test_exec_distinguishes_transient_curl_failures_from_configuration_errors(
    monkeypatch, code, retryable
):
    monkeypatch.setattr(
        transfer.subprocess,
        "run",
        Mock(return_value=SimpleNamespace(returncode=code, stdout="", stderr="request failed")),
    )
    with pytest.raises(FileTransferError, match="request failed") as error:
        transfer._pod_json("storage-test", "storage-nodes-1", "curl", 10)
    assert error.value.retryable is retryable


def test_invalid_receipt_is_reported_as_an_upload_failure(monkeypatch):
    monkeypatch.setattr(
        transfer.subprocess,
        "run",
        Mock(return_value=SimpleNamespace(returncode=0, stdout="not json", stderr="")),
    )
    result = run_transfer()
    assert result["stage"] == "upload"
    assert result["status"] == "failed"
    assert "invalid transfer receipt" in result["error"]
    json.dumps(result)  # Error reports must remain serializable for file_transfer.json.

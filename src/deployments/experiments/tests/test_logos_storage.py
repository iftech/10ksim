import json
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes.client import ApiClient
from pydantic import ValidationError

from src.deployments.experiments import base_experiment, logos_storage
from src.deployments.experiments.base_experiment import ExperimentFailed
from src.deployments.experiments.logos_storage import (
    API_PORT,
    BOOTSTRAP,
    NODES,
    ExpConfig,
    LogosStorageExperiment,
    build_nodes,
    build_service,
)
from src.utils.yaml_utils import get_YAML


def config(**kwargs):
    return ExpConfig(image={"repo": "test/storage", "tag": "test"}, **kwargs)


def test_private_network_bootstrap_contract():
    bootstrap = build_nodes("storage-test", config(), bootstrap=True)
    nodes = build_nodes("storage-test", config())
    anchor = bootstrap.spec.template.spec.containers[0]
    node = nodes.spec.template.spec.containers[0]

    assert bootstrap.spec.replicas == 1
    assert nodes.spec.replicas == 2
    assert "--no-bootstrap-node" in anchor.args
    assert "--no-bootstrap-node" not in node.args
    assert "BOOTSTRAP_NODE_URL" not in {env.name for env in anchor.env}
    env = {env.name: env.value for env in node.env}
    assert env["BOOTSTRAP_NODE_URL"] == f"http://{BOOTSTRAP}-service.storage-test.svc:{API_PORT}"
    assert env["BOOTSTRAP_NODE_URL_WAIT"] == "300"
    assert node.command is None  # Must retain the SPR-fetching Docker entrypoint.
    assert node.args[0] == "storage"

    for container in (anchor, node):
        assert container.env[0].name == "POD_IP"
        assert container.env[0].value_from.field_ref.field_path == "status.podIP"
        assert container.env[1].value == "extip:$(POD_IP)"
        assert "--listen-port=2345" in container.args
        assert not any(arg.startswith("--mix-") for arg in container.args)


@pytest.mark.parametrize("bootstrap", [True, False])
def test_service_routes_to_its_own_group_and_probe_port(bootstrap):
    nodes = build_nodes("storage-test", config(), bootstrap=bootstrap)
    service = build_service("storage-test", bootstrap=bootstrap)
    container = nodes.spec.template.spec.containers[0]

    assert service.spec.selector == nodes.spec.selector.match_labels
    assert nodes.spec.template.metadata.labels == service.spec.selector
    assert service.metadata.name == nodes.spec.service_name
    assert (
        service.spec.selector
        != build_service("storage-test", bootstrap=not bootstrap).spec.selector
    )
    assert service.spec.cluster_ip == "None"
    ports = {port.name: port.container_port for port in container.ports}
    for port in service.spec.ports:
        assert ports[port.target_port] == port.port
        assert port.protocol == "TCP"
    probe = container.readiness_probe.http_get
    assert probe.path == "/api/storage/v1/spr"
    assert ports[probe.port] == API_PORT
    assert nodes.spec.template.spec.volumes[0].empty_dir is not None


def test_image_is_required_and_network_needs_two_ordinary_nodes():
    with pytest.raises(ValidationError, match="image"):
        ExpConfig()
    with pytest.raises(ValidationError, match="num_nodes"):
        config(num_nodes=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [True, False])
async def test_bootstrap_is_ready_before_nodes_start_and_dry_run_skips_runtime(
    monkeypatch, tmp_path, dry_run
):
    deploy = AsyncMock()
    sleep = AsyncMock()
    capture = Mock(return_value=3)
    monkeypatch.setattr(LogosStorageExperiment, "deploy", deploy)
    monkeypatch.setattr(logos_storage.asyncio, "sleep", sleep)
    monkeypatch.setattr(logos_storage, "capture_pod_logs", capture)
    exp = LogosStorageExperiment(
        api_client=ApiClient(),
        config=config(duration=5, startup_timeout=17),
        namespace="storage-test",
        output_folder=tmp_path,
        dry_run=dry_run,
    )
    monkeypatch.setattr(LogosStorageExperiment, "log_event", Mock())

    await exp._run()

    deployed = [call.args[0] for call in deploy.await_args_list]
    assert [(obj.kind, obj.metadata.name) for obj in deployed] == [
        ("Service", f"{BOOTSTRAP}-service"),
        ("StatefulSet", BOOTSTRAP),
        ("Service", f"{NODES}-service"),
        ("StatefulSet", NODES),
    ]
    assert deploy.await_args_list[1].kwargs == {"wait_for_ready": True, "timeout": 17}
    if dry_run:
        sleep.assert_not_awaited()
        capture.assert_not_called()
    else:
        sleep.assert_awaited_once_with(5)
        capture.assert_called_once_with(
            "storage-test",
            tmp_path / "kubectl_logs",
            exp.api_client,
            label_selector="app=logos-storage",
        )


@pytest.mark.asyncio
async def test_startup_failure_captures_logs_before_cleanup(monkeypatch, tmp_path):
    deploy = AsyncMock(side_effect=[None, TimeoutError("bootstrap never ready")])
    capture = Mock(return_value=1)
    monkeypatch.setattr(LogosStorageExperiment, "deploy", deploy)
    monkeypatch.setattr(LogosStorageExperiment, "log_event", Mock())
    monkeypatch.setattr(logos_storage, "capture_pod_logs", capture)
    exp = LogosStorageExperiment(
        api_client=ApiClient(),
        config=config(),
        namespace="storage-test",
        output_folder=tmp_path,
    )

    with pytest.raises(TimeoutError, match="bootstrap never ready"):
        await exp._run()

    assert deploy.await_count == 2
    capture.assert_called_once()


@pytest.mark.asyncio
async def test_dry_run_saves_all_manifests_and_runner_metadata(monkeypatch, tmp_path):
    apply = Mock()
    cleanup = Mock()
    monkeypatch.setattr(base_experiment, "kubectl_apply", apply)
    monkeypatch.setattr(base_experiment, "get_cleanup", cleanup)
    monkeypatch.setattr(LogosStorageExperiment, "_wait_until_clear", Mock())
    transfer = Mock()
    monkeypatch.setattr(logos_storage, "run_file_transfer", transfer)
    exp = LogosStorageExperiment(
        api_client=ApiClient(),
        config=config(verify_transfer=True),
        namespace="storage-test",
        output_folder=tmp_path,
        dry_run=True,
    )

    await exp.run()

    manifests = [get_YAML().load(path) for path in (tmp_path / "deployment_yamls").glob("*.yaml")]
    # Service and StatefulSet names must not overwrite each other's YAML.
    assert len(manifests) == 4
    assert {manifest["kind"] for manifest in manifests} == {"Service", "StatefulSet"}
    assert apply.call_count == 4
    assert all(call.kwargs["dry_run"] for call in apply.call_args_list)
    cleanup.assert_not_called()
    transfer.assert_not_called()
    assert exp.metadata["stack"]["nodes_per_statefulset"] == [1, 2]
    assert (tmp_path / "metadata.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("passed", [True, False])
async def test_transfer_result_and_logs_survive_cleanup_and_failure(monkeypatch, tmp_path, passed):
    steps = []
    report = {
        "status": "passed" if passed else "failed",
        "stage": "complete" if passed else "verify",
        "cid": "zTestCid",
    }
    if not passed:
        report["error"] = "Downloaded SHA-256 does not match the uploaded file"
    transfer = Mock(return_value=report)
    monkeypatch.setattr(logos_storage, "run_file_transfer", transfer)
    monkeypatch.setattr(base_experiment, "kubectl_apply", Mock())
    monkeypatch.setattr(base_experiment, "wait_for_rollout", AsyncMock())
    monkeypatch.setattr(
        base_experiment, "get_cleanup", Mock(return_value=lambda: steps.append("cleanup"))
    )
    monkeypatch.setattr(LogosStorageExperiment, "_wait_until_clear", Mock())

    def capture(*_args, **_kwargs):
        steps.append("logs")
        assert json.loads((tmp_path / "file_transfer.json").read_text()) == report
        return 3

    monkeypatch.setattr(logos_storage, "capture_pod_logs", capture)
    exp = LogosStorageExperiment(
        api_client=ApiClient(),
        config=config(verify_transfer=True, duration=0),
        namespace="storage-test",
        output_folder=tmp_path,
    )

    if passed:
        await exp.run()
    else:
        with pytest.raises(ExperimentFailed, match="SHA-256"):
            await exp.run()

    transfer.assert_called_once_with(
        namespace="storage-test",
        source_pod="storage-nodes-0",
        target_pod="storage-nodes-1",
        file_size_bytes=1048576,
        timeout=60,
        api_port=8080,
    )
    assert steps == ["logs", "cleanup", "cleanup", "cleanup", "cleanup"]
    assert (tmp_path / "metadata.json").exists()

"""Start a private Logos Storage network and optionally verify a file transfer."""

import asyncio
import json

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HTTPGetAction,
    V1LabelSelector,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
    V1Volume,
    V1VolumeMount,
)
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

from src.deployments.core.configs.container import Image
from src.deployments.core.configs.helpers.utils import find_container_config
from src.deployments.core.pod_logs import capture_pod_logs
from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.logos_storage.transfer import run_file_transfer
from src.deployments.pod_api_requester.builder import (
    DEFAULT_REQUESTER_APP,
    PodApiRequesterBuilder,
)
from src.deployments.registry import experiment

APP = "logos-storage"
BOOTSTRAP = "storage-bootstrap"
NODES = "storage-nodes"
P2P_PORT = 2345
API_PORT = 8080
METRICS_PORT = 8008
PORTS = {"p2p": P2P_PORT, "api": API_PORT, "metrics": METRICS_PORT}


class ExpConfig(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True)

    image: Image
    """Image with the upstream Docker entrypoint and the current Storage CLI."""
    requester_image: Image = Field(
        default_factory=lambda: Image(repo="albertodst/pod-api-requester", tag="logos-storage-v1")
    )
    """Requester image providing /storage/upload and /storage/download."""
    num_nodes: int = Field(default=2, ge=2)
    """Ordinary storage nodes, in addition to one bootstrap node."""
    duration: NonNegativeInt = 120
    """Seconds to keep the network up after readiness and the optional transfer check."""
    startup_timeout: PositiveInt = 300
    """Seconds to wait for each StatefulSet, including the image pull."""
    log_level: str = "INFO"
    capture_pod_logs: bool = True
    verify_transfer: bool = False
    """Upload fresh content to node 0 and verify downloads through every other ordinary node."""
    download_concurrency: PositiveInt = 3
    """Maximum simultaneous receiver checks, each with its own transfer timeout."""
    file_size_bytes: PositiveInt = 1048576
    """Random payload size for the transfer check (default: 1 MiB)."""
    transfer_timeout: PositiveInt = 60
    """Seconds allowed for upload, and for each active receiver including discovery retries."""


def build_requester(namespace: str, config: ExpConfig) -> PodApiRequesterBuilder:
    requester = (
        PodApiRequesterBuilder()
        .with_namespace(namespace)
        .with_mode("server")
        .with_image(config.requester_image)
    )
    container = find_container_config(requester.config, requester._container_name)
    container.readiness_probe = V1Probe(
        http_get=V1HTTPGetAction(path="/openapi.json", port=8645),
        period_seconds=2,
        timeout_seconds=2,
        failure_threshold=3,
    )
    return requester


def build_service(namespace: str, *, bootstrap: bool = False) -> V1Service:
    name = BOOTSTRAP if bootstrap else NODES
    labels = {"app": APP, "group": name}
    return V1Service(
        api_version="v1",
        kind="Service",
        metadata=V1ObjectMeta(name=f"{name}-service", namespace=namespace, labels=labels),
        spec=V1ServiceSpec(
            cluster_ip="None",
            publish_not_ready_addresses=True,
            selector=labels,
            ports=[
                V1ServicePort(name=key, port=port, target_port=key, protocol="TCP")
                for key, port in PORTS.items()
            ],
        ),
    )


def build_nodes(namespace: str, config: ExpConfig, *, bootstrap: bool = False) -> V1StatefulSet:
    name = BOOTSTRAP if bootstrap else NODES
    labels = {"app": APP, "group": name}
    env = [
        V1EnvVar(
            name="POD_IP",
            value_from=V1EnvVarSource(field_ref=V1ObjectFieldSelector(field_path="status.podIP")),
        ),
        # Kubernetes expands POD_IP using the preceding environment variable.
        V1EnvVar(name="STORAGE_NAT", value="extip:$(POD_IP)"),
    ]
    args = [
        "storage",
        "--data-dir=/data",
        "--listen-ip=0.0.0.0",
        f"--listen-port={P2P_PORT}",
        "--api-bindaddr=0.0.0.0",
        f"--api-port={API_PORT}",
        "--metrics",
        "--metrics-address=0.0.0.0",
        f"--metrics-port={METRICS_PORT}",
        f"--log-level={config.log_level}",
    ]
    if bootstrap:
        args.append("--no-bootstrap-node")
    else:
        # The image entrypoint fetches /api/storage/v1/spr and sets STORAGE_BOOTSTRAP_NODE.
        env.extend(
            [
                V1EnvVar(
                    name="BOOTSTRAP_NODE_URL",
                    value=f"http://{BOOTSTRAP}-service.{namespace}.svc:{API_PORT}",
                ),
                V1EnvVar(name="BOOTSTRAP_NODE_URL_WAIT", value=str(config.startup_timeout)),
            ]
        )

    container = V1Container(
        name="storage",
        image=str(config.image),
        image_pull_policy="Always",
        # Retain the image entrypoint, which handles the bootstrap SPR.
        args=args,
        env=env,
        ports=[
            V1ContainerPort(name=key, container_port=port, protocol="TCP")
            for key, port in PORTS.items()
        ],
        readiness_probe=V1Probe(
            http_get=V1HTTPGetAction(path="/api/storage/v1/spr", port="api"),
            period_seconds=2,
            timeout_seconds=2,
            failure_threshold=3,
        ),
        volume_mounts=[V1VolumeMount(name="data", mount_path="/data")],
    )
    return V1StatefulSet(
        api_version="apps/v1",
        kind="StatefulSet",
        metadata=V1ObjectMeta(name=name, namespace=namespace, labels=labels),
        spec=V1StatefulSetSpec(
            service_name=f"{name}-service",
            replicas=1 if bootstrap else config.num_nodes,
            pod_management_policy="Parallel",
            selector=V1LabelSelector(match_labels=labels),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels=labels),
                spec=V1PodSpec(
                    containers=[container],
                    volumes=[V1Volume(name="data", empty_dir=V1EmptyDirVolumeSource())],
                ),
            ),
        ),
    )


@experiment(name="logos-storage")
class LogosStorageExperiment(BaseExperiment[ExpConfig]):
    """Start a private network, with an optional file-transfer check and Mix disabled."""

    async def _check_transfer(self):
        self.log_event("file_transfer_start")
        report = await run_file_transfer(
            namespace=self.namespace,
            source_pod=f"{NODES}-0",
            target_pods=[f"{NODES}-{index}" for index in range(1, self.config.num_nodes)],
            file_size_bytes=self.config.file_size_bytes,
            timeout=self.config.transfer_timeout,
            api_port=API_PORT,
            concurrency=self.config.download_concurrency,
        )
        if self.output_folder is not None:
            (self.output_folder / "file_transfer.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )
        self.log_event({"event": "file_transfer_finished", **report})
        if report["status"] != "passed":
            self.fail_run(f"File transfer failed during {report['stage']}: {report['error']}")

    async def _run(self):
        self.log_event("run_start")
        try:
            for bootstrap in (True, False):
                await self.deploy(
                    build_service(self.namespace, bootstrap=bootstrap), wait_for_ready=False
                )
                await self.deploy(
                    build_nodes(self.namespace, self.config, bootstrap=bootstrap),
                    wait_for_ready=True,
                    timeout=self.config.startup_timeout,
                )

            if self.config.verify_transfer:
                requester = build_requester(self.namespace, self.config)
                dependencies = requester.build_dependencies()
                config_map = dependencies["requester_base"]["config_maps"][0]
                config_map.data["config.yaml"] = (
                    "targets: []\nendpoints: []\nrequests: []\nactions: []\n"
                )
                await self.deploy(dependencies, wait_for_ready=False)
                await self.deploy(
                    requester.build(), wait_for_ready=True, timeout=self.config.startup_timeout
                )

            if self.dry_run:
                return

            self.log_event("nodes_ready")
            if self.config.verify_transfer:
                await self._check_transfer()
            await asyncio.sleep(self.config.duration)
            self.log_event("observation_finished")
        finally:
            if not self.dry_run and self.config.capture_pod_logs and self.output_folder is not None:
                captured = await asyncio.to_thread(
                    capture_pod_logs,
                    self.namespace,
                    self.output_folder / "kubectl_logs",
                    self.api_client,
                    label_selector=(
                        f"app in ({APP},{DEFAULT_REQUESTER_APP})"
                        if self.config.verify_transfer
                        else f"app={APP}"
                    ),
                )
                self.log_event({"event": "log_capture_finished", "pods": captured})

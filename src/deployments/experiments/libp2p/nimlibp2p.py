import asyncio
import logging
import random
import time
import traceback
from typing import ClassVar, Literal, Optional

from kubernetes.client import V1Probe, V1ServicePort, V1StatefulSet, V1TCPSocketAction
from pydantic import BaseModel, ConfigDict, Field, NonNegativeFloat, NonNegativeInt, model_validator

from src.deployments.core.builders import ServiceBuilder
from src.deployments.core.configs.container import Image
from src.deployments.core.k8s_rollout import resolved_images
from src.deployments.core.pod_logs import capture_pod_logs
from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.libp2p.bridge import Bridge
from src.deployments.libp2p.builders.builders import Libp2pStatefulSetBuilder
from src.deployments.libp2p.builders.builders import Option as NimLibp2p
from src.deployments.libp2p.builders.helpers import readiness_probe_metrics
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.nimlibp2p import libp2p_dst_node_publish
from src.deployments.pod_api_requester.pod_api_requester import PodApiApplicationError, PodApiError
from src.deployments.registry import experiment

logger = logging.getLogger(__name__)

Muxer = Literal["yamux", "quic", "mplex"]
Discovery = Literal["static", "kad-dht"]

BOOTSTRAP_NAME = "bootstrap"


class ExpConfig(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True)

    num_relay_nodes: NonNegativeInt = 30
    num_messages: NonNegativeInt = 20
    message_size_bytes: NonNegativeInt = 1000
    delay_cold_start: NonNegativeFloat = 60
    delay_after_publish: NonNegativeFloat = 1
    muxer: Muxer = "yamux"
    image: Image = Image(repo="pearsonwhite/dst-nimlibp2p-logging", tag="wip-4.2-1.16.0-amd")
    bootstrap_image: Optional[Image] = None
    """Image for the kad anchor when it is a separate binary; defaults to `image`."""
    discovery: Discovery = "static"
    """ "kad-dht" discovers peers through a bootstrap node; "static" uses the CONNECTTO dial."""
    connect_to: NonNegativeInt = 10
    """Number of nodes to try to connect to for each node when starting up"""
    bootstrap_nodes: NonNegativeInt = 1
    network_delay: NonNegativeInt = 0
    network_jitter: NonNegativeInt = 0
    network_bandwidth_mbit: NonNegativeInt = 0  # 0 = uncapped; folded into the netem qdisc
    network_loss_pct: float = Field(default=0, ge=0, le=100)  # percent; folded into the netem qdisc
    node_start_delay: NonNegativeInt = 60
    post_publish_dwell: NonNegativeInt = 90
    max_connections: NonNegativeInt = 250
    """Per-node connection cap."""
    max_failed_publishes: NonNegativeInt = 0
    """Publishes that may fail before the run is treated as invalid."""
    capture_pod_logs: bool = True
    """Pull pod logs with the run, so the analysis does not depend on the log collector."""
    log_capture_lead: NonNegativeInt = 45
    """Seconds of the dwell reserved for the capture; 1000 pods take about 30s."""
    wait_nodes_ready: bool = True

    @model_validator(mode="after")
    def _check_bootstrap_nodes(self):
        if self.discovery == "kad-dht" and self.bootstrap_nodes < 1:
            raise ValueError("kad-dht discovery requires bootstrap_nodes >= 1")
        return self


def bootstrap_dns(namespace: str) -> str:
    return f"{BOOTSTRAP_NAME}.{namespace}.svc.cluster.local"


def build_nodes(
    namespace: str,
    params: ExpConfig,
) -> V1StatefulSet:
    builder = (
        Libp2pStatefulSetBuilder()
        .with_libp2p_config(
            name="pod",
            namespace=namespace,
            num_nodes=params.num_relay_nodes,
            dns_searches=["nimp2p-service"],
        )
        .with_option(NimLibp2p.peers, params.num_relay_nodes)
        .with_option(NimLibp2p.self_trigger, True)
        .with_option(NimLibp2p.muxer, params.muxer)
        .with_option(NimLibp2p.cold_start_delay, params.node_start_delay)
        .with_option(NimLibp2p.max_connections, params.max_connections)
        .with_readiness_probe(readiness_probe_metrics())
        .with_image(params.image)
    )
    if params.discovery == "kad-dht":
        builder = (
            builder.with_option(NimLibp2p.node_role, "RoleNormal")
            .with_option(NimLibp2p.discovery, "kad-dht")
            .with_option(NimLibp2p.service, bootstrap_dns(namespace))
        )
    else:
        builder = builder.with_option(NimLibp2p.service, "nimp2p-service").with_option(
            NimLibp2p.connect_to, params.connect_to
        )
    if (
        params.network_delay
        or params.network_jitter
        or params.network_bandwidth_mbit
        or params.network_loss_pct
    ):
        builder = builder.with_network_delay(
            delay=params.network_delay,
            jitter=params.network_jitter,
            rate_mbit=params.network_bandwidth_mbit or None,
            loss_pct=params.network_loss_pct or None,
        )

    return builder.build()


def build_bootstrap_service(namespace: str):
    return (
        ServiceBuilder()
        .with_name(BOOTSTRAP_NAME)
        .with_namespace(namespace)
        .with_cluster_ip("None")
        .with_selector("app", "zerotenkay")
        .with_selector("role", "bootstrap")
        .with_port(V1ServicePort(name="p2p", port=5000, target_port=5000))
        .build()
    )


def build_static_service(namespace: str):
    # Headless DNS for the static dial; publish_not_ready_addresses lets nodes
    # resolve each other before any is ready, else the mesh can't bootstrap.
    return (
        ServiceBuilder()
        .with_name("nimp2p-service")
        .with_namespace(namespace)
        .with_cluster_ip("None")
        .with_publish_not_ready_addresses(True)
        .with_selector("app", "zerotenkay")
        .with_port(V1ServicePort(name="p2p-quic", port=5000, target_port=5000, protocol="UDP"))
        .with_port(V1ServicePort(name="p2p-tcp", port=5000, target_port=5000, protocol="TCP"))
        .with_port(V1ServicePort(name="metrics", port=8008, target_port=8008, protocol="TCP"))
        .with_port(V1ServicePort(name="publish", port=8645, target_port=8645, protocol="TCP"))
        .build()
    )


def build_bootstrap_nodes(namespace: str, params: ExpConfig) -> V1StatefulSet:
    # Every node holds a link to the anchor, so it must accept more than num_relay_nodes.
    # Probe the metrics port (always TCP, unlike the p2p port under quic).
    return (
        Libp2pStatefulSetBuilder()
        .with_libp2p_config(
            name=BOOTSTRAP_NAME, namespace=namespace, num_nodes=params.bootstrap_nodes
        )
        .with_label("role", "bootstrap")
        .with_option(NimLibp2p.node_role, "RoleBootstrap")
        .with_option(NimLibp2p.discovery, "kad-dht")
        .with_option(NimLibp2p.muxer, params.muxer)
        .with_option(NimLibp2p.max_connections, params.num_relay_nodes + 100)
        .with_readiness_probe(
            V1Probe(
                tcp_socket=V1TCPSocketAction(port=8008),
                initial_delay_seconds=5,
                period_seconds=2,
                failure_threshold=3,
            )
        )
        .with_image(params.bootstrap_image or params.image)
        .build()
    )


async def publish(config, namespace, random_name) -> bool:
    """Publish one message. Returns whether it reached the node."""
    try:
        target = Target(
            name="libp2p-node",
            name_template=random_name,
            service="nimp2p-service",
            port=8645,
        )
        await libp2p_dst_node_publish(
            namespace=namespace, target=target, msg_size_bytes=config.message_size_bytes
        )
        return True
    except PodApiApplicationError as e:
        logger.error(f"PodApiApplicationError: {e} {traceback.format_exc()}")
    except PodApiError as e:
        logger.error(f"PodApiError: {e} {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Other exception: {e} {traceback.format_exc()}")
    return False


@experiment(name="nimlibp2p")
class NimLibp2pExperiment(BaseExperiment[ExpConfig]):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    post_run_analysis: ClassVar[str] = "src.analysis.post_run.nimlibp2p:run_nimlibp2p_analysis"

    def _get_metadata(self) -> dict:
        return Bridge().get_metadata(self.events_log_path)

    async def _after_nodes(self, nodes: V1StatefulSet) -> None:
        """Runs once the nodes are up, before the cold start they form the mesh during.

        Scenarios override this to shape the network the mesh will form over. Nodes hold
        off dialling for `node_start_delay`, so anything done here lands before they
        connect, provided that delay outlasts the deploy.
        """

    async def _mid_run(self, nodes: V1StatefulSet) -> None:
        """Runs alongside the publish loop. Scenarios override this to disturb the network."""

    def _publishable_nodes(self) -> int:
        """How many of the relays the publisher may target, counted from index 0."""
        return self.config.num_relay_nodes

    async def _run(self):
        self.log_event("run_start")

        # Publisher, with the config map, role and service it mounts and resolves.
        requester = PodApiRequesterBuilder().with_namespace(self.namespace).with_mode("server")
        await self.deploy(deployment=requester.build_dependencies(), exist_ok=True)
        await self.deploy(deployment=requester.build(), wait_for_ready=True)

        # Backs the StatefulSet's per-pod DNS and is what the publisher resolves nodes
        # through, so both discovery modes need it, not just the static dial.
        node_service = build_static_service(self.namespace)
        self.dump_yaml(node_service, "nimp2p-service")
        await self.deploy(deployment=node_service, exist_ok=True)

        # Bootstrap (kad-dht only): anchor node + headless discovery service. Deployed
        # before the nodes so the mesh can form through it once the nodes wake up.
        if self.config.discovery == "kad-dht":
            bootstrap_service = build_bootstrap_service(self.namespace)
            self.dump_yaml(bootstrap_service, "bootstrap-service")
            await self.deploy(deployment=bootstrap_service)

            bootstrap = build_bootstrap_nodes(namespace=self.namespace, params=self.config)
            self.dump_yaml(bootstrap, "bootstrap")
            await self.deploy(deployment=bootstrap, wait_for_ready=True)

        # Nodes
        nodes = build_nodes(
            namespace=self.namespace,
            params=self.config,
        )
        name = nodes.metadata.name
        namespace = nodes.metadata.namespace

        await self.deploy(deployment=nodes, wait_for_ready=self.config.wait_nodes_ready)

        await self._after_nodes(nodes)

        await asyncio.sleep(self.config.delay_cold_start)

        # After the cold start, not the deploy: a scenario that does not wait for readiness
        # has pods with no container status yet, which would record no digest at all.
        self.log_event(
            {
                "event": "images_resolved",
                "requested": f"{self.config.image.repo}:{self.config.image.tag}",
                "resolved": resolved_images(name, namespace, self.api_client),
            }
        )

        logger.info(f"Starting publish loop for nodes in `{name}`")

        self.log_event("start_messages")

        mid_run = asyncio.create_task(self._mid_run(nodes))

        tasks = []
        for msg_index in range(self.config.num_messages):
            index = random.randint(0, self._publishable_nodes() - 1)
            random_name = f"{name}-{index}"
            self.log_event({"event": "publish", "node": random_name, "index": msg_index})
            tasks.append(asyncio.create_task(publish(self.config, namespace, random_name)))
            await asyncio.sleep(self.config.delay_after_publish)
        published = await asyncio.gather(*tasks)
        failed = published.count(False)
        self.log_event({"event": "publish_summary", "attempted": len(published), "failed": failed})

        self.log_event("publisher_messages_finished")

        await mid_run

        await self._dwell_and_capture()
        self.log_event("publisher_wait_finished")

        if failed > self.config.max_failed_publishes:
            self.fail_run(
                f"{failed} of {len(published)} messages were never published, so delivery "
                f"is measured against a denominator the run did not send"
            )

        self.log_event("internal_run_finished")

    async def _dwell_and_capture(self) -> None:
        """Wait out the post-publish dwell, capturing pod logs before the pods go away.

        The capture has to land inside the dwell, not after it: cleanup starts in the same
        second the dwell ends and pulling a thousand logs takes longer than the pods last.
        """
        dwell = self.config.post_publish_dwell
        if not self.config.capture_pod_logs or self.output_folder is None:
            await asyncio.sleep(dwell)
            return

        started = time.monotonic()
        # Let late deliveries land before reading the logs, but leave room to pull them.
        await asyncio.sleep(max(dwell - self.config.log_capture_lead, 0))

        self.log_event("log_capture_start")
        captured = await asyncio.to_thread(
            capture_pod_logs,
            self.namespace,
            self.output_folder / "kubectl_logs",
            self.api_client,
        )
        self.log_event({"event": "log_capture_finished", "pods": captured})

        await asyncio.sleep(max(dwell - (time.monotonic() - started), 0))

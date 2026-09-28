import asyncio
import logging
import random
import traceback
from typing import Dict, List, Literal

from kubernetes.client import V1ServicePort
from pydantic import BaseModel, ConfigDict, Field, NonNegativeFloat, NonNegativeInt

from src.deployments.core.builders import ServiceBuilder
from src.deployments.core.configs.container import Image
from src.deployments.experiments.base_experiment import BaseExperiment, V1Deployable
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import PodApiApplicationError, PodApiError
from src.deployments.pod_api_requester.waku import (
    DEFAULT_CONTENT_TOPIC,
    waku_lightpush_publish,
    waku_publish,
)
from src.deployments.registry import experiment
from src.deployments.waku.bridge import Bridge
from src.deployments.waku.builders.builders import WakuStatefulSetBuilder

logger = logging.getLogger(__name__)

LogLevel = Literal["INFO", "DEBUG", "TRACE"]
Protocol = Literal["relay", "lightpush"]

WAKU_REST_PORT = 8645

BOOTSTRAP_SERVICE = "zerotesting-bootstrap"
FILTER_SERVER_SERVICE = "zerotesting-service"
LIGHTPUSH_SERVER_SERVICE = "zerotesting-lightpush-server"
STORE_SERVICE = "zerotesting-store"
FILTER_CLIENT_SERVICE = "zerotesting-filter"
LIGHTPUSH_CLIENT_SERVICE = "zerotesting-lightpush-client"

FILTER_SERVER_APP = "zerotenkay"
LIGHTPUSH_SERVER_APP = "zerotenkay-lightpush-server"
STORE_APP = "zerotenkay-store"
FILTER_CLIENT_APP = "zerotenkay-filter"
LIGHTPUSH_CLIENT_APP = "zerotenkay-lightpush-client"

OWNED_SERVICES = {
    LIGHTPUSH_SERVER_SERVICE: LIGHTPUSH_SERVER_APP,
    STORE_SERVICE: STORE_APP,
    FILTER_CLIENT_SERVICE: FILTER_CLIENT_APP,
    LIGHTPUSH_CLIENT_SERVICE: LIGHTPUSH_CLIENT_APP,
}
"""Services this experiment creates. `zerotesting-service` and `-bootstrap` are namespace fixtures."""


class ExpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    num_bootstrap_nodes: NonNegativeInt = 5
    num_filter_servers: NonNegativeInt = 100
    num_lightpush_servers: NonNegativeInt = 100
    num_store_nodes: NonNegativeInt = 10
    num_filter_clients: NonNegativeInt = 500
    num_lightpush_clients: NonNegativeInt = 500
    num_messages: NonNegativeInt = 600
    msg_size_kbytes: NonNegativeInt = 1
    content_topic: str = DEFAULT_CONTENT_TOPIC
    protocols: List[Protocol] = ["relay", "lightpush"]
    delay_cold_start: NonNegativeFloat = 300
    delay_after_publish: NonNegativeFloat = 1
    post_publish_dwell: NonNegativeFloat = 60
    log_level: LogLevel = "INFO"
    cmd_type: int = 1
    num_enrs: NonNegativeInt = 3
    image: Image = Field(
        default_factory=lambda: Image.from_str("soutullostatus/nwaku-jq-curl:v0.37.0-rc.4")
    )


def cluster_id_of(cmd_type: int) -> int:
    if cmd_type == 1:
        return 2
    if cmd_type == 2:
        return 0
    raise ValueError(f"Invalid cmd_type: `{cmd_type}`")


def _base(name: str, namespace: str, num_nodes: NonNegativeInt) -> WakuStatefulSetBuilder:
    return WakuStatefulSetBuilder().with_waku_config(
        name=name, namespace=namespace, num_nodes=num_nodes
    )


def _finish(builder: WakuStatefulSetBuilder, config: ExpConfig) -> V1Deployable:
    """Applied last so it overrides whatever the role set."""
    builder.with_args({"--log-level": config.log_level}, on_duplicate="replace")
    builder.with_image(config.image)
    return builder.build()


def build_services(namespace: str) -> List[V1Deployable]:
    return [
        ServiceBuilder()
        .with_name(service)
        .with_namespace(namespace)
        .with_selector("app", app)
        .with_port(V1ServicePort(protocol="TCP", port=WAKU_REST_PORT, target_port=WAKU_REST_PORT))
        .with_cluster_ip("None")
        .build()
        for service, app in OWNED_SERVICES.items()
    ]


def build_bootstrap(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("bootstrap-0", namespace, config.num_bootstrap_nodes)
        .with_bootstrap(config.cmd_type)
        .with_env("SERVICE", BOOTSTRAP_SERVICE)
    )
    return _finish(builder, config)


def build_filter_servers(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("fserver-0", namespace, config.num_filter_servers)
        .with_regression(config.cmd_type, config.num_enrs)
        .with_filter_server()
        .with_env("SERVICE", BOOTSTRAP_SERVICE)
    )
    return _finish(builder, config)


def build_lightpush_servers(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("lpserver-0", namespace, config.num_lightpush_servers)
        .with_regression(config.cmd_type, config.num_enrs)
        .with_lightpush_server(LIGHTPUSH_SERVER_APP, LIGHTPUSH_SERVER_SERVICE)
        .with_env("SERVICE", BOOTSTRAP_SERVICE)
    )
    return _finish(builder, config)


def build_store_nodes(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("store-0", namespace, config.num_store_nodes)
        .with_regression(config.cmd_type, config.num_enrs)
        .with_store()
        .with_env("SERVICE", BOOTSTRAP_SERVICE)
    )
    return _finish(builder, config)


def build_filter_clients(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("fclient-0", namespace, config.num_filter_clients)
        .with_light_client(config.cmd_type, FILTER_CLIENT_APP, FILTER_CLIENT_SERVICE)
        .with_filter_addrs(1, f"{FILTER_SERVER_SERVICE}.{namespace}")
        .with_args({"--content-topic": config.content_topic}, on_duplicate="replace")
    )
    return _finish(builder, config)


def build_lightpush_clients(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = (
        _base("lpclient-0", namespace, config.num_lightpush_clients)
        .with_light_client(config.cmd_type, LIGHTPUSH_CLIENT_APP, LIGHTPUSH_CLIENT_SERVICE)
        .with_lightpush_addrs(1, f"{LIGHTPUSH_SERVER_SERVICE}.{namespace}")
    )
    return _finish(builder, config)


def build_nodes(namespace: str, config: ExpConfig) -> Dict[str, V1Deployable]:
    return {
        "bootstrap": build_bootstrap(namespace, config),
        "filter_server": build_filter_servers(namespace, config),
        "lightpush_server": build_lightpush_servers(namespace, config),
        "store": build_store_nodes(namespace, config),
        "filter_client": build_filter_clients(namespace, config),
        "lightpush_client": build_lightpush_clients(namespace, config),
    }


async def publish(
    protocol: Protocol,
    namespace: str,
    pod_name: str,
    service: str,
    msg_size_kbytes: NonNegativeInt,
    cluster_id: int,
    content_topic: str,
):
    target = Target(name="waku-node", name_template=pod_name, service=service, port=WAKU_REST_PORT)
    try:
        if protocol == "relay":
            await waku_publish(
                namespace=namespace,
                target=target,
                content_topic=content_topic,
                msg_size_kbytes=msg_size_kbytes,
                cluster_id=cluster_id,
            )
        else:
            await waku_lightpush_publish(
                namespace=namespace,
                target=target,
                content_topic=content_topic,
                msg_size_kbytes=msg_size_kbytes,
                cluster_id=cluster_id,
            )
    except PodApiApplicationError as e:
        logger.error(f"PodApiApplicationError: {e} {traceback.format_exc()}")
    except PodApiError as e:
        logger.error(f"PodApiError: {e} {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Other exception: {e} {traceback.format_exc()}")


@experiment(name="full-logos-delivery")
class FullLogosDeliveryExperiment(BaseExperiment[ExpConfig]):
    """Relay, filter, lightpush and store nodes in one network."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _get_metadata(self) -> dict:
        return Bridge().get_metadata(self.events_log_path)

    def log_event(self, event):
        logger.info(event)
        return super().log_event(event)

    def _publish_target(self, protocol: Protocol) -> tuple:
        """Relay goes straight into the mesh; lightpush goes through an edge node."""
        if protocol == "relay":
            return "fserver-0", self.config.num_filter_servers, FILTER_SERVER_SERVICE
        return "lpclient-0", self.config.num_lightpush_clients, LIGHTPUSH_CLIENT_SERVICE

    async def _publish_loop(self, cluster_id: int):
        if not self.config.protocols:
            raise ValueError("No protocols to publish with.")

        self.log_event("start_messages")
        tasks = []
        for message_index in range(0, self.config.num_messages):
            protocol = self.config.protocols[message_index % len(self.config.protocols)]
            name, num_nodes, service = self._publish_target(protocol)
            pod_name = f"{name}-{random.randint(0, num_nodes - 1)}"
            self.log_event(
                {
                    "event": "publish",
                    "protocol": protocol,
                    "node": pod_name,
                    "message_index": message_index,
                }
            )
            tasks.append(
                asyncio.create_task(
                    publish(
                        protocol,
                        self.namespace,
                        pod_name,
                        service,
                        self.config.msg_size_kbytes,
                        cluster_id,
                        self.config.content_topic,
                    )
                )
            )
            await asyncio.sleep(self.config.delay_after_publish)
        await asyncio.gather(*tasks)
        self.log_event("publisher_messages_finished")

    async def _run(self):
        self.log_event("run_start")
        cluster_id = cluster_id_of(self.config.cmd_type)

        publisher_builder = (
            PodApiRequesterBuilder().with_namespace(self.namespace).with_mode("server")
        )
        publisher = {"pod": publisher_builder.build(), **publisher_builder.build_dependencies()}
        await self.deploy(deployment=publisher, wait_for_ready=True, exist_ok=True)

        await self.deploy(deployment=build_services(self.namespace), exist_ok=True)

        deployments = build_nodes(self.namespace, self.config)

        await self.deploy(deployment=deployments["bootstrap"], wait_for_ready=True)

        # The relay nodes share one mesh, so they have to come up together to reach mesh health.
        relays = [deployments[key] for key in ("filter_server", "lightpush_server", "store")]
        await self.deploy(deployment=relays, wait_for_ready=True)

        # Clients resolve their service node address at init, so servers must be up first.
        clients = [deployments[key] for key in ("filter_client", "lightpush_client")]
        await self.deploy(deployment=clients, wait_for_ready=True)

        await asyncio.sleep(self.config.delay_cold_start)
        self.log_event("nodes_settled")

        await self._publish_loop(cluster_id)

        await asyncio.sleep(self.config.post_publish_dwell)

        self.log_event("internal_run_finished")

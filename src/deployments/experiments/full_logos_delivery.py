import asyncio
import json
import logging
import os
import random
import time
import traceback
from pathlib import Path
from typing import Dict, List, Literal
from urllib.parse import urlencode

from kubernetes.client import V1ServicePort
from pydantic import BaseModel, ConfigDict, Field, NonNegativeFloat, NonNegativeInt

from src.deployments.core.builders import ServiceBuilder
from src.deployments.core.configs.container import Image
from src.deployments.core.pod_interaction import exec_command_in_pod
from src.deployments.experiments.base_experiment import BaseExperiment, V1Deployable
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import PodApiApplicationError, PodApiError
from src.deployments.pod_api_requester.waku import (
    DEFAULT_CONTENT_TOPIC,
    pubsub_topic,
    waku_lightpush_publish,
    waku_publish,
)
from src.deployments.registry import experiment
from src.deployments.waku.bridge import Bridge
from src.deployments.waku.builders.builders import WakuStatefulSetBuilder
from src.deployments.waku.builders.helpers import WAKU_CONTAINER_NAME

logger = logging.getLogger(__name__)

LogLevel = Literal["INFO", "DEBUG", "TRACE"]
Protocol = Literal["relay", "lightpush"]

WAKU_REST_PORT = 8645
STORE_PAGE_SIZE = 100
FILTER_SUBSCRIBE_BATCH = 50
FILTER_PING_INTERVAL_S = 120
"""Well inside the filter server's 5 minute subscription lifetime, so a slow round still fits."""

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


async def store_message_hashes(
    namespace: str,
    pod_name: str,
    content_topic: str,
    topic: str,
    page_size: int = STORE_PAGE_SIZE,
) -> List[str]:
    """Message hashes one store node holds, following the pagination cursor."""
    hashes: List[str] = []
    cursor = ""
    while True:
        params = {"contentTopics": content_topic, "pubsubTopic": topic, "pageSize": page_size}
        if cursor:
            params["cursor"] = cursor
        url = f"http://127.0.0.1:{WAKU_REST_PORT}/store/v3/messages?{urlencode(params)}"
        command = exec_command_in_pod(
            namespace,
            pod_name,
            ["/bin/sh", "-c", f"curl -s -H 'accept: application/json' '{url}'"],
            container=WAKU_CONTAINER_NAME,
        )
        await command.collect_output_async()
        if not command.ok:
            raise RuntimeError(f"Store query failed on `{pod_name}`: `{command.output}`")

        page = json.loads(command.output)
        messages = page.get("messages") or []
        hashes.extend(message["messageHash"] for message in messages)
        cursor = page.get("paginationCursor")
        if not messages or not cursor:
            return hashes


async def _filter_rest_call(namespace: str, pod_name: str, curl_args: str, action: str):
    """Run one filter REST call inside an edge node and require an OK status."""
    # The exec handshake blocks, and this runs alongside the publish loop.
    command = await asyncio.to_thread(
        exec_command_in_pod,
        namespace,
        pod_name,
        ["/bin/sh", "-c", f"curl -s {curl_args}"],
        container=WAKU_CONTAINER_NAME,
    )
    await command.collect_output_async()
    if not command.ok:
        raise RuntimeError(f"{action} failed on `{pod_name}`: `{command.output}`")
    if json.loads(command.output).get("statusDesc") != "OK":
        raise RuntimeError(f"{action} rejected on `{pod_name}`: `{command.output}`")


async def filter_subscribe(namespace: str, pod_name: str, content_topic: str, topic: str):
    """Ask one edge node to subscribe. --filternode only names the peer to ask."""
    body = json.dumps(
        {"requestId": pod_name, "contentFilters": [content_topic], "pubsubTopic": topic}
    )
    url = f"http://127.0.0.1:{WAKU_REST_PORT}/filter/v2/subscriptions"
    await _filter_rest_call(
        namespace,
        pod_name,
        f"-X POST {url} -H 'Content-Type: application/json' -d '{body}'",
        "Subscribe",
    )


async def filter_ping(namespace: str, pod_name: str):
    """Refresh one edge node's subscription, which the server drops after 5 minutes."""
    url = f"http://127.0.0.1:{WAKU_REST_PORT}/filter/v2/subscriptions/{pod_name}"
    await _filter_rest_call(namespace, pod_name, url, "Ping")


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

    def dump(self, obj, file_name):
        out_path = Path(self.output_folder) / file_name
        os.makedirs(out_path.parent, exist_ok=True)
        with open(out_path, "w") as out_file:
            out_file.write(obj)

    async def dump_store_messages(self, topic: str):
        """Read each store node's archive separately, so one empty node is visible."""
        if self.dry_run:
            return
        for index in range(0, self.config.num_store_nodes):
            pod_name = f"store-0-{index}"
            try:
                hashes = await store_message_hashes(
                    self.namespace, pod_name, self.config.content_topic, topic
                )
            except Exception as e:
                logger.error(f"Failed to read the archive of `{pod_name}`: {e}")
                continue
            self.dump(json.dumps(hashes), Path("store_messages") / f"{pod_name}.json")
            self.log_event({"event": "store_archive", "node": pod_name, "messages": len(hashes)})

    async def _each_filter_client(self, action, *args) -> int:
        """Run `action` on every filter client in batches, returning how many succeeded."""
        pods = [f"fclient-0-{index}" for index in range(0, self.config.num_filter_clients)]
        succeeded = 0
        for start in range(0, len(pods), FILTER_SUBSCRIBE_BATCH):
            batch = pods[start : start + FILTER_SUBSCRIBE_BATCH]
            results = await asyncio.gather(
                *(action(self.namespace, pod, *args) for pod in batch),
                return_exceptions=True,
            )
            for pod, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"`{action.__name__}` failed on `{pod}`: {result}")
                else:
                    succeeded += 1
        return succeeded

    async def subscribe_filter_clients(self, topic: str):
        """Subscribe every filter client, in batches so the API server keeps up."""
        if self.dry_run:
            return
        subscribed = await self._each_filter_client(
            filter_subscribe, self.config.content_topic, topic
        )
        self.log_event(
            {
                "event": "filter_subscribed",
                "subscribed": subscribed,
                "clients": self.config.num_filter_clients,
            }
        )

    async def keep_filter_subscriptions_alive(self, stop: asyncio.Event):
        """Ping every filter client in subscribe order until `stop`, so none reaches its TTL."""
        if self.dry_run:
            return
        while not stop.is_set():
            started = time.monotonic()
            pinged = await self._each_filter_client(filter_ping)
            elapsed = time.monotonic() - started
            self.log_event(
                {
                    "event": "filter_pinged",
                    "pinged": pinged,
                    "clients": self.config.num_filter_clients,
                    "round_s": round(elapsed, 1),
                }
            )
            try:
                await asyncio.wait_for(stop.wait(), max(0.0, FILTER_PING_INTERVAL_S - elapsed))
            except asyncio.TimeoutError:
                pass

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

        await self.subscribe_filter_clients(pubsub_topic(cluster_id))

        stop_pings = asyncio.Event()
        pings = asyncio.create_task(self.keep_filter_subscriptions_alive(stop_pings))
        await self._publish_loop(cluster_id)
        stop_pings.set()
        await pings

        await asyncio.sleep(self.config.post_publish_dwell)

        await self.dump_store_messages(pubsub_topic(cluster_id))

        self.log_event("internal_run_finished")

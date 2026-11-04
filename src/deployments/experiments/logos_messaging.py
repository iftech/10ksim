# Python Imports
import asyncio
import json
import logging
import random
from datetime import timedelta
from typing import List, Literal, Optional

import aiohttp
from kubernetes.client import V1Probe
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    model_validator,
)

# Project Imports
from src.deployments.core.configs.container import Image
from src.deployments.core.event_window_bridge import EventBound, EventWindow
from src.deployments.experiments.base_experiment import BaseExperiment, V1Deployable
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.messaging import (
    REST_PORT,
    clear_requester_cache,
    messaging_send,
)
from src.deployments.pod_api_requester.pod_api_requester import PodApiError, requester_address
from src.deployments.registry import experiment
from src.deployments.waku.bridge import Bridge
from src.deployments.waku.builders.builders import WakuStatefulSetBuilder
from src.deployments.waku.builders.helpers import find_waku_command, find_waku_container_config

logger = logging.getLogger(__name__)

LogLevel = Literal["INFO", "DEBUG", "TRACE"]

LOGOS_DELIVERY_BINARY = "/usr/bin/logosdeliverynode"
CMD_TYPE = 2
"""Cluster 0, one shard. The Messaging API needs autosharding; cluster ids 1 and 2 load presets."""
REQUEST_ERRORS = (PodApiError, asyncio.TimeoutError, aiohttp.ClientError, json.JSONDecodeError)


class ExpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    num_relay_nodes: PositiveInt = 20
    num_bootstrap_nodes: PositiveInt = 3
    num_store_nodes: NonNegativeInt = 0
    """Store nodes without the Messaging API."""
    store_max_connections: Optional[PositiveInt] = None
    """`--max-connections` on the Store nodes, the regression default when unset."""
    num_edge_nodes: NonNegativeInt = 0
    """Messaging API nodes in Edge mode, sent to like the relays."""
    reliability: bool = False
    """Confirm each send with a Store node."""
    idle_before_edge: NonNegativeFloat = 0
    """Wait between the relays and Store nodes being ready and deploying the Edge nodes."""
    delay_cold_start: NonNegativeFloat = 0
    """Wait between all nodes being ready and the first send."""
    num_messages: NonNegativeInt = 20
    msg_size_kbytes: PositiveInt = 1
    delay_after_publish: NonNegativeFloat = 1
    post_publish_dwell: NonNegativeFloat = 60
    content_topic: str = "/dst/1/messaging/proto"
    log_level: LogLevel = "INFO"
    image: Image = Field(
        default_factory=lambda: Image.from_str(
            "radiken/nwaku-pr@sha256:ecf351ff02e1770cd7d5a0a9717d08b867983d8086f442f46fb3247da6269918"
        )
    )
    requester_image: Image = Field(
        default_factory=lambda: Image.from_str(
            "radiken/pod-api-requester@sha256:eb89ef86b3cb7608a8d869bc1c5a0962bf2737bd0f9b41162c5f049fb281e5d0"
        )
    )

    @model_validator(mode="after")
    def check_store_nodes(self):
        if self.reliability and not self.num_store_nodes:
            raise ValueError("reliability needs Store nodes")
        if self.num_edge_nodes and not self.num_store_nodes:
            raise ValueError("Edge nodes need a Store peer to report PartiallyConnected")
        return self


class MessagingBridge(Bridge):
    interval: Literal["complete", "messages"] = "messages"

    def event_windows(self) -> List[EventWindow]:
        # The Waku bridge's `complete` and `stable` windows, plus the sending window.
        return super().event_windows() + [
            EventWindow(
                key="messages",
                start=EventBound("start_messages", timedelta(seconds=-10)),
                end=EventBound("dwell_finished", timedelta(seconds=10)),
            ),
        ]


def build_bootstrap(namespace: str, config: ExpConfig) -> V1Deployable:
    return (
        WakuStatefulSetBuilder()
        .with_waku_config(
            name="bootstrap-0", namespace=namespace, num_nodes=config.num_bootstrap_nodes
        )
        .with_bootstrap(CMD_TYPE)
        .with_env("SERVICE", "zerotesting-bootstrap")
        .with_args({"--log-level": config.log_level, "--filter": False}, on_duplicate="replace")
        .with_binary(LOGOS_DELIVERY_BINARY)
        .with_image(config.image)
        .build()
    )


def node_builder(
    name: str, num_nodes: int, namespace: str, config: ExpConfig, args: dict
) -> WakuStatefulSetBuilder:
    return (
        WakuStatefulSetBuilder()
        .with_waku_config(name=name, namespace=namespace, num_nodes=num_nodes)
        .with_regression(CMD_TYPE, config.num_bootstrap_nodes)
        .with_env("SERVICE", "zerotesting-bootstrap")
        .with_env("RELAY_MSG_INFO_LOGS", "1")
        .with_args({"--log-level": config.log_level, **args}, on_duplicate="replace")
    )


def messaging_builder(
    name: str, num_nodes: int, namespace: str, config: ExpConfig, args: dict
) -> WakuStatefulSetBuilder:
    """Messaging API nodes that subscribe to the content topic at startup."""
    args = {
        "--entry-layer": "messaging",
        "--reliability": str(config.reliability).lower(),
        **args,
    }
    builder = node_builder(name, num_nodes, namespace, config, args)
    container = find_waku_container_config(builder.config)
    container.command_config.insert_commands([subscribe_on_start(config.content_topic)], index=0)
    return builder


def build_relays(namespace: str, config: ExpConfig) -> V1Deployable:
    builder = messaging_builder("relay-0", config.num_relay_nodes, namespace, config, {})
    container = find_waku_container_config(builder.config)
    container.with_readiness_probe(
        messaging_readiness_probe(container.readiness_probe, config.content_topic), overwrite=True
    )
    return builder.with_binary(LOGOS_DELIVERY_BINARY).with_image(config.image).build()


def build_edge_nodes(namespace: str, config: ExpConfig) -> V1Deployable:
    """Edge mode nodes. They find their filter, lightpush and Store peers through discv5."""
    builder = messaging_builder(
        "edge-0", config.num_edge_nodes, namespace, config, {"--mode": "Edge"}
    )
    container = find_waku_container_config(builder.config)
    command = find_waku_command(container.command_config)
    # Edge mode only sets flags that are unset, so an explicit --relay would keep relay on.
    command.args = [
        arg for arg in command.args if not (isinstance(arg, tuple) and arg[0] == "--relay")
    ]
    container.with_readiness_probe(
        edge_readiness_probe(container.readiness_probe, config.content_topic), overwrite=True
    )
    return builder.with_binary(LOGOS_DELIVERY_BINARY).with_image(config.image).build()


def build_store_nodes(namespace: str, config: ExpConfig) -> V1Deployable:
    """Relays without the Messaging API that archive every message in a Postgres sidecar."""
    args = {"--max-connections": config.store_max_connections} if config.store_max_connections else {}
    builder = node_builder("store-0", config.num_store_nodes, namespace, config, args).with_store()
    builder.config.namespace = namespace
    return builder.with_binary(LOGOS_DELIVERY_BINARY).with_image(config.image).build()


def subscribe_on_start(content_topic: str) -> str:
    """Background loop that subscribes the node once its REST API is up."""
    body = json.dumps([content_topic])
    return (
        f"(until curl -sf -o /dev/null -X POST -H 'Content-Type: application/json' "
        f"-d '{body}' localhost:{REST_PORT}/messaging/v1/subscriptions; do sleep 1; done) &"
    )


def subscription_check(content_topic: str) -> str:
    """Shell check that the node is subscribed to the topic. It latches once it passes."""
    return (
        f"[ -f /tmp/subscribed ] || {{ curl -sf localhost:{REST_PORT}/messaging/v1/subscriptions"
        f" | grep -qF '\"{content_topic}\"' && touch /tmp/subscribed; }} || exit 1;\n"
    )


def messaging_readiness_probe(mesh_probe: V1Probe, content_topic: str) -> V1Probe:
    """`mesh_probe` (topic mesh >= dLow), after a subscription check that latches once it passes."""
    shell, flag, check = mesh_probe._exec.command
    mesh_probe._exec.command = [shell, flag, subscription_check(content_topic) + check]
    return mesh_probe


def edge_readiness_probe(probe: V1Probe, content_topic: str) -> V1Probe:
    """`probe` timings, the subscription check, then `/health` at least `PartiallyConnected`."""
    connected = (
        f"curl -sf localhost:{REST_PORT}/health"
        f' | grep -qE \'"connectionStatus":"(Partially)?Connected"\'\n'
    )
    shell, flag, _ = probe._exec.command
    probe._exec.command = [shell, flag, subscription_check(content_topic) + connected]
    return probe


@experiment(name="logos-messaging")
class LogosMessagingExperiment(BaseExperiment[ExpConfig]):
    """Relay nodes on the Messaging API, sent to over REST and measured from their logs."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    post_run_analysis = "src.analysis.post_run.logos_messaging:run_logos_messaging_analysis"

    def _get_metadata(self) -> dict:
        return MessagingBridge().get_metadata(self.events_log_path)

    async def _send(self, requester, index: int, pod: str) -> None:
        event = {"event": "send", "index": index, "node": pod}
        try:
            status, text = await messaging_send(
                requester, pod, self.config.content_topic, self.config.msg_size_kbytes
            )
            event["status"] = status
            if status == 200:
                event["requestId"] = json.loads(text)["requestId"]
            else:
                event["error"] = text
        except (*REQUEST_ERRORS, KeyError) as e:
            event["status"] = None
            event["error"] = str(e)
        self.log_event(event)

    async def _run(self):
        self.log_event("run_start")

        requester_builder = (
            PodApiRequesterBuilder()
            .with_namespace(self.namespace)
            .with_mode("server")
            .with_image(self.config.requester_image)
        )
        await self.deploy(deployment=requester_builder.build_dependencies(), exist_ok=True)
        await self.deploy(deployment=requester_builder.build(), wait_for_ready=True)

        await self.deploy(deployment=build_bootstrap(self.namespace, self.config))
        relays = build_relays(self.namespace, self.config)
        stores = (
            [build_store_nodes(self.namespace, self.config)] if self.config.num_store_nodes else []
        )
        await self.deploy(deployment=[relays, *stores])
        senders = [relays]
        if self.config.num_edge_nodes:
            if not self.dry_run:
                await asyncio.sleep(self.config.idle_before_edge)
            senders.append(build_edge_nodes(self.namespace, self.config))
            await self.deploy(deployment=senders[-1])
        if self.dry_run:
            self.log_event("dry_run_done")
            return

        await asyncio.sleep(self.config.delay_cold_start)
        pods = [f"{s.metadata.name}-{index}" for s in senders for index in range(s.spec.replicas)]
        requester = requester_address(self.namespace)
        await clear_requester_cache(requester)

        self.log_event("start_messages")
        sends = []
        for index in range(self.config.num_messages):
            sends.append(asyncio.create_task(self._send(requester, index, random.choice(pods))))
            await asyncio.sleep(self.config.delay_after_publish)
        await asyncio.gather(*sends)
        self.log_event("publisher_messages_finished")

        await asyncio.sleep(self.config.post_publish_dwell)
        self.log_event("dwell_finished")
        self.log_event("internal_run_finished")

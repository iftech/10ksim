# Python Imports
import asyncio
import json
import logging
import random
from datetime import timedelta
from typing import List, Literal

import aiohttp
from kubernetes.client import V1ExecAction, V1Probe
from pydantic import BaseModel, ConfigDict, Field, NonNegativeFloat, NonNegativeInt, PositiveInt

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
from src.deployments.waku.builders.helpers import find_waku_container_config

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


class MessagingBridge(Bridge):
    interval: Literal["complete", "messages"] = "messages"

    def event_windows(self) -> List[EventWindow]:
        return [
            EventWindow(
                key="complete",
                start=EventBound("wait_for_clear_finished"),
                end=EventBound("internal_run_finished", timedelta(seconds=30)),
            ),
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
        .with_args({"--log-level": config.log_level}, on_duplicate="replace")
        .with_binary(LOGOS_DELIVERY_BINARY)
        .with_image(config.image)
        .build()
    )


def build_relays(namespace: str, config: ExpConfig) -> V1Deployable:
    args = {
        "--log-level": config.log_level,
        "--entry-layer": "messaging",
        "--reliability": "false",
    }
    builder = (
        WakuStatefulSetBuilder()
        .with_waku_config(name="relay-0", namespace=namespace, num_nodes=config.num_relay_nodes)
        .with_regression(CMD_TYPE, config.num_bootstrap_nodes)
        .with_env("SERVICE", "zerotesting-bootstrap")
        .with_args(args, on_duplicate="replace")
        .with_binary(LOGOS_DELIVERY_BINARY)
        .with_image(config.image)
    )
    container = find_waku_container_config(builder.config)
    container.command_config.insert_commands([subscribe_on_start(config.content_topic)], index=0)
    container.with_readiness_probe(messaging_readiness_probe(config.content_topic), overwrite=True)
    return builder.build()


def subscribe_on_start(content_topic: str) -> str:
    """Background loop that subscribes the node once its REST API is up."""
    body = json.dumps([content_topic])
    return (
        f"(until curl -sf -o /dev/null -X POST -H 'Content-Type: application/json' "
        f"-d '{body}' localhost:{REST_PORT}/messaging/v1/subscriptions; do sleep 1; done) &"
    )


def messaging_readiness_probe(content_topic: str) -> V1Probe:
    """Ready once the node is subscribed to the topic and `/health` reports `Connected`."""
    check = (
        f"curl -sf localhost:{REST_PORT}/messaging/v1/subscriptions"
        f" | grep -qF '\"{content_topic}\"'"
        f" && curl -sf localhost:{REST_PORT}/health"
        f' | grep -qF \'"connectionStatus":"Connected"\''
    )
    return V1Probe(
        _exec=V1ExecAction(command=["/bin/sh", "-c", check]),
        success_threshold=5,
        initial_delay_seconds=5,
        period_seconds=1,
        failure_threshold=2,
        timeout_seconds=5,
    )


@experiment(name="logos-messaging")
class LogosMessagingExperiment(BaseExperiment[ExpConfig]):
    """Relay nodes on the Messaging API, sent to over REST and measured from their logs."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

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
        await self.deploy(deployment=relays)
        if self.dry_run:
            self.log_event("dry_run_done")
            return

        pods = [f"{relays.metadata.name}-{index}" for index in range(relays.spec.replicas)]
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

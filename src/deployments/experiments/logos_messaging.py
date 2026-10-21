# Python Imports
import asyncio
import base64
import json
import logging
import os
import random
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Callable, Dict, List, Literal, Optional, Set, Tuple

import aiohttp
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
)

# Project Imports
from src.deployments.core.configs.container import Image
from src.deployments.core.event_window_bridge import EventBound, EventWindow
from src.deployments.experiments.base_experiment import BaseExperiment, V1Deployable
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.messaging import REST_PORT, clear_requester_cache, node_call
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
API_DIR = "api"
NODE_METRICS_DIR = "node_metrics"
TERMINAL_SEND_EVENTS = {"propagated", "sent", "error"}
METRICS_PORT = 8008
REQUEST_ERRORS = (PodApiError, asyncio.TimeoutError, aiohttp.ClientError, json.JSONDecodeError)


class ExpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    num_relay_nodes: PositiveInt = 20
    num_bootstrap_nodes: PositiveInt = 3
    num_messages: NonNegativeInt = 20
    msg_size_kbytes: PositiveInt = 1
    delay_after_publish: NonNegativeFloat = 1
    poll_interval: PositiveFloat = 1
    post_publish_dwell: NonNegativeFloat = 60
    connect_timeout: PositiveFloat = 600
    content_topic: str = "/dst/1/messaging/proto"
    cache_capacity: PositiveInt = 1000
    log_level: LogLevel = "INFO"
    image: Image = Field(
        default_factory=lambda: Image.from_str(
            "radiken/nwaku-pr@sha256:ecf351ff02e1770cd7d5a0a9717d08b867983d8086f442f46fb3247da6269918"
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
                end=EventBound("polling_finished", timedelta(seconds=10)),
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
        "--rest-messaging-cache-capacity": config.cache_capacity,
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
    find_waku_container_config(builder.config).command_config.insert_commands(
        [subscribe_on_start(config.content_topic)], index=0
    )
    return builder.build()


def subscribe_on_start(content_topic: str) -> str:
    """Background loop that subscribes the node once its REST API is up."""
    body = json.dumps([content_topic])
    return (
        f"(until curl -sf -o /dev/null -X POST -H 'Content-Type: application/json' "
        f"-d '{body}' localhost:{REST_PORT}/messaging/v1/subscriptions; do sleep 1; done) &"
    )


def make_payload(msg_id: str, size_bytes: int) -> str:
    head = f"{msg_id}|".encode()
    return base64.b64encode(head + os.urandom(max(0, size_bytes - len(head)))).decode()


def payload_msg_id(payload: str) -> Optional[str]:
    try:
        head, sep, _ = base64.b64decode(payload).partition(b"|")
    except ValueError:
        return None
    return head.decode(errors="replace") if sep else None


class ApiRecorder:
    """Appends API records as JSON lines, one file per kind."""

    def __init__(self, folder: Path):
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)

    def write(self, kind: str, record: dict) -> None:
        with open(self.folder / f"{kind}.jsonl", "a") as out:
            out.write(json.dumps(record) + "\n")


@experiment(name="logos-messaging")
class LogosMessagingExperiment(BaseExperiment[ExpConfig]):
    """Relay nodes on the Messaging API: send, then read sends and receipts back over REST."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _recorder: Optional[ApiRecorder] = None
    _pending: Dict[str, Set[str]] = {}
    _finished: Set[str] = set()

    def _get_metadata(self) -> dict:
        return MessagingBridge().get_metadata(self.events_log_path)

    async def _wait_for(
        self, requester: Tuple[str, int], pods: List[str], route: str, ready: Callable[[str], bool]
    ) -> Set[str]:
        """Polls `route` on every pod until `ready(body)`. Returns the pods that never were."""
        deadline = time.monotonic() + self.config.connect_timeout
        pending = set(pods)
        while pending and time.monotonic() < deadline:
            for pod in sorted(pending):
                try:
                    status, text = await node_call(requester, pod, route)
                    if status == 200 and ready(text):
                        pending.discard(pod)
                except (*REQUEST_ERRORS, KeyError) as e:
                    logger.warning(f"`{route}` failed on `{pod}`: {e}")
            if pending:
                await asyncio.sleep(5)
        return pending

    async def _send(self, requester: Tuple[str, int], index: int, pod: str) -> None:
        msg_id = f"{index:05d}"
        body = {
            "payload": make_payload(msg_id, self.config.msg_size_kbytes * 1000),
            "contentTopic": self.config.content_topic,
        }
        record = {"msg_id": msg_id, "node": pod, "post_ns": time.time_ns()}
        try:
            status, text = await node_call(
                requester, pod, "/messaging/v1/messages", method="POST", body=body
            )
            record["status"] = status
            if status == 200:
                record["requestId"] = json.loads(text)["requestId"]
                if record["requestId"] not in self._finished:
                    self._pending[pod].add(record["requestId"])
            else:
                record["error"] = text
        except (*REQUEST_ERRORS, KeyError) as e:
            record["status"] = None
            record["error"] = str(e)
        record["done_ns"] = time.time_ns()
        self._recorder.write("sends", record)

    async def _read(self, requester: Tuple[str, int], pod: str, route: str) -> Optional[list]:
        try:
            status, text = await node_call(requester, pod, route)
            if status == 200:
                return json.loads(text)
            error = f"HTTP {status}: {text}"
        except REQUEST_ERRORS as e:
            error = str(e)
        self._recorder.write(
            "poll_errors", {"node": pod, "route": route, "poll_ns": time.time_ns(), "error": error}
        )
        return None

    async def _poll_received(self, requester: Tuple[str, int], pod: str) -> None:
        records = await self._read(requester, pod, "/messaging/v1/events/received")
        poll_ns = time.time_ns()
        for record in records or []:
            message = record.get("message", {})
            self._recorder.write(
                "received",
                {
                    "node": pod,
                    "poll_ns": poll_ns,
                    "seq": record.get("seq"),
                    "messageHash": record.get("messageHash"),
                    "source": record.get("source"),
                    "contentTopic": message.get("contentTopic"),
                    "timestamp": message.get("timestamp"),
                    "msg_id": payload_msg_id(message.get("payload", "")),
                },
            )

    async def _poll_send_events(self, requester: Tuple[str, int], pod: str) -> None:
        statuses = await self._read(requester, pod, "/messaging/v1/events/send")
        poll_ns = time.time_ns()
        for status in statuses or []:
            request_id = status.get("requestId")
            for event in status.get("events", []):
                self._recorder.write(
                    "send_events",
                    {
                        "node": pod,
                        "poll_ns": poll_ns,
                        "requestId": request_id,
                        "kind": event.get("kind"),
                        "messageHash": event.get("messageHash"),
                        "timestamp": event.get("timestamp"),
                        "error": event.get("error"),
                    },
                )
                if event.get("kind") in TERMINAL_SEND_EVENTS:
                    self._finished.add(request_id)
                    self._pending[pod].discard(request_id)

    async def _poll_loop(
        self, requester: Tuple[str, int], pods: List[str], stop: asyncio.Event
    ) -> None:
        """Polls every pod each interval, plus one round after `stop` is set."""
        while True:
            started = time.monotonic()
            await asyncio.gather(*(self._poll_received(requester, pod) for pod in pods))
            senders = [pod for pod, pending in self._pending.items() if pending]
            await asyncio.gather(*(self._poll_send_events(requester, pod) for pod in senders))
            if stop.is_set():
                return
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self.config.poll_interval - elapsed))

    async def _dump_metrics(
        self, requester: Tuple[str, int], pods: List[str], attempts: int = 3
    ) -> None:
        """Read before cleanup removes the pods."""
        metrics_dir = Path(self.output_folder) / NODE_METRICS_DIR
        metrics_dir.mkdir(parents=True, exist_ok=True)
        for pod in pods:
            for attempt in range(1, attempts + 1):
                try:
                    status, text = await node_call(requester, pod, "/metrics", port=METRICS_PORT)
                except REQUEST_ERRORS as e:
                    status, text = None, str(e)
                if status == 200:
                    (metrics_dir / f"{pod}.txt").write_text(text)
                    break
                logger.warning(f"Metrics of `{pod}` failed ({attempt}/{attempts}): {text[:200]}")

    async def _run(self):
        self.log_event("run_start")
        self._recorder = ApiRecorder(Path(self.output_folder) / API_DIR)
        self._pending = defaultdict(set)
        self._finished = set()

        requester_builder = (
            PodApiRequesterBuilder().with_namespace(self.namespace).with_mode("server")
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

        topic = self.config.content_topic
        not_subscribed = await self._wait_for(
            requester, pods, "/messaging/v1/subscriptions", lambda text: topic in json.loads(text)
        )
        if not_subscribed:
            self.fail_run(f"{len(not_subscribed)} nodes not subscribed: {sorted(not_subscribed)}")
        self.log_event("subscribed")

        not_connected = await self._wait_for(
            requester,
            pods,
            "/health",
            lambda text: json.loads(text)["connectionStatus"] == "Connected",
        )
        if not_connected:
            self.fail_run(
                f"{len(not_connected)} nodes not Connected after "
                f"{self.config.connect_timeout}s: {sorted(not_connected)}"
            )
        self.log_event("connected")

        stop = asyncio.Event()
        poller = asyncio.create_task(self._poll_loop(requester, pods, stop))

        self.log_event("start_messages")
        sends = []
        for index in range(self.config.num_messages):
            sends.append(asyncio.create_task(self._send(requester, index, random.choice(pods))))
            await asyncio.sleep(self.config.delay_after_publish)
        await asyncio.gather(*sends)
        self.log_event("publisher_messages_finished")

        await asyncio.sleep(self.config.post_publish_dwell)
        stop.set()
        await poller
        self.log_event("polling_finished")

        await self._dump_metrics(requester, pods)
        self.log_event("internal_run_finished")

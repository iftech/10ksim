"""Run groups of nodes from given images and report which nodes exited, and how.

A node signals a failed check by exiting non-zero, so the run needs no knowledge of what
the node does. Each group runs as an Indexed Job: a StatefulSet would restart a node that
exits and lose its exit code. Groups start in order, each once the previous is up, so a
bootstrap group goes first.

Node i of group g is reachable as `g-<i>.nodes` and gets NODE_INDEX and NUM_NODES (its
group's size) in its environment, plus the group's own `env`.
"""

import asyncio
import json
import logging
import random
import time
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Callable, Dict, List, Literal, Optional, Tuple

from kubernetes import client
from kubernetes.client import (
    ApiException,
    V1Container,
    V1EnvVar,
    V1EnvVarSource,
    V1Job,
    V1JobSpec,
    V1ObjectFieldSelector,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    PrivateAttr,
    model_validator,
)

from src.deployments.core.builders import ServiceBuilder
from src.deployments.core.configs.container import Image
from src.deployments.core.pod_logs import capture_pod_logs
from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.nimlibp2p import libp2p_dst_node_publish
from src.deployments.pod_api_requester.pod_api_requester import requester_address
from src.deployments.registry import experiment

logger = logging.getLogger(__name__)

APP = "run-nodes"
SERVICE = "nodes"
INDEX_LABEL = "batch.kubernetes.io/job-completion-index"

NodeState = Literal["pending", "running", "exited", "failed"]


class NodeGroup(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True, coerce_numbers_to_str=True)

    name: str = Field(pattern=r"^[a-z]([-a-z0-9]{0,38}[a-z0-9])?$")
    """Job name; its nodes are `<name>-<i>`."""
    image: Image
    count: PositiveInt
    env: Dict[str, str] = {}


class PublishConfig(BaseModel):
    """Publish like the regression: POST :<port>/publish on random nodes of `group`."""

    model_config = ConfigDict(use_attribute_docstrings=True)

    group: str
    messages: PositiveInt = 20
    message_size_bytes: PositiveInt = 1000
    delay: NonNegativeFloat = 1
    """Seconds between messages."""
    start_after: NonNegativeInt = 60
    """Seconds after every node has started, for the mesh to form."""
    port: PositiveInt = 8645


class ExpConfig(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True)

    nodes: List[NodeGroup] = Field(min_length=1)
    duration: NonNegativeInt = 600
    """Seconds the nodes run once all have started."""
    startup_timeout: PositiveInt = 300
    """Seconds every node gets to start, image pull included."""
    poll_interval: PositiveInt = 5
    publish: Optional[PublishConfig] = None

    @model_validator(mode="after")
    def _check(self):
        names = [group.name for group in self.nodes]
        if len(set(names)) != len(names):
            raise ValueError(f"Group names must be unique: {names}")
        if self.publish:
            if self.publish.group not in names:
                raise ValueError(f"publish.group `{self.publish.group}` is not in {names}")
            span = self.publish.start_after + self.publish.messages * self.publish.delay
            if span > self.duration:
                raise ValueError(f"Publishing takes {span}s, longer than duration {self.duration}s")
        return self

    @property
    def num_nodes(self) -> int:
        return sum(group.count for group in self.nodes)


@dataclass
class NodeStatus:
    node: str
    group: str
    state: NodeState
    exit_code: Optional[int] = None
    reason: Optional[str] = None
    image_id: Optional[str] = None


def build_service(namespace: str) -> V1Service:
    # Headless, no ports: only gives each node a DNS name.
    return (
        ServiceBuilder()
        .with_name(SERVICE)
        .with_namespace(namespace)
        .with_cluster_ip("None")
        .with_publish_not_ready_addresses(True)
        .with_selector("app", APP)
        .build()
    )


def build_job(namespace: str, group: NodeGroup) -> V1Job:
    labels = {"app": APP, "group": group.name}
    index = V1EnvVarSource(
        field_ref=V1ObjectFieldSelector(field_path=f"metadata.labels['{INDEX_LABEL}']")
    )
    env = {"NUM_NODES": str(group.count), **group.env}
    container = V1Container(
        name="node",
        image=str(group.image),
        # Tags get re-pushed between runs.
        image_pull_policy="Always",
        env=[V1EnvVar(name="NODE_INDEX", value_from=index)]
        + [V1EnvVar(name=key, value=value) for key, value in env.items()],
    )
    return V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=group.name, namespace=namespace, labels=labels),
        spec=V1JobSpec(
            completion_mode="Indexed",
            completions=group.count,
            parallelism=group.count,
            # Per index, so one failed node neither restarts nor stops the others.
            backoff_limit_per_index=0,
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels=labels),
                # With the Job's `<name>-<i>` hostnames, makes `<name>-<i>.nodes` resolve.
                spec=V1PodSpec(restart_policy="Never", subdomain=SERVICE, containers=[container]),
            ),
        ),
    )


def node_status(pod: V1Pod) -> NodeStatus:
    labels = pod.metadata.labels or {}
    group = labels.get("group", "")
    node = f"{group}-{labels[INDEX_LABEL]}" if INDEX_LABEL in labels else pod.metadata.name
    statuses = pod.status.container_statuses or []
    if not statuses:
        return NodeStatus(node, group, "pending", reason=pod.status.phase)

    status = statuses[0]
    image_id = status.image_id or None
    terminated = status.state.terminated
    if terminated is not None:
        state = "exited" if terminated.exit_code == 0 else "failed"
        return NodeStatus(node, group, state, terminated.exit_code, terminated.reason, image_id)
    if status.state.running is not None:
        return NodeStatus(node, group, "running", image_id=image_id)
    waiting = status.state.waiting
    return NodeStatus(node, group, "pending", reason=waiting.reason if waiting else None)


def summarize(nodes: List[NodeStatus], num_nodes: int) -> dict:
    """Counts per state; an index with no pod at all counts as not started."""
    counts = Counter(node.state for node in nodes)
    started = counts["running"] + counts["exited"] + counts["failed"]
    return {
        "running": counts["running"],
        "exited": counts["exited"],
        "failed": counts["failed"],
        "not_started": num_nodes - started,
    }


def render_summary(summary: dict, nodes: List[NodeStatus], failures: List[str]) -> str:
    """Markdown report of a run, for the GitHub run page."""
    lines = [f"## run-nodes: {'failed' if failures else 'passed'}", ""]
    lines += [f"- {reason}" for reason in failures] + ([""] if failures else [])
    lines += [
        f"- Ended: {summary['ended']} after {summary['run_seconds']}s",
        f"- Nodes: {summary['num_nodes']} ({summary['running']} running, {summary['exited']} "
        f"exited 0, {summary['failed']} failed, {summary['not_started']} never started)",
    ]
    if "published" in summary:
        lines.append(f"- Published: {summary['published']} ({summary['publish_failed']} failed)")
    lines += [f"- Image `{group}`: `{image}`" for group, image in summary["groups"].items()]

    trouble = [node for node in nodes if node.state in ("failed", "pending")]
    if trouble:
        lines += ["", "| Node | State | Exit code | Reason |", "|---|---|---|---|"]
        lines += [
            f"| {n.node} | {n.state} | {'' if n.exit_code is None else n.exit_code} "
            f"| {n.reason or ''} |"
            for n in trouble
        ]
    return "\n".join(lines) + "\n"


def _node_order(node: NodeStatus):
    # node-2 before node-10
    return node.group, len(node.node), node.node


@experiment(name="run-nodes")
class RunNodesExperiment(BaseExperiment[ExpConfig]):
    """Run groups of nodes; a node that exits non-zero fails the run."""

    _logged_exits: set = PrivateAttr(default_factory=set)
    _published: List[bool] = PrivateAttr(default_factory=list)

    def _poll(self) -> Dict[str, NodeStatus]:
        pods = (
            client.CoreV1Api(self.api_client)
            .list_namespaced_pod(self.namespace, label_selector=f"app={APP}")
            .items
        )
        return {status.node: status for status in map(node_status, pods)}

    async def _watch(self, until: Callable[[List[NodeStatus]], bool], timeout: float) -> bool:
        """Poll the nodes until `until` holds or `timeout` passes. Returns whether it held."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                nodes = list(self._poll().values())
            except ApiException as e:
                logger.warning(f"Could not list nodes: {e.status}")
            else:
                self._log_exits(nodes)
                if until(nodes):
                    return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(self.config.poll_interval, remaining))

    def _log_exits(self, nodes: List[NodeStatus]) -> None:
        for node in nodes:
            if node.state in ("exited", "failed") and node.node not in self._logged_exits:
                self._logged_exits.add(node.node)
                self.log_event({"event": "node_exited", **asdict(node)})

    async def _start_groups(self) -> bool:
        """Deploy the groups in order, each once the previous is up. Returns whether all are."""
        deadline = time.monotonic() + self.config.startup_timeout
        for group in self.config.nodes:
            await self.deploy(deployment=build_job(self.namespace, group), wait_for_ready=False)
            if self.dry_run:
                continue

            def started(nodes, group=group):
                up = [n for n in nodes if n.group == group.name and n.state != "pending"]
                return len(up) == group.count

            if not await self._watch(started, deadline - time.monotonic()):
                return False
            self.log_event({"event": "group_started", "group": group.name})
        return True

    async def _publish(self, requester: Tuple[str, int]) -> None:
        cfg = self.config.publish
        count = next(group.count for group in self.config.nodes if group.name == cfg.group)
        await asyncio.sleep(cfg.start_after)
        self.log_event("publish_start")
        tasks = []
        for index in range(cfg.messages):
            node = f"{cfg.group}-{random.randrange(count)}"
            self.log_event({"event": "publish", "node": node, "index": index})
            tasks.append(asyncio.create_task(self._publish_one(node, requester)))
            await asyncio.sleep(cfg.delay)
        await asyncio.gather(*tasks)

    async def _publish_one(self, node: str, requester: Tuple[str, int]) -> None:
        cfg = self.config.publish
        target = Target(name="node", name_template=node, service=SERVICE, port=cfg.port)
        try:
            await libp2p_dst_node_publish(
                namespace=self.namespace,
                target=target,
                msg_size_bytes=cfg.message_size_bytes,
                requester=requester,
            )
            self._published.append(True)
        except Exception as e:
            logger.error(f"Publish to `{node}` failed: {e}")
            self._published.append(False)

    async def _run(self):
        self.log_event("run_start")
        cfg = self.config
        if cfg.publish:
            requester = PodApiRequesterBuilder().with_namespace(self.namespace).with_mode("server")
            await self.deploy(deployment=requester.build_dependencies(), exist_ok=True)
            await self.deploy(deployment=requester.build(), wait_for_ready=True)
        await self.deploy(deployment=build_service(self.namespace), wait_for_ready=False)

        started_at = time.monotonic()
        all_started = await self._start_groups()
        if self.dry_run:
            return

        def all_done(nodes):
            return len(nodes) == cfg.num_nodes and all(
                node.state in ("exited", "failed") for node in nodes
            )

        if not all_started:
            ended = "startup timeout"
        else:
            self.log_event("nodes_started")
            publishing = None
            if cfg.publish:
                address = requester_address(self.namespace)
                publishing = asyncio.create_task(self._publish(address))
            done = await self._watch(all_done, cfg.duration)
            ended = "all nodes exited" if done else "duration elapsed"
            if publishing:
                publishing.cancel()
                await asyncio.gather(publishing, return_exceptions=True)
        run_seconds = round(time.monotonic() - started_at)

        captured = await asyncio.to_thread(
            capture_pod_logs,
            self.namespace,
            self.output_folder / "kubectl_logs",
            self.api_client,
            label_selector=f"app={APP}",
        )
        self.log_event({"event": "log_capture_finished", "pods": captured})

        nodes = sorted(self._poll().values(), key=_node_order)
        summary = {
            "groups": {group.name: str(group.image) for group in cfg.nodes},
            "num_nodes": cfg.num_nodes,
            "ended": ended,
            "run_seconds": run_seconds,
            **summarize(nodes, cfg.num_nodes),
        }
        if cfg.publish:
            summary["published"] = len(self._published)
            summary["publish_failed"] = self._published.count(False)
        self.log_event({"event": "run_summary", **summary})
        (self.output_folder / "node_results.json").write_text(
            json.dumps({**summary, "nodes": [asdict(node) for node in nodes]}, indent=2)
        )

        failed = [f"{n.node} ({n.exit_code})" for n in nodes if n.state == "failed"]
        if failed:
            self.fail_run(f"{len(failed)} of {cfg.num_nodes} nodes exited non-zero: {failed}")
        if summary["not_started"]:
            reasons = sorted({str(n.reason) for n in nodes if n.state == "pending"})
            self.fail_run(
                f"{summary['not_started']} of {cfg.num_nodes} nodes never started: {reasons}"
            )
        if summary.get("publish_failed"):
            self.fail_run(f"{summary['publish_failed']} of {summary['published']} publishes failed")
        (self.output_folder / "summary.md").write_text(
            render_summary(summary, nodes, self._failures)
        )
        self.log_event("internal_run_finished")

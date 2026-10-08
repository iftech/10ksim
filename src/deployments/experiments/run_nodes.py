"""Run N copies of one image and report which nodes exited, and how.

A node signals a failed check by exiting non-zero, so the run needs no knowledge of what
the node does. Nodes run as an Indexed Job: a StatefulSet would restart a node that
exits and lose its exit code.
"""

import asyncio
import json
import logging
import time
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Callable, Dict, List, Literal, Optional

from kubernetes import client
from kubernetes.client import (
    ApiException,
    V1Container,
    V1Job,
    V1JobSpec,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
)
from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt, PrivateAttr

from src.deployments.core.configs.container import Image
from src.deployments.core.pod_logs import capture_pod_logs
from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.registry import experiment

logger = logging.getLogger(__name__)

NAME = "node"
INDEX_LABEL = "batch.kubernetes.io/job-completion-index"

NodeState = Literal["pending", "running", "exited", "failed"]


class ExpConfig(BaseModel):
    model_config = ConfigDict(use_attribute_docstrings=True)

    image: Image
    num_nodes: PositiveInt = 10
    duration: NonNegativeInt = 600
    """Seconds the nodes run once all have started."""
    startup_timeout: PositiveInt = 300
    """Seconds every node gets to start, image pull included."""
    poll_interval: PositiveInt = 5


@dataclass
class NodeStatus:
    node: str
    state: NodeState
    exit_code: Optional[int] = None
    reason: Optional[str] = None
    image_id: Optional[str] = None


def build_job(namespace: str, config: ExpConfig) -> V1Job:
    labels = {"app": NAME}
    container = V1Container(
        name=NAME,
        image=str(config.image),
        # Tags get re-pushed between runs.
        image_pull_policy="Always",
    )
    return V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=NAME, namespace=namespace, labels=labels),
        spec=V1JobSpec(
            completion_mode="Indexed",
            completions=config.num_nodes,
            parallelism=config.num_nodes,
            # Per index, so one failed node neither restarts nor stops the others.
            backoff_limit_per_index=0,
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels=labels),
                spec=V1PodSpec(restart_policy="Never", containers=[container]),
            ),
        ),
    )


def node_status(pod: V1Pod) -> NodeStatus:
    labels = pod.metadata.labels or {}
    node = f"{NAME}-{labels[INDEX_LABEL]}" if INDEX_LABEL in labels else pod.metadata.name
    statuses = pod.status.container_statuses or []
    if not statuses:
        return NodeStatus(node, "pending", reason=pod.status.phase)

    status = statuses[0]
    image_id = status.image_id or None
    terminated = status.state.terminated
    if terminated is not None:
        state = "exited" if terminated.exit_code == 0 else "failed"
        return NodeStatus(node, state, terminated.exit_code, terminated.reason, image_id)
    if status.state.running is not None:
        return NodeStatus(node, "running", image_id=image_id)
    waiting = status.state.waiting
    return NodeStatus(node, "pending", reason=waiting.reason if waiting else None)


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


def _node_order(node: NodeStatus):
    # node-2 before node-10
    return len(node.node), node.node


@experiment(name="run-nodes")
class RunNodesExperiment(BaseExperiment[ExpConfig]):
    """Run N nodes of one image; a node that exits non-zero fails the run."""

    _logged_exits: set = PrivateAttr(default_factory=set)

    def _poll(self) -> Dict[str, NodeStatus]:
        pods = (
            client.CoreV1Api(self.api_client)
            .list_namespaced_pod(self.namespace, label_selector=f"app={NAME}")
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

    async def _run(self):
        self.log_event("run_start")
        cfg = self.config
        await self.deploy(deployment=build_job(self.namespace, cfg), wait_for_ready=False)
        if self.dry_run:
            return

        def all_started(nodes):
            return summarize(nodes, cfg.num_nodes)["not_started"] == 0

        def all_done(nodes):
            return len(nodes) == cfg.num_nodes and all(
                node.state in ("exited", "failed") for node in nodes
            )

        started_at = time.monotonic()
        if not await self._watch(all_started, cfg.startup_timeout):
            ended = "startup timeout"
        else:
            self.log_event("nodes_started")
            done = await self._watch(all_done, cfg.duration)
            ended = "all nodes exited" if done else "duration elapsed"
        run_seconds = round(time.monotonic() - started_at)

        captured = await asyncio.to_thread(
            capture_pod_logs,
            self.namespace,
            self.output_folder / "kubectl_logs",
            self.api_client,
            label_selector=f"app={NAME}",
        )
        self.log_event({"event": "log_capture_finished", "pods": captured})

        nodes = sorted(self._poll().values(), key=_node_order)
        summary = {
            "image": str(cfg.image),
            "num_nodes": cfg.num_nodes,
            "ended": ended,
            "run_seconds": run_seconds,
            **summarize(nodes, cfg.num_nodes),
        }
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
        self.log_event("internal_run_finished")

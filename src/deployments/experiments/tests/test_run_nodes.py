import json
from unittest.mock import MagicMock

import pytest
from kubernetes.client import ApiClient
from pydantic import ValidationError

from src.deployments.experiments import run_nodes
from src.deployments.experiments.base_experiment import ExperimentFailed
from src.deployments.experiments.run_nodes import (
    INDEX_LABEL,
    ExpConfig,
    NodeStatus,
    RunNodesExperiment,
    node_status,
    summarize,
)


def _pod(index, *, running=False, exit_code=None, reason=None, waiting=None, no_status=False):
    pod = MagicMock()
    pod.metadata.labels = {INDEX_LABEL: str(index), "group": "node"}
    pod.status.phase = "Pending"
    if no_status:
        pod.status.container_statuses = None
        return pod
    status = MagicMock(image_id="docker.io/x@sha256:abc")
    status.state.running = MagicMock() if running else None
    status.state.terminated = (
        None if exit_code is None else MagicMock(exit_code=exit_code, reason=reason)
    )
    status.state.waiting = MagicMock(reason=waiting) if waiting else None
    pod.status.container_statuses = [status]
    return pod


def _group(name="node", count=2):
    return {"name": name, "image": {"repo": "x", "tag": "y"}, "count": count}


@pytest.mark.parametrize(
    "pod, state, exit_code, reason",
    [
        (_pod(0, running=True), "running", None, None),
        (_pod(0, exit_code=0, reason="Completed"), "exited", 0, "Completed"),
        (_pod(0, exit_code=137, reason="OOMKilled"), "failed", 137, "OOMKilled"),
        (_pod(0, waiting="ErrImagePull"), "pending", None, "ErrImagePull"),
        (_pod(0, no_status=True), "pending", None, "Pending"),
    ],
)
def test_node_state_comes_from_the_container(pod, state, exit_code, reason):
    status = node_status(pod)
    assert (status.node, status.state, status.exit_code, status.reason) == (
        "node-0",
        state,
        exit_code,
        reason,
    )


def test_an_index_without_a_pod_counts_as_not_started():
    nodes = [NodeStatus("node-0", "node", "running"), NodeStatus("node-1", "node", "failed", 2)]
    assert summarize(nodes, num_nodes=3) == {
        "running": 1,
        "exited": 0,
        "failed": 1,
        "not_started": 1,
    }


@pytest.mark.parametrize(
    "cfg, error",
    [
        ({"nodes": [_group(), _group()]}, "unique"),
        ({"nodes": [_group()], "publish": {"group": "bootstrap"}}, "not in"),
        ({"nodes": [_group()], "duration": 30, "publish": {"group": "node"}}, "longer than"),
    ],
)
def test_a_config_that_cannot_run_is_rejected(cfg, error):
    with pytest.raises(ValidationError, match=error):
        ExpConfig(**cfg)


def _experiment(tmp_path, mocker, pods, **cfg):
    mocker.patch.object(RunNodesExperiment, "deploy", mocker.AsyncMock())
    mocker.patch.object(run_nodes, "capture_pod_logs", return_value=len(pods))
    mocker.patch.object(
        RunNodesExperiment,
        "_poll",
        return_value={s.node: s for s in map(node_status, pods)},
    )
    config = ExpConfig(**{"nodes": [_group(count=len(pods))], "poll_interval": 1, **cfg})
    return RunNodesExperiment(
        api_client=ApiClient(), config=config, namespace="ns", output_folder=tmp_path / "run"
    )


@pytest.mark.asyncio
async def test_nodes_still_running_at_the_end_pass(tmp_path, mocker):
    exp = _experiment(tmp_path, mocker, [_pod(0, running=True), _pod(1, running=True)], duration=0)
    await exp.run()

    result = json.loads((exp.output_folder / "node_results.json").read_text())
    assert (result["running"], result["ended"]) == (2, "duration elapsed")


@pytest.mark.asyncio
async def test_a_node_that_exits_non_zero_fails_the_run(tmp_path, mocker):
    pods = [_pod(0, running=True), _pod(1, exit_code=3, reason="Error")]
    exp = _experiment(tmp_path, mocker, pods, duration=0)
    with pytest.raises(ExperimentFailed, match=r"1 of 2 nodes exited non-zero: \['node-1 \(3\)'\]"):
        await exp.run()

    result = json.loads((exp.output_folder / "node_results.json").read_text())
    assert (result["running"], result["failed"]) == (1, 1)
    summary = (exp.output_folder / "summary.md").read_text()
    assert "## run-nodes: failed" in summary and "| node-1 | failed | 3 | Error |" in summary


@pytest.mark.asyncio
async def test_nodes_that_never_start_fail_the_run_with_the_reason(tmp_path, mocker):
    pods = [_pod(0, running=True), _pod(1, waiting="ImagePullBackOff")]
    exp = _experiment(tmp_path, mocker, pods, startup_timeout=1)
    with pytest.raises(
        ExperimentFailed, match=r"1 of 2 nodes never started: \['ImagePullBackOff'\]"
    ):
        await exp.run()

    result = json.loads((exp.output_folder / "node_results.json").read_text())
    assert result["ended"] == "startup timeout"


@pytest.mark.asyncio
async def test_a_failed_publish_fails_the_run(tmp_path, mocker):
    mocker.patch.object(run_nodes, "requester_address", return_value=("10.0.0.1", 30000))
    mocker.patch.object(
        run_nodes,
        "libp2p_dst_node_publish",
        mocker.AsyncMock(side_effect=[{}, RuntimeError("refused")]),
    )
    publish = {"group": "node", "messages": 2, "delay": 0, "start_after": 0}
    exp = _experiment(
        tmp_path,
        mocker,
        [_pod(0, running=True), _pod(1, running=True)],
        duration=1,
        publish=publish,
    )
    with pytest.raises(ExperimentFailed, match="1 of 2 publishes failed"):
        await exp.run()

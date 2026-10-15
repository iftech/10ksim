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
    node_problem,
    node_status,
    summarize,
)


def _pod(
    index,
    *,
    group="node",
    running=False,
    exit_code=None,
    reason=None,
    waiting=None,
    no_status=False,
):
    pod = MagicMock()
    pod.metadata.labels = {INDEX_LABEL: str(index), "group": group}
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


def _group(name="node", count=2, **extra):
    return {"name": name, "image": {"repo": "x", "tag": "y"}, "count": count, **extra}


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


def test_a_failed_node_without_a_reason_is_still_a_problem():
    node = NodeStatus("node-0", "node", "failed", exit_code=3, reason=None)
    assert node_problem(node, terminating=set()) == "exited 3"


@pytest.mark.parametrize(
    "cfg, error",
    [
        ({"nodes": [_group(), _group()]}, "unique"),
        ({"nodes": [_group()], "publish": {"group": "bootstrap"}}, "not in"),
        ({"nodes": [_group()], "duration": 30, "publish": {"group": "node"}}, "longer than"),
        ({"nodes": [_group(count=0)]}, "nothing to run"),
        ({"nodes": [_group("a"), _group("b", count=0)], "publish": {"group": "b"}}, "not in"),
    ],
)
def test_a_config_that_cannot_run_is_rejected(cfg, error):
    with pytest.raises(ValidationError, match=error):
        ExpConfig(**cfg)


def _experiment(tmp_path, mocker, pods, groups=None, **cfg):
    mocker.patch.object(RunNodesExperiment, "deploy", mocker.AsyncMock())
    mocker.patch.object(run_nodes, "capture_pod_logs", return_value=len(pods))
    mocker.patch.object(
        RunNodesExperiment,
        "_poll",
        return_value={s.node: s for s in map(node_status, pods)},
    )
    groups = groups or [_group(count=len(pods))]
    config = ExpConfig(**{"nodes": groups, "poll_interval": 1, **cfg})
    return RunNodesExperiment(
        api_client=ApiClient(), config=config, namespace="ns", output_folder=tmp_path / "run"
    )


def _results(exp):
    summary = (exp.output_folder / "summary.md").read_text()
    return json.loads((exp.output_folder / "node_results.json").read_text()), summary


@pytest.mark.asyncio
async def test_nodes_still_running_at_the_end_pass(tmp_path, mocker):
    exp = _experiment(tmp_path, mocker, [_pod(0, running=True), _pod(1, running=True)], duration=0)
    await exp.run()

    result, summary = _results(exp)
    assert (result["running"], result["ended"], result["problems"]) == (2, "duration elapsed", 0)
    assert "## run-nodes: passed" in summary


@pytest.mark.asyncio
async def test_a_node_that_exits_non_zero_is_reported_without_failing_the_run(tmp_path, mocker):
    pods = [_pod(0, running=True), _pod(1, exit_code=3, reason="Error")]
    exp = _experiment(tmp_path, mocker, pods, duration=0)
    await exp.run()

    result, summary = _results(exp)
    assert (result["failed"], result["problems"]) == (1, 1)
    assert "## run-nodes: 1 of 2 nodes failed" in summary
    assert "| node-1 | failed | 3 | Error |" in summary


@pytest.mark.asyncio
async def test_nodes_that_never_start_fail_the_run_with_the_reason(tmp_path, mocker):
    pods = [_pod(0, running=True), _pod(1, waiting="ImagePullBackOff")]
    exp = _experiment(tmp_path, mocker, pods, startup_timeout=1)
    with pytest.raises(
        ExperimentFailed, match=r"1 of 2 nodes never started: \['ImagePullBackOff'\]"
    ):
        await exp.run()

    result, summary = _results(exp)
    assert result["ended"] == "startup timeout"
    assert "## run-nodes: run error" in summary


@pytest.mark.asyncio
async def test_a_failed_publish_is_reported_without_failing_the_run(tmp_path, mocker):
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
    await exp.run()

    result, summary = _results(exp)
    assert (result["publish_failed"], result["problems"]) == (1, 1)
    assert "## run-nodes: 1 of 2 publishes failed" in summary


@pytest.mark.asyncio
async def test_the_run_ends_once_the_self_terminating_nodes_have_exited(tmp_path, mocker):
    groups = [_group("boot", count=1), _group("dummy", count=1, self_terminating=True)]
    pods = [_pod(0, group="boot", running=True), _pod(0, group="dummy", exit_code=0)]
    exp = _experiment(tmp_path, mocker, pods, groups=groups, duration=60)
    await exp.run()

    result, _ = _results(exp)
    assert (result["ended"], result["running"], result["problems"]) == (
        "self-terminating nodes exited",
        1,
        0,
    )
    assert result["run_seconds"] < 60


@pytest.mark.asyncio
async def test_a_self_terminating_node_still_running_at_the_timeout_failed(tmp_path, mocker):
    groups = [_group("boot", count=1), _group("dummy", count=1, self_terminating=True)]
    pods = [_pod(0, group="boot", running=True), _pod(0, group="dummy", running=True)]
    exp = _experiment(tmp_path, mocker, pods, groups=groups, duration=0)
    await exp.run()

    result, summary = _results(exp)
    assert (result["ended"], result["unfinished"], result["problems"]) == ("timeout", 1, 1)
    assert "| dummy-0 | running |  | still running at the timeout |" in summary


@pytest.mark.asyncio
async def test_a_group_with_count_0_is_not_deployed(tmp_path, mocker):
    groups = [_group("node", count=1), _group("off", count=0)]
    exp = _experiment(tmp_path, mocker, [_pod(0, running=True)], groups=groups, duration=0)
    await exp.run()

    deployed = [call.kwargs["deployment"].metadata.name for call in exp.deploy.call_args_list]
    assert "node" in deployed and "off" not in deployed

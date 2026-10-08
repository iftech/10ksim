import asyncio
from types import SimpleNamespace

import pytest
from kubernetes.client import ApiClient
from pydantic import ValidationError

from src.deployments.experiments.libp2p import partition
from src.deployments.experiments.libp2p.partition import (
    NetworkPartition,
    NetworkPartitionCut,
    PartitionConfig,
    PartitionCutConfig,
)

NODES = SimpleNamespace(metadata=SimpleNamespace(name="pod"), spec=SimpleNamespace(replicas=4))


@pytest.fixture
def steps(mocker):
    """Records labels, policy deploys, heals and sleeps in the order they happen."""
    log = []

    async def sleep(seconds):
        log.append(("sleep", seconds))

    def label(names, _ns, labels, _api):
        log.append(("label", labels[partition.SIDE_LABEL]))
        return len(list(names))

    mocker.patch.object(asyncio, "sleep", sleep)
    mocker.patch.object(partition, "label_pods", label)
    mocker.patch.object(partition, "check_partition_applied")
    mocker.patch.object(partition, "delete_network_policy", lambda *_: log.append(("heal",)))
    mocker.patch.object(partition, "get_pods_for_statefulset", lambda *_: range(4))

    async def deploy(self, deployment, wait_for_ready):
        log.append(("policies", len(deployment)))

    mocker.patch.object(partition.PartitionBase, "deploy", deploy)
    mocker.patch.object(partition.PartitionBase, "dump_yaml")
    return log


def _experiment(cls, config, tmp_path):
    exp = cls(api_client=ApiClient(), config=config, namespace="ns", output_folder=tmp_path)
    exp.events_log_path = tmp_path / "events.log"
    return exp


@pytest.mark.asyncio
async def test_cut_labels_at_readiness_and_applies_only_policies_at_the_cut(tmp_path, steps):
    config = PartitionCutConfig(num_relay_nodes=4, discovery="kad-dht", cut_at=30, heal_at=120)
    exp = _experiment(NetworkPartitionCut, config, tmp_path)
    await exp._after_nodes(NODES)
    assert steps == [("label", "a"), ("label", "b"), ("label", "a")]
    steps.clear()
    await exp._mid_run(NODES)
    assert steps == [("sleep", 30), ("policies", 2), ("sleep", 120), ("heal",), ("heal",)]


@pytest.mark.asyncio
async def test_split_before_formation_labels_and_cuts_together(tmp_path, steps):
    exp = _experiment(NetworkPartition, PartitionConfig(num_relay_nodes=4), tmp_path)
    await exp._after_nodes(NODES)
    assert steps == [("label", "a"), ("label", "b"), ("policies", 2)]


@pytest.mark.asyncio
async def test_cut_without_an_anchor_labels_only_the_nodes(tmp_path, steps):
    config = PartitionCutConfig(num_relay_nodes=4, discovery="static")
    exp = _experiment(NetworkPartitionCut, config, tmp_path)
    await exp._after_nodes(NODES)
    assert steps == [("label", "a"), ("label", "b")]


@pytest.mark.parametrize("side", ["a", "b", ""])
def test_anchor_side_accepts_a_side_or_none(side):
    assert PartitionCutConfig(num_relay_nodes=4, anchor_side=side).anchor_side == side


@pytest.mark.parametrize("side", ["c", "A", "both"])
def test_anchor_side_rejects_anything_else(side):
    with pytest.raises(ValidationError):
        PartitionCutConfig(num_relay_nodes=4, anchor_side=side)

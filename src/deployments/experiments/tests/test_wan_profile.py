from src.deployments.experiments.libp2p.degraded import DegradedConfig
from src.deployments.experiments.libp2p.nimlibp2p import ExpConfig, build_nodes
from src.deployments.experiments.libp2p.shadow_gossipsub import ExpConfig as ShadowConfig
from src.deployments.libp2p.bridge import WAN_BANDWIDTH_MBIT, WAN_LATENCY_MS


def test_cluster_runs_are_shaped_by_default():
    config = ExpConfig()
    assert config.network_delay == WAN_LATENCY_MS
    assert config.network_bandwidth_mbit == WAN_BANDWIDTH_MBIT


def test_shadow_runs_are_shaped_by_default():
    """Both platforms take the profile from one constant so they cannot drift apart."""
    config = ShadowConfig()
    assert config.latency_ms == WAN_LATENCY_MS
    assert config.bandwidth_mbit == WAN_BANDWIDTH_MBIT


def test_the_qdisc_is_actually_attached_without_a_values_file():
    nodes = build_nodes(namespace="test", params=ExpConfig())
    init_containers = nodes.spec.template.spec.init_containers or []
    commands = " ".join(" ".join(c.command or []) + " ".join(c.args or []) for c in init_containers)
    assert "netem" in commands
    assert f"{WAN_LATENCY_MS}ms" in commands


def test_a_scenario_can_still_override_the_profile():
    assert DegradedConfig().network_delay == 200


def test_shaping_can_be_turned_off_explicitly():
    config = ExpConfig(network_delay=0, network_bandwidth_mbit=0)
    nodes = build_nodes(namespace="test", params=config)
    assert not (nodes.spec.template.spec.init_containers or [])

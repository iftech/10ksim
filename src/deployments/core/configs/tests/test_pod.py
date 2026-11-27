from __future__ import annotations

from src.deployments.core.configs.container import ContainerConfig, Image
from src.deployments.core.configs.pod import (
    PodSpecConfig,
    build_pod_spec,
)


def test_with_dns_search_reorders_existing_search():
    config = PodSpecConfig(dns_config=None)
    config.with_dns_search("a.example")
    config.with_dns_search("b.example")
    config.with_dns_search("a.example", overwrite=True)
    assert config.dns_config.searches == ["b.example", "a.example"]


def test_remove_dns_search_missing_ok():
    config = PodSpecConfig(dns_config=None)
    config.remove_dns_search("missing", missing_ok=True)
    assert config.dns_config is None


def test_add_container_appends_and_prepends():
    config = PodSpecConfig()
    c1 = ContainerConfig(
        name="c1",
        image=Image(repo="busybox", tag="latest"),
        image_pull_policy="IfNotPresent",
    )
    c2 = ContainerConfig(
        name="c2",
        image=Image(repo="busybox", tag="latest"),
        image_pull_policy="IfNotPresent",
    )
    config.add_container(c1)
    config.add_container(c2, order="prepend")
    assert [c.name for c in config.container_configs] == ["c2", "c1"]


def test_with_required_node_affinity_sets_required_expression():
    config = PodSpecConfig()
    config.with_required_node_affinity("kubernetes.io/hostname", "NotIn", ["node-05"])

    selector = config.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    expression = selector.node_selector_terms[0].match_expressions[0]
    assert expression.key == "kubernetes.io/hostname"
    assert expression.operator == "NotIn"
    assert expression.values == ["node-05"]


def test_build_pod_spec_uses_node_affinity():
    config = PodSpecConfig()
    config.with_required_node_affinity("kubernetes.io/hostname", "In", ["node-01", "node-02"])

    spec = build_pod_spec(config)

    selector = spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    expression = selector.node_selector_terms[0].match_expressions[0]
    assert expression.key == "kubernetes.io/hostname"
    assert expression.operator == "In"
    assert expression.values == ["node-01", "node-02"]

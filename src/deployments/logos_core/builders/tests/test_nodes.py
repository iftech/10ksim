from __future__ import annotations

from kubernetes.client import (
    V1PodSecurityContext,
    V1Service,
)

from src.deployments.core.configs.container import ContainerConfig, Image
from src.deployments.logos_core.builders.nodes import (
    NodesBuilder,
    apply_container_config,
    default_resources,
)


def test_apply_container_config_sets_name_image_ports_resources_and_env():
    config = ContainerConfig(name="old", image_pull_policy="IfNotPresent")
    image = Image(repo="repo", tag="tag")
    result = apply_container_config(config, "new-name", image, ["enr-a", "enr-b"])
    assert result is config
    assert config.name == "new-name"
    assert config.image == image
    assert [p.container_port for p in config.ports] == [8645, 8008, 8080]
    assert config.resources == default_resources()
    assert config.env[0].name == "ENR0"
    assert config.env[0].value == "enr-a"
    assert config.env[1].name == "ENR1"
    assert config.env[1].value == "enr-b"
    assert config.env[2].name == "POD_NAME"
    assert config.env[3].name == "POD_UID"
    assert config.env[2].value_from.field_ref.field_path == "metadata.name"
    assert config.env[3].value_from.field_ref.field_path == "metadata.uid"


def test_with_debug_enables_security_context():
    builder = NodesBuilder().with_config(namespace="ns").with_debug(True)
    pod_spec = builder.config.stateful_set_spec.pod_template_spec_config.pod_spec_config
    assert pod_spec.security_context == V1PodSecurityContext(run_as_user=0, fs_group=0)


def test_build_dependencies_returns_service():
    builder = NodesBuilder().with_config(namespace="ns")
    deps = builder.build_dependencies()
    assert "services" in deps
    service = deps["services"][0]
    assert isinstance(service, V1Service)
    assert service.metadata.name == builder._service_name
    assert service.metadata.namespace == "ns"
    assert service.spec.cluster_ip == "None"
    assert service.spec.selector == {"app": builder._app}


def test_reconcile_applies_service_account_name():
    builder = NodesBuilder().with_config(namespace="ns")
    builder.with_service_account_name("custom-sa")
    pod_spec = builder.config.stateful_set_spec.pod_template_spec_config.pod_spec_config
    assert pod_spec.service_account_name == "custom-sa"


def test_reconcile_applies_identity():
    builder = NodesBuilder().with_config(namespace="ns", name="node-a").with_app("app-a")
    assert builder.config.name == "node-a"
    assert builder.config.namespace == "ns"
    assert builder.config.stateful_set_spec.pod_template_spec_config.name == "node-a"
    assert builder.config.stateful_set_spec.pod_template_spec_config.namespace == "ns"
    assert builder.config.stateful_set_spec.pod_template_spec_config.labels["app"] == "app-a"

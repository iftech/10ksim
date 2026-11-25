from __future__ import annotations

import pytest
from kubernetes.client import V1ConfigMap, V1Role, V1RoleBinding, V1Service

from src.deployments.core.configs.container import Image
from src.deployments.core.configs.helpers.utils import find_container_config
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.pod_api_requester import redact_payloads


def test_default_init_enables_requester_base():
    builder = PodApiRequesterBuilder()
    assert builder._requester_base_enabled is True
    assert builder.name == "publisher"
    assert builder.app == "zerotenkay-publisher"


def test_with_namespace_and_mode_keeps_requester_base_enabled():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    assert builder._requester_base_enabled is True
    assert builder.namespace == "ns"
    assert builder._mode == "server"


def test_with_container_name_creates_container_if_missing():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    builder.with_container_name("custom-container")
    container = find_container_config(builder.config.pod_spec_config, "custom-container")
    assert container.name == "custom-container"
    assert container.image_pull_policy == "Always"


def test_with_image_updates_container_image():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    builder.with_container_name("custom-container")
    image = Image(repo="repo/test", tag="v1")
    builder.with_image(image)
    container = find_container_config(builder.config.pod_spec_config, "custom-container")
    assert container.image.repo == "repo/test"
    assert container.image.tag == "v1"


def test_with_service_name_updates_dns_search():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    builder.with_service_name("svc-a")
    assert "svc-a.ns.svc.cluster.local" in builder.config.pod_spec_config.dns_config.searches


def test_with_dns_search_delegates_to_pod_spec():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    builder.with_dns_search("custom.ns.svc.cluster.local")
    assert "custom.ns.svc.cluster.local" in builder.config.pod_spec_config.dns_config.searches


def test_debug_mode_adds_debug_env_and_sleep_command():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("debug")
    container = find_container_config(builder.config.pod_spec_config, builder._container_name)
    assert any(env.name == "LOGGING_LEVEL" and env.value == "DEBUG" for env in container.env)
    assert container.command_config.commands[-1].command == "sleep"
    assert container.command_config.commands[-1].args == ["infinity"]


def test_build_dependencies_returns_requester_base_bundle():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    deps = builder.build_dependencies()
    base = deps["requester_base"]
    assert "services" in base
    assert "roles" in base
    assert "role_bindings" in base
    assert "config_maps" in base
    assert isinstance(base["services"][0], V1Service)
    assert isinstance(base["roles"][0], V1Role)
    assert isinstance(base["role_bindings"][0], V1RoleBinding)
    assert isinstance(base["config_maps"][0], V1ConfigMap)


def test_build_dependencies_service_is_nodeport_with_expected_port():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    service = builder.build_dependencies()["requester_base"]["services"][0]
    assert service.spec.type == "NodePort"
    assert service.spec.ports[0].port == 8000
    assert service.spec.ports[0].target_port == 8645


def test_build_requires_namespace():
    builder = PodApiRequesterBuilder().with_mode("server")
    with pytest.raises(ValueError):
        builder.build()


def test_build_requires_mode():
    builder = PodApiRequesterBuilder().with_namespace("ns")
    with pytest.raises(ValueError):
        builder.build()


def test_redact_payloads_replaces_nested_payloads():
    data = {"message": {"payload": "abc", "contentTopic": "/t"}, "items": [{"payload": "d"}]}

    assert redact_payloads(data) == {
        "message": {"payload": "<redacted>", "contentTopic": "/t"},
        "items": [{"payload": "<redacted>"}],
    }

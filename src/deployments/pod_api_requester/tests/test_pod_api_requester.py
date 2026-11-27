from __future__ import annotations

import pytest

from src.deployments.core.configs.helpers.utils import find_container_config
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder


def test_with_service_name_updates_dns_search():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("server")
    builder.with_service_name("svc-a")
    assert "svc-a.ns.svc.cluster.local" in builder.config.pod_spec_config.dns_config.searches


def test_debug_mode_adds_debug_env_and_sleep_command():
    builder = PodApiRequesterBuilder().with_namespace("ns").with_mode("debug")
    container = find_container_config(builder.config.pod_spec_config, builder._container_name)
    assert any(env.name == "LOGGING_LEVEL" and env.value == "DEBUG" for env in container.env)
    assert container.command_config.commands[-1].command == "sleep"
    assert container.command_config.commands[-1].args == ["infinity"]


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

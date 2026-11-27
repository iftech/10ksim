from __future__ import annotations

from kubernetes.client import V1Service, V1ServicePort

from src.deployments.core.configs.service import ServiceConfig, build_service


def test_build_returns_service_with_type_and_port():
    config = ServiceConfig(name="svc", namespace="ns")
    config.service_spec.spec_type = "NodePort"
    config.service_spec.with_selector("app", "demo")
    config.service_spec.with_port(V1ServicePort(port=80, name="http"))
    service = build_service(config)
    assert isinstance(service, V1Service)
    assert service.spec.type == "NodePort"
    assert service.spec.selector == {"app": "demo"}
    assert service.spec.ports[0].port == 80


def test_build_service_sets_publish_not_ready_addresses():
    config = ServiceConfig(name="svc", namespace="ns")
    config.service_spec.publish_not_ready_addresses = True
    service = build_service(config)
    assert service.spec.publish_not_ready_addresses is True

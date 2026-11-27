import pytest
from kubernetes.client import (
    V1ContainerPort,
    V1EnvVar,
    V1Probe,
)

from src.deployments.core.configs.container import ContainerConfig, Image, build_container


def test_with_image_raises_on_duplicate_without_overwrite():
    config = ContainerConfig(
        name="c",
        image=Image(repo="repo1", tag="tag1"),
        image_pull_policy="Always",
    )
    with pytest.raises(ValueError):
        config.with_image(Image(repo="repo2", tag="tag2"))


def test_with_port_replaces_existing_when_overwrite_true():
    config = ContainerConfig(name="c", image_pull_policy="Always")
    port1 = V1ContainerPort(container_port=8080)
    port2 = V1ContainerPort(container_port=8080, name="http")
    config.with_port(port1)
    config.with_port(port2, overwrite=True)
    assert config.ports == [port2]


def test_with_port_raises_on_duplicate_without_overwrite():
    config = ContainerConfig(name="c", image_pull_policy="Always")
    port = V1ContainerPort(container_port=8080)
    config.with_port(port)
    with pytest.raises(ValueError):
        config.with_port(port)


def test_env_var_at_index_zero_can_be_overwritten():
    config = ContainerConfig(name="c", image_pull_policy="Always")
    config.with_env_var(V1EnvVar(name="A", value="1"))
    config.with_env_var(V1EnvVar(name="A", value="2"), overwrite=True)
    assert config.env[0].value == "2"


def test_with_readiness_probe_accepts_dict():
    config = ContainerConfig(name="c", image_pull_policy="Always")
    probe_dict = {
        "httpGet": {"path": "/health", "port": 8008},
        "initialDelaySeconds": 1,
    }
    config.with_readiness_probe(probe_dict)
    assert isinstance(config.readiness_probe, V1Probe)


def test_build_container_maps_fields():
    config = ContainerConfig(
        name="c",
        image=Image(repo="repo", tag="tag"),
        image_pull_policy="Always",
    )
    config.with_env_var(V1EnvVar(name="A", value="1"))
    config.with_port(V1ContainerPort(container_port=8080))
    container = build_container(config)
    assert container.name == "c"
    assert container.image == "repo:tag"
    assert container.env[0].name == "A"
    assert container.ports[0].container_port == 8080

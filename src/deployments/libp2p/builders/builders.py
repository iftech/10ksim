# Python Imports
from typing import List, Literal, Self

from kubernetes.client import V1EnvVar, V1Probe, V1StatefulSet
from pydantic import PositiveInt

# Project Imports
from src.deployments.core.builders import StatefulSetBuilder
from src.deployments.core.configs.container import Image
from src.deployments.libp2p.builders.helpers import (
    LIBP2P_CONTAINER_NAME,
    find_libp2p_container_config,
)
from src.deployments.libp2p.builders.nodes import Nodes


class Option:
    peers = "PEERS"
    connect_to = "CONNECTTO"
    muxer = "MUXER"
    fragments = "FRAGMENTS"
    self_trigger = "SELFTRIGGER"
    service = "SERVICE"
    max_connections = "MAXCONNECTIONS"
    cold_start_delay = "COLDSTARTDELAY"
    startup_jitter = "STARTUP_JITTER_STEP_MS"
    node_role = "NODE_ROLE"
    discovery = "DISCOVERY"


class Libp2pStatefulSetBuilder(StatefulSetBuilder):
    def build(self) -> V1StatefulSet:
        if not self.config.name:
            raise ValueError(f"Must configure node first. Config: `{self.config}`")
        return super().build()

    def with_libp2p_config(
        self,
        name: str,
        namespace: str,
        num_nodes: PositiveInt,
        dns_searches: List[str] = None,
        service: str = "nimp2p-service",
    ) -> Self:
        self.config.name = name
        self.config.namespace = namespace
        self.config.pod_management_policy = "Parallel"
        self.config.stateful_set_spec = Nodes.create_stateful_set_spec_config(
            service=service, namespace=namespace, dns_searches=dns_searches
        )
        self.config.stateful_set_spec.replicas = num_nodes

        return self

    def with_option(self, key, value) -> Self:
        container = find_libp2p_container_config(self.config)
        container.with_env_var(V1EnvVar(name=key, value=str(value)))
        return self

    def with_image(self, image: Image) -> Self:
        self.with_image_in_container(
            image=image, container_name=LIBP2P_CONTAINER_NAME, overwrite=True
        )
        return self

    def with_readiness_probe(self, probe: V1Probe) -> Self:
        container = find_libp2p_container_config(self.config)
        container.with_readiness_probe(probe, overwrite=True)
        return self

    def with_pull_policy(self, policy: Literal["IfNotPresent", "Always", "Never"]) -> Self:
        config = find_libp2p_container_config(self.config)
        config.image_pull_policy = policy
        return self

# Python imports
from typing import List, Literal, Optional, Self

from kubernetes.client import V1EnvVar
from pydantic import PositiveInt

# Project imports
from src.deployments.core.builders import StatefulSetBuilder
from src.deployments.core.configs.container import Image
from src.deployments.core.configs.helpers.utils import with_container_command_args
from src.deployments.waku.builders import bootstrap as WakuBootstrapNode
from src.deployments.waku.builders import light_client as LightClient
from src.deployments.waku.builders import store as Store
from src.deployments.waku.builders.enr_or_addr import Addrs, Enr, FilterAddrs
from src.deployments.waku.builders.helpers import (
    WAKU_COMMAND_STR,
    WAKU_CONTAINER_NAME,
    find_waku_container_config,
)
from src.deployments.waku.builders.nodes import Nodes
from src.deployments.waku.builders.regression import RegressionNodes


class WakuStatefulSetBuilder(StatefulSetBuilder):
    def build(self):
        if not self.config.name:
            raise ValueError(f"Must configure node first. Config: `{self.config}`")
        return super().build()

    def with_waku_config(self, name: str, namespace: str, num_nodes: PositiveInt) -> Self:
        self.config.name = name
        self.config.namespace = namespace
        self.config.apiVersion = "apps/v1"
        self.config.kind = "StatefulSet"
        self.config.pod_management_policy = "Parallel"
        self.config.stateful_set_spec = Nodes.create_stateful_set_spec_config(namespace)
        self.config.stateful_set_spec.replicas = num_nodes
        return self

    def with_regression(self, cmd_type: int, num_enrs: int) -> Self:
        if not self.config.name:
            raise ValueError(f"Must configure node first. Config: `{self.config}`")
        self.with_args(RegressionNodes.create_args(cmd_type))
        self.with_enr(num_enrs, [f"zerotesting-bootstrap.{self.config.namespace}"])
        self.config.stateful_set_spec.pod_template_spec_config.pod_spec_config.with_dns_search(
            f"zerotesting-bootstrap.{self.config.namespace}.svc.cluster.local"
        )
        container = find_waku_container_config(self.config)
        container.with_resources(Nodes.create_resources())
        return self

    def with_bootstrap(self, cmd_type: int) -> Self:
        if not self.config.name:
            raise ValueError(f"Must configure node first. Config: `{self.config}`")
        WakuBootstrapNode.apply_stateful_set_config(
            self.config, self.config.namespace, overwrite=True
        )
        self.with_args(WakuBootstrapNode.create_args(cmd_type))
        return self

    def with_store(self) -> Self:
        Store.apply_stateful_set_config(self.config)
        return self

    def with_nice_command(self, increment: int) -> Self:
        """Runs node with cpu priority.

        :param increment:
            Positive: Lower priority.
            Zero: The default priority for processes.
            Negative: Higher priority.
        """
        Nodes.apply_nice_command(self.config, increment)
        return self

    def with_args(
        self,
        args: List[str],
        *,
        on_duplicate: Literal["error", "ignore", "replace"] = "error",
    ) -> Self:
        with_container_command_args(
            self.config, "waku", WAKU_COMMAND_STR, args, on_duplicate=on_duplicate
        )
        return self

    def with_enr(
        self,
        num: int,
        service_names: List[str] | str,
        init_container_image: Optional[Image] = None,
    ) -> Self:
        if isinstance(service_names, str):
            service_names = [service_names]
        Enr.pod_spec(
            self.config.stateful_set_spec.pod_template_spec_config.pod_spec_config,
            num=num,
            service_names=service_names,
            init_container_image=init_container_image,
        )
        return self

    def with_filter_server(self) -> Self:
        """A relay node that also serves filter subscriptions."""
        self.with_args({"--filter": True}, on_duplicate="replace")
        return self

    def with_lightpush_server(self, app: str, service_name: str) -> Self:
        """A relay node that also serves lightpush requests."""
        self.with_args({"--lightpush": True}, on_duplicate="replace")
        spec = self.config.stateful_set_spec
        spec.with_app(app)
        spec.with_service_name(service_name, overwrite=True)
        spec.pod_template_spec_config.with_app(app)
        return self

    def with_light_client(self, cmd_type: int, app: str, service_name: str) -> Self:
        """Configure this set as edge nodes: relay off, health probe, no discovery."""
        if not self.config.name:
            raise ValueError(f"Must configure node first. Config: `{self.config}`")
        self.with_args(LightClient.create_args(cmd_type))
        LightClient.apply_stateful_set_config(self.config, app, service_name, overwrite=True)
        return self

    def _with_service_addrs(
        self,
        feature,
        num: int,
        service_names: List[str] | str,
        init_container_image: Optional[Image] = None,
    ) -> Self:
        """Point this node at `num` service nodes resolved from `service_names`."""
        if isinstance(service_names, str):
            service_names = [service_names]
        feature.pod_spec(
            self.config.stateful_set_spec.pod_template_spec_config.pod_spec_config,
            num=num,
            service_names=service_names,
            init_container_image=init_container_image,
        )
        return self

    def with_lightpush_addrs(
        self,
        num: int,
        service_names: List[str] | str,
        init_container_image: Optional[Image] = None,
    ) -> Self:
        return self._with_service_addrs(Addrs, num, service_names, init_container_image)

    def with_filter_addrs(
        self,
        num: int,
        service_names: List[str] | str,
        init_container_image: Optional[Image] = None,
    ) -> Self:
        return self._with_service_addrs(FilterAddrs, num, service_names, init_container_image)

    def with_image(self, image: Image) -> Self:
        self.with_image_in_container(
            image=image, container_name=WAKU_CONTAINER_NAME, overwrite=True
        )
        return self

    def with_pull_policy(self, policy: Literal["IfNotPresent", "Always", "Never"]) -> Self:
        config = find_waku_container_config(self.config)
        config.image_pull_policy = policy
        return self

    def with_env(self, key: str, value: str) -> Self:
        config = find_waku_container_config(self.config)
        config.with_env_var(V1EnvVar(name=key, value=str(value)))
        return self

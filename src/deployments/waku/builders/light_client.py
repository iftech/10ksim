# Python Imports
from typing import Dict

# Project Imports
from src.deployments.core.builders import default_readiness_probe_health
from src.deployments.core.configs.container import ContainerConfig
from src.deployments.core.configs.pod import PodSpecConfig, PodTemplateSpecConfig
from src.deployments.core.configs.statefulset import StatefulSetConfig, StatefulSetSpecConfig
from src.deployments.waku.builders.helpers import find_waku_container_config
from src.deployments.waku.builders.nodes import Nodes


def create_args(cmd_type: int) -> Dict:
    """Args for an edge node: no relay, so no mesh and no discovery."""
    base = {
        "--relay": False,
        "--log-level": "INFO",
        "--metrics-server-address": "0.0.0.0",
        "--metrics-server": True,
        "--nat": "extip:${IP}",
        "--rest-address": "0.0.0.0",
        "--rest-admin": True,
        "--rest": True,
    }
    if cmd_type == 1:
        return {**base, "--cluster-id": 2, "--shard": 0}
    elif cmd_type == 2:
        return {**base, "--num-shards-in-network": 1, "--shard": 0}

    raise ValueError(f"Invalid cmd_type: `{cmd_type}`")


def apply_container_config(config: ContainerConfig, *, overwrite: bool = False):
    config.with_readiness_probe(default_readiness_probe_health(), overwrite=overwrite)
    config.with_resources(Nodes.create_resources(), overwrite=True)


def apply_pod_spec_config(config: PodSpecConfig, *, overwrite: bool = False):
    apply_container_config(find_waku_container_config(config), overwrite=overwrite)


def apply_pod_template_spec_config(
    config: PodTemplateSpecConfig, app: str, *, overwrite: bool = False
):
    config.with_app(app)
    apply_pod_spec_config(config.pod_spec_config, overwrite=overwrite)


def apply_stateful_set_spec_config(
    config: StatefulSetSpecConfig, app: str, service_name: str, *, overwrite: bool = False
):
    config.with_app(app)
    config.with_service_name(service_name, overwrite=overwrite)
    apply_pod_template_spec_config(config.pod_template_spec_config, app, overwrite=overwrite)


def apply_stateful_set_config(
    config: StatefulSetConfig, app: str, service_name: str, *, overwrite: bool = False
):
    apply_stateful_set_spec_config(config.stateful_set_spec, app, service_name, overwrite=overwrite)

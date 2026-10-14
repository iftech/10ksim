# Python Imports
import os
from typing import Optional

from kubernetes import client
from kubernetes.client.models import V1Node

_kube_config = None


def set_config_file(config: str):
    global _kube_config
    _kube_config = os.path.expanduser(config)


def get_config_file() -> Optional[str]:
    return _kube_config


def is_local() -> bool:
    """
    Detects if Kubernetes cluster is local.
    """
    configuration = client.Configuration.get_default_copy()
    server_url = configuration.host
    local_hosts = ["localhost", "127.0.0.1", "host.docker.internal", "host.minikube.internal"]
    return any(host in server_url.lower() for host in local_hosts)


def get_node_ip(node: V1Node) -> str:
    """
    Returns the IP used for making API requests to a node.
    """
    if is_local():
        return "localhost"

    # If we have an external IP, use that.
    # Otherwise use the InternalIP.
    try:
        return next(
            (address.address for address in node.status.addresses if address.type == "ExternalIP")
        )
    except StopIteration:
        pass
    try:
        return next(
            address.address for address in node.status.addresses if address.type == "InternalIP"
        )
    except StopIteration as e:
        raise ValueError(f"Failed to find IP for node. Node: `{node.metadata.name}`") from e

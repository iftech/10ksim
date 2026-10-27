"""Mount the Mix setup scripts; public relay information is gathered inside the cluster."""

from pathlib import Path

from kubernetes.client import V1ConfigMap, V1ObjectMeta

MIX_SCRIPTS_CONFIG_MAP = "storage-mix-scripts"
MIX_SCRIPTS_DIR = "/mix-scripts"
MIX_CONFIG_DIR = "/mix-config"
MIX_INIT_CONTAINER = "prepare-mix"


def build_mix_scripts(namespace: str) -> V1ConfigMap:
    scripts = Path(__file__).parent / "scripts"
    return V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=V1ObjectMeta(name=MIX_SCRIPTS_CONFIG_MAP, namespace=namespace),
        data={name: (scripts / name).read_text() for name in ("prepare_mix.sh", "start_storage.sh")},
    )

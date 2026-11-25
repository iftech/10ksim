# Project Imports
from src.deployments.waku.builders.builders import WakuStatefulSetBuilder
from src.deployments.waku.builders.enr_or_addr import Addrs, Enr


def _pod_spec_config():
    builder = WakuStatefulSetBuilder().with_waku_config(
        name="node-0", namespace="zerotesting", num_nodes=1
    )
    return builder.config.stateful_set_spec.pod_template_spec_config.pod_spec_config


def _mounted_volume_names(pod_spec_config):
    containers = (pod_spec_config.init_containers or []) + (pod_spec_config.container_configs or [])
    return {mount.name for container in containers for mount in container.volume_mounts or []}


def test_enr_declares_every_volume_it_mounts():
    config = _pod_spec_config()

    Enr.pod_spec(config, num=1, service_names=["zerotesting-bootstrap.zerotesting"])

    mounted = _mounted_volume_names(config)
    assert "enr-data" in mounted
    assert mounted <= {volume.name for volume in config.volumes}


def test_addrs_declares_every_volume_it_mounts():
    config = _pod_spec_config()

    Addrs.pod_spec(config, num=1, service_names=["zerotesting-lightpush-server.zerotesting"])

    mounted = _mounted_volume_names(config)
    assert "address-data" in mounted
    assert mounted <= {volume.name for volume in config.volumes}


def test_with_store_builds():
    stateful_set = (
        WakuStatefulSetBuilder()
        .with_waku_config(name="store-0", namespace="other-namespace", num_nodes=1)
        .with_store()
        .build()
    )

    assert stateful_set.metadata.namespace == "other-namespace"
    searches = stateful_set.spec.template.spec.dns_config.searches
    assert "zerotesting-store.other-namespace.svc.cluster.local" in searches

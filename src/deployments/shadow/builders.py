# Builders for Shadow simulator runs: config values -> kubernetes-client objects
# and yaml dicts. Pure data, no I/O. See the "Using Shadow at DST" runbook.
from typing import Literal, Optional

from kubernetes.client import (
    V1Capabilities,
    V1ConfigMap,
    V1ConfigMapVolumeSource,
    V1Container,
    V1EmptyDirVolumeSource,
    V1Job,
    V1JobSpec,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
    V1ResourceRequirements,
    V1SeccompProfile,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString

# The pod-api-requester app is baked into the Shadow base image; the publisher host
# runs it in batch mode and reads its traffic config from the mounted ConfigMap.
_REQUESTER_APP_PATH = "/app/api_requester.py"
_PUBLISHER_CONFIG = "publisher.yaml"  # ConfigMap key == filename mounted under /sim/config

# Mount targets inside the Shadow runner container.
_BIN_MOUNT = "/sim/bin"
_CONFIG_MOUNT = "/sim/config"
_RUN_MOUNT = "/sim/run"  # PVC-backed; Shadow writes shadow.data/ here

# Shadow's syscall interposer needs LD_PRELOAD + ptrace.
_SHADOW_SECURITY = V1SecurityContext(
    seccomp_profile=V1SeccompProfile(type="Unconfined"),
    capabilities=V1Capabilities(add=["SYS_PTRACE"]),
)


def _peer_env(
    *,
    num_nodes: int,
    connect_to: int,
    muxer: str,
    discovery: str,
    start_sleep: int,
    metrics_interval_s: int,
    lsquic_tick_floor_us: int,
) -> dict:
    """Environment for a relay (pod-*) node."""
    env = {
        "PEERS": str(num_nodes),
        "CONNECTTO": str(connect_to),
        "SHADOWENV": "true",  # env.nim requires the literal string "true"
        "MUXER": muxer,
        "DISCOVERY": discovery,
        "STARTSLEEP": str(start_sleep),
        "METRICS_INTERVAL_S": str(metrics_interval_s),
    }
    if lsquic_tick_floor_us > 0:
        # Needs the tick-floor test-node image (stock images ignore the env var).
        env["LSQUIC_TICK_FLOOR_US"] = str(lsquic_tick_floor_us)
    if discovery == "kad-dht":
        env["NODE_ROLE"] = "RoleNormal"
        env["SERVICE"] = "bootstrap-0"
    return env


# 11.{100+s//250}.{s%250}.{10+h}; the 11.100+ base keeps clear of the addresses Shadow
# assigns to unpinned hosts (bootstrap, publisher).
_MAX_HOSTS_PER_SUBNET = 245
_MAX_SUBNETS = (255 - 100) * 250 - 1


def _peer_ip(index: int, hosts_per_subnet: int) -> str:
    """Address for peer `index`, packing `hosts_per_subnet` peers into each /24."""
    if not 1 <= hosts_per_subnet <= _MAX_HOSTS_PER_SUBNET:
        raise ValueError(
            f"hosts_per_subnet must be 1..{_MAX_HOSTS_PER_SUBNET}, got {hosts_per_subnet}"
        )
    subnet, host = divmod(index, hosts_per_subnet)
    if subnet > _MAX_SUBNETS:
        raise ValueError(f"IP plan exhausted at peer {index} (max {_MAX_SUBNETS + 1} subnets)")
    return f"11.{100 + subnet // 250}.{subnet % 250}.{10 + host}"


def _peer_hosts(
    num_nodes: int, peer_env: dict, start_jitter_ms: int, hosts_per_subnet: int
) -> dict:
    """The N peer hosts (pod-0..pod-(N-1)). start_jitter_ms staggers per-pod process
    start so peers don't wake and dial at one simulated instant (lockstep wakes force
    simultaneous-dial collisions that never occur on real hosts)."""
    return {
        f"pod-{i}": {
            "network_node_id": 0,
            "ip_addr": _peer_ip(i, hosts_per_subnet),
            "processes": [
                {
                    "path": "./main",
                    "start_time": f"{5000 + i * start_jitter_ms}ms",
                    # daemon: don't error when alive at stop_time
                    "expected_final_state": "running",
                    "environment": peer_env,
                }
            ],
        }
        for i in range(num_nodes)
    }


def _bootstrap_host(
    *,
    num_nodes: int,
    muxer: str,
    start_sleep: int,
    metrics_interval_s: int,
    lsquic_tick_floor_us: int,
    binary: str = "./main",
) -> dict:
    """The kad-dht anchor (bootstrap-0); peers discover through it by hostname."""
    env = {
        "PEERS": str(num_nodes),
        "SHADOWENV": "true",
        "MUXER": muxer,
        "DISCOVERY": "kad-dht",
        "NODE_ROLE": "RoleBootstrap",
        # the single anchor must accept every node's bootstrap dial, so lift its cap
        # above the network size (default is 250).
        "MAXCONNECTIONS": str(num_nodes + 100),
        "STARTSLEEP": str(start_sleep),
        "METRICS_INTERVAL_S": str(metrics_interval_s),
    }
    if lsquic_tick_floor_us > 0:
        env["LSQUIC_TICK_FLOOR_US"] = str(lsquic_tick_floor_us)
    return {
        "network_node_id": 0,
        "processes": [
            {
                "path": binary,
                "start_time": "5s",
                "expected_final_state": "running",
                "environment": env,
            }
        ],
    }


def _publisher_host(publisher_start_s: int, requester_app_path: str) -> dict:
    """The publisher host: runs the pod-api-requester in batch mode against the peers."""
    return {
        "network_node_id": 0,
        "processes": [
            {
                "path": "/usr/bin/python3",
                "args": (
                    f"{requester_app_path}"
                    f" --mode batch"
                    f" --config {_CONFIG_MOUNT}/{_PUBLISHER_CONFIG}"
                ),
                "start_time": f"{publisher_start_s}s",
            }
        ],
    }


_SWITCH_BANDWIDTH_MBIT = 1000
_SWITCH_LATENCY_MS = 0


def _wan_gml(latency_ms: int, bandwidth_mbit: int, loss_pct: float = 0.0) -> LiteralScalarString:
    """A one-node GML graph with a self-loop, so every host (all on network_node_id 0)
    sees `latency_ms` one-way delay and `bandwidth_mbit` up/down. Shadow's GML parser
    wants multi-line GML (one attribute per line); return it as a YAML literal block so
    the inline value keeps its newlines."""
    gml = f"""graph [
  directed 0
  node [
    id 0
    host_bandwidth_up "{bandwidth_mbit} Mbit"
    host_bandwidth_down "{bandwidth_mbit} Mbit"
  ]
  edge [
    source 0
    target 0
    latency "{latency_ms} ms"
    packet_loss {loss_pct / 100}
  ]
]
"""
    return LiteralScalarString(gml)


def render_shadow_yaml(
    *,
    num_nodes: int,
    sim_stop_time_s: int,
    publisher_start_s: int,
    connect_to: int = 2,
    muxer: Literal["yamux", "mplex", "quic"] = "yamux",
    discovery: Literal["static", "kad-dht"] = "static",
    start_sleep: int = 60,
    metrics_interval_s: int = 15,
    seed: int = 1,
    model_unblocked_syscall_latency: bool = False,
    strace_logging_mode: str = "off",
    lsquic_tick_floor_us: int = 0,
    start_jitter_ms: int = 0,
    latency_ms: Optional[int] = None,
    bandwidth_mbit: Optional[int] = None,
    loss_pct: float = 0.0,
    hosts_per_subnet: int = 1,
    requester_app_path: str = _REQUESTER_APP_PATH,
    bootstrap_binary: str = "./main",
) -> dict:
    """Build the shadow.yaml dict: N peer hosts running `./main` + a publisher host
    running the pod-api-requester in batch mode against the peers' `/publish`
    endpoints. The traffic shape (message count, size, pacing) lives in the
    requester's own config (see `render_publisher_config`), mounted at
    `{_CONFIG_MOUNT}/{_PUBLISHER_CONFIG}`.

    discovery selects mesh formation: "static" dials CONNECTTO peers by pod-N
    hostname; "kad-dht" adds a `bootstrap-0` anchor host that peers discover
    through (Shadow resolves it by hostname, so no k8s Service is needed).

    hosts_per_subnet: 1 gives every peer its own /24; raise it to share prefixes."""
    if connect_to >= num_nodes:
        raise ValueError(f"connect_to ({connect_to}) must be smaller than num_nodes ({num_nodes}).")

    peer_env = _peer_env(
        num_nodes=num_nodes,
        connect_to=connect_to,
        muxer=muxer,
        discovery=discovery,
        start_sleep=start_sleep,
        metrics_interval_s=metrics_interval_s,
        lsquic_tick_floor_us=lsquic_tick_floor_us,
    )
    hosts = _peer_hosts(num_nodes, peer_env, start_jitter_ms, hosts_per_subnet)
    if discovery == "kad-dht":
        hosts["bootstrap-0"] = _bootstrap_host(
            num_nodes=num_nodes,
            muxer=muxer,
            start_sleep=start_sleep,
            metrics_interval_s=metrics_interval_s,
            lsquic_tick_floor_us=lsquic_tick_floor_us,
            binary=bootstrap_binary,
        )
    hosts["publisher"] = _publisher_host(publisher_start_s, requester_app_path)

    if latency_ms is None and bandwidth_mbit is None and not loss_pct:
        graph = {"type": "1_gbit_switch"}
    else:
        if latency_ms is not None and latency_ms < 0:
            raise ValueError(f"latency_ms must be >= 0, got {latency_ms}")
        if bandwidth_mbit is not None and bandwidth_mbit <= 0:
            raise ValueError(f"bandwidth_mbit must be > 0, got {bandwidth_mbit}")
        if not 0 <= loss_pct <= 100:
            raise ValueError(f"loss_pct must be 0..100, got {loss_pct}")
        graph = {
            "type": "gml",
            "inline": _wan_gml(
                _SWITCH_LATENCY_MS if latency_ms is None else latency_ms,
                _SWITCH_BANDWIDTH_MBIT if bandwidth_mbit is None else bandwidth_mbit,
                loss_pct,
            ),
        }

    # Always render the seed so the run's shadow.yaml records it (Shadow defaults to 1).
    config = {
        "general": {
            "stop_time": f"{sim_stop_time_s}s",
            "progress": True,
            "seed": seed,
        },
        "network": {"graph": graph},
        "hosts": hosts,
    }
    if model_unblocked_syscall_latency:
        config["general"]["model_unblocked_syscall_latency"] = True
    if strace_logging_mode != "off":
        # Global (all hosts) and voluminous — a straced host writes ~100s of MB per
        # simulated minute of activity. Diagnostics at small N only.
        config["experimental"] = {"strace_logging_mode": strace_logging_mode}
    return config


def render_publisher_config(
    *,
    num_nodes: int,
    num_messages: int,
    msg_size_bytes: int,
    delay_seconds: float,
    port: int = 8645,
    topic: str = "test",
) -> dict:
    """Build the pod-api-requester batch config reproducing the legacy traffic_sync.py
    injector: publish `num_messages` messages round-robin across pod-0..pod-(N-1),
    `delay_seconds` apart, to each peer's `/publish` endpoint. Targets are an explicit
    `hosts` list (pod-0..pod-(N-1)) resolved by Shadow DNS, so no Kubernetes API is
    contacted.

    `pod_count = num_messages` walks the sorted host list with wraparound, matching
    traffic_sync's `pod-(i % num_nodes)` selection (and its `version: 1` / `msgSize`
    JSON body)."""
    return {
        "targets": [
            {
                "name": "peers",
                "hosts": [f"pod-{i}" for i in range(num_nodes)],
                "port": port,
            }
        ],
        "endpoints": [
            {
                "name": "libp2p-publish",
                "url": "http://{node}:{port}/publish",
                "headers": {"Content-Type": "application/json"},
                "params": {"topic": topic, "msgSize": msg_size_bytes, "version": 1},
                "type": "POST",
                "paged": False,
            }
        ],
        "requests": [
            {
                "name": "publish",
                "endpoint": "libp2p-publish",
                "retries": 0,
                "retry_delay": 0,
            }
        ],
        "actions": [
            {
                "name": "inject-traffic",
                "requests": ["publish"],
                "targets": ["peers"],
                "order": "ascending",
                "pod_start_index": 0,
                "pod_count": num_messages,
                "delay": delay_seconds,
                "loop_order": "foreach_request_target_each_pod",
            }
        ],
    }


def _dump_yaml(obj: dict) -> str:
    """Serialize a dict to a YAML string using ruamel.yaml (matches the rest of 10ksim)."""
    import io

    buf = io.StringIO()
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.dump(obj, buf)
    return buf.getvalue()


def build_configmap(
    *,
    namespace: str,
    name: str,
    shadow_yaml: dict,
    publisher_config: dict,
) -> V1ConfigMap:
    """ConfigMap with shadow.yaml + the pod-api-requester batch config, mounted at
    /sim/config/. The requester app itself is baked into the Shadow base image."""
    return V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=V1ObjectMeta(name=name, namespace=namespace),
        data={
            "shadow.yaml": _dump_yaml(shadow_yaml),
            _PUBLISHER_CONFIG: _dump_yaml(publisher_config),
        },
    )


def _fetch_binary(name: str, image: str, binary: str) -> V1Container:
    """Init container copying an image's /node/main into the shared bin volume."""
    return V1Container(
        name=name,
        image=image,
        image_pull_policy="Always",
        # image ENTRYPOINT is the binary; override to copy it out
        command=[
            "sh",
            "-c",
            f"cp /node/main {_BIN_MOUNT}/{binary} && chmod +x {_BIN_MOUNT}/{binary}",
        ],
        volume_mounts=[V1VolumeMount(name="bin", mount_path=_BIN_MOUNT)],
    )


def build_shadow_job(
    *,
    namespace: str,
    name: str,
    configmap_name: str,
    pvc_name: str,
    test_node_image: str,
    shadow_base_image: str,
    bootstrap_image: Optional[str] = None,
    node_pin: Optional[str] = None,
    cpu_request: str = "2",
    cpu_limit: str = "4",
    memory_request: str = "4Gi",
    memory_limit: str = "8Gi",
) -> V1Job:
    """k8s Job that runs Shadow: init container stages the nim binary, main container
    is the Shadow runner."""
    init_containers = [_fetch_binary("fetch-test-node", test_node_image, "main")]
    binaries = ["main"]
    if bootstrap_image:
        init_containers.append(_fetch_binary("fetch-bootstrap", bootstrap_image, "bootstrap"))
        binaries.append("bootstrap")

    # Stage binary + config into the PVC run dir, then exec Shadow.
    main_command = [
        "/bin/bash",
        "-eu",
        "-c",
        (
            "".join(f"cp {_BIN_MOUNT}/{b} {_RUN_MOUNT}/{b} && " for b in binaries)
            + f"cp {_CONFIG_MOUNT}/shadow.yaml {_RUN_MOUNT}/shadow.yaml && "
            f"cd {_RUN_MOUNT} && "
            "exec shadow shadow.yaml"
        ),
    ]
    main_container = V1Container(
        name="shadow",
        image=shadow_base_image,
        image_pull_policy="Always",
        command=main_command,
        security_context=_SHADOW_SECURITY,
        resources=V1ResourceRequirements(
            requests={"cpu": cpu_request, "memory": memory_request},
            limits={"cpu": cpu_limit, "memory": memory_limit},
        ),
        volume_mounts=[
            V1VolumeMount(name="bin", mount_path=_BIN_MOUNT),
            V1VolumeMount(name="config", mount_path=_CONFIG_MOUNT),
            V1VolumeMount(name="run", mount_path=_RUN_MOUNT),
        ],
    )

    pod_spec = V1PodSpec(
        restart_policy="Never",
        init_containers=init_containers,
        containers=[main_container],
        volumes=[
            V1Volume(name="bin", empty_dir=V1EmptyDirVolumeSource()),
            V1Volume(
                name="config",
                config_map=V1ConfigMapVolumeSource(name=configmap_name, default_mode=0o755),
            ),
            V1Volume(
                name="run",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name),
            ),
        ],
        node_selector={"kubernetes.io/hostname": node_pin} if node_pin else None,
    )

    return V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=name, namespace=namespace, labels={"app": name}),
        spec=V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=86400,
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"app": name}),
                spec=pod_spec,
            ),
        ),
    )


def build_pvc(
    *,
    namespace: str,
    name: str,
    storage: str = "5Gi",
    storage_class: str = "longhorn",
) -> V1PersistentVolumeClaim:
    """PVC holding the run's shadow.data/ output (RWO; Job and reader pod share a node)."""
    return V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=V1ObjectMeta(name=name, namespace=namespace),
        spec=V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=storage_class,
            resources=V1ResourceRequirements(requests={"storage": storage}),
        ),
    )


def build_log_reader_pod(
    *,
    namespace: str,
    name: str,
    pvc_name: str,
    image: str,
    node_pin: Optional[str] = None,
) -> V1Pod:
    """Short-lived pod mounting the run PVC so `kubectl cp` can copy shadow.data/ out
    after the Job finishes. Same node as the Job (RWO); reuses the base image (has tar)."""
    return V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(name=name, namespace=namespace, labels={"app": name}),
        spec=V1PodSpec(
            restart_policy="Never",
            containers=[
                V1Container(
                    name="reader",
                    image=image,
                    command=["sleep", "3600"],
                    volume_mounts=[V1VolumeMount(name="run", mount_path=_RUN_MOUNT)],
                )
            ],
            volumes=[
                V1Volume(
                    name="run",
                    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name
                    ),
                )
            ],
            node_selector={"kubernetes.io/hostname": node_pin} if node_pin else None,
        ),
    )

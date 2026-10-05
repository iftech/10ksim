# Shadow GossipSub experiment: N nim libp2p peers + 1 publisher inside Shadow on a
# single k8s pod. See the "Using Shadow at DST" runbook in Notion.
import asyncio
import logging
from typing import ClassVar, Literal, Optional

from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt, PositiveInt

from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.libp2p.bridge import WAN_BANDWIDTH_MBIT, WAN_LATENCY_MS
from src.deployments.registry import experiment
from src.deployments.shadow.builders import (
    build_configmap,
    build_pvc,
    build_shadow_job,
    render_publisher_config,
    render_shadow_yaml,
)
from src.deployments.shadow.runtime import pull_shadow_logs, wait_for_job_complete

logger = logging.getLogger(__name__)


class ExpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # Sim shape
    num_nodes: NonNegativeInt = 10
    num_messages: NonNegativeInt = 5
    message_size_bytes: NonNegativeInt = 1000
    delay_seconds: NonNegativeFloat = 2.0
    connect_to: NonNegativeInt = 2
    muxer: Literal["yamux", "mplex", "quic"] = "yamux"
    discovery: Literal["static", "kad-dht"] = "static"  # CONNECTTO dial vs kad bootstrap
    start_sleep: NonNegativeInt = 60  # node STARTSLEEP before mesh formation
    # Timing (simulated seconds). Publisher starts after the mesh forms (~60s).
    publisher_start_s: NonNegativeInt = 90
    sim_stop_time_s: NonNegativeInt = 180
    # storeMetrics scrape cadence (s); short so the last scrape is post-traffic.
    metrics_interval_s: NonNegativeInt = 15
    # Determinism + diagnostics. seed is rendered into shadow.yaml (Shadow default 1);
    # strace is global and heavy — small-N diagnosis only.
    seed: NonNegativeInt = 1
    model_unblocked_syscall_latency: bool = False
    strace_logging_mode: str = "off"  # off | standard | deterministic
    # Floor (µs) for lsquic engine tick re-arms; needs the tick-floor node image.
    # 0 = stock lsquic behavior (which livelocks quic under Shadow).
    lsquic_tick_floor_us: NonNegativeInt = 0
    # Per-pod process start stagger (pod-i starts at 5000 + i*jitter ms); 0 = lockstep.
    start_jitter_ms: NonNegativeInt = 0
    latency_ms: Optional[NonNegativeInt] = WAN_LATENCY_MS
    bandwidth_mbit: Optional[PositiveInt] = WAN_BANDWIDTH_MBIT
    loss_pct: NonNegativeFloat = 0
    # Peers per /24; 1 = every peer on its own subnet.
    hosts_per_subnet: PositiveInt = 1
    # Job-pod resources, sized for ~10 peers; bump for bigger sims.
    cpu_request: str = "2"
    cpu_limit: str = "4"
    memory_request: str = "4Gi"
    memory_limit: str = "8Gi"
    # Images (explicit tags let old runs be replayed).
    test_node_image: str = "radiken/dst-test-node-shadow:latest"
    shadow_base_image: str = "radiken/dst-shadow-base:latest"
    node_pin: Optional[str] = "node-05.ih-eu-mda1.misc.vaclab"  # avoid node-01 (control plane)
    pvc_storage: str = "5Gi"  # run PVC for shadow.data/ (~200KB/peer)
    wait_timeout_s: NonNegativeInt = 1800


@experiment(name="shadow-gossipsub")
class ShadowGossipsubExperiment(BaseExperiment[ExpConfig]):
    """Run a GossipSub mesh + publisher inside the Shadow simulator."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    post_run_analysis: ClassVar[str] = (
        "src.analysis.post_run.shadow_gossipsub:run_shadow_gossipsub_analysis"
    )

    async def _run(self):
        self.log_event("run_start")
        cfg = self.config
        namespace = self.namespace
        # unique per run (output folder name carries a random suffix). Cap the length
        # so the derived `shadow-<run_id>-reader` log-reader pod stays within the k8s
        # 63-char name limit.
        run_id = self.output_folder.name.lower().replace("_", "-")[:45].strip("-")
        cm_name = f"shadow-{run_id}"
        job_name = f"shadow-{run_id}"
        pvc_name = f"shadow-{run_id}-data"

        shadow_yaml = render_shadow_yaml(
            num_nodes=cfg.num_nodes,
            sim_stop_time_s=cfg.sim_stop_time_s,
            publisher_start_s=cfg.publisher_start_s,
            connect_to=cfg.connect_to,
            muxer=cfg.muxer,
            discovery=cfg.discovery,
            start_sleep=cfg.start_sleep,
            metrics_interval_s=cfg.metrics_interval_s,
            seed=cfg.seed,
            model_unblocked_syscall_latency=cfg.model_unblocked_syscall_latency,
            strace_logging_mode=cfg.strace_logging_mode,
            lsquic_tick_floor_us=cfg.lsquic_tick_floor_us,
            start_jitter_ms=cfg.start_jitter_ms,
            latency_ms=cfg.latency_ms,
            bandwidth_mbit=cfg.bandwidth_mbit,
            loss_pct=cfg.loss_pct,
            hosts_per_subnet=cfg.hosts_per_subnet,
        )
        publisher_config = render_publisher_config(
            num_nodes=cfg.num_nodes,
            num_messages=cfg.num_messages,
            msg_size_bytes=cfg.message_size_bytes,
            delay_seconds=cfg.delay_seconds,
        )

        pvc = build_pvc(namespace=namespace, name=pvc_name, storage=cfg.pvc_storage)
        configmap = build_configmap(
            namespace=namespace,
            name=cm_name,
            shadow_yaml=shadow_yaml,
            publisher_config=publisher_config,
        )
        job = build_shadow_job(
            namespace=namespace,
            name=job_name,
            configmap_name=cm_name,
            pvc_name=pvc_name,
            test_node_image=cfg.test_node_image,
            shadow_base_image=cfg.shadow_base_image,
            node_pin=cfg.node_pin,
            cpu_request=cfg.cpu_request,
            cpu_limit=cfg.cpu_limit,
            memory_request=cfg.memory_request,
            memory_limit=cfg.memory_limit,
        )

        self.dump_yaml(pvc, f"pvc-{pvc_name}")
        self.dump_yaml(configmap, f"configmap-{cm_name}")
        self.dump_yaml(job, f"job-{job_name}")

        # PVC first so LIFO cleanup deletes it last, after the Job releases it.
        pvc_dict = self.api_client.sanitize_for_serialization(pvc)
        cm_dict = self.api_client.sanitize_for_serialization(configmap)
        job_dict = self.api_client.sanitize_for_serialization(job)

        await self.deploy_yaml(deployment_yaml=pvc_dict, wait_for_ready=False)
        await self.deploy_yaml(deployment_yaml=cm_dict, wait_for_ready=False)
        # Jobs aren't handled by wait_for_rollout; we poll with wait_for_job_complete.
        await self.deploy_yaml(deployment_yaml=job_dict, wait_for_ready=False)

        if self.dry_run:
            self.log_event("dry_run_done")
            return

        self.log_event({"event": "wait_for_job", "job": job_name})
        state = await wait_for_job_complete(
            api_client=self.api_client,
            namespace=namespace,
            job_name=job_name,
            timeout_s=cfg.wait_timeout_s,
        )
        self.log_event({"event": "job_done", "state": state})

        # Pull output before cleanup deletes the pod.
        logs_dir = self.output_folder / "shadow_logs"
        try:
            await asyncio.to_thread(
                pull_shadow_logs,
                api_client=self.api_client,
                namespace=namespace,
                job_name=job_name,
                pvc_name=pvc_name,
                reader_image=cfg.shadow_base_image,
                dest_dir=logs_dir,
                node_pin=cfg.node_pin,
            )
            self.log_event({"event": "logs_pulled", "dest": str(logs_dir)})
        except Exception as e:
            # don't let a log-pull failure mask the run state
            logger.exception("Failed to pull Shadow logs")
            self.log_event({"event": "logs_pull_failed", "error": str(e)})
            # Keep the PVC so shadow.data can still be copied by hand; cleanup skips unknown kinds.
            pvc_dict["kind"] = "RetainedPersistentVolumeClaim"
            self.log_event({"event": "pvc_retained", "pvc": pvc_name})

        if state == "failed":
            raise RuntimeError(f"Shadow Job `{namespace}/{job_name}` failed")

        self.log_event("internal_run_finished")

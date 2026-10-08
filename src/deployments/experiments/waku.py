import asyncio
import logging
import os
import random
import traceback
from pathlib import Path
from typing import Dict, Literal, Optional, Union

from kubernetes import client
from pydantic import BaseModel, ConfigDict, Field, NonNegativeFloat, NonNegativeInt

from src.deployments.core.configs.container import Image
from src.deployments.core.configs.statefulset import StatefulSetConfig
from src.deployments.core.pod_interaction import exec_command_in_pod
from src.deployments.experiments.base_experiment import BaseExperiment, V1Deployable
from src.deployments.libp2p.builders.nodes import Nodes
from src.deployments.pod_api_requester.builder import PodApiRequesterBuilder
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import (
    PodApiApplicationError,
    PodApiError,
    requester_address,
)
from src.deployments.pod_api_requester.waku import waku_publish
from src.deployments.registry import experiment
from src.deployments.waku.bridge import Bridge
from src.deployments.waku.builders.builders import WakuStatefulSetBuilder

logger = logging.getLogger(__name__)

Muxer = Literal["default", "quic"]

LogLevel = Literal["INFO", "DEBUG"]


class ExpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    num_messages: NonNegativeInt = 20
    num_relay_nodes: NonNegativeInt = 20
    num_bootstrap_nodes: NonNegativeInt = 5
    msg_size_kbytes: NonNegativeInt = 1
    delay_cold_start: NonNegativeFloat = 1
    delay_after_publish: NonNegativeFloat = 0.5
    muxer: Muxer = "default"
    log_level: LogLevel = "INFO"
    format_json: bool = False
    cmd_type: int = 1
    image: Image = Field(
        default_factory=lambda: Image.from_str("soutullostatus/nwaku-jq-curl:v0.37.0-rc.4")
    )
    grab_metrics: bool = False


def build_relay_nodes(namespace: str, config: ExpConfig) -> V1Deployable:
    nodes_builder = (
        WakuStatefulSetBuilder()
        .with_waku_config(name="relay-0", namespace=namespace, num_nodes=config.num_relay_nodes)
        .with_regression(config.cmd_type, config.num_bootstrap_nodes)
        .with_env("SERVICE", "zerotesting-bootstrap")
        .with_args({"--log-level": config.log_level}, on_duplicate="replace")
        .with_image(config.image)
    )
    if config.format_json:
        nodes_builder.with_args({"--log-format": "JSON"}, on_duplicate="replace")
    if config.muxer == "quic":
        nodes_builder.with_args({"--quic-support": True}, on_duplicate="replace").with_args(
            {"--quic-port": 60000}, on_duplicate="replace"
        )
    return nodes_builder.build()


def build_bootstrap_nodes(namespace: str, config: ExpConfig) -> V1Deployable:
    bootstrap_builder = (
        WakuStatefulSetBuilder()
        .with_waku_config(
            name="bootstrap-0", namespace=namespace, num_nodes=config.num_bootstrap_nodes
        )
        .with_bootstrap(config.cmd_type)
        .with_env("SERVICE", "zerotesting-bootstrap")
        .with_args({"--log-level": config.log_level}, on_duplicate="replace")
        .with_image(config.image)
    )
    if config.format_json:
        bootstrap_builder.with_args({"--log-format": "JSON"}, on_duplicate="replace")
    if config.muxer == "quic":
        bootstrap_builder.with_args({"--quic-support": True}, on_duplicate="replace").with_args(
            {"--quic-port": 60000}, on_duplicate="replace"
        )
    return bootstrap_builder.build()


def build_nodes(namespace: str, config: ExpConfig) -> Dict[str, V1Deployable]:
    bootstrap = build_bootstrap_nodes(namespace, config)
    nodes = build_relay_nodes(namespace, config)
    return {
        "bootstrap": bootstrap,
        "relay": nodes,
    }


async def grab_metrics(namespace, pod_name, metrics_port: int = 8008) -> str:
    command = exec_command_in_pod(
        namespace,
        pod_name,
        ["/bin/sh", "-c", f"curl -s localhost:{metrics_port}/metrics"],
    )
    await command.collect_output_async()
    if not command.ok:
        raise RuntimeError(f"Command failed: {command}")
    logger.debug(f"Retreived /metrics from pod: `{pod_name}` response: `{command.output}`")
    return command.output


def build_store_nodes(namespace: str) -> dict:
    config = StatefulSetConfig()
    builder = WakuStatefulSetBuilder(config)

    deployment = (
        builder.with_waku_config(name="store-0", namespace=namespace, num_nodes=10)
        .with_args(Nodes.create_standard_args())
        .with_enr(3, [f"zerotesting-bootstrap.{namespace}"])
        .with_store()
        .build()
    )

    api_client = client.ApiClient()
    return api_client.sanitize_for_serialization(deployment)


async def publish(namespace, random_name, msg_size_kbytes, cluster_id, requester):
    try:
        target = Target(
            name="waku-node",
            name_template=random_name,
            service="zerotesting-service",
            port=8645,
        )
        await waku_publish(
            namespace=namespace,
            target=target,
            msg_size_kbytes=msg_size_kbytes,
            cluster_id=cluster_id,
            requester=requester,
        )
    except PodApiApplicationError as e:
        logger.error(f"PodApiApplicationError: {e} {traceback.format_exc()}")
    except PodApiError as e:
        logger.error(f"PodApiError: {e} {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Other exception: {e} {traceback.format_exc()}")


@experiment(name="waku")
class WakuExperiment(BaseExperiment[ExpConfig]):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _get_metadata(self) -> dict:
        return Bridge().get_metadata(self.events_log_path)

    def log_event(self, event):
        logger.info(event)
        return super().log_event(event)

    def dump(self, obj, file_name):
        out_path = Path(self.output_folder) / file_name
        os.makedirs(out_path.parent, exist_ok=True)
        with open(out_path, "w") as out_file:
            out_file.write(obj)

    async def dump_metrics(
        self, node_name: str, num_nodes: NonNegativeInt, folder_prefix: Optional[Union[Path, str]]
    ):
        folder_prefix = Path(folder_prefix) if folder_prefix else Path("")

        async def _dump(node_index: int):
            logger.debug(f"Grabbing metrics for {node_name}-{node_index}")
            try:
                metrics = await grab_metrics(self.namespace, f"{node_name}-{node_index}")
                self.dump(metrics, folder_prefix / f"metrics_{node_name}-{node_index}.log")
            except (client.exceptions.ApiException, RuntimeError, ValueError) as e:
                logger.error(f"Exception when grabbing /metrics: {e}")

        for index in range(0, num_nodes):
            # Running as a batch causes failures, so run one at a time.
            await _dump(node_index=index)

    async def _run(self):
        self.log_event("run_start")

        requester = PodApiRequesterBuilder().with_namespace(self.namespace).with_mode("server")

        if self.config.cmd_type == 1:
            cluster_id = 2
        elif self.config.cmd_type == 2:
            cluster_id = 0
        else:
            raise ValueError()

        # Publisher, with the config map, role and service it mounts and resolves.
        await self.deploy(deployment=requester.build_dependencies(), exist_ok=True)
        await self.deploy(deployment=requester.build(), wait_for_ready=True)

        # Nodes
        deployments = build_nodes(self.namespace, self.config)
        for deployment in deployments.values():
            await self.deploy(deployment=deployment)

        await asyncio.sleep(self.config.delay_cold_start)
        relay_nodes = deployments["relay"]
        num_relay_nodes = relay_nodes.spec.replicas
        relay_name = relay_nodes.metadata.name
        bootstrap_name = deployments["bootstrap"].metadata.name
        namespace = relay_nodes.metadata.namespace
        logger.info(f"Starting publish loop for nodes in `{relay_name}`")

        if self.config.grab_metrics:
            await self.dump_metrics(relay_name, 10, "pre_publish")

        address = requester_address(namespace)
        self.log_event("start_messages")
        tasks = []
        for message_index in range(0, self.config.num_messages):
            index = random.randint(0, num_relay_nodes - 1)
            random_name = f"{relay_name}-{index}"
            self.log_event(
                {"event": "publish", "node": random_name, "message_index": message_index}
            )
            tasks.append(
                asyncio.create_task(
                    publish(
                        namespace, random_name, self.config.msg_size_kbytes, cluster_id, address
                    )
                )
            )
            await asyncio.sleep(self.config.delay_after_publish)
        await asyncio.gather(*tasks)

        self.log_event("publisher_messages_finished")

        await asyncio.sleep(20)
        self.log_event("publisher_wait_finished")

        if self.config.grab_metrics:
            logger.info("Grabbing some metrics")
            for name, num_nodes, desc in [
                (relay_name, self.config.num_relay_nodes, "relay metrics"),
                (bootstrap_name, self.config.num_bootstrap_nodes, "bootstrap metrics"),
            ]:
                try:
                    await self.dump_metrics(name, min(10, num_nodes), "post_publish")
                except Exception as e:
                    logger.error(f"Failed in grabbing {desc}: {e}")

        self.log_event("internal_run_finished")

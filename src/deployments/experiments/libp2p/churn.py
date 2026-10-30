"""Regression scenario: a share of the nodes drop out mid-run and rejoin."""

import asyncio
import logging
from typing import ClassVar, List

from kubernetes.client import V1StatefulSet
from pydantic import Field, NonNegativeInt, model_validator

from src.deployments.core.k8s_rollout import scale_statefulset, wait_for_rollout
from src.deployments.experiments.libp2p.disturbance import (
    DisturbanceNotApplied,
    check_nodes_left,
)
from src.deployments.experiments.libp2p.nimlibp2p import ExpConfig, NimLibp2pExperiment
from src.deployments.registry import experiment

logger = logging.getLogger(__name__)


class ChurnConfig(ExpConfig):
    num_messages: NonNegativeInt = 300
    """Five minutes at one a second: through the downtime and the rejoin."""
    churn_fraction: float = Field(default=0.2, gt=0, lt=1)
    """Share taken down, from the highest ordinals."""
    churn_at: NonNegativeInt = 30
    """Seconds after the first message."""
    churn_downtime: NonNegativeInt = 60
    churn_rejoin_timeout: NonNegativeInt = 600

    @model_validator(mode="after")
    def _check_churn_takes_nodes_down(self):
        if int(self.num_relay_nodes * self.churn_fraction) < 1:
            raise ValueError(
                f"churn_fraction {self.churn_fraction} of {self.num_relay_nodes} nodes takes "
                "none of them down, so the run would report success without any churn"
            )
        return self

    @model_validator(mode="after")
    def _check_publishing_outlasts_downtime(self):
        """Publishing must run past the rejoin, or the churn is never measured."""
        rejoin = self.churn_at + self.churn_downtime
        if self.num_messages * self.delay_after_publish <= rejoin:
            raise ValueError(
                f"{self.num_messages} messages {self.delay_after_publish}s apart stop before "
                f"the rejoin at {rejoin}s, so the run would publish nothing after it"
            )
        return self


def churn_targets(nodes: V1StatefulSet, fraction: float) -> List[str]:
    """Highest-ordinal pods, which is what a scale-down removes."""
    replicas = nodes.spec.replicas
    count = int(replicas * fraction)
    return [f"{nodes.metadata.name}-{i}" for i in range(replicas - count, replicas)]


@experiment(name="nimlibp2p-churn")
class NodeChurn(NimLibp2pExperiment):
    """Regression run where a share of the nodes drop out mid-run and rejoin.

    Scale down rather than delete: a deleted pod is replaced within seconds.
    """

    config: ChurnConfig
    post_run_analysis: ClassVar[str] = "src.analysis.post_run.churn:run_churn_analysis"

    def _publishable_nodes(self) -> int:
        """Churned nodes return on new addresses; publishing to a stale one hangs."""
        churned = int(self.config.num_relay_nodes * self.config.churn_fraction)
        return self.config.num_relay_nodes - churned

    async def _mid_run(self, nodes: V1StatefulSet) -> None:
        await asyncio.sleep(self.config.churn_at)

        targets = churn_targets(nodes, self.config.churn_fraction)
        replicas = nodes.spec.replicas
        name = nodes.metadata.name

        self.log_event({"event": "churn_down", "nodes": targets})
        logger.info(f"Taking down {len(targets)} nodes: {targets[0]}..{targets[-1]}")
        await scale_statefulset(name, self.namespace, replicas - len(targets), self.api_client)
        try:
            check_nodes_left(name, self.namespace, replicas - len(targets), self.api_client)
        except DisturbanceNotApplied as e:
            # Mid-run, so the run keeps going and collects its data before reporting invalid.
            self.fail_run(str(e))
        self.log_event({"event": "churn_is_down", "nodes": targets})

        await asyncio.sleep(self.config.churn_downtime)

        self.log_event({"event": "churn_rejoin", "nodes": targets})
        await scale_statefulset(name, self.namespace, replicas, self.api_client)
        await wait_for_rollout(nodes, self.api_client, timeout=self.config.churn_rejoin_timeout)
        self.log_event({"event": "churn_rejoined", "nodes": targets})

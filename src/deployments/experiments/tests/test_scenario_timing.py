import pytest
from pydantic import ValidationError

from src.deployments.experiments.libp2p.churn import ChurnConfig
from src.deployments.experiments.libp2p.partition import PartitionConfig


@pytest.mark.parametrize("messages, delay", [(20, 1), (90, 1), (45, 2)])
def test_churn_rejects_publishing_that_stops_before_the_rejoin(messages, delay):
    with pytest.raises(ValidationError, match="publish nothing after"):
        ChurnConfig(
            num_messages=messages, delay_after_publish=delay, churn_at=30, churn_downtime=60
        )


@pytest.mark.parametrize("messages, delay", [(20, 1), (120, 1), (60, 2)])
def test_partition_rejects_publishing_that_stops_before_the_heal(messages, delay):
    with pytest.raises(ValidationError, match="publish nothing after"):
        PartitionConfig(num_messages=messages, delay_after_publish=delay, heal_at=120)

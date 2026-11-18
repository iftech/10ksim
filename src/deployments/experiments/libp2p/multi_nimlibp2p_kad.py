import os
from typing import Iterable, List, Optional

from src.deployments.core.configs.container import Image
from src.deployments.experiments.libp2p.nimlibp2p import NimLibp2pExperiment
from src.deployments.experiments.multi_experiment import Multiple
from src.deployments.registry import experiment

# nim-libp2p regression matrix: each version x muxer is one full kad-dht run.
# The sweep is env-configurable so the same experiment drives a smoke test, a
# single-version pass (runs are sequential to keep measurements clean), or the
# full matrix -- see REGRESSION_* below.
IMAGES = {
    "2.0.0": Image(repo="radiken/dst-test-node-regression", tag="v2.0.0-kad"),
    "2.1.0": Image(repo="radiken/dst-test-node-regression", tag="v2.1.0-kad"),
    "2.4.0": {
        "image": Image(repo="mamoutoudiarra/regression-node-amd64", tag="v2.4.0"),
        "bootstrap_image": Image(repo="mamoutoudiarra/regression-bootstrap-amd64", tag="v2.4.0"),
    },
    "2.5.0": {
        "image": Image(repo="radiken/dst-test-node-regression", tag="v2.5.0"),
        "bootstrap_image": Image(repo="radiken/dst-test-node-regression-bootstrap", tag="v2.5.0"),
    },
}


def _image_params(version: str) -> dict:
    spec = IMAGES[version]
    return dict(spec) if isinstance(spec, dict) else {"image": spec}


def _env_list(name: str, default: str) -> list:
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]


VERSIONS = _env_list("REGRESSION_VERSIONS", "2.1.0,2.0.0")
MUXERS = _env_list("REGRESSION_MUXERS", "mplex,yamux,quic")
MESSAGE_SIZES = [int(x) for x in _env_list("REGRESSION_SIZES", "1000")]
NUM_NODES = int(os.environ.get("REGRESSION_NUM_NODES", "1000"))
NUM_MESSAGES = int(os.environ.get("REGRESSION_NUM_MESSAGES", "600"))
# delay_cold_start is how long we wait after the nodes are deployed (their start
# delay + kad-dht mesh formation) before the publisher fires; shorten it via the
# env var for small smoke runs.
COLD_START = int(os.environ.get("REGRESSION_COLD_START", str(7 * 60)))


@experiment(name="multi_nimlibp2p_kad")
class MultiNimlibp2pKad(Multiple):
    """nim-libp2p regression sweep: kad-dht, version x muxer."""

    def model_post_init(self, __context) -> None:
        self.config.name = NimLibp2pExperiment.name
        super().model_post_init(__context)

    def get_params_paths(self) -> Optional[dict]:
        return None

    def exp_params(self) -> Iterable[dict]:
        base = {
            "discovery": "kad-dht",
            "bootstrap_nodes": 1,
            "num_relay_nodes": NUM_NODES,
            "num_messages": NUM_MESSAGES,
            "delay_after_publish": 1,  # 1 msg/s
            "delay_cold_start": COLD_START,
            "node_start_delay": 5 * 60,
        }
        for version in VERSIONS:
            for size in MESSAGE_SIZES:
                for muxer in MUXERS:
                    yield {
                        **base,
                        **_image_params(version),
                        "version": version,
                        "muxer": muxer,
                        "message_size_bytes": size,
                    }

    def get_params_list(self) -> List[dict]:
        return list(self.exp_params())

    def get_name_from_params(self, params: dict) -> str:
        return (
            f"version_{params['version']}"
            f"__muxer_{params['muxer']}"
            f"__size_{params['message_size_bytes']}"
            f"__nodes_{params['num_relay_nodes']}"
        )

import logging
from typing import Self

from src.analysis.mesh_analysis.analyzers.adaptor import AnalysisAdaptor

logger = logging.getLogger(__name__)


class Nimlibp2pAnalysisAdaptor(AnalysisAdaptor):

    def with_reliability_from_metadata(self) -> Self:
        stack = self._metadata["stack"]
        reliability = [
            ss
            for ss in zip(stack["stateful_sets"], stack["nodes_per_statefulset"])
            if "bootstrap" not in ss[0]
        ]
        params = self._metadata["params"]
        self.with_reliability_check(
            stateful_sets=[ss[0] for ss in reliability],
            nodes_per_ss=[ss[1] for ss in reliability],
            expected_num_peers=params["num_nodes"],
            expected_num_messages=params["num_messages"],
        )
        return self

    def with_ss_check_from_metadata(self) -> Self:
        stateful_sets = self._metadata["stack"]["stateful_sets"]
        nodes_per_statefulset = self._metadata["stack"]["nodes_per_statefulset"]
        self.with_ss_check(stateful_sets, nodes_per_statefulset)
        return self

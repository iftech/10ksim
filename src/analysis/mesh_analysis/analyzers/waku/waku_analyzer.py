# Python Imports
import ast
import base64
import logging
from typing import List, Self

import seaborn as sns
from pydantic import NonNegativeInt

# Project Imports
from src.analysis.mesh_analysis.analyzers.adaptor import AnalysisAdaptor
from src.analysis.mesh_analysis.analyzers.analyzer import OnFail
from src.analysis.mesh_analysis.analyzers.libp2p.analyzer import Nimlibp2pAnalyzer
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer
from src.analysis.mesh_analysis.readers.tracers.waku_tracer import WakuTracer
from src.analysis.utils import list_utils

logger = logging.getLogger(__name__)
sns.set_theme()


class WakuAnalyzer(Nimlibp2pAnalyzer, AnalysisAdaptor):

    msg_hash_key: str = "msg_hash"

    def supports(self, exp_name: str) -> Self:
        if exp_name != "waku":
            raise ValueError(
                f"Experiment type not supported by this analyzer. "
                f"Experiment: `{exp_name}` Analyzer: `{self.__class__}`"
            )
        return self

    def with_filter_check(self, *, on_fail: OnFail = "continue") -> Self:
        return self._with_parameterized_check(
            self.check_filter_messages,
            on_fail=on_fail,
        )

    def with_store_check(self, *, on_fail: OnFail = "continue") -> Self:
        return self._with_parameterized_check(
            self.check_store_messages,
            on_fail=on_fail,
        )

    def with_reliability_check(
        self,
        stateful_sets: List[str],
        nodes_per_ss: List[NonNegativeInt],
        expected_num_peers: NonNegativeInt,
        expected_num_messages: NonNegativeInt,
        *,
        on_fail: OnFail = "continue",
    ) -> Self:
        return self._with_parameterized_check(
            self.analyze_reliability,
            on_fail=on_fail,
            stateful_sets=stateful_sets,
            nodes_per_ss=nodes_per_ss,
            expected_num_peers=expected_num_peers,
            expected_num_messages=expected_num_messages,
            has_shards=True,
            peer_identifier="receiver_peer_id",
        )

    def reliability_tracer(self, extra_fields) -> MessageTracer:
        return (
            WakuTracer()
            .with_received_pattern_group()
            .with_sent_pattern_group()
            .with_extra_fields(extra_fields)
        )

    def check_store_messages(self):
        """
        It checks that the messages obtained by get-store-messages pod are the same messages detected in
        analyze_reliability. This is used to detect if the store nodes can retrieve all messages.
        It has to be used after analyze_reliability, and this function only makes sense if there were store nodes
        in the experiment.
        :return:
        """
        waku_tracer = WakuTracer().with_wildcard_pattern()
        data = self.data_puller.get_pod_logs(
            waku_tracer, pod_identifier="get-store-messages", order_by="(_time)"
        )

        log_list = data[0][0]  # We will always have 1 pattern group with 1 pattern
        messages_list = ast.literal_eval(log_list[-1][-1])  # Last line in get-store-messages
        messages_list = ["0x" + base64.b64decode(msg).hex() for msg in messages_list]
        logger.debug(f"Messages from store: {messages_list}")

        if len(self._message_hashes) != len(messages_list):
            logger.error("Number of messages does not match")
        elif set(self._message_hashes) == set(messages_list):
            logger.info("Messages from store match with received messages")
        else:
            logger.error("Messages from store does not match with received messages")
            logger.error(f"Received messages: {self._message_hashes}")
            logger.error(f"Store messages: {messages_list}")

        result = list_utils.dump_list_to_file(
            messages_list, self._dump_analysis_path / "store_messages.txt"
        )
        if result.is_ok():
            logger.info(f"Messages from store saved in {result.ok_value}")

    def check_filter_messages(self):
        """
        It checks that the messages obtained by get-filter-messages pod are the same messages detected in
        analyze_reliability. This is used to detect if the filter nodes received all messages.
        It has to be used after analyze_reliability, and this function only makes sense if there were filter nodes
        in the experiment.
        :return:
        """
        waku_tracer = WakuTracer().with_wildcard_pattern()
        data = self.data_puller.get_pod_logs(
            waku_tracer, pod_identifier="get-filter-messages", order_by="(_time)"
        )

        log_list = data[0][0]  # We will always have 1 pattern group with 1 pattern
        all_ok_boolean = ast.literal_eval(log_list[-1][-1])  # Last line in get-filter-messages

        all_ok = ast.literal_eval(all_ok_boolean)
        if all_ok:
            logger.info("Messages from filter match in length.")
        else:
            logger.error("Messages from filter do not match.")

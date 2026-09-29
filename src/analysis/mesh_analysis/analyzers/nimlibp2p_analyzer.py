import logging
import traceback
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Self

import pandas as pd
import seaborn as sns
from pydantic import BaseModel, NonNegativeInt
from result import Err, Ok, Result

# Project Imports
from src.analysis.mesh_analysis.analyzers.analyzer import AnalysisResult, Analyzer, OnFail
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer
from src.analysis.mesh_analysis.readers.tracers.nimlibp2p_tracer import Nimlibp2pTracer
from src.analysis.utils import file_utils, path_utils

logger = logging.getLogger(__name__)
sns.set_theme()


class Node(BaseModel):
    name: str
    id: Optional[str]
    missing: Optional[NonNegativeInt] = None
    """How many of the run's messages this node never received."""


class MissingMessages(BaseModel):
    shard: Optional[NonNegativeInt]
    messages: List[str | int]
    nodes: List[Node]


class MessageReliabilityResult(BaseModel):
    num_unique_messages: NonNegativeInt
    num_peers: NonNegativeInt
    all_in_same_shard: bool
    missing_messages: List[MissingMessages]


class StatefulSetNodes(BaseModel):
    name: str
    expected: NonNegativeInt
    actual: NonNegativeInt


class MessageReliabilityAnalysis(BaseModel):
    ss_nodes: List[StatefulSetNodes]
    reliability_result: MessageReliabilityResult


class Nimlibp2pAnalyzer(Analyzer):
    """
    Handles the analysis of Nimlibp2p message reliability from either local log files or online data.

    The class ensures that every Nimlibp2p node received every expected message. It facilitates both local
    and online analysis of message reliability, merging and processing dataframes, and dumping results.
    In cases of missing messages, the class logs details and optionally dumps relevant node logs. It
    supports parallel processing to improve analysis efficiency.

    """

    msg_hash_key: str = "msgId"
    enable_cache: bool = False

    def with_ss_check(
        self,
        stateful_sets: List[str],
        expected_ss_nodes: List[NonNegativeInt],
        *,
        on_fail: OnFail = "continue",
    ) -> Self:
        return self._with_parameterized_check(
            self.check_ss_nodes,
            on_fail=on_fail,
            stateful_sets=stateful_sets,
            expected_ss_nodes=expected_ss_nodes,
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
            has_shards=False,
            peer_identifier="kubernetes.pod_name",
        )

    def check_ss_nodes(
        self,
        stateful_sets: List[str],
        expected_ss_nodes: List[NonNegativeInt],
    ) -> AnalysisResult:
        ss_nodes: List[StatefulSetNodes] = self._num_statefulset_nodes(
            stateful_sets, expected_ss_nodes
        )
        passed = all(map(lambda ss: ss.expected == ss.actual, ss_nodes))

        if not passed:
            error_message = f"Number of nodes in cluster does not match with provided data.\nStatefulSets: {ss_nodes}"
            logger.error(error_message)

        return AnalysisResult(
            name="num_ss_nodes",
            intermediates={
                "ss_nodes": ss_nodes,
                **({"failed": error_message} if not passed else {}),
            },
            status="passed" if passed else "failed",
        )

    def _num_statefulset_nodes(
        self,
        stateful_sets: List[str],
        expected_ss_nodes: List[NonNegativeInt],
    ) -> List[StatefulSetNodes]:
        num_nodes_per_ss = self.data_puller.get_number_nodes(stateful_sets)

        results = []
        for i, num_nodes in enumerate(num_nodes_per_ss):
            results.append(
                StatefulSetNodes(
                    name=stateful_sets[i],
                    expected=expected_ss_nodes[i],
                    actual=num_nodes,
                )
            )

        return results

    def reliability_tracer(self, extra_fields) -> MessageTracer:
        return (
            Nimlibp2pTracer()
            .with_extra_fields(extra_fields)
            .with_received_pattern_group()
            .with_sent_pattern_group()
        )

    def analyze_reliability(
        self,
        stateful_sets: List[str],
        nodes_per_ss: List[NonNegativeInt],
        expected_num_peers: NonNegativeInt,
        expected_num_messages: NonNegativeInt,
        has_shards: bool,
        peer_identifier: str,
    ) -> AnalysisResult:
        """:param peer_identifier: The identifier used for nodes. For example: Nimlibp2p uses kubernetes.pod_name.
        Waku uses receiver_peer_id because log lines for received messages include receiver_peer_id. Nimlibp2p does not.

        :param has_shards: If True, nodes will be expected to have the format "{pod_name}-{shard}-{pod_index}".

        """

        # For local data puller, use "kubernetes.pod_name" as the header for file name.
        # For Victoria use "kubernetes.pod_name" and "kubernetes.pod_node_name".
        extra_fields = (
            ["kubernetes.pod_name"]
            if self.data_puller.is_local()
            else ["kubernetes.pod_name", "kubernetes.pod_node_name"]
        )
        tracer = self.reliability_tracer(extra_fields)

        reliability_result = self._analyze_reliability_cluster(
            stateful_sets, nodes_per_ss, tracer, has_shards, peer_identifier
        )
        passed = (
            reliability_result.all_in_same_shard
            and reliability_result.num_peers == expected_num_peers
            and reliability_result.num_unique_messages == expected_num_messages
        )
        results_dict = reliability_result.model_dump()
        results_dict.update(
            {
                "expected_num_peers": expected_num_peers,
                "expected_num_messages": expected_num_messages,
            }
        )
        if not has_shards:
            del results_dict["all_in_same_shard"]
        return AnalysisResult(
            name="reliability",
            intermediates=results_dict,
            status="passed" if passed else "failed",
        )

    def _analyze_reliability_cluster(
        self,
        stateful_sets: List[str],
        nodes_per_ss: List[NonNegativeInt],
        tracer: MessageTracer,
        has_shards: bool,
        peer_identifier: str,
    ) -> MessageReliabilityResult:
        dfs = None
        if self.enable_cache:
            received = Path(self._get_dump_path("received"))
            sent = Path(self._get_dump_path("sent"))
            if received.exists() and sent.exists():
                dfs = [pd.read_csv(received), pd.read_csv(sent)]
                columns = [self.msg_hash_key, "timestamp"]
                if has_shards:
                    columns = ["shard"] + columns
                for df in dfs:
                    df.set_index(columns, inplace=True)
                    df.sort_index(inplace=True)
        if dfs is None:
            dfs = self.data_puller.get_all_node_dataframes(tracer, stateful_sets, nodes_per_ss)
            # Strip suffix for local read.
            for dfs_dicts in dfs:
                for _key, df_list in dfs_dicts.items():
                    for df in df_list:
                        df["kubernetes.pod_name"] = df["kubernetes.pod_name"].str.removesuffix(
                            ".log"
                        )
            dfs = self._merge_dfs(dfs, has_shards)
            self.adjust_dfs(dfs)
            result = self._dump_dfs(dfs)
            if result.is_err():
                logger.warning(f"Issue dumping message summary. {result.err_value}")

        reliability_results = self._has_message_reliability_issues(
            "shard" if has_shards else None,
            self.msg_hash_key,
            peer_identifier,
            dfs[0],
            dfs[1],
            self._dump_analysis_path,
        )

        if reliability_results.missing_messages:
            logger.info("Dumping logs from nodes with issues")
            pod_names = {
                node.name for msg in reliability_results.missing_messages for node in msg.nodes
            }
            self._dump_logs(pod_names)

        return reliability_results

    def _dump_logs(self, nodes_with_issues: Iterable[str]):
        try:
            self.data_puller._dump_logs(nodes_with_issues, self.dump_analysis_dir)
        except Exception as e:
            logger.error(f"Error dumping nodes: {e}")
            full_trace = traceback.format_exc()
            logger.error(f"exception: {full_trace}")

    def adjust_dfs(self, dfs: List[pd.DataFrame]):
        # We either had legacy lightpush requests xor lightpush requests.
        # Thus, dfs[0] is our received
        self.fill_unknown_in_received_df(dfs[0])

    def fill_unknown_in_received_df(self, df: pd.DataFrame):
        """Map from kubernetes.pod_name to receiver_peer_id
        for cases where my_peer_id was not included in sender (legacy lightpush requests)"""
        unknown_key = Nimlibp2pTracer.unknown_sender_str
        if "receiver_peer_id" in df.columns:
            pod_to_peer_map = (
                df.loc[df["receiver_peer_id"] != unknown_key]
                .drop_duplicates("kubernetes.pod_name")
                .set_index("kubernetes.pod_name")["receiver_peer_id"]
            )
            df.loc[df["receiver_peer_id"] == unknown_key, "receiver_peer_id"] = df.loc[
                df["receiver_peer_id"] == unknown_key, "kubernetes.pod_name"
            ].map(pod_to_peer_map)

    def _get_dump_path(self, data_type: Literal["received", "sent"]):
        return self._dump_analysis_path / "summary" / f"{data_type}.csv"

    def _dump_dfs(self, dfs: List[pd.DataFrame]) -> Result:
        received = dfs[0].reset_index()
        received = received.astype(str)
        logger.info("Dumping received information")
        result = file_utils.dump_df_as_csv(received, self._get_dump_path("received"), False)
        if result.is_err():
            logger.warning(result.err_value)
            return Err(result.err_value)

        sent = dfs[1].reset_index()
        sent = sent.astype(str)
        logger.info("Dumping sent information")
        result = file_utils.dump_df_as_csv(sent, self._get_dump_path("sent"), False)
        if result.is_err():
            logger.warning(result.err_value)
            return Err(result.err_value)

        return Ok(None)

    def _merge_dfs(self, dfs: List[Dict[str, pd.DataFrame]], has_shard: bool) -> List[pd.DataFrame]:
        logger.info("Merging and sorting information")

        received_df = pd.concat(
            [pd.concat(group["received"], ignore_index=True) for group in dfs],
            ignore_index=True,
        )
        if has_shard:
            received_df = received_df.assign(
                shard=received_df["kubernetes.pod_name"].str.extract(r".*-(\d+)-").astype(int)
            )
        columns = [self.msg_hash_key, "timestamp"]
        if has_shard:
            columns = ["shard"] + columns
        received_df.set_index(columns, inplace=True)
        received_df.sort_index(inplace=True)

        sent_df = pd.concat(
            [pd.concat(group["sent"], ignore_index=True) for group in dfs],
            ignore_index=True,
        )
        if has_shard:
            sent_df = sent_df.assign(
                shard=sent_df["kubernetes.pod_name"].str.extract(r".*-(\d+)-").astype(int)
            )
        sent_df.set_index(columns, inplace=True)
        sent_df.sort_index(inplace=True)

        return [received_df, sent_df]

    def _has_message_reliability_issues(
        self,
        shard_identifier: Optional[str],
        msg_identifier: str,
        peer_identifier: str,
        received_df: pd.DataFrame,
        sent_df: pd.DataFrame,
        issue_dump_location: Path,
    ) -> MessageReliabilityResult:
        num_peers = len(received_df[peer_identifier].unique())
        logger.info(f"Nº of Peers: {num_peers}")
        unique_messages = len(received_df.index.get_level_values(self.msg_hash_key).unique())
        logger.info(f"Nº of unique messages: {unique_messages}")

        missing_messages = self._get_peers_missing_messages(
            shard_identifier, msg_identifier, peer_identifier, received_df
        )

        received_df = received_df.reset_index()
        has_shard_violations = False
        if shard_identifier is not None:
            shard_groups = received_df.groupby(self.msg_hash_key)[shard_identifier].nunique()
            shard_violations = shard_groups[shard_groups > 1]
            has_shard_violations = not shard_violations.empty

            if shard_violations.empty:
                logger.info(f"All {self.msg_hash_key} values appear in only one shard.")
            else:
                logger.warning(f"These {self.msg_hash_key} values appear in multiple shards:")
                logger.warning(shard_violations)

        for missing in missing_messages:
            msg_sent_data = self._check_if_msg_has_been_sent(missing, sent_df, peer_identifier)
            for data in msg_sent_data:
                peer_id = data[0].split("*")[-1]
                logger.info(f"Peer {peer_id} message information dumped in {issue_dump_location}")
                match path_utils.prepare_path_for_file(
                    issue_dump_location / f"{data[0].split('*')[-1]}.csv"
                ):
                    case Ok(location_path):
                        data[1].to_csv(location_path)
                    case Err(err):
                        logger.error(err)
                        exit(1)

        return MessageReliabilityResult(
            all_in_same_shard=not has_shard_violations,
            num_unique_messages=unique_messages,
            num_peers=num_peers,
            missing_messages=missing_messages,
        )

    def _get_peers_missing_messages(
        self,
        shard_identifier: Optional[str],
        msg_identifier: str,
        peer_identifier: str,
        df: pd.DataFrame,
    ) -> List[MissingMessages]:
        if shard_identifier is not None:
            all_missing = []
            for shard, df_shard in df.groupby(level=shard_identifier):
                missing = self._get_peers_missing_messages_for_shard(
                    shard, msg_identifier, peer_identifier, df_shard
                )
                all_missing.append(missing)
            return list(filter(None, all_missing))
        else:
            missing = self._get_peers_missing_messages_for_shard(
                shard_identifier, msg_identifier, peer_identifier, df
            )
            return [missing] if missing else []

    def _get_peers_missing_messages_for_shard(
        self,
        shard: Optional[str],
        msg_identifier: str,
        peer_identifier: str,
        df: pd.DataFrame,
    ) -> Optional[MissingMessages]:
        unique_messages = len(df.index.get_level_values(msg_identifier).unique())
        grouped = df.groupby([msg_identifier, peer_identifier]).size().reset_index(name="count")
        pivot_df = grouped.pivot_table(
            index=msg_identifier,
            columns=peer_identifier,
            values="count",
            fill_value=0,
        )

        peers_missing_msg = pivot_df.columns[pivot_df.sum() != unique_messages].to_list()
        missing_messages = pivot_df.index[pivot_df.eq(0).any(axis=1)].tolist()

        if not peers_missing_msg:
            logger.info(f"All peers received all messages for shard {shard}")
            return None

        logger.warning(f"Nodes missed messages on shard {shard}")
        logger.warning(f"Nodes who missed messages: {peers_missing_msg}")
        logger.warning(f"Missing messages: {missing_messages}")

        column_sums = pivot_df.sum()
        filtered_sums = column_sums[column_sums != unique_messages]
        nodes: List[Node] = []
        for item in list(filtered_sums.items()):
            pod, count = item
            missing_hashes = pivot_df[pivot_df[pod] == 0].index.tolist()
            missing_hashes.extend(pivot_df[pivot_df[pod].isna()].index.tolist())
            pod_name = df[df[peer_identifier] == item[0]]["kubernetes.pod_name"].iloc[0]
            node_id = None if peer_identifier == "kubernetes.pod_name" else item[0]
            nodes.append(
                Node(name=pod_name, id=node_id, missing=max(0, int(unique_messages - count)))
            )
            logger.warning(
                f"Node {item[0]} ({pod_name}) {item[1]}/{unique_messages}: {missing_hashes}"
            )
        return MissingMessages(shard=shard, messages=missing_messages, nodes=nodes)

    def _log_received_messages(
        self,
        df: pd.DataFrame,
        unique_messages: int,
        complete_df: pd.DataFrame,
        peer_identifier: str,
    ) -> List[Node]:
        """:return: List of pod names."""
        column_sums = df.sum()
        filtered_sums = column_sums[column_sums != unique_messages]
        results: List[Node] = []
        for item in list(filtered_sums.items()):
            pod_name, count = item
            missing_hashes = df[df[pod_name] == 0].index.tolist()
            missing_hashes.extend(df[df[pod_name].isna()].index.tolist())
            pod_name = complete_df[complete_df[peer_identifier] == item[0]][
                "kubernetes.pod_name"
            ].iloc[0]
            node_id = None if peer_identifier == "kubernetes.pod_name" else item[0]
            results.append(
                Node(name=pod_name, id=node_id, missing=max(0, int(unique_messages - count)))
            )
            logger.warning(
                f"Node {item[0]} ({pod_name}) {item[1]}/{unique_messages}: {missing_hashes}"
            )

        return results

    def _check_if_msg_has_been_sent(
        self,
        missing: MissingMessages,
        sent_df: pd.DataFrame,
        peer_identifier: str,
    ) -> List:
        messages_sent_to_peer = []
        for node in missing.nodes:
            peer = node.id or node.name
            try:
                filtered_df = sent_df.loc[(slice(None), missing.messages), :]
                filtered_df = filtered_df[filtered_df[peer_identifier] == peer]
                messages_sent_to_peer.append((peer, filtered_df))
            except KeyError as _:
                logger.warning(
                    f"Message {missing.messages} has not been sent to {peer} by any other node on shard {missing.shard}."
                )

        return messages_sent_to_peer

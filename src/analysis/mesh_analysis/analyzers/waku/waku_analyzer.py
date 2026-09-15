# Python Imports
import ast
import base64
import json
import logging
from pathlib import Path
from typing import List, Optional, Self, Tuple

import pandas as pd
import seaborn as sns
from pydantic import NonNegativeInt

# Project Imports
from src.analysis.mesh_analysis.analyzers.analyzer import AnalysisResult, OnFail
from src.analysis.mesh_analysis.analyzers.nimlibp2p_analyzer import Nimlibp2pAnalyzer
from src.analysis.mesh_analysis.analyzers.waku.delivery_latency import (
    delivery_latency,
    write_delivery_latency,
)
from src.analysis.mesh_analysis.readers.tracers.message_tracer import MessageTracer
from src.analysis.mesh_analysis.readers.tracers.waku_tracer import WakuTracer
from src.analysis.plotting.latency_plotter import (
    DELAY_COLUMN,
    LatencyPlotConfig,
    LatencyPlotter,
    latency_table,
)
from src.analysis.utils import list_utils

logger = logging.getLogger(__name__)
sns.set_theme()


class WakuAnalyzer(Nimlibp2pAnalyzer):
    msg_hash_key: str = "msg_hash"

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

    def with_delivery_latency_check(
        self,
        lightpush_sets: List[Tuple[str, int]],
        filter_sets: List[Tuple[str, int]],
        *,
        on_fail: OnFail = "continue",
    ) -> Self:
        return self._with_parameterized_check(
            self.check_delivery_latency,
            on_fail=on_fail,
            lightpush_sets=lightpush_sets,
            filter_sets=filter_sets,
        )

    def with_store_archive_check(
        self, folder: Path, *, received_csv: Optional[Path] = None, on_fail: OnFail = "continue"
    ) -> Self:
        return self._with_parameterized_check(
            self.check_store_archives,
            on_fail=on_fail,
            folder=folder,
            received_csv=received_csv,
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

    def check_store_archives(
        self, folder: Path, received_csv: Optional[Path] = None
    ) -> AnalysisResult:
        """Compare each store node's archive with what relay delivered; run after reliability."""
        received_csv = Path(received_csv or self._dump_analysis_path / "summary" / "received.csv")
        archives = sorted(Path(folder).glob("store-*.json"))
        intermediates = {"folder": str(folder), "received_csv": str(received_csv)}

        def skipped(reason: str) -> AnalysisResult:
            logger.error(reason)
            return AnalysisResult(
                name="store_archives",
                intermediates={**intermediates, "failed": reason},
                status="skipped",
            )

        if not archives:
            return skipped(f"No store archive dumps found. folder: `{folder}`")
        if not received_csv.exists():
            return skipped(f"No delivery summary to compare against. path: `{received_csv}`")

        expected = set(pd.read_csv(received_csv)[self.msg_hash_key].unique())
        if not expected:
            return skipped(f"Delivery summary holds no messages. path: `{received_csv}`")

        nodes = {}
        for archive in archives:
            with open(archive) as archive_file:
                # Store v3 already returns hashes in the 0x form the relay logs use.
                hashes = {msg.lower() for msg in json.load(archive_file)}
            missing = expected - hashes
            unexpected = hashes - expected
            nodes[archive.stem] = {
                "held": len(hashes),
                "missing": len(missing),
                "unexpected": len(unexpected),
            }
            if not missing and not unexpected:
                logger.info(f"`{archive.stem}` holds all {len(expected)} messages")
            else:
                logger.error(
                    f"`{archive.stem}` holds {len(hashes)} of {len(expected)} messages. "
                    f"missing: `{len(missing)}` unexpected: `{len(unexpected)}`"
                )

        complete = sum(
            1 for node in nodes.values() if not node["missing"] and not node["unexpected"]
        )
        logger.info(f"Store nodes with a complete archive: {complete} of {len(archives)}")
        return AnalysisResult(
            name="store_archives",
            intermediates={
                **intermediates,
                "expected_num_messages": len(expected),
                "complete_nodes": complete,
                "num_store_nodes": len(archives),
                "nodes": nodes,
            },
            status="passed" if complete == len(archives) else "failed",
        )

    def _pull_group(
        self, tracer: WakuTracer, group: str, sets: List[Tuple[str, int]]
    ) -> pd.DataFrame:
        """Every node's rows for one pattern group, as a single frame."""
        columns = [self.msg_hash_key, "timestamp", "kubernetes.pod_name"]
        if not sets:
            return pd.DataFrame(columns=columns)
        extra_fields = (
            ["kubernetes.pod_name"]
            if self.data_puller.is_local()
            else ["kubernetes.pod_name", "kubernetes.pod_node_name"]
        )
        tracer.with_extra_fields(extra_fields)
        nodes = self.data_puller.get_all_node_dataframes(
            tracer, [name for name, _ in sets], [count for _, count in sets]
        )
        frames = [frame for node in nodes for frame in node[group]]
        if not frames:
            return pd.DataFrame(columns=columns)
        merged = pd.concat(frames, ignore_index=True)
        merged["kubernetes.pod_name"] = merged["kubernetes.pod_name"].str.removesuffix(".log")
        return merged

    def check_delivery_latency(
        self, lightpush_sets: List[Tuple[str, int]], filter_sets: List[Tuple[str, int]]
    ) -> AnalysisResult:
        """
        Delivery latency per protocol, each measured from when its message entered the network.

        Has to run after analyze_reliability, whose delivery summary supplies the relay
        receipts. Writes one CSV per path and a CDF of all of them under `latency/`.
        """
        received_csv = self._dump_analysis_path / "summary" / "received.csv"
        if not received_csv.exists():
            reason = f"No delivery summary to measure from. path: `{received_csv}`"
            logger.error(reason)
            return AnalysisResult(
                name="delivery_latency", intermediates={"failed": reason}, status="skipped"
            )

        received = pd.read_csv(received_csv, parse_dates=["timestamp"])
        lightpush = self._pull_group(
            WakuTracer().with_lightpush_handled_pattern_group(), "lightpush_handled", lightpush_sets
        )
        filter_received = self._pull_group(
            WakuTracer().with_filter_received_pattern_group(), "filter_received", filter_sets
        )
        paths = delivery_latency(received, lightpush, filter_received)

        out_dir = self._dump_analysis_path / "latency"
        written = write_delivery_latency(paths, out_dir)
        LatencyPlotter(
            configs=[LatencyPlotConfig(name="delivery_latency", runs=written, out_dir=out_dir)]
        ).create_plots()
        table = latency_table(written, percentiles=(50, 90, 99))
        logger.info(f"Delivery latency (ms):\n{table.to_string()}")

        # Kept rather than dropped: cross machine clock error makes a few ms either way normal.
        negatives = {name: int((frame[DELAY_COLUMN] < 0).sum()) for name, frame in paths.items()}
        if any(negatives.values()):
            logger.warning(f"Deliveries with a negative measured delay: {negatives}")

        expected = ["relay"] + ["lightpush"] * bool(lightpush_sets) + ["filter"] * bool(filter_sets)
        missing = [name for name in expected if name not in paths]
        unmatched = len(filter_received) - len(paths.get("filter", []))
        intermediates = {
            "paths": table.to_dict(),
            "lightpush_handled": len(lightpush),
            "filter_receipts": len(filter_received),
            "unmatched_filter_receipts": unmatched,
            "negative_delays": negatives,
            "folder": str(out_dir),
        }
        if missing or unmatched:
            intermediates[
                "failed"
            ] = f"missing paths: `{missing}` unmatched filter receipts: `{unmatched}`"
        return AnalysisResult(
            name="delivery_latency",
            intermediates=intermediates,
            status="failed" if missing or unmatched else "passed",
        )

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

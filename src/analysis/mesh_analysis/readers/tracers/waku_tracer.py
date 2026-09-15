import logging
from typing import ClassVar, List, Self

import numpy as np
import pandas as pd

# Project Imports
from src.analysis.mesh_analysis.readers.tracers.message_tracer import (
    MessageTracer,
    PatternGroup,
    TracePair,
)

logger = logging.getLogger(__name__)


class WakuTracer(MessageTracer):
    unknown_sender_str: ClassVar[str] = "Unknown"

    def with_extra_fields(self, extra_fields: List[str]) -> Self:
        self.extra_fields = extra_fields
        return self

    def with_received_pattern_group(self) -> Self:
        self.patterns.append(
            PatternGroup(
                "received",
                trace_pairs=[
                    TracePair(
                        regex=r"[Rr]eceived relay message.*?my_peer_id[\s\":=]+([\w*]+).*?msg_hash[\s\":=]+(0x[\da-f]+).*?from_peer_id[\s\":=]+([\w*]+).*?receivedTime[\s\":=]+(\d+)",
                        convert=self._trace_received_in_logs,
                    ),
                    TracePair(
                        regex=
                        # Legacy lightpush
                        # Example from jswaku:
                        # NTC 2025-11-20 13:50:35.376+00:00 handling lightpush request topics="waku lightpush legacy" tid=7 file=protocol.nim:48 peer_id=12D*YCde2H requestId=46e649c7-f0db-409c-afed-c34f17e2ff7b pubsubTopic=/waku/2/rs/2/0 msg_hash=0x1441e3e14e6f957d2a45332378cda900e066022412d6a1c47c95e587d82e6eb2 receivedTime=1763646635380167168
                        r'[Hh]andling lightpush request.*?topics="waku lightpush legacy".*?peer_id=([\w*]+).*?msg_hash=(0x[\da-f]+).*?receivedTime=(\d+)',
                        convert=self._trace_legacy_lightpush_in_logs,
                    ),
                    TracePair(
                        regex=
                        # Example from nwaku:
                        # NTC 2025-11-20 13:06:16.015+00:00 handling lightpush request topics="waku lightpush" tid=7 file=protocol.nim:79 my_peer_id=16U*GiNg1a peer_id=16U*wJXtuH requestId=db01d1a6519de2145f10 pubsubTopic="some(\"/waku/2/rs/2/0\")" msg_hash=0x17cfd30767acac9b86c18333ba918abef93cc23f65b6c98c845c682584f92583 receivedTime=1763643976019361536
                        r"[Hh]andling lightpush request.*?my_peer_id=([\w*]+).*?peer_id=([\w*]+).*?msg_hash=(0x[\da-f]+).*?receivedTime=(\d+)",
                        convert=self._trace_lightpush_in_logs,
                    ),
                ],
                # i(): newer nwaku capitalizes these lines.
                query='(i("received relay message") OR i("handling lightpush request"))',
            )
        )
        return self

    def with_lightpush_handled_pattern_group(self) -> Self:
        """Service node handling a lightpush request: when the message entered the network."""
        self.patterns.append(
            PatternGroup(
                "lightpush_handled",
                trace_pairs=[
                    TracePair(
                        regex=r"[Hh]andling lightpush request.*?my_peer_id=([\w*]+).*?peer_id=([\w*]+).*?msg_hash=(0x[\da-f]+).*?receivedTime=(\d+)",
                        convert=self._trace_lightpush_in_logs,
                    ),
                ],
                query='i("handling lightpush request")',
            )
        )
        return self

    def with_filter_received_pattern_group(self) -> Self:
        """Edge node receiving a filter push from its service node."""
        self.patterns.append(
            PatternGroup(
                "filter_received",
                trace_pairs=[
                    TracePair(
                        regex=r"Received message push.*?peerId[\s\":=]+([\w*]+).*?msg_hash[\s\":=]+(0x[\da-f]+).*?receivedTime[\s\":=]+(\d+)",
                        convert=self._trace_filter_received_in_logs,
                    ),
                ],
                query='i("received message push")',
            )
        )
        return self

    def with_sent_pattern_group(self) -> Self:
        sent_pattern_group = PatternGroup(
            name="sent",
            trace_pairs=[
                TracePair(
                    regex=r"[Ss]ent relay message.*?my_peer_id[\s\":=]+([\w*]+).*?msg_hash[\s\":=]+(0x[\da-f]+).*?to_peer_id[\s\":=]+([\w*]+).*?sentTime[\s\":=]+(\d+)",
                    convert=self._trace_sent_in_logs,
                ),
                TracePair(
                    regex=r"publishWithConn.*?my_peer_id=([\w*]+).*?peer_id=([\w*]+).*?msg_hash=(0x[\da-f]+).*?sentTime=(\d+)",
                    convert=self._trace_mixnet_in_logs,
                ),
            ],
            query='i("sent relay message")',
        )
        self.patterns.append(sent_pattern_group)
        return self

    def _trace_received_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["receiver_peer_id", "msg_hash", "sender_peer_id", "timestamp"]
        columns.extend(self.extra_fields)

        df = self._create_dataframe_with_timestamp(parsed_logs, columns)

        return df

    def _trace_sent_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["sender_peer_id", "msg_hash", "receiver_peer_id", "timestamp"]
        columns.extend(self.extra_fields)

        df = self._create_dataframe_with_timestamp(parsed_logs, columns)
        return df

    def _trace_mixnet_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["sender_peer_id", "receiver_peer_id", "msg_hash", "timestamp"]
        columns.extend(self.extra_fields)

        df = self._create_dataframe_with_timestamp(parsed_logs, columns)
        return df

    def _trace_filter_received_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["sender_peer_id", "msg_hash", "timestamp"]
        columns.extend(self.extra_fields)

        df = self._create_dataframe_with_timestamp(parsed_logs, columns)
        return df

    def _trace_lightpush_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["receiver_peer_id", "sender_peer_id", "msg_hash", "timestamp"]
        columns.extend(self.extra_fields)

        df = self._create_dataframe_with_timestamp(parsed_logs, columns)
        return df

    def _trace_legacy_lightpush_in_logs(self, parsed_logs: List) -> pd.DataFrame:
        columns = ["receiver_peer_id", "sender_peer_id", "msg_hash", "timestamp"]
        columns.extend(self.extra_fields)

        parsed_logs = [[self.__class__.unknown_sender_str] + row for row in parsed_logs if row]
        df = self._create_dataframe_with_timestamp(parsed_logs, columns)
        return df

    def _trace_all_logs(self, parsed_logs: List) -> List:
        return parsed_logs

    def _create_dataframe_with_timestamp(self, parsed_logs: List[str], columns: List[str]):
        try:
            df = pd.DataFrame(parsed_logs, columns=columns)
            df["timestamp"] = df["timestamp"].astype(np.uint64)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ns")
        except ValueError as e:
            lines = len(parsed_logs)
            try:
                logs_columns = len(parsed_logs[0])
            except IndexError:
                logs_columns = "N/A"
            raise ValueError(
                f"Failed to create dataframe from parsed logs.\n"
                f"parsed_logs has {logs_columns} columns and {lines} entries\n"
                f"expected {len(columns)} columns: {columns}"
            ) from e

        return df

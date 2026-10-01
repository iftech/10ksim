"""Post-run analysis for the network-partition scenarios."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List

import pandas as pd

from src.analysis.post_run.scenario_common import (
    POD_COLUMN,
    Row,
    event_time,
    latency_summary,
    prepare,
    write_table,
)
from src.deployments.core.event_log import find_events

if TYPE_CHECKING:
    from src.deployments.experiments.libp2p.nimlibp2p import NimLibp2pExperiment

logger = logging.getLogger(__name__)

PHASES = ("before", "split", "healed")


def load_publishes(dump_dir: Path) -> pd.DataFrame:
    """One row per message from the publishing node's `Sent message` line."""
    sent = pd.read_csv(dump_dir / "summary" / "sent.csv", dtype={"msgId": str})
    return pd.DataFrame(
        {
            "msgId": sent["msgId"],
            "sent": pd.to_datetime(sent["timestamp"]),
            "entry": sent[POD_COLUMN].str.rsplit("-", n=1).str[-1].astype(int),
        }
    )


def per_message_reach(
    publishes: pd.DataFrame,
    df: pd.DataFrame,
    split: int,
    num_nodes: int,
    applied: datetime,
    heal: datetime,
) -> pd.DataFrame:
    """One row per published message: nodes reached on its own and the far side, and its phase."""
    reached = (
        df.assign(msgId=df["msgId"].astype(str), side=df["ordinal"] >= split)
        .groupby(["msgId", "side"])["ordinal"]
        .nunique()
        .unstack(fill_value=0)
        .reindex(columns=[False, True], fill_value=0)
    )
    per_msg = publishes.join(reached, on="msgId").fillna({False: 0, True: 0})
    on_b = per_msg["entry"] >= split
    per_msg["own"] = per_msg[True].where(on_b, per_msg[False]).astype(int)
    per_msg["far"] = per_msg[False].where(on_b, per_msg[True]).astype(int)
    per_msg["own_size"] = on_b.map({False: split, True: num_nodes - split})
    per_msg["reach"] = per_msg["own"] + per_msg["far"]
    per_msg["phase"] = pd.cut(
        per_msg["sent"],
        bins=[pd.Timestamp.min, applied, heal, pd.Timestamp.max],
        labels=PHASES,
        right=False,
    )
    return per_msg.drop(columns=[False, True]).sort_values("sent").reset_index(drop=True)


def partition_table(
    per_msg: pd.DataFrame,
    df: pd.DataFrame,
    applied: datetime,
    heal: datetime,
    num_nodes: int,
) -> List[Row]:
    """Whole before the cut, contained under the split, reconverging after the heal."""
    # Messages only their publisher received never entered the network; counted apart.
    alone = per_msg[per_msg["reach"] <= 1]
    spread = per_msg[per_msg["reach"] > 1]
    before = spread[spread["phase"] == "before"]
    under = spread[spread["phase"] == "split"]
    after = spread[spread["phase"] == "healed"]
    rows: List[Row] = []

    if not before.empty:
        whole = (before["reach"] >= num_nodes).sum()
        rows.append(
            (
                "delivery before the cut",
                "100% of all nodes",
                f"{whole} of {len(before)} messages reached all {num_nodes}",
            )
        )

    share = 100 * under["own"] / under["own_size"]
    full_own = (under["own"] >= under["own_size"]).sum()
    crossed = under[under["far"] > 0]
    own_result = f"{full_own} of {len(under)} messages reached their whole half"
    if not under.empty:
        own_result += f"; per-message reach min {share.min():.0f}%, median {share.median():.0f}%"
    last_crossing = (
        f"{(crossed['sent'].max() - applied).total_seconds():.0f}s after the split was applied"
        if not crossed.empty
        else "no message crossed"
    )
    rows += [
        ("delivery under the split, own half", "100% of that half", own_result),
        (
            "delivery under the split, far half",
            "0",
            f"{len(crossed)} of {len(under)} messages crossed",
        ),
        ("last message crossing the split", "no bound yet", last_crossing),
    ]

    by_phase = ", ".join(f"{p} {(alone['phase'] == p).sum()}" for p in PHASES)
    rows.append(
        (
            "messages no node but the publisher received",
            "0",
            f"{len(alone)} of {len(per_msg)} published ({by_phase})",
        )
    )

    full = after[after["reach"] >= num_nodes]
    if full.empty:
        reconverged = "no message reached the whole network after the heal"
        settled = f"0 of {len(after)} messages reached all {num_nodes}"
    else:
        first = full.iloc[0]["sent"]
        # Messages published before the first full-reach one are still crossing the merge.
        since = after[after["sent"] >= first]
        reconverged = f"{(first - heal).total_seconds():.0f}s"
        settled = (
            f"{(since['reach'] >= num_nodes).sum()} of {len(since)} messages "
            f"reached all {num_nodes}"
        )
    rows += [
        ("time to reconverge after the heal", "no bound yet", reconverged),
        ("delivery after reconvergence", "100% of all nodes", settled),
        (
            "latency across the merge",
            "a brief tail spike, then back to baseline",
            latency_summary(df[df["sent"] > heal]),
        ),
    ]
    return rows


def run_partition_analysis(experiment: "NimLibp2pExperiment") -> None:
    dump_dir, df = prepare(experiment)
    cfg = experiment.config
    log = experiment.events_log_path

    applied_events = find_events(log, {"event": "partition_applied"})
    applied = event_time(applied_events)
    heal = event_time(find_events(log, {"event": "partition_heal"}))
    if applied is None or heal is None:
        raise ValueError("Partition analysis needs partition_applied and partition_heal events")

    publishes = load_publishes(dump_dir)
    summary = find_events(log, {"event": "publish_summary"})
    if summary and summary[-1]["attempted"] != len(publishes):
        logger.warning(
            f"{len(publishes)} Sent message lines against {summary[-1]['attempted']} publishes "
            "attempted; messages without a line are missing from the table"
        )

    split = applied_events[-1]["side_a"]
    per_msg = per_message_reach(publishes, df, split, cfg.num_relay_nodes, applied, heal)
    write_table(
        dump_dir,
        "partition_result",
        partition_table(per_msg, df, applied, heal, cfg.num_relay_nodes),
    )

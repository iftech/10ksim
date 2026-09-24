"""Waku delivery latency per path, from when each message entered the network."""

from pathlib import Path
from typing import Dict

import pandas as pd

from src.analysis.plotting.latency_plotter import DELAY_COLUMN

MSG = "msg_hash"
TIMESTAMP = "timestamp"
POD = "kubernetes.pod_name"


def _delay_ms(frame: pd.DataFrame, entry: pd.Series) -> pd.DataFrame:
    if frame.empty:
        return frame.assign(**{DELAY_COLUMN: pd.Series(dtype="float64")})
    delay = (frame[TIMESTAMP] - frame[MSG].map(entry)).dt.total_seconds() * 1000
    return frame.assign(**{DELAY_COLUMN: delay})


def relay_receipts(received: pd.DataFrame, lightpush: pd.DataFrame) -> pd.DataFrame:
    """Reliability's received rows without the lightpush handling rows (same message and pod)."""
    handled = pd.MultiIndex.from_frame(lightpush[[MSG, POD]])
    rows = pd.MultiIndex.from_frame(received[[MSG, POD]])
    return received[~rows.isin(handled)]


def entry_times(relay: pd.DataFrame, lightpush: pd.DataFrame) -> pd.Series:
    """When each message entered the network: lightpush handling, else first relay receipt."""
    first_receipt = relay.groupby(MSG)[TIMESTAMP].min()
    if lightpush.empty:
        return first_receipt
    return lightpush.groupby(MSG)[TIMESTAMP].min().combine_first(first_receipt)


def relay_delays(relay: pd.DataFrame, lightpush: pd.DataFrame) -> pd.DataFrame:
    """Receipts of relay published messages, leaving out the publish target's own."""
    published = relay[~relay[MSG].isin(lightpush[MSG])]
    targets = published.groupby(MSG)[TIMESTAMP].idxmin()
    receipts = published.drop(index=targets.values)
    return _delay_ms(receipts, published.groupby(MSG)[TIMESTAMP].min())


def lightpush_delays(relay: pd.DataFrame, lightpush: pd.DataFrame) -> pd.DataFrame:
    """Relay receipts of lightpush messages, after the service node handled the request."""
    handled = lightpush.groupby(MSG)[TIMESTAMP].min()
    return _delay_ms(relay[relay[MSG].isin(handled.index)], handled)


def filter_delays(filter_received: pd.DataFrame, entry: pd.Series) -> pd.DataFrame:
    """Edge node receipts over filter, after the message entered the network."""
    return _delay_ms(filter_received[filter_received[MSG].isin(entry.index)], entry)


def delivery_latency(
    received: pd.DataFrame, lightpush: pd.DataFrame, filter_received: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """Delay rows per path. A path with no data is left out."""
    relay = relay_receipts(received, lightpush)
    paths = {
        "relay": relay_delays(relay, lightpush),
        "lightpush": lightpush_delays(relay, lightpush),
        "filter": filter_delays(filter_received, entry_times(relay, lightpush)),
    }
    return {name: frame for name, frame in paths.items() if not frame.empty}


def write_delivery_latency(paths: Dict[str, pd.DataFrame], out_dir: Path) -> Dict[str, Path]:
    """One CSV per path, in the shape LatencyPlotter reads."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for name, frame in paths.items():
        written[name] = out_dir / f"{name}.csv"
        frame.to_csv(written[name], index=False)
    return written

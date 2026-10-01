from datetime import timedelta

import pandas as pd

from src.analysis.post_run.partition import partition_table, per_message_reach
from src.analysis.post_run.tests.conftest import T0

CUT = T0 + timedelta(seconds=50)
HEAL = T0 + timedelta(seconds=100)


def publishes(rows):
    """(msg_id, entry ordinal, seconds_after_T0) per published message."""
    return pd.DataFrame(
        [{"msgId": str(m), "entry": e, "sent": T0 + timedelta(seconds=s)} for m, e, s in rows]
    )


def table(pubs, df, split=2, num_nodes=4, applied=CUT):
    per_msg = per_message_reach(publishes(pubs), df, split, num_nodes, applied, HEAL)
    return dict(
        (item, result) for item, _, result in partition_table(per_msg, df, applied, HEAL, num_nodes)
    )


def test_partition_contained_then_reconverged(deliveries, fanout):
    # Split before publishing: no before-the-cut row.
    df = deliveries(fanout(1, [0, 1], 10) + fanout(2, [0, 1], 20) + fanout(3, range(4), 130))
    rows = table([(1, 0, 10), (2, 1, 20), (3, 0, 130)], df, applied=T0)
    assert "delivery before the cut" not in rows
    assert rows["delivery under the split, own half"].startswith(
        "2 of 2 messages reached their whole half"
    )
    assert rows["delivery under the split, far half"] == "0 of 2 messages crossed"
    assert rows["last message crossing the split"] == "no message crossed"
    assert rows["time to reconverge after the heal"] == "30s"
    assert rows["delivery after reconvergence"] == "1 of 1 messages reached all 4"


def test_cut_reports_the_whole_network_before_it_and_when_it_held(deliveries, fanout):
    # Message 2 is published 5s into the cut and still crosses; message 3 does not.
    df = deliveries(fanout(1, range(4), 10) + fanout(2, range(4), 55) + fanout(3, [0, 1], 70))
    rows = table([(1, 0, 10), (2, 0, 55), (3, 1, 70)], df)
    assert rows["delivery before the cut"] == "1 of 1 messages reached all 4"
    assert rows["delivery under the split, far half"] == "1 of 2 messages crossed"
    assert rows["last message crossing the split"] == "5s after the split was applied"


def test_reconvergence_counts_from_the_earliest_full_message(deliveries, fanout):
    """Picked by publish time, not msgId; messages before it are still crossing the merge."""
    df = deliveries(fanout(5, [0, 1], 105) + fanout(900, range(4), 110) + fanout(7, range(3), 200))
    rows = table([(5, 0, 105), (900, 0, 110), (7, 0, 200)], df)
    assert rows["time to reconverge after the heal"] == "10s"
    assert rows["delivery after reconvergence"] == "1 of 2 messages reached all 4"


def test_says_so_when_it_never_reconverges(deliveries, fanout):
    df = deliveries(fanout(1, [0, 1], 60) + fanout(2, [0, 1], 130))
    rows = table([(1, 0, 60), (2, 0, 130)], df)
    assert rows["time to reconverge after the heal"].startswith("no message reached")
    assert rows["delivery after reconvergence"] == "0 of 1 messages reached all 4"


def test_own_half_reports_the_spread_of_reach(deliveries, fanout):
    """Side b has three nodes, so reaching two of them is partial delivery at 67%."""
    df = deliveries(fanout(1, [2, 3, 4], 60) + fanout(2, [2, 3], 70))
    rows = table([(1, 2, 60), (2, 3, 70)], df, num_nodes=5)
    assert rows["delivery under the split, own half"] == (
        "1 of 2 messages reached their whole half; per-message reach min 67%, median 83%"
    )


def test_own_half_is_the_publishers_side(deliveries, fanout):
    """A message entering on side b that only reaches side a has crossed, and missed its half."""
    df = deliveries(fanout(1, [0, 1, 2], 60))
    rows = table([(1, 2, 60)], df)
    assert rows["delivery under the split, far half"] == "1 of 1 messages crossed"
    assert rows["delivery under the split, own half"].startswith(
        "0 of 1 messages reached their whole half"
    )


def test_publisher_only_messages_get_their_own_row(deliveries, fanout):
    """A refused publish reaches its publisher at most, and is kept out of the phase rows."""
    df = deliveries(fanout(1, [0, 1], 60) + fanout(2, [1], 70) + fanout(4, range(4), 130))
    rows = table([(1, 0, 60), (2, 1, 70), (3, 0, 120), (4, 0, 130)], df)
    assert rows["messages no node but the publisher received"] == (
        "2 of 4 published (before 0, split 1, healed 1)"
    )
    assert rows["delivery under the split, own half"].startswith(
        "1 of 1 messages reached their whole half"
    )
    assert rows["delivery after reconvergence"] == "1 of 1 messages reached all 4"


def test_counts_each_node_once(deliveries, fanout):
    """Side a has three nodes; two of them, one logging twice, are not the whole half."""
    df = deliveries(fanout(1, [0, 1], 60) + fanout(1, [1], 60))
    rows = table([(1, 0, 60)], df, split=3)
    assert rows["delivery under the split, own half"].startswith(
        "0 of 1 messages reached their whole half"
    )

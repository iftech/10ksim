from src.analysis.post_run.logos_messaging import (
    accepted_sends,
    delivery,
    end_to_end_latency,
    live_receipts,
    log_check,
    metrics_check,
    parse_metrics,
    propagation_time,
    send_outcomes,
    seq_gaps,
    settings_reliability,
    unexplained_missing,
)

NODES = ["relay-0-0", "relay-0-1", "relay-0-2"]


def send(msg_id, node, request_id=None, status=200, post_ns=0, done_ns=10):
    record = {
        "msg_id": msg_id,
        "node": node,
        "status": status,
        "post_ns": post_ns,
        "done_ns": done_ns,
    }
    if request_id:
        record["requestId"] = request_id
    return record


def read(msg_id, node, poll_ns=0, source="live", seq=1, timestamp=None):
    return {
        "msg_id": msg_id,
        "node": node,
        "poll_ns": poll_ns,
        "source": source,
        "seq": seq,
        "timestamp": timestamp,
        "messageHash": f"0x{msg_id}",
    }


def test_delivery_excludes_sender_and_names_missing_nodes():
    sends = accepted_sends([send("a", "relay-0-0", "r1"), send("b", "relay-0-1", "r2")])
    receipts, _ = live_receipts(
        [
            read("a", "relay-0-0"),
            read("a", "relay-0-1"),
            read("a", "relay-0-2"),
            read("b", "relay-0-0"),
        ]
    )

    result = delivery(sends, receipts, NODES)

    assert result["expected"] == 4
    assert result["observed"] == 3
    assert result["residual"] == 1
    assert result["messages_complete"] == 1
    assert result["self_receipts"] == 1
    assert result["missing_by_node"] == {"relay-0-2": 1}


def test_history_and_repeated_reads_are_not_live_receipts():
    receipts, duplicates = live_receipts(
        [read("a", "relay-0-1"), read("a", "relay-0-1"), read("a", "relay-0-2", source="history")]
    )

    assert receipts == {"a": {"relay-0-1"}}
    assert duplicates == 1


def test_seq_gaps_count_skipped_records_from_one():
    records = [read("a", "relay-0-0", seq=s) for s in (2, 3, 6)] + [read("b", "relay-0-1", seq=1)]

    assert seq_gaps(records) == {"relay-0-0": 3, "relay-0-1": 0}


def test_metrics_check_counts_own_messages_against_the_live_counter():
    receipts = {"a": {"relay-0-0", "relay-0-1"}, "b": {"relay-0-0"}}
    metrics = {
        "relay-0-0": {'logos_delivery_recv_messages_total{source="live"}': 2.0},
        "relay-0-1": {'logos_delivery_recv_messages_total{source="live"}': 2.0},
    }

    result = metrics_check(receipts, metrics)

    assert result["mismatches"] == {"relay-0-1": {"api": 1, "counter": 2}}


def test_parse_metrics_keeps_labels():
    text = (
        "# HELP logos_delivery_recv_messages_total x\n"
        'logos_delivery_recv_messages_total{source="live"} 3.0\n'
        "logos_delivery_rest_received_dropped_total 0.0\n"
        "libp2p_peers 4.0\n"
    )

    assert parse_metrics(text) == {
        'logos_delivery_recv_messages_total{source="live"}': 3.0,
        "logos_delivery_rest_received_dropped_total": 0.0,
    }


def test_send_outcomes_sort_event_sequences():
    sends = [
        send("a", "relay-0-0", "r1"),
        send("b", "relay-0-0", "r2"),
        send("c", "relay-0-0", "r3"),
        send("d", "relay-0-0", status=429),
    ]
    events = [
        {"requestId": "r1", "kind": "propagated"},
        {"requestId": "r2", "kind": "error"},
    ]

    result = send_outcomes(sends, events)

    assert result["accepted"] == 3
    assert result["propagated_only"] == 1
    assert result["error"] == 1
    assert result["no_events"] == 1
    assert [f["msg_id"] for f in result["http_failures"]] == ["d"]


def test_end_to_end_latency_uses_first_live_read_at_other_nodes():
    sends = accepted_sends([send("a", "relay-0-0", "r1", post_ns=100)])
    received = [
        read("a", "relay-0-0", poll_ns=150),
        read("a", "relay-0-1", poll_ns=300),
        read("a", "relay-0-1", poll_ns=900),
        read("a", "relay-0-2", poll_ns=500, source="history"),
    ]

    assert end_to_end_latency(sends, received) == [200]


def test_propagation_time_is_on_the_sender_clock():
    sends = accepted_sends([send("a", "relay-0-0", "r1")])
    events = [{"requestId": "r1", "kind": "propagated", "timestamp": 1_030}]

    assert propagation_time(sends, events, {"a": 1_000}) == [30]


def test_log_check_compares_non_sender_receipts_per_message():
    sends = accepted_sends([send("a", "relay-0-0", "r1"), send("b", "relay-0-1", "r2")])
    receipts = {"a": {"relay-0-0", "relay-0-1", "relay-0-2"}, "b": {"relay-0-0"}}
    log_receipts = {"0xa": {"relay-0-1"}, "0xb": {"relay-0-0", "relay-0-2"}}

    result = log_check(sends, receipts, {"a": "0xa", "b": "0xb"}, log_receipts)

    assert result["api"] == 3
    assert result["logs"] == 3
    assert result["mismatches"] == [
        {"msg_id": "a", "api_only": ["relay-0-2"], "log_only": []},
        {"msg_id": "b", "api_only": [], "log_only": ["relay-0-2"]},
    ]


def test_settings_reliability_reads_the_startup_line():
    line = "INF 2026-10-20 Messaging API settings topics=x reliability=false anonymityLevel=None"

    assert settings_reliability(line) == "false"
    assert settings_reliability("INF Messaging API settings topics=x") is None


def test_unexplained_missing_is_what_seq_gaps_do_not_cover():
    missing = {"relay-0-1": 2, "relay-0-2": 1}
    gaps = {"relay-0-1": 2, "relay-0-3": 1}

    assert unexplained_missing(missing, gaps) == {"relay-0-2": 1, "relay-0-3": -1}

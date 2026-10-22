from src.analysis.post_run.logos_messaging import (
    delivery,
    delivery_latency,
    parse_received_line,
    parse_send_line,
    propagation_time,
    relay_cross_check,
    send_outcomes,
    settings_reliability,
)

NODES = ["relay-0-0", "relay-0-1", "relay-0-2"]


def send(index, node, request_id=None, status=200):
    event = {"event": "send", "index": index, "node": node, "status": status}
    if request_id:
        event["requestId"] = request_id
    return event


def test_parse_received_line():
    line = (
        "INF 2026-10-21 00:10:14.041+00:00 Message received                           tid=8 "
        "file=recv_service.nim:141 msg_hash=0x61df contentTopic=/dst/1/messaging/proto source=live"
    )

    assert parse_received_line(line) == ("0x61df", 1792541414041000000)
    assert parse_received_line("INF 2026-10-21 00:10:14.041+00:00 Received relay message") is None


def test_parse_send_lines():
    propagated = (
        "INF 2026-10-21 00:10:14.032+00:00 Message successfully propagated            "
        'topics="send service" tid=8 file=send_service.nim:306 requestId=03b4 msgHash=0x61df'
    )
    failed = (
        "ERR 2026-10-21 00:10:15.000+00:00 Failed to send message                      "
        'topics="send service" requestId=77aa msgHash=0x9b01 error="no peers"'
    )

    assert parse_send_line(propagated) == ("propagated", "03b4", "0x61df", 1792541414032000000)
    assert parse_send_line(failed)[:3] == ("failed", "77aa", "0x9b01")


def test_send_outcomes_match_request_ids_and_lost_replies():
    sends = [
        send(0, "relay-0-0", "r1"),
        send(1, "relay-0-1", "r2"),
        send(2, "relay-0-1", "r3"),
        send(3, "relay-0-2", status=None),
        send(4, "relay-0-0", status=None),
    ]
    lines = [
        ("relay-0-0", "propagated", "r1", "0xa", 10),
        ("relay-0-1", "failed", "r2", "0xb", 10),
        ("relay-0-2", "propagated", "r9", "0xd", 10),
    ]

    result = send_outcomes(sends, lines)

    assert result["accepted"] == 3
    assert result["propagated"] == 2
    assert result["failed"] == 1
    assert result["no_log_line"] == 1
    assert result["reply_lost"] == [3]
    assert [f["index"] for f in result["http_failures"]] == [4]
    assert result["messages"] == {"0xa": "relay-0-0", "0xd": "relay-0-2"}


def test_delivery_leaves_out_the_sender_and_names_missing_nodes():
    messages = {"0xa": "relay-0-0", "0xb": "relay-0-1"}
    lines = [
        ("relay-0-0", "0xa", 0),
        ("relay-0-1", "0xa", 5),
        ("relay-0-2", "0xa", 6),
        ("relay-0-0", "0xb", 7),
    ]

    result = delivery(messages, lines, NODES)

    assert result["expected"] == 4
    assert result["observed"] == 3
    assert result["messages_complete"] == 1
    assert result["self_receipts"] == 1
    assert result["missing_by_node"] == {"relay-0-2": 1}


def test_latency_is_measured_from_the_sender_own_line():
    messages = {"0xa": "relay-0-0"}
    received = [
        ("relay-0-0", "0xa", 1_000),
        ("relay-0-1", "0xa", 1_030),
        ("relay-0-1", "0xa", 1_020),
        ("relay-0-2", "0xother", 1_040),
    ]
    sends = [("relay-0-0", "propagated", "r1", "0xa", 1_004)]

    assert delivery_latency(messages, received) == [20]
    assert propagation_time(messages, sends, received) == [4]


def test_relay_cross_check_compares_receivers_per_message():
    messages = {"0xa": "relay-0-0"}
    received = [("relay-0-1", "0xa", 0), ("relay-0-2", "0xa", 0)]

    result = relay_cross_check(messages, received, {"0xa": {"relay-0-1"}})

    assert result["residual"] == 1
    assert result["mismatches"] == [
        {"hash": "0xa", "messaging_only": ["relay-0-2"], "relay_only": []}
    ]


def test_settings_reliability_reads_the_startup_line():
    line = "INF 2026-10-20 Messaging API settings topics=x reliability=false anonymityLevel=None"

    assert settings_reliability(line) == "false"
    assert settings_reliability("INF Messaging API settings topics=x") is None

from src.deployments.experiments.logos_messaging import (
    make_payload,
    payload_msg_id,
    send_event_targets,
)


def test_payload_carries_the_message_id():
    payload = make_payload("00042", 1000)

    assert payload_msg_id(payload) == "00042"
    assert payload_msg_id("bm8gc2VwYXJhdG9y") is None


def test_send_events_are_polled_where_a_reply_was_lost():
    pending = {"relay-0-1": {"r1"}, "relay-0-2": set()}

    assert send_event_targets(pending, {"relay-0-3"}) == ["relay-0-1", "relay-0-3"]

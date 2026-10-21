from src.deployments.experiments.logos_messaging import make_payload, payload_msg_id


def test_payload_carries_the_message_id():
    payload = make_payload("00042", 1000)

    assert payload_msg_id(payload) == "00042"
    assert payload_msg_id("bm8gc2VwYXJhdG9y") is None

# Python Imports
import base64
import logging
import os
from typing import Union

from pydantic import NonNegativeInt

# Project Imports
from src.deployments.pod_api_requester.configs import Endpoint, Target
from src.deployments.pod_api_requester.pod_api_requester import _DEFAULTS, pod_api_request, wrap_arg

logger = logging.getLogger(__name__)

DEFAULT_CONTENT_TOPIC = "/my-app/1/dst/proto"


def random_payload(msg_size_kbytes: NonNegativeInt) -> str:
    """Base64 payload of random bytes, so every message gets its own hash."""
    return base64.b64encode(os.urandom(msg_size_kbytes * 1000)).decode("ascii").rstrip("=")


def pubsub_topic(cluster_id: NonNegativeInt, shard: NonNegativeInt = 0) -> str:
    return f"/waku/2/rs/{cluster_id}/{shard}"


async def waku_publish(
    namespace: str,
    target: Union[Target, str],
    content_topic: str = DEFAULT_CONTENT_TOPIC,
    cluster_id: NonNegativeInt = 2,
    port: NonNegativeInt = 8645,
    msg_size_kbytes: NonNegativeInt = 10,
) -> dict:
    return await pod_api_request(
        namespace=namespace,
        service_name=_DEFAULTS["service_name"],
        app=_DEFAULTS["app"],
        url_template="http://{target_ip}:{node_port}/waku/relay",
        data={
            "target": wrap_arg(target),
            "content_topic": content_topic,
            "cluster_id": cluster_id,
            "port": port,
            "msg_size_kbytes": msg_size_kbytes,
        },
    )


async def waku_lightpush_publish(
    namespace: str,
    target: Union[Target, str],
    content_topic: str = DEFAULT_CONTENT_TOPIC,
    cluster_id: NonNegativeInt = 2,
    shard: NonNegativeInt = 0,
    msg_size_kbytes: NonNegativeInt = 10,
) -> dict:
    """Publish through an edge node's lightpush endpoint."""
    endpoint = Endpoint(
        name="waku_lightpush_message",
        url="http://{node}:{port}/lightpush/v3/message",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        params={
            "pubsubTopic": pubsub_topic(cluster_id, shard),
            "message": {
                "payload": random_payload(msg_size_kbytes),
                "contentTopic": content_topic,
                "version": 1,
            },
        },
        type="POST",
        paged=False,
    )
    return await pod_api_request(
        namespace=namespace,
        service_name=_DEFAULTS["service_name"],
        app=_DEFAULTS["app"],
        url_template="http://{target_ip}:{node_port}/process",
        data={
            "target": wrap_arg(target),
            "endpoint": wrap_arg(endpoint),
        },
    )

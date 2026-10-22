# Python Imports
import json
import re
from typing import Tuple

# Project Imports
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import (
    PodApiHttpError,
    PodApiRequesterError,
    post_async,
    wrap_arg,
)

REST_PORT = 8645


def pod_target(pod: str, port: int = REST_PORT) -> Target:
    """Exact pod name. A `service` makes the requester read it once per pod on every call."""
    return Target(name=pod, name_template=f"^{re.escape(pod)}$", port=port)


async def messaging_send(
    requester: Tuple[str, int], pod: str, content_topic: str, msg_size_kbytes: int
) -> Tuple[int, str]:
    """Send a random message from `pod` through the requester. Returns the node's status and body."""
    data = {
        "target": wrap_arg(pod_target(pod)),
        "content_topic": content_topic,
        "port": REST_PORT,
        "msg_size_kbytes": msg_size_kbytes,
    }
    requester_ip, requester_port = requester
    response = await post_async(f"http://{requester_ip}:{requester_port}/messaging/send", data)
    if response.status_code != 200:
        raise PodApiRequesterError(response)
    result = json.loads(response.text)
    if "response" not in result:
        raise PodApiHttpError(result)
    return result["response"]["status_code"], result["response"]["text"]


async def clear_requester_cache(requester: Tuple[str, int]) -> None:
    """Drop the requester's cached pod IPs, which can be from an earlier run."""
    ip, port = requester
    response = await post_async(f"http://{ip}:{port}/cache/clear", {})
    if response.status_code != 200:
        raise PodApiRequesterError(response)

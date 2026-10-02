import logging
from typing import Optional, Tuple, Union

from pydantic import NonNegativeInt

from src.deployments.pod_api_requester.configs import Endpoint, Target
from src.deployments.pod_api_requester.pod_api_requester import _DEFAULTS, pod_api_request, wrap_arg

logger = logging.getLogger(__name__)


async def libp2p_dst_node_publish(
    namespace: str,
    target: Union[Target, str],
    *,
    topic: str = "test",
    msg_size_bytes: NonNegativeInt = 1,
    requester: Optional[Tuple[str, int]] = None,
) -> dict:
    endpoint = Endpoint(
        name="nimlibp2p_message",
        url="http://{node}:{port}/publish",
        headers={"Content-Type": "application/json"},
        params={"topic": topic, "msgSize": msg_size_bytes, "version": 1},
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
        requester=requester,
    )

# Python Imports
import logging
from typing import Optional, Tuple, Union

from pydantic import NonNegativeInt

# Project Imports
from src.deployments.pod_api_requester.configs import Target
from src.deployments.pod_api_requester.pod_api_requester import _DEFAULTS, pod_api_request, wrap_arg

logger = logging.getLogger(__name__)


async def waku_publish(
    namespace: str,
    target: Union[Target, str],
    content_topic: str = "/my-app/1/dst/proto",
    cluster_id: NonNegativeInt = 2,
    port: NonNegativeInt = 8645,
    msg_size_kbytes: NonNegativeInt = 10,
    requester: Optional[Tuple[str, int]] = None,
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
        requester=requester,
    )

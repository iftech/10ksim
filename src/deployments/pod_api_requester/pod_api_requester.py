# Python Imports
import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import aiohttp
import aiohttp.http_exceptions
from kubernetes import client
from pydantic import BaseModel, NonNegativeInt

# Project Imports
from src.deployments.core.k8s_kubeconfig import get_node_ip
from src.deployments.pod_api_requester.configs import Endpoint, Target

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "service_name": "zerotesting-publisher",
    "app": "zerotenkay-publisher",
}


class PodApiError(Exception):
    """Base for all Pod API errors."""


class PodApiClientError(PodApiError):
    """Error in making a request to pod-api-requester."""


class PodApiValidationError(PodApiClientError):
    """Invalid arguments to helper functions."""


class PodApiRequesterError(PodApiError):
    """
    Request to pod-api-requester was successful,
    but pod-api-requester returned an error.
    """


class PodApiResponseError(PodApiError):
    """
    pod-api-requester successfully made a request to a pod,
    but the target pod returned some kind of error.
    """


class PodApiHttpError(PodApiResponseError):
    """
    pod-api-requester successfully made a request to a pod,
    but the target pod returned something other than 200.
    """


class PodApiApplicationError(PodApiResponseError):
    """
    Raised when all of the following are true:
    * pod-api-requester successfully made a request to a pod
    * The target pod returned 200 OK
    * The pod's data contains an "error" key.
    """


def read_deployment_file(path: Path, namespace: str):
    raise NotImplementedError("TODO")
    # with open(Path(__file__).parent / path, "r") as deployment_yaml:
    #     import yaml
    #     deployment_spec = yaml.safe_load(deployment_yaml.read())
    #     deployment_spec["metadata"]["namespace"] = namespace
    #     return deployment_spec


async def launch_prerequisites(namespace: str):
    raise NotImplementedError("TODO")
    # TODO: publisher-service, role/bind, config.yaml


def wrap_arg(arg: Union[Target, Endpoint, str]) -> dict:
    """Wrap Target or Endpoint argument as either a config or name."""
    if isinstance(arg, str):
        kind = "name"
    else:
        kind = "config"
        if arg.name is None:
            arg = {**arg, **{"name": "dummy"}}
        arg = arg.model_dump()
    return {"kind": kind, "value": arg}


def redact_payloads(obj):
    """Copy `obj` with every "payload" value replaced, so logs stay readable."""
    if isinstance(obj, dict):
        return {k: "<redacted>" if k == "payload" else redact_payloads(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_payloads(item) for item in obj]
    return obj


async def request(
    # TODO: Consider passing in publisher k8s object and extract namespace, service_name, and app from there.
    namespace: str,
    target: Union[Target, str],
    endpoint: Union[Endpoint, str],
) -> dict:
    data = {
        "target": wrap_arg(target),
        "endpoint": wrap_arg(endpoint),
    }

    return await pod_api_request(
        namespace=namespace,
        service_name=_DEFAULTS["service_name"],
        app=_DEFAULTS["app"],
        url_template="http://{target_ip}:{node_port}/process",
        data=data,
    )


class PodResponse(BaseModel):
    status_code: int
    reason: str
    text: str
    headers: Optional[Dict[str, str]] = None


REQUEST_TIMEOUT_S = 30
"""Cap on one pod-api-requester call.

aiohttp's default is 300s and its connector pool holds 100 sockets, so requests to pods
that are gone hold their slots long enough to starve a publish loop rather than failing.
"""


async def post_async(url, data, timeout_s: float = REQUEST_TIMEOUT_S):
    """Execute an async POST request.

    :param data: JSON data for the request."""
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=data) as response:
            text = await response.text()
            return PodResponse(
                status_code=response.status,
                reason=response.reason,
                text=text,
                headers=dict(response.headers),
            )


def _get_api_requester_info(
    namespace: str,
    service_name: str,
    app: str,
    *,
    publisher_pod: str | NonNegativeInt = 0,
) -> Tuple[str, str]:
    """Find the pod-api-requester pod in the cluster."""
    v1 = client.CoreV1Api()

    try:
        pods = v1.list_namespaced_pod(namespace=namespace, label_selector=f"app={app}")
        if isinstance(publisher_pod, str):
            pod = next(pod for pod in pods.items if pod.metadata.name == publisher_pod)
        else:
            pod = pods.items[publisher_pod]
    except IndexError as e:
        logger.error(f"No pod found. app: `{app}` pod_index: `{publisher_pod}`")
        raise PodApiClientError("No publisher pod found") from e
    except StopIteration as e:
        logger.error(f"No pod found. app: `{app}` pod_name: `{publisher_pod}`")
        raise PodApiClientError("No publisher pod found") from e

    # Get publisher IP.
    node = v1.read_node(name=pod.spec.node_name)
    target_ip = get_node_ip(node)

    # Get publisher port.
    service = v1.read_namespaced_service(service_name, namespace)
    node_port = service.spec.ports[0].node_port
    if node_port is None:
        raise PodApiClientError(
            f"Failed to find port for service. Service: `{service.metadata.name}`"
        )

    return target_ip, node_port


async def pod_api_request(
    namespace: str,
    service_name: str,
    app: str,
    url_template: str,
    data: dict,
    *,
    publisher_pod: str | NonNegativeInt = 0,
) -> dict:
    target_ip, node_port = _get_api_requester_info(
        namespace=namespace,
        service_name=service_name,
        app=app,
        publisher_pod=publisher_pod,
    )

    url = url_template.format(target_ip=target_ip, node_port=node_port)

    logger.info(f"publishing message. url: `{url}` data: `{redact_payloads(data)}`")
    try:
        response = await post_async(url, data)
        response_obj = json.loads(response.text)
    except asyncio.TimeoutError as e:
        raise PodApiClientError(
            f"pod-api-requester did not answer within {REQUEST_TIMEOUT_S}s."
        ) from e
    except aiohttp.ClientError as e:
        raise PodApiClientError("Failed to make the request to pod-api-requester.") from e
    except json.JSONDecodeError as e:
        # This is unexpected. Even if there is an error, pod-api-requester is expected to return a JSON-deserializable response.
        raise PodApiRequesterError("Deserialization failed for pod-api-requester response.")

    if response.status_code != 200:
        err = response_obj["detail"]
        if isinstance(err, (list, tuple)):
            err = "\n".join([str(e).replace("\\n", "\n") for e in err])
        else:
            err = str(err).replace("\\n", "\n")
        logger.error(err)
        raise PodApiRequesterError(response)

    try:
        targ_pod_response = response_obj["response"]
    except KeyError as e:
        err = response_obj.get("exception", "<no exception key>").replace("\n", "\n")
        logger.error(f"pod-api-requester's request attempt failed. Exception: `{err}`")
        raise PodApiHttpError(response_obj) from e

    if targ_pod_response["status_code"] != 200:
        logger.error(f"pod-api-requester received an error. pod_response: `{targ_pod_response}`")
        # Extract as far as we can to get the underlying error.
        err = targ_pod_response
        try:
            err = json.loads(targ_pod_response["text"])
            # JsWaku puts the error under the key "error".
            err = err["error"]
            response_obj["inner_error"] = err
        except (json.JSONDecodeError, KeyError) as e:
            pass
        try:
            err = err.replace("\n", "\n")
        except:
            pass
        raise PodApiHttpError(err)

    logger.info(f"Response: `{redact_payloads(response_obj)}`")
    return response_obj


if __name__ == "__main__":
    asyncio.run(main())

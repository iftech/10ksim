"""Pull pod logs straight off the cluster, as a fallback for the log collector.

VictoriaLogs has intermittently shipped nothing for a namespace, which silently breaks
the whole delivery analysis. Capturing the logs with the run makes it independent of the
collector, at the cost of the dwell time it takes to pull them.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

from kubernetes import client
from kubernetes.client import ApiException, V1Pod

logger = logging.getLogger(__name__)


def capture_pod_logs(
    namespace: str,
    dest: Path,
    api_client=None,
    *,
    label_selector: Optional[str] = None,
    filename: Callable[[V1Pod], str] = lambda pod: pod.metadata.name,
    workers: int = 32,
) -> int:
    """Write each pod's log to `dest/<filename(pod)>.log`. Returns how many were captured.

    Only the current container is returned, so a pod that restarted mid-run is captured
    from the restart onwards: authoritative for pods that stayed up, incomplete for the
    ones a churn scenario took down.
    """
    api = client.CoreV1Api(api_client or client.ApiClient())
    # The capture is a fallback: nothing in it may take the run down.
    try:
        pods = api.list_namespaced_pod(namespace=namespace, label_selector=label_selector).items
        dest.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.warning(f"Could not list pods in `{namespace}` to capture logs: {e!r}")
        return 0

    def capture(pod: V1Pod) -> bool:
        name = pod.metadata.name
        try:
            log = api.read_namespaced_pod_log(name=name, namespace=namespace)
            (dest / f"{filename(pod)}.log").write_text(log)
        except ApiException as e:
            logger.warning(f"Could not read logs from pod `{name}`: {e.status}")
            return False
        except Exception as e:
            logger.warning(f"Could not capture logs from pod `{name}`: {e!r}")
            return False
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        captured = sum(pool.map(capture, pods))

    logger.info(f"Captured {captured} of {len(pods)} pod logs into `{dest}/`")
    return captured

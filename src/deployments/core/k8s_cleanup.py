# Python Imports
import logging
import time
from typing import Callable, Dict, List, Optional

from kubernetes import client
from kubernetes.client import ApiClient, ApiException
from ruamel import yaml

logger = logging.getLogger(__name__)


def get_cleanup_resources(yamls: List[yaml.YAMLObject], types: Optional[List[str]] = None):
    """
    Get dict of resources to cleanup based on yamls.

    :param types: Which type of objects to gather from the yamls. If None uses default list.
    :return: Return dictionary of where key is the type of resource and values are lists of resource names.
    :rtype: Dict[Tuple[str, list[str]]]
    """
    resources = {
        "Deployment": [],
        "StatefulSet": [],
        "DaemonSet": [],
        "ReplicaSet": [],
        "ReplicationController": [],
        "Job": [],
        "CronJob": [],
        "Pod": [],
        "Service": [],
        "ConfigMap": [],
        "PersistentVolumeClaim": [],
        "Role": [],
        "RoleBinding": [],
        "ServiceAccount": [],
        "NetworkPolicy": [],
    }
    types = (
        types
        if types
        else [
            "Deployment",
            "StatefulSet",
            "DaemonSet",
            "ReplicaSet",
            "Pod",
            "Job",
            "Service",
            "Role",
            "RoleBinding",
            "ConfigMap",
            "ServiceAccount",
            "PersistentVolumeClaim",
            "NetworkPolicy",
        ]
    )
    for yaml in yamls:
        try:
            if yaml["kind"] in types:
                resources[yaml["kind"]].append(yaml["metadata"]["name"])
        except KeyError:
            pass
    return {resource[0]: resource[1] for resource in resources.items() if resource[1]}


def delete_pod(name, namespace, *, grace_period=0):
    v1 = client.CoreV1Api()
    v1.delete_namespaced_pod(
        name=name,
        namespace=namespace,
        body=client.V1DeleteOptions(grace_period_seconds=grace_period),
        grace_period_seconds=grace_period,
        propagation_policy="Foreground",
    )


def delete_network_policy(
    name: str, namespace: str, api_client: Optional[ApiClient] = None
) -> None:
    client.NetworkingV1Api(api_client or client.ApiClient()).delete_namespaced_network_policy(
        name=name, namespace=namespace, body=client.V1DeleteOptions()
    )


def cleanup_resources(
    resources: dict,
    namespace: str,
    api_client,
):
    """
    Delete resources in the recommended order: controllers first, then pods, then services.
    resources: Dict mapping kind to list of names.
    """
    logger.info(f"Cleanup resources: `{resources}`")
    deletion_order = [
        "NetworkPolicy",
        "Deployment",
        "StatefulSet",
        "DaemonSet",
        "ReplicaSet",
        "ReplicationController",
        "Job",
        "CronJob",
        "Pod",
        "Service",
        "ConfigMap",
        "RoleBinding",
        "Role",
        "ServiceAccount",
        "PersistentVolumeClaim",
    ]

    map = {
        "Deployment": lambda name: client.AppsV1Api(api_client).delete_namespaced_deployment(
            name, namespace
        ),
        "StatefulSet": lambda name: client.AppsV1Api(api_client).delete_namespaced_stateful_set(
            name, namespace
        ),
        "DaemonSet": lambda name: client.AppsV1Api(api_client).delete_namespaced_daemon_set(
            name, namespace
        ),
        "ReplicaSet": lambda name: client.AppsV1Api(api_client).delete_namespaced_replica_set(
            name, namespace
        ),
        "ReplicationController": lambda name: client.CoreV1Api(
            api_client
        ).delete_namespaced_replication_controller(name, namespace),
        "Job": lambda name: client.BatchV1Api(api_client).delete_namespaced_job(
            name,
            namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        ),
        "CronJob": lambda name: client.BatchV1Api(api_client).delete_namespaced_cron_job(
            name, namespace
        ),
        "Pod": lambda name: client.CoreV1Api(api_client).delete_namespaced_pod(name, namespace),
        "Service": lambda name: client.CoreV1Api(api_client).delete_namespaced_service(
            name, namespace
        ),
        "ConfigMap": lambda name: client.CoreV1Api(api_client).delete_namespaced_config_map(
            name, namespace
        ),
        "PersistentVolumeClaim": lambda name: client.CoreV1Api(
            api_client
        ).delete_namespaced_persistent_volume_claim(name, namespace),
        "ServiceAccount": lambda name: client.CoreV1Api(
            api_client
        ).delete_namespaced_service_account(name, namespace, body=client.V1DeleteOptions()),
        "Role": lambda name: client.RbacAuthorizationV1Api(api_client).delete_namespaced_role(
            name, namespace, body=client.V1DeleteOptions()
        ),
        "RoleBinding": lambda name: client.RbacAuthorizationV1Api(
            api_client
        ).delete_namespaced_role_binding(name, namespace, body=client.V1DeleteOptions()),
        "NetworkPolicy": lambda name: client.NetworkingV1Api(
            api_client
        ).delete_namespaced_network_policy(name, namespace, body=client.V1DeleteOptions()),
    }

    for kind in deletion_order:
        logger.debug(f"Checking kind: `{kind}` in `{resources}`")
        names = resources.get(kind, [])
        logger.debug(f"Found {len(names)} names")
        for name in names:
            logger.debug(f"Checking name: `{name}`")
            deleter = map.get(kind)
            if not deleter:
                logger.warning(f"Unsupported kind for cleanup: `{kind}`")
                continue
            try:
                logger.info(f"Requesting deletion of {kind} `{name}` in namespace `{namespace}`.")
                deleter(name)
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"{kind} `{name}` not found.")
                else:
                    logger.warning(f"Error deleting {kind} `{name}`: {e}")


def poll_cleanup_status(
    resources: Dict[str, List[str]],
    namespace: str,
    api_client: ApiClient,
) -> bool:
    """
    Returns True if all specified resources are gone (deleted), False otherwise.
    """
    map = {
        "Deployment": lambda name: client.AppsV1Api(api_client).read_namespaced_deployment(
            name, namespace
        ),
        "StatefulSet": lambda name: client.AppsV1Api(api_client).read_namespaced_stateful_set(
            name, namespace
        ),
        "DaemonSet": lambda name: client.AppsV1Api(api_client).read_namespaced_daemon_set(
            name, namespace
        ),
        "ReplicaSet": lambda name: client.AppsV1Api(api_client).read_namespaced_replica_set(
            name, namespace
        ),
        "ReplicationController": lambda name: client.CoreV1Api(
            api_client
        ).read_namespaced_replication_controller(name, namespace),
        "Job": lambda name: client.BatchV1Api(api_client).read_namespaced_job(name, namespace),
        "CronJob": lambda name: client.BatchV1Api(api_client).read_namespaced_cron_job(
            name, namespace
        ),
        "Pod": lambda name: client.CoreV1Api(api_client).read_namespaced_pod(name, namespace),
        "Service": lambda name: client.CoreV1Api(api_client).read_namespaced_service(
            name, namespace
        ),
        "ConfigMap": lambda name: client.CoreV1Api(api_client).read_namespaced_config_map(
            name, namespace
        ),
        "ServiceAccount": lambda name: client.CoreV1Api(api_client).read_namespaced_service_account(
            name, namespace
        ),
        "Role": lambda name: client.RbacAuthorizationV1Api(api_client).read_namespaced_role(
            name, namespace
        ),
        "RoleBinding": lambda name: client.RbacAuthorizationV1Api(
            api_client
        ).read_namespaced_role_binding(name, namespace),
    }

    for kind, names in resources.items():
        reader = map.get(kind)
        if not reader:
            continue
        for name in names:
            try:
                reader(name)  # If no exception, resource still exists
                return False
            except ApiException as e:
                if e.status == 404:
                    continue  # Resource is gone
                else:
                    raise
    return True  # All resources are gone


def wait_for_cleanup(
    resources: Dict[str, List[str]],
    namespace: str,
    api_client,
    timeout: int = 600,
    polling_interval: int = 8,
):
    """
    Wait until all specified resources are deleted.
    """
    start_time = time.time()
    logger.info(
        f"Waiting for cleanup of resources in namespace `{namespace}` (timeout: {timeout}s)..."
    )
    while True:
        try:
            cleaned_up = poll_cleanup_status(resources, namespace, api_client)
        except ApiException as e:
            logger.warning(f"Error polling cleanup status: {e}")
            time.sleep(polling_interval)
            continue

        logger.info(f"Cleanup status: {'complete' if cleaned_up else 'incomplete'}.")

        if cleaned_up:
            logger.info(f"All specified resources cleaned up in namespace `{namespace}`.")
            return

        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(
                f"Timeout waiting for cleanup of resources in namespace `{namespace}`."
            )

        time.sleep(polling_interval)


def get_cleanup(
    api_client: ApiClient, namespace: str, deployments: List[yaml.YAMLObject]
) -> Callable[[], None]:
    def cleanup():
        logger.debug("Cleaning up resources.")
        resources_to_cleanup = get_cleanup_resources(deployments)
        logger.debug(f"Resources to clean up: `{resources_to_cleanup}`")

        logger.debug(f"Start cleanup.")
        try:
            cleanup_resources(resources_to_cleanup, namespace, api_client)
        except client.exceptions.ApiException as e:
            logger.error(
                f"Exception cleaning up resources. Resources: `{resources_to_cleanup}` exception: `{e}`",
                exc_info=True,
            )
        logger.debug(f"Waiting for cleanup. Resources: `{resources_to_cleanup}`")
        wait_for_cleanup(resources_to_cleanup, namespace, api_client)
        logger.info(f"Finished cleanup. Resources: `{resources_to_cleanup}`")

    return cleanup


def poll_namespace_has_objects(
    namespace: str, api_client: ApiClient, types: Optional[List[str]] = None
):
    """
    Poll kubernetes to see if the namespace has any objects of the given types in it*.

    *By default, does not check for secrets, configmaps, PVCs or services

    Also note that there may be some services running that won't be found
    for kubernetes versions lower than v1.25 which uses the batch/v1beta1 endpoint.

    :return: True if any such resources are found, False otherwise.
    :rtype: bool
    """
    types = types if types else ["Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Pod"]
    logger.debug(f"Checking in namespace `{namespace}` for types: `{types}`")
    v1 = client.CoreV1Api(api_client)
    apps_v1 = client.AppsV1Api(api_client)
    batch_v1 = client.BatchV1Api(api_client)

    resource_checks = {
        "Pod": lambda: v1.list_namespaced_pod(namespace).items,
        "Service": lambda: v1.list_namespaced_service(namespace).items,
        "PersistentVolumeClaim": lambda: v1.list_namespaced_persistent_volume_claim(
            namespace
        ).items,
        "Deployment": lambda: apps_v1.list_namespaced_deployment(namespace).items,
        "StatefulSet": lambda: apps_v1.list_namespaced_stateful_set(namespace).items,
        "ReplicaSet": lambda: apps_v1.list_namespaced_replica_set(namespace).items,
        "DaemonSet": lambda: apps_v1.list_namespaced_daemon_set(namespace).items,
        "Job": lambda: batch_v1.list_namespaced_job(namespace).items,
        "ReplicationController": lambda: v1.list_namespaced_replication_controller(namespace).items,
        "CronJob": lambda: batch_v1.list_namespaced_cron_job(namespace).items,
    }

    for type, check in resource_checks.items():
        try:
            if type not in types:
                continue
            if check():
                return True
        except ApiException:
            continue
    return False


def wait_for_no_objs_in_namespace(
    namespace: str,
    timeout: int = 300,
    api_client: ApiClient = None,
    polling_interval: int = 2,
    types: Optional[List[str]] = None,
):
    """
    Wait until the namespace has no objects of any of the given types.

    :param timeout: Timeout in seconds.
    :type timeout: int
    :param types: Which type of objects to check for.
        See `poll_namespace_has_objects` for default value of `types`.
    :type types: list, optional
    :raises TimeoutError: If timeout is exceeded.
    """
    start_time = time.time()
    logger.info(
        f"Waiting for namespace to be clean. Namespace: `{namespace}` (timeout: {timeout}s)..."
    )
    while True:
        has_objects = poll_namespace_has_objects(
            namespace,
            api_client,
            types,
        )

        if not has_objects:
            logger.info(f"`{namespace}` is empty.")
            return

        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(
                f"Timeout waiting for namespace to be empty. Namespace: `{namespace}`"
            )

        time.sleep(polling_interval)

# Script to put token into Kubernetes cluster as a secret
import base64
import logging
import os

from kubernetes import client

debug_env = os.environ.get("DEBUG", "").lower()
log_level = logging.DEBUG if debug_env == "true" else logging.INFO

logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    # pod_name = os.environ.get("HOSTNAME")
    pod_name = os.environ.get("POD_NAME")
    pod_uid = os.environ.get("POD_UID")
    token_file = os.path.expanduser(f"~/.logoscore/daemon/tokens/{pod_name}.json")
    with open(token_file, "r") as f:
        token = f.read().strip()
    encoded_token = base64.b64encode(token.encode()).decode()

    secret_name = f"{pod_name}-logoscore-secret"
    _api_client, v1 = init()

    pod_uid = os.environ.get("POD_UID")
    owner_ref = client.V1OwnerReference(
        api_version="v1",
        kind="Pod",
        name=pod_name,
        uid=pod_uid,
        controller=True,
        block_owner_deletion=True,
    )

    namespace = get_namespace()
    secret = client.V1Secret(
        api_version="v1",
        kind="Secret",
        metadata=client.V1ObjectMeta(
            name=secret_name,
            namespace=namespace,
            owner_references=[owner_ref],
        ),
        data={"token": encoded_token},
        type="Opaque",
    )

    v1.create_namespaced_secret(namespace=namespace, body=secret)
    logger.info("Secret created successfully.")


def get_namespace():
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
        return f.read().strip()


def init():
    """Init Kubernetes client in a way that allows us create the secret."""
    HOST = "https://kubernetes.default.svc"
    CA_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    with open(TOKEN_FILE, "r") as f:
        token = f.read().strip()

    configuration = client.Configuration()
    configuration.host = HOST
    configuration.ssl_ca_cert = CA_CERT
    configuration.api_key["authorization"] = token
    configuration.api_key_prefix["authorization"] = "Bearer"

    api_client = client.ApiClient(configuration)

    api_client.default_headers["Authorization"] = f"Bearer {token}"

    auth_header = api_client.default_headers.get("Authorization")
    logger.debug(f"Client Header: {auth_header}")

    v1 = client.CoreV1Api(api_client)
    return api_client, v1


if __name__ == "__main__":
    main()

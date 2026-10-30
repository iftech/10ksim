# Python Imports
from pathlib import Path
from typing import List, Literal, Optional

from kubernetes.client import V1EmptyDirVolumeSource, V1Volume, V1VolumeMount
from pydantic import PositiveInt

# Project Imports
from src.deployments.core.configs.command import CommandConfig
from src.deployments.core.configs.container import ContainerConfig, Image
from src.deployments.core.configs.pod import PodSpecConfig
from src.deployments.waku.builders.helpers import find_waku_command, find_waku_container_config


def getEnrOrAddress_initContainer_dict(
    type_: Literal["enr", "address"],
    num: PositiveInt,
    service_names: List[str],
    init_container_image: Image,
) -> dict:
    defaults = (
        {
            "shortname": "addrs",
            "version": "v0.1.0",
            "repo": "soutullostatus/getaddress",
        }
        if type_ == "address"
        else {
            "shortname": "enr",
            "version": "v0.5.0",
            "repo": "soutullostatus/getenr",
        }
    )

    image = (
        str(init_container_image)
        if init_container_image is not None
        else f"{defaults['repo']}:{defaults['version']}"
    )
    return {
        "name": f"grab{type_}",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "volumeMounts": [
            {
                "name": f"{type_}-data",
                "mountPath": f"/etc/{defaults['shortname']}",
            }
        ],
        "command": [f"/app/get{type_}.sh"],
        "args": [str(num)] + service_names,
    }


class _BaseFeature:
    command_arg_key: str  # To be overridden by subclasses, e.g. "--discv5-bootstrap-node"
    volume_name: str  # Example: "enr-data"
    init_container_type: str  # Examples: "enr" or "address"
    env_vars: str  # Examples: "ENR" or "addrs"
    source_file: str  # Examples: "/etc/enr/enr.env", "/etc/addrs/addrs.env"

    @classmethod
    def command_config(cls, config: CommandConfig, num: PositiveInt):
        # Add command to source environment and print values.
        # Example: [". /etc/enr/enr.env", "echo ENRs are $ENR1 $ENR2 $ENR3"]
        source_vars = " ".join([f"${cls.env_vars}{index}" for index in range(1, num + 1)])
        plural = "" if cls.env_vars.endswith("s") else "s"
        source = [f". {cls.source_file}", f"echo {cls.env_vars}{plural} are {source_vars}"]
        config.insert_commands(source, index=0)

        # Add waku flags (e.g. --discv5-bootstrap-node=$ENR1, etc.)
        args = [(cls.command_arg_key, f"${cls.env_vars}{index}") for index in range(1, num + 1)]
        command = find_waku_command(config)
        command.add_args(args)

    @classmethod
    def container(
        cls,
        config: ContainerConfig,
        num: PositiveInt,
    ):
        cls.command_config(config.command_config, num)
        config.with_volume_mount(
            V1VolumeMount(mount_path=Path(cls.source_file).parent.as_posix(), name=cls.volume_name)
        )

    @classmethod
    def pod_spec(
        cls,
        config: PodSpecConfig,
        num: PositiveInt,
        service_names: List[str],
        init_container_image: Optional[Image] = None,
    ):
        config.with_volume(V1Volume(name=cls.volume_name, empty_dir=V1EmptyDirVolumeSource()))
        config.add_init_container(
            getEnrOrAddress_initContainer_dict(
                cls.init_container_type,
                num,
                service_names,
                init_container_image,
            )
        )
        waku_container_config = find_waku_container_config(config)
        cls.container(waku_container_config, num)


class Enr(_BaseFeature):
    command_arg_key = "--discv5-bootstrap-node"
    volume_name = "enr-data"
    init_container_type = "enr"
    env_vars = "ENR"
    source_file = "/etc/enr/enr.env"


class Addrs(_BaseFeature):
    command_arg_key = "--lightpushnode"
    volume_name = "address-data"
    init_container_type = "address"
    env_vars = "addrs"
    source_file = "/etc/addrs/addrs.env"


class FilterAddrs(Addrs):
    command_arg_key = "--filternode"

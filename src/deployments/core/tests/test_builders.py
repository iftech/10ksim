import pytest
from kubernetes.client import (
    V1Container,
    V1LabelSelector,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServiceSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
)

from src.deployments.core.builders import (
    ContainerBuilder,
    ContainerCommandBuilder,
    PodBuilder,
    PodSpecBuilder,
    ServiceBuilder,
    StatefulSetBuilder,
)
from src.deployments.core.configs.container import ContainerConfig, Image
from src.deployments.core.configs.helpers.utils import init_container_delay
from src.deployments.core.dependency_decorator import depends_on

# --------------------------------------------------------------------------- #
# Helper Functions
# --------------------------------------------------------------------------- #


def _create_image(repo: str = "dst/test", tag: str = "v1.0.0") -> Image:
    """Create a real Image object for testing."""
    return Image(repo=repo, tag=tag)


def _create_statefulset_with_default_values() -> V1StatefulSet:
    """Create expected default V1StatefulSet for testing."""
    return V1StatefulSet(
        api_version="apps/v1",
        kind="StatefulSet",
        metadata=V1ObjectMeta(name=None, namespace="default", labels=None),
        spec=V1StatefulSetSpec(
            replicas=1,  # Default from StatefulSetSpecConfig
            service_name=None,
            pod_management_policy=None,
            volume_claim_templates=None,
            selector=V1LabelSelector(match_labels=None),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(name=None, namespace=None, labels=None, annotations=None),
                spec=V1PodSpec(containers=[], init_containers=None, volumes=None, dns_config=None),
            ),
        ),
    )


def _create_pod_with_default_values() -> V1Pod:
    """Create expected default V1Pod for testing."""
    return V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(name=None, namespace=None, labels=None),
        spec=V1PodSpec(containers=[], init_containers=None, volumes=None, dns_config=None),
    )


def _create_service_with_default_values() -> V1Service:
    """Create expected default V1Service for testing."""
    return V1Service(
        api_version="v1",
        kind="Service",
        metadata=V1ObjectMeta(name=None, namespace=None, labels=None),
        spec=V1ServiceSpec(
            cluster_ip=None, selector=None, ports=None, type=None, publish_not_ready_addresses=None
        ),
    )


def _required_node_affinity_expression(pod_spec: V1PodSpec):
    selector = pod_spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    return selector.node_selector_terms[0].match_expressions[0]


# --------------------------------------------------------------------------- #
# StatefulSetBuilder Tests
# --------------------------------------------------------------------------- #
class TestStatefulSetBuilder:
    """Tests for the StatefulSetBuilder class."""

    def test_build_with_no_config_returns_valid_statefulset(self):
        """Should build a valid V1StatefulSet with default config values."""
        builder = StatefulSetBuilder()
        builder.config.namespace = "default"  # Required by builder
        new_sts = builder.build()
        expected_sts = _create_statefulset_with_default_values()
        expected_sts.metadata.namespace = "default"

        # Verify the result matches expected defaults
        assert isinstance(new_sts, V1StatefulSet)
        assert new_sts == expected_sts

    def test_with_label_adds_label_to_config_and_spec(self):
        """Should add label to config, selector, and pod template."""
        builder = StatefulSetBuilder()
        builder.with_label("cluster", "vaclab")

        assert builder.config.labels == {"cluster": "vaclab"}
        assert builder.config.stateful_set_spec.selector_labels == {"cluster": "vaclab"}
        assert builder.config.stateful_set_spec.pod_template_spec_config.labels == {
            "cluster": "vaclab"
        }

    def test_with_avoided_machines_sets_not_in_node_affinity(self):
        """Should prevent pods from scheduling on listed machines."""
        builder = StatefulSetBuilder()
        builder.config.namespace = "default"

        result = builder.with_avoided_machines(["node-05", "node-06"])
        stateful_set = builder.build()
        expression = _required_node_affinity_expression(stateful_set.spec.template.spec)

        assert result is builder
        assert expression.key == "kubernetes.io/hostname"
        assert expression.operator == "NotIn"
        assert expression.values == ["node-05", "node-06"]

    def test_with_allowed_machines_sets_in_node_affinity(self):
        """Should restrict pods to listed machines."""
        builder = StatefulSetBuilder()
        builder.config.namespace = "default"

        result = builder.with_allowed_machines("node-01")
        stateful_set = builder.build()
        expression = _required_node_affinity_expression(stateful_set.spec.template.spec)

        assert result is builder
        assert expression.key == "kubernetes.io/hostname"
        assert expression.operator == "In"
        assert expression.values == ["node-01"]

    def test_init_container_delay_folds_rate_into_netem(self):
        """delay + a bandwidth cap share one netem qdisc (a separate tbf root would collide)."""
        assert init_container_delay(50, 0, 50).command == [
            "tc qdisc add dev eth0 root netem delay 50ms rate 50mbit"
        ]
        assert init_container_delay(100, 10).command == [
            "tc qdisc add dev eth0 root netem delay 100ms 10ms distribution normal"
        ]

    def test_with_bandwidth_limit_with_no_rates_returns_without_adding_container(self, mocker):
        """Should return early when neither ingress_rate nor egress_rate is provided."""
        builder = StatefulSetBuilder()
        mock_init_bandwidth = mocker.patch(
            "src.deployments.core.builders.init_container_bandwidth_limit"
        )

        result = builder.with_bandwidth_limit()

        assert isinstance(result, StatefulSetBuilder)
        assert (
            builder.config.stateful_set_spec.pod_template_spec_config.pod_spec_config.init_containers
            is None
        )
        mock_init_bandwidth.assert_not_called()

    def test_build_with_no_namespace_raises_exception(self):
        """Should raise ValueError if namespace is not set."""
        builder = StatefulSetBuilder()
        with pytest.raises(ValueError) as exc_info:
            builder.build()
        assert "You must set the namespace before building the StatefulSet." in str(exc_info.value)


# --------------------------------------------------------------------------- #
# PodBuilder Tests
# --------------------------------------------------------------------------- #
class TestPodBuilder:
    """Tests for the PodBuilder class."""

    def test_build_with_no_config_returns_valid_pod(self):
        """Should build a valid V1Pod with default values."""
        builder = PodBuilder()
        result = builder.build()
        expected_pod = _create_pod_with_default_values()
        assert isinstance(result, V1Pod)
        assert result == expected_pod

    def test_with_allowed_machines_sets_in_node_affinity(self):
        """Should restrict a pod to listed machines."""
        builder = PodBuilder()

        result = builder.with_allowed_machines(["node-01", "node-02"])
        pod = builder.build()
        expression = _required_node_affinity_expression(pod.spec)

        assert result is builder
        assert expression.key == "kubernetes.io/hostname"
        assert expression.operator == "In"
        assert expression.values == ["node-01", "node-02"]

    def test_dependency_reconciles_on_field_change(self, mocker):
        class ChildBuilder(PodBuilder):
            @depends_on("name")
            def _touch(self):
                pass

        child_builder = ChildBuilder()
        touch_func = mocker.patch.object(ChildBuilder, "_touch", autospec=True)
        child_builder.with_name("x")
        touch_func.assert_called_once_with(child_builder)


# --------------------------------------------------------------------------- #
# ServiceBuilder Tests
# --------------------------------------------------------------------------- #
class TestServiceBuilder:
    """Tests for the ServiceBuilder class."""

    def test_build_with_no_config_returns_valid_service(self):
        """Should build a valid V1Service with default values."""
        builder = ServiceBuilder()
        result = builder.build()
        expected_service = _create_service_with_default_values()
        assert isinstance(result, V1Service)
        assert result == expected_service


# --------------------------------------------------------------------------- #
# ContainerBuilder Tests
# --------------------------------------------------------------------------- #
class TestContainerBuilder:
    """Tests for the ContainerBuilder class."""

    def test_with_command_script_adds_command_lines_to_config(self):
        """Should add script lines to command config."""
        config = ContainerConfig(name="test", image=_create_image(), image_pull_policy="Always")
        builder = ContainerBuilder(config)
        script = ["echo 'Hello'", "ls -la", "pwd"]
        builder.with_command_script(script)
        # Commands are inserted via insert_command method
        assert len(builder.config.command_config.commands) >= 3
        assert builder.config.command_config.commands[0].command == "echo 'Hello'"
        assert builder.config.command_config.commands[1].command == "ls -la"
        assert builder.config.command_config.commands[2].command == "pwd"


# --------------------------------------------------------------------------- #
# PodSpecBuilder Tests
# --------------------------------------------------------------------------- #
class TestPodSpecBuilder:
    """Tests for the PodSpecBuilder class."""

    def test_add_container_supports_dict_and_v1container(self):
        """Should support adding containers as dict or V1Container."""
        builder = PodSpecBuilder()
        container_dict = {
            "name": "dict-container",
            "image": "busybox:latest",
            "imagePullPolicy": "IfNotPresent",
        }
        v1_container = V1Container(
            name="v1-container", image="nginx:latest", image_pull_policy="Always"
        )
        builder.add_container(container_dict)
        builder.add_container(v1_container)
        assert len(builder.config.container_configs) == 2
        assert builder.config.container_configs[0].name == "dict-container"
        assert builder.config.container_configs[1].name == "v1-container"

    def test_with_avoided_machines_sets_not_in_node_affinity(self):
        """Should prevent pods from scheduling on listed machines."""
        builder = PodSpecBuilder()

        result = builder.with_avoided_machines("node-05")
        pod_spec = builder.build()
        expression = _required_node_affinity_expression(pod_spec)

        assert result is builder
        assert expression.key == "kubernetes.io/hostname"
        assert expression.operator == "NotIn"
        assert expression.values == ["node-05"]


# --------------------------------------------------------------------------- #
# ContainerCommandBuilder Tests
# --------------------------------------------------------------------------- #
class TestContainerCommandBuilder:
    """Tests for the ContainerCommandBuilder class."""

    def test_add_line_with_none_args_converts_adds_empty_args(self):
        """Should handle None args by converting to empty list."""
        builder = ContainerCommandBuilder()
        builder.add_line("pwd", None)

        assert len(builder.config.commands) == 1
        assert builder.config.commands[0].command == "pwd"
        assert builder.config.commands[0].args == []

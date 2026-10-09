from unittest.mock import MagicMock

import pytest
from kubernetes.client.rest import ApiException

from src.deployments.core import k8s_cleanup
from src.deployments.core.k8s_cleanup import (
    cleanup_resources,
    poll_cleanup_status,
    poll_namespace_has_objects,
    wait_for_no_objs_in_namespace,
)


def _list_result(items):
    r = MagicMock()
    r.items = items
    return r


# --------------------------------------------------------------------------- #
# poll_namespace_has_objects  (lists resources via k8s client)
# --------------------------------------------------------------------------- #
class TestPollNamespaceHasObjects:
    def _patch_apis(self, mocker, *, core=None, apps=None, batch=None):
        core = core or MagicMock()
        apps = apps or MagicMock()
        batch = batch or MagicMock()
        mocker.patch.object(k8s_cleanup.client, "CoreV1Api", return_value=core)
        mocker.patch.object(k8s_cleanup.client, "AppsV1Api", return_value=apps)
        mocker.patch.object(k8s_cleanup.client, "BatchV1Api", return_value=batch)
        return core, apps, batch

    def test_true_when_a_statefulset_exists(self, mocker):
        core, apps, _ = self._patch_apis(mocker)
        core.list_namespaced_pod.return_value = _list_result([])
        apps.list_namespaced_deployment.return_value = _list_result([])
        apps.list_namespaced_stateful_set.return_value = _list_result([object()])
        apps.list_namespaced_daemon_set.return_value = _list_result([])
        apps.list_namespaced_replica_set.return_value = _list_result([])

        assert poll_namespace_has_objects("ns", MagicMock()) is True

    def test_false_when_namespace_empty(self, mocker):
        core, apps, _ = self._patch_apis(mocker)
        core.list_namespaced_pod.return_value = _list_result([])
        apps.list_namespaced_deployment.return_value = _list_result([])
        apps.list_namespaced_stateful_set.return_value = _list_result([])
        apps.list_namespaced_daemon_set.return_value = _list_result([])
        apps.list_namespaced_replica_set.return_value = _list_result([])

        result = poll_namespace_has_objects(
            "ns",
            MagicMock(),
            types=["Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Pod"],
        )
        assert result is False

    @pytest.mark.parametrize(
        "type_name, group, method",
        [
            ("Pod", "core", "list_namespaced_pod"),
            ("Service", "core", "list_namespaced_service"),
            ("PersistentVolumeClaim", "core", "list_namespaced_persistent_volume_claim"),
            ("ReplicationController", "core", "list_namespaced_replication_controller"),
            ("Deployment", "apps", "list_namespaced_deployment"),
            ("StatefulSet", "apps", "list_namespaced_stateful_set"),
            ("ReplicaSet", "apps", "list_namespaced_replica_set"),
            ("DaemonSet", "apps", "list_namespaced_daemon_set"),
            ("Job", "batch", "list_namespaced_job"),
            ("CronJob", "batch", "list_namespaced_cron_job"),
        ],
    )
    def test_api_exception_is_swallowed(self, mocker, type_name, group, method):
        mocks = dict(zip(("core", "apps", "batch"), self._patch_apis(mocker)))
        getattr(mocks[group], method).side_effect = ApiException()
        assert poll_namespace_has_objects("ns", MagicMock(), types=[type_name]) is False

    def test_core_api_is_constructed_with_the_passed_client(self, mocker):
        core = MagicMock()
        core.list_namespaced_pod.return_value = _list_result([])
        core_api = mocker.patch.object(k8s_cleanup.client, "CoreV1Api", return_value=core)
        mocker.patch.object(k8s_cleanup.client, "AppsV1Api")
        mocker.patch.object(k8s_cleanup.client, "BatchV1Api")

        api_client = MagicMock()
        poll_namespace_has_objects("ns", api_client, types=["Pod"])

        core_api.assert_called_once_with(api_client)


# --------------------------------------------------------------------------- #
# wait_for_no_objs_in_namespace  (loop around poll_namespace_has_objects)
# --------------------------------------------------------------------------- #
class TestWaitForNoObjsInNamespace:
    def test_returns_when_namespace_clears(self, mocker):
        poll = mocker.patch.object(
            k8s_cleanup, "poll_namespace_has_objects", side_effect=[True, False]
        )
        sleep = mocker.patch.object(k8s_cleanup.time, "sleep")

        wait_for_no_objs_in_namespace("ns", api_client=MagicMock())

        assert poll.call_count == 2
        sleep.assert_called_once()

    def test_timeout_raises(self, mocker):
        mocker.patch.object(k8s_cleanup, "poll_namespace_has_objects", return_value=True)
        mocker.patch.object(k8s_cleanup.time, "sleep")

        with pytest.raises(TimeoutError):
            wait_for_no_objs_in_namespace("ns", timeout=-1, api_client=MagicMock())


# --------------------------------------------------------------------------- #
# poll_cleanup_status  (reads named resources via k8s client)
# --------------------------------------------------------------------------- #
class TestPollCleanupStatus:
    def test_false_while_a_statefulset_still_exists(self, mocker):
        apps = MagicMock()
        mocker.patch.object(k8s_cleanup.client, "AppsV1Api", return_value=apps)

        assert poll_cleanup_status({"StatefulSet": ["nodes-0"]}, "ns", MagicMock()) is False
        apps.read_namespaced_stateful_set.assert_called_once_with("nodes-0", "ns")

    def test_cronjob_is_read_via_batch_v1(self, mocker):
        batch = MagicMock()
        mocker.patch.object(k8s_cleanup.client, "BatchV1Api", return_value=batch)

        assert poll_cleanup_status({"CronJob": ["c0"]}, "ns", MagicMock()) is False
        batch.read_namespaced_cron_job.assert_called_once_with("c0", "ns")


# --------------------------------------------------------------------------- #
# cleanup_resources  (deletes resources via k8s client)
# --------------------------------------------------------------------------- #
class TestCleanupResources:
    def test_deletes_each_named_statefulset(self, mocker):
        apps = MagicMock()
        mocker.patch.object(k8s_cleanup.client, "AppsV1Api", return_value=apps)

        cleanup_resources({"StatefulSet": ["nodes-0", "nodes-1"]}, "ns", api_client=MagicMock())

        apps.delete_namespaced_stateful_set.assert_any_call("nodes-0", "ns")
        apps.delete_namespaced_stateful_set.assert_any_call("nodes-1", "ns")
        assert apps.delete_namespaced_stateful_set.call_count == 2

    def test_404_is_swallowed(self, mocker):
        core = MagicMock()
        core.delete_namespaced_pod.side_effect = ApiException(status=404)
        mocker.patch.object(k8s_cleanup.client, "CoreV1Api", return_value=core)

        cleanup_resources({"Pod": ["p0"]}, "ns", api_client=MagicMock())  # must not raise

        core.delete_namespaced_pod.assert_called_once_with("p0", "ns")

    def test_cronjob_is_deleted_via_batch_v1(self, mocker):
        batch = MagicMock()
        mocker.patch.object(k8s_cleanup.client, "BatchV1Api", return_value=batch)

        cleanup_resources({"CronJob": ["c0"]}, "ns", api_client=MagicMock())

        batch.delete_namespaced_cron_job.assert_called_once_with("c0", "ns")

    def test_controllers_deleted_before_pods(self, mocker):
        manager = MagicMock()
        apps = MagicMock()
        core = MagicMock()
        manager.attach_mock(apps, "apps")
        manager.attach_mock(core, "core")
        mocker.patch.object(k8s_cleanup.client, "AppsV1Api", return_value=apps)
        mocker.patch.object(k8s_cleanup.client, "CoreV1Api", return_value=core)

        cleanup_resources({"Pod": ["p0"], "StatefulSet": ["s0"]}, "ns", api_client=MagicMock())

        names = [c[0] for c in manager.mock_calls]
        assert names.index("apps.delete_namespaced_stateful_set") < names.index(
            "core.delete_namespaced_pod"
        )

    def test_unsupported_or_missing_kinds_are_ignored(self, mocker):
        apps_api = mocker.patch.object(k8s_cleanup.client, "AppsV1Api")
        core_api = mocker.patch.object(k8s_cleanup.client, "CoreV1Api")

        cleanup_resources({"Secret": ["s0"]}, "ns", api_client=MagicMock())  # must not raise

        apps_api.assert_not_called()
        core_api.assert_not_called()

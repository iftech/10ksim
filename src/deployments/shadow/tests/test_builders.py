import io

import pytest
from ruamel.yaml import YAML

from src.deployments.shadow.builders import (
    _bootstrap_host,
    _peer_env,
    _peer_hosts,
    _peer_ip,
    _publisher_host,
    build_configmap,
    render_publisher_config,
    render_shadow_yaml,
)


def _load_yaml(text: str):
    return YAML(typ="safe").load(io.StringIO(text))


# --------------------------------------------------------------------------- #
# render_publisher_config  (pod-api-requester batch config, reproduces traffic_sync)
# --------------------------------------------------------------------------- #
class TestRenderPublisherConfig:

    def test_request_and_action_cross_references_resolve(self):
        cfg = render_publisher_config(
            num_nodes=2, num_messages=1, msg_size_bytes=10, delay_seconds=0
        )
        # the action references the request by name, the request references the endpoint
        assert cfg["requests"][0]["endpoint"] == cfg["endpoints"][0]["name"]
        assert cfg["actions"][0]["requests"] == [cfg["requests"][0]["name"]]
        assert cfg["actions"][0]["targets"] == [cfg["targets"][0]["name"]]


# --------------------------------------------------------------------------- #
# host builders (the pieces render_shadow_yaml assembles)
# --------------------------------------------------------------------------- #
class TestHostBuilders:
    def _env(self, **over):
        base = dict(
            num_nodes=5,
            connect_to=2,
            muxer="yamux",
            discovery="static",
            start_sleep=60,
            metrics_interval_s=15,
            lsquic_tick_floor_us=0,
        )
        base.update(over)
        return _peer_env(**base)

    def test_peer_env_static_omits_kad_and_tickfloor(self):
        env = self._env()
        assert env["MUXER"] == "yamux" and env["DISCOVERY"] == "static"
        assert "NODE_ROLE" not in env and "SERVICE" not in env
        assert "LSQUIC_TICK_FLOOR_US" not in env

    def test_peer_env_kad_adds_role_and_service(self):
        env = self._env(discovery="kad-dht")
        assert env["NODE_ROLE"] == "RoleNormal"
        assert env["SERVICE"] == "bootstrap-0"

    def test_peer_env_tickfloor_only_when_set(self):
        assert (
            _peer_env(
                num_nodes=5,
                connect_to=2,
                muxer="quic",
                discovery="static",
                start_sleep=60,
                metrics_interval_s=15,
                lsquic_tick_floor_us=10000,
            )["LSQUIC_TICK_FLOOR_US"]
            == "10000"
        )

    def test_peer_hosts_stagger_start_time_by_jitter(self):
        hosts = _peer_hosts(3, {"MUXER": "yamux"}, start_jitter_ms=50, hosts_per_subnet=1)
        assert [hosts[f"pod-{i}"]["processes"][0]["start_time"] for i in range(3)] == [
            "5000ms",
            "5050ms",
            "5100ms",
        ]
        assert all(hosts[h]["processes"][0]["expected_final_state"] == "running" for h in hosts)

    def test_peer_ip_gives_each_peer_its_own_subnet_by_default(self):
        ips = [_peer_ip(i, 1) for i in range(1000)]
        assert len(set(ips)) == 1000
        assert len({ip.rsplit(".", 1)[0] for ip in ips}) == 1000
        assert ips[0] == "11.100.0.10"
        assert ips[250] == "11.101.0.10"  # rolls into the next second octet

    def test_peer_ip_packs_subnets_when_asked(self):
        ips = [_peer_ip(i, 10) for i in range(1000)]
        assert len({ip.rsplit(".", 1)[0] for ip in ips}) == 100  # 10 hosts per /24
        assert ips[0] == "11.100.0.10" and ips[9] == "11.100.0.19"
        assert ips[10] == "11.100.1.10"

    def test_every_planned_octet_is_a_valid_address(self):
        for hosts_per_subnet in (1, 10, 245):
            for i in (0, 1, 999, 5000):
                octets = [int(o) for o in _peer_ip(i, hosts_per_subnet).split(".")]
                assert all(0 <= o <= 255 for o in octets), (i, hosts_per_subnet, octets)

    def test_packing_beyond_a_subnet_is_rejected_not_silently_wrong(self):
        # 250 would overflow the host octet to 11.100.0.256
        with pytest.raises(ValueError):
            _peer_ip(0, 250)
        with pytest.raises(ValueError):
            _peer_ip(0, 0)

    def test_peer_ips_avoid_shadow_assigned_range(self):
        # Shadow assigns unpinned hosts from 11.0.x
        assert all(not _peer_ip(i, 1).startswith("11.0.") for i in range(1000))

    def test_peer_hosts_pin_the_planned_addresses(self):
        hosts = _peer_hosts(3, {"MUXER": "yamux"}, start_jitter_ms=0, hosts_per_subnet=1)
        assert [hosts[f"pod-{i}"]["ip_addr"] for i in range(3)] == [
            _peer_ip(i, 1) for i in range(3)
        ]

    def test_bootstrap_host_lifts_connection_cap(self):
        h = _bootstrap_host(
            num_nodes=1000,
            muxer="yamux",
            start_sleep=60,
            metrics_interval_s=15,
            lsquic_tick_floor_us=0,
        )
        env = h["processes"][0]["environment"]
        assert env["NODE_ROLE"] == "RoleBootstrap"
        assert env["MAXCONNECTIONS"] == "1100"  # num_nodes + 100

    def test_publisher_host_runs_requester_batch(self):
        h = _publisher_host(90, "/app/api_requester.py")
        proc = h["processes"][0]
        assert proc["path"] == "/usr/bin/python3"
        assert "--mode batch" in proc["args"]
        assert proc["start_time"] == "90s"


# --------------------------------------------------------------------------- #
# render_shadow_yaml  (peer hosts + publisher host)
# --------------------------------------------------------------------------- #
class TestRenderShadowYaml:
    def test_peer_hosts_plus_publisher(self):
        sy = render_shadow_yaml(num_nodes=4, sim_stop_time_s=180, publisher_start_s=90)
        hosts = sy["hosts"]
        assert sorted(h for h in hosts if h.startswith("pod-")) == [f"pod-{i}" for i in range(4)]
        assert "publisher" in hosts
        assert len(hosts) == 5  # 4 peers + 1 publisher

    def test_publisher_runs_requester_in_batch_mode(self):
        sy = render_shadow_yaml(num_nodes=4, sim_stop_time_s=180, publisher_start_s=90)
        proc = sy["hosts"]["publisher"]["processes"][0]
        assert proc["path"] == "/usr/bin/python3"
        assert (
            proc["args"] == "/app/api_requester.py --mode batch --config /sim/config/publisher.yaml"
        )
        assert proc["start_time"] == "90s"
        assert "traffic_sync" not in proc["args"]  # the swap removed the legacy injector

    def test_requester_app_path_override(self):
        sy = render_shadow_yaml(
            num_nodes=4, sim_stop_time_s=180, publisher_start_s=90, requester_app_path="/opt/req.py"
        )
        assert sy["hosts"]["publisher"]["processes"][0]["args"].startswith(
            "/opt/req.py --mode batch"
        )

    def test_peer_process_is_daemon_with_env(self):
        sy = render_shadow_yaml(
            num_nodes=3,
            sim_stop_time_s=120,
            publisher_start_s=60,
            connect_to=2,
            metrics_interval_s=15,
        )
        peer = sy["hosts"]["pod-0"]["processes"][0]
        assert peer["path"] == "./main"
        assert peer["start_time"] == "5000ms"
        assert peer["expected_final_state"] == "running"
        env = peer["environment"]
        assert env["PEERS"] == "3"
        assert env["CONNECTTO"] == "2"
        assert env["SHADOWENV"] == "true"
        assert env["METRICS_INTERVAL_S"] == "15"

    def test_wan_network_uses_gml_with_link_properties(self):
        sy = render_shadow_yaml(
            num_nodes=3,
            sim_stop_time_s=180,
            publisher_start_s=90,
            latency_ms=10,
            bandwidth_mbit=50,
        )
        graph = sy["network"]["graph"]
        assert graph["type"] == "gml"
        gml = str(graph["inline"])
        assert 'latency "10 ms"' in gml
        assert 'host_bandwidth_up "50 Mbit"' in gml

    def _graph(self, **kwargs):
        return render_shadow_yaml(num_nodes=3, sim_stop_time_s=180, publisher_start_s=90, **kwargs)[
            "network"
        ]["graph"]

    def test_unset_network_keeps_the_builtin_switch(self):
        assert self._graph()["type"] == "1_gbit_switch"

    def test_bandwidth_above_the_switch_is_applied_not_ignored(self):
        gml = str(self._graph(bandwidth_mbit=2000)["inline"])
        assert 'host_bandwidth_up "2000 Mbit"' in gml

    def test_one_knob_set_keeps_the_switch_value_for_the_other(self):
        assert 'host_bandwidth_up "1000 Mbit"' in str(self._graph(latency_ms=50)["inline"])
        assert 'latency "0 ms"' in str(self._graph(bandwidth_mbit=50)["inline"])

    def test_explicit_zero_latency_still_builds_a_link(self):
        assert self._graph(latency_ms=0)["type"] == "gml"

    def test_out_of_range_values_are_rejected(self):
        with pytest.raises(ValueError):
            self._graph(latency_ms=-1)
        with pytest.raises(ValueError):
            self._graph(bandwidth_mbit=0)
        with pytest.raises(ValueError):
            self._graph(bandwidth_mbit=-10)

    def test_connect_to_must_be_less_than_num_nodes(self):
        with pytest.raises(ValueError):
            render_shadow_yaml(num_nodes=2, sim_stop_time_s=10, publisher_start_s=5, connect_to=2)


# --------------------------------------------------------------------------- #
# build_configmap  (ships shadow.yaml + publisher.yaml)
# --------------------------------------------------------------------------- #
class TestBuildConfigmap:
    def test_keys_metadata_and_yaml_roundtrip(self):
        sy = render_shadow_yaml(num_nodes=3, sim_stop_time_s=180, publisher_start_s=90)
        pub = render_publisher_config(
            num_nodes=3, num_messages=2, msg_size_bytes=1000, delay_seconds=2.0
        )
        cm = build_configmap(
            namespace="zerotesting-shadow", name="shadow-x", shadow_yaml=sy, publisher_config=pub
        )

        assert set(cm.data.keys()) == {"shadow.yaml", "publisher.yaml"}
        assert cm.metadata.name == "shadow-x"
        assert cm.metadata.namespace == "zerotesting-shadow"
        # the data values are valid YAML that round-trips back to the inputs
        assert _load_yaml(cm.data["publisher.yaml"])["targets"][0]["hosts"] == [
            f"pod-{i}" for i in range(3)
        ]
        assert "publisher" in _load_yaml(cm.data["shadow.yaml"])["hosts"]
        assert "traffic_sync.py" not in cm.data  # legacy injector no longer shipped

"""Exercise relay discovery with fake HTTP responses and the real shell/JSON tooling."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from src.deployments.logos_storage import mix

SCRIPTS = Path(mix.__file__).parent / "scripts"


@pytest.fixture
def relay_setup(tmp_path):
    if not shutil.which("bash") or not shutil.which("jq"):
        pytest.skip("Mix setup script checks require bash and jq")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    info_dir = tmp_path / "info"
    info_dir.mkdir()
    config_dir = tmp_path / "config"
    infos = []
    for index in range(4):
        info = {
            "table": {"localNode": {"peerId": f"peer-{index}"}},
            "addrs": [
                "/mix-transport/ignored",
                "/ip4/0.0.0.0/tcp/2345",
                "/ip4/127.0.0.1/tcp/2345",
                f"/ip4/10.0.0.{index + 10}/tcp/2345",
            ],
            "mixPubKey": f"{index + 1:064x}",
            "libp2pPubKey": "ab" * 32,
            "spr": f"spr-{index}",
        }
        infos.append(info)
        (info_dir / f"{index}.json").write_text(json.dumps(info))

    # Replace curl only; no network or cluster is used by these checks.
    curl = bin_dir / "curl"
    curl.write_text(
        "#!/bin/bash\n"
        'for url in "$@"; do :; done\n'
        'case "$url" in\n'
        "  http://relay-[0-3]:8080/api/storage/v1/debug/info)\n"
        "    index=${url#http://relay-}\n"
        "    index=${index%%:*}\n"
        '    cat "$MIX_TEST_INFO_DIR/$index.json" ;;\n'
        "  *) exit 1 ;;\n"
        "esac\n"
    )
    curl.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        "MIX_TEST_INFO_DIR": str(info_dir),
        "MIX_CONFIG_DIR": str(config_dir),
        "MIX_RELAYS": "relay-0 relay-1 relay-2 relay-3",
        "MIX_RELAY_COUNT": "4",
        "MIX_API_PORT": "8080",
        "MIX_P2P_PORT": "2345",
        "MIX_SETUP_TIMEOUT": "10",
    }
    return env, infos, info_dir, config_dir


def prepare(env):
    return subprocess.run(
        ["bash", str(SCRIPTS / "prepare_mix.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_discovery_writes_a_public_pool_and_all_proxy_sprs(relay_setup):
    env, infos, _, config_dir = relay_setup
    result = prepare(env)
    assert result.returncode == 0, result.stderr
    pool = json.loads((config_dir / "pool.json").read_text())
    assert pool == {
        "version": 1,
        "relays": [
            {
                "peerId": info["table"]["localNode"]["peerId"],
                "multiAddr": info["addrs"][-1],
                "mixPubKey": info["mixPubKey"],
                "libp2pPubKey": info["libp2pPubKey"],
            }
            for info in infos
        ],
    }
    assert (config_dir / "proxies.txt").read_text().splitlines() == [info["spr"] for info in infos]
    assert {path.name for path in config_dir.iterdir()} == {"pool.json", "proxies.txt"}


def test_duplicate_relay_identities_do_not_produce_a_pool(relay_setup):
    env, infos, info_dir, config_dir = relay_setup
    infos[1]["table"]["localNode"]["peerId"] = infos[0]["table"]["localNode"]["peerId"]
    (info_dir / "1.json").write_text(json.dumps(infos[1]))
    result = prepare(env)
    assert result.returncode != 0
    assert "distinct relay identities and addresses" in result.stderr
    assert not (config_dir / "pool.json").exists()


def test_missing_mix_identity_times_out_before_starting_storage(relay_setup):
    env, infos, info_dir, config_dir = relay_setup
    del infos[0]["mixPubKey"]
    (info_dir / "0.json").write_text(json.dumps(infos[0]))
    env["MIX_SETUP_TIMEOUT"] = "1"
    result = prepare(env)
    assert result.returncode != 0
    assert "timed out waiting for relay relay-0" in result.stderr
    assert not (config_dir / "pool.json").exists()


def test_storage_wrapper_preserves_arguments_and_adds_every_proxy(tmp_path):
    if not shutil.which("bash"):
        pytest.skip("Mix startup script check requires bash")
    (tmp_path / "pool.json").write_text('{"version": 1, "relays": []}')
    (tmp_path / "proxies.txt").write_text("spr-one\nspr-two\n")
    entrypoint = tmp_path / "entrypoint.sh"
    entrypoint.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
    entrypoint.chmod(0o755)
    args = ["storage", "--mix-enabled", "--mix-pool=/mix-config/pool.json"]
    result = subprocess.run(
        ["bash", str(SCRIPTS / "start_storage.sh"), str(entrypoint), *args],
        env={**os.environ, "MIX_CONFIG_DIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        *args,
        "--dht-mix-proxy=spr-one",
        "--dht-mix-proxy=spr-two",
    ]

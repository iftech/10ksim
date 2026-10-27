#!/bin/bash
set -euo pipefail

: "${MIX_RELAYS:?Mix relay DNS names are required}"
: "${MIX_RELAY_COUNT:?Mix relay count is required}"
: "${MIX_API_PORT:?Mix API port is required}"
: "${MIX_P2P_PORT:?Mix P2P port is required}"
: "${MIX_SETUP_TIMEOUT:?Mix setup timeout is required}"

config_dir=${MIX_CONFIG_DIR:-/mix-config}
mkdir -p "$config_dir"
workdir=$(mktemp -d "$config_dir/setup.XXXXXX")
trap 'rm -rf "$workdir"' EXIT
deadline=$((SECONDS + MIX_SETUP_TIMEOUT))

# Like the upstream NAT example, collect keys and SPRs after the relays start.
# Select a plain IPv4/TCP address; a Mix advertisement cannot be used as a relay endpoint.
for relay in $MIX_RELAYS; do
  while true; do
    remaining=$((deadline - SECONDS))
    if (( remaining <= 0 )); then
      echo "Mix setup timed out waiting for relay $relay" >&2
      exit 1
    fi
    request_timeout=$remaining
    if (( request_timeout > 5 )); then request_timeout=5; fi
    if curl --silent --show-error --fail --connect-timeout 2 --max-time "$request_timeout" \
      "http://$relay:$MIX_API_PORT/api/storage/v1/debug/info" > "$workdir/info.json"; then
      if jq --compact-output --exit-status --arg port "$MIX_P2P_PORT" '
        {
          peerId: (.table.localNode.peerId // .id),
          multiAddr: ([.addrs[]?
            | select(test(
              "^/ip4/([0-9]{1,3}\\.){3}[0-9]{1,3}/tcp/" + $port + "(/p2p/[^/]+)?$"
            ))
            | select(test("^/ip4/(0\\.0\\.0\\.0|127\\.)") | not)
          ] | first),
          mixPubKey: .mixPubKey,
          libp2pPubKey: .libp2pPubKey,
          spr: .spr
        }
        | select([.peerId, .multiAddr, .mixPubKey, .libp2pPubKey, .spr]
          | all(type == "string" and length > 0))
        | select(.mixPubKey | test("^[0-9a-fA-F]{64}$"))
        | select(.libp2pPubKey | test("^[0-9a-fA-F]+$"))
      ' "$workdir/info.json" > "$workdir/entry.json"; then
        cat "$workdir/entry.json" >> "$workdir/relays.jsonl"
        echo "Collected Mix relay $relay"
        break
      fi
    fi
    echo "Waiting for Mix identity and advertised address from $relay" >&2
    sleep 1
  done
done

jq --slurp '{version: 1, relays: map(del(.spr))}' "$workdir/relays.jsonl" > "$workdir/pool.json"
if ! jq --exit-status --argjson count "$MIX_RELAY_COUNT" '
  (.relays | length) == $count
  and ([.relays[].peerId] | unique | length) == $count
  and ([.relays[].multiAddr] | unique | length) == $count
' "$workdir/pool.json" > /dev/null; then
  echo "Mix pool must contain the expected number of distinct relay identities and addresses" >&2
  exit 1
fi
jq --raw-output '.spr' "$workdir/relays.jsonl" > "$workdir/proxies.txt"
mv "$workdir/pool.json" "$config_dir/pool.json"
mv "$workdir/proxies.txt" "$config_dir/proxies.txt"
echo "Mix pool ready: $MIX_RELAY_COUNT relays"

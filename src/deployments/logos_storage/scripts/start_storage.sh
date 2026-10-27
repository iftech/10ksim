#!/bin/bash
set -euo pipefail

# The first argument is the original image entrypoint, preserving its bootstrap setup.
entrypoint=$1
shift
config_dir=${MIX_CONFIG_DIR:-/mix-config}
test -s "$config_dir/pool.json"
test -s "$config_dir/proxies.txt"
while IFS= read -r spr; do
  if [[ -z "$spr" ]]; then
    echo "Mix proxy SPR cannot be empty" >&2
    exit 1
  fi
  set -- "$@" "--dht-mix-proxy=$spr"
done < "$config_dir/proxies.txt"
exec "$entrypoint" "$@"

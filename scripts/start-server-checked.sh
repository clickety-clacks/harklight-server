#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
release="$(readlink -f -- "$root/current")"
python3 "$root/bin/harklight-check-store" "$root/state/relay.sqlite3" "$release/BUILD_INFO"
exec "$root/bin/start-server-unchecked" "$@"

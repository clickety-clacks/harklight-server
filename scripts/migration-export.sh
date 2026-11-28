#!/usr/bin/env bash
set -euo pipefail
package_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
# This operator command must never start the Server or a listener.
export ATTENTION_SERVER_ENABLE=0
export HARKLIGHT_MIGRATION_ARGS
HARKLIGHT_MIGRATION_ARGS=$(printf '%s\0' "$@" | base64 -w0)
exec "$package_dir/bin/attention_server" eval \
  'Application.ensure_all_started(:crypto); args = System.fetch_env!("HARKLIGHT_MIGRATION_ARGS") |> Base.decode64!() |> String.split(<<0>>, trim: true); AttentionServer.Migration.CLI.main(args)'

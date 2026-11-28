#!/usr/bin/env bash
set -euo pipefail
package_dir=$(cd "${1:?package directory}" && pwd)
fixture_dir=${2:?absolute synthetic fixture directory}
mode=${3:-seed}
[[ $fixture_dir == /* ]]
[[ $mode == seed || $mode == verify || $mode == expire ]]
if [[ $mode == seed ]]; then
  [[ ! -e $fixture_dir ]] || { echo 'Seed requires a fresh directory.' >&2; exit 2; }
else
  [[ -f $fixture_dir/manifest.json ]] || { echo 'Synthetic manifest is missing.' >&2; exit 2; }
fi
export ATTENTION_SERVER_ENABLE=0 HARKLIGHT_FIXTURE_DIR="$fixture_dir" HARKLIGHT_FIXTURE_MODE="$mode"
export HARKLIGHT_FIXTURE_CODE="$package_dir/fixtures/compatibility"
exec "$package_dir/bin/attention_server" eval \
  'Code.require_file(Path.join(System.fetch_env!("HARKLIGHT_FIXTURE_CODE"), "run.exs"))'

#!/usr/bin/env bash
set -euo pipefail

results="$RUNNER_TEMP/legacy-results"
packages="$RUNNER_TEMP/legacy-packages"
mkdir -p "$packages"
release_base=https://github.com/clickety-clacks/harklight-server/releases/download
for version in 0.2.25 0.2.27; do
  name="harklight-server-$version-linux-x86_64"
  mkdir -p "$packages/$version"
  (
    cd "$packages/$version"
    curl -fsSL --retry 3 -O "$release_base/v$version/$name.tar.gz"
    curl -fsSL --retry 3 -O "$release_base/v$version/SHA256SUMS"
    sha256sum --check --ignore-missing SHA256SUMS
    tar -xzf "$name.tar.gz" --strip-components=1
  )
done
mkdir -p "$packages/0.2.28"
tar -xzf "$results/candidate-dist/harklight-server-0.2.28-linux-x86_64.tar.gz" \
  -C "$packages/0.2.28" --strip-components=1
grep -Fqx "source_commit=$(cat "$results/implementation.txt")" "$packages/0.2.28/BUILD_INFO"

isolated() {
  sudo systemd-run --quiet --wait --pipe --collect \
    --property=MemoryMax=4G --property=MemorySwapMax=0 --property=PrivateNetwork=yes \
    --uid="$(id -u)" --working-directory="$RUNNER_TEMP" /usr/bin/env \
    ATTENTION_SERVER_ENABLE=0 ERL_FLAGS='+S 2:2 +SDcpu 1 +SDio 1' \
    FIXTURE_DB="$FIXTURE_DB" FIXTURE_MARKER="$FIXTURE_MARKER" FIXTURE_ACTION="$FIXTURE_ACTION" \
    FIXTURE_SCRIPT="$RUNNER_TEMP/format31-legacy-fixture.exs" "$@"
}
digest() {
  sha256sum "$FIXTURE_DB" | cut -d' ' -f1
}

for FIXTURE_MARKER in missing present; do
  mkdir -p "$RUNNER_TEMP/legacy-$FIXTURE_MARKER"
  FIXTURE_DB="$RUNNER_TEMP/legacy-$FIXTURE_MARKER/relay.sqlite3"
  FIXTURE_ACTION=seed
  isolated "$packages/0.2.25/bin/attention_server" eval 'Code.require_file(System.fetch_env!("FIXTURE_SCRIPT"))'
  original="$(digest)"
  if isolated "$packages/0.2.27/bin/harklight" store convert-stopped "$FIXTURE_DB" --from-release v0.2.25 \
    > "$RUNNER_TEMP/legacy-baseline-$FIXTURE_MARKER.log" 2>&1; then
    echo "Expected published v0.2.27 conversion to fail"
    exit 1
  fi
  grep -q 'KeyError.*key :session_handle not found' "$RUNNER_TEMP/legacy-baseline-$FIXTURE_MARKER.log"
  if [[ "$FIXTURE_MARKER" == missing ]]; then
    grep -E 'AgentAPI.Advisories.for_message/3|Store.record_cutover_client_changes/3' "$RUNNER_TEMP/legacy-baseline-$FIXTURE_MARKER.log"
  else
    grep -F 'Store.DerivedIndex.item_fields/2' "$RUNNER_TEMP/legacy-baseline-$FIXTURE_MARKER.log"
  fi
  [[ "$(digest)" == "$original" ]]
  echo "Published v0.2.27 failed with KeyError :session_handle. FORMAT30 SHA-256 unchanged: $original"

  isolated "$packages/0.2.28/bin/harklight" store convert-stopped "$FIXTURE_DB" --from-release v0.2.25
  isolated "$packages/0.2.28/bin/harklight" store convert-stopped "$FIXTURE_DB" --from-release v0.2.25
  FIXTURE_ACTION=verify
  isolated "$packages/0.2.28/bin/attention_server" eval 'Code.require_file(System.fetch_env!("FIXTURE_SCRIPT"))'
done

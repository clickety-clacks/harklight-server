#!/usr/bin/env bash
set -euo pipefail

source_commit="${1:?expected source commit}"
probe_script="${2:?fixture script path}"
prior_version="${3:?prior release version}"
prior_format="${4:?prior snapshot format}"
results_dir="${5:?results directory}"
candidate_version=0.2.21
candidate_name="harklight-server-$candidate_version-linux-x86_64"
prior_name="harklight-server-$prior_version-linux-x86_64"
run_dir="$(mktemp -d)"
addressed_root="$HOME/.local/share/harklight-server"
[[ ! -e "$addressed_root" && ! -L "$addressed_root" ]]
mkdir -p "$addressed_root"/{bin,releases} "$run_dir"/{prior,candidate,bin,snapshot}
trap 'rm -rf -- "$addressed_root" "$run_dir"' EXIT

release_base=https://github.com/clickety-clacks/harklight-server/releases/download
(
  cd "$run_dir/prior"
  curl --fail --silent --show-error --location --retry 3 --remote-name "$release_base/v$prior_version/$prior_name.tar.gz"
  curl --fail --silent --show-error --location --retry 3 --remote-name "$release_base/v$prior_version/SHA256SUMS"
  sha256sum --check --ignore-missing SHA256SUMS
  tar -zxf "$prior_name.tar.gz"
)
mv "$run_dir/prior/$prior_name" "$addressed_root/releases/v$prior_version"
prior_package="$addressed_root/releases/v$prior_version"
candidate_dist="$results_dir/candidate-dist"
(
  cd "$candidate_dist"
  sha256sum --check SHA256SUMS
  tar -zxf "$candidate_name.tar.gz" -C "$run_dir/candidate"
)
candidate_package="$run_dir/candidate/$candidate_name"
grep -Fqx "source_commit=$source_commit" "$candidate_package/BUILD_INFO"
grep -Fqx 'source_status=' "$candidate_package/BUILD_INFO"

export HARKLIGHT_REHEARSAL_PROBE="$probe_script"
export HARKLIGHT_REHEARSAL_DB="$run_dir/snapshot/relay.sqlite3"
export HARKLIGHT_REHEARSAL_ACTION=seed HARKLIGHT_REHEARSAL_FORMAT="$prior_format"
export HARKLIGHT_REHEARSAL_ROOT="$addressed_root"
export HARKLIGHT_REHEARSAL_ACTIVE="$run_dir/active"
export HARKLIGHT_REHEARSAL_CANDIDATE="v$candidate_version"
export HARKLIGHT_REHEARSAL_PRIOR="v$prior_version"
export HARKLIGHT_REHEARSAL_PRIOR_FORMAT="$prior_format"
export HARKLIGHT_REHEARSAL_REAL_READLINK="$(command -v readlink)"
"$prior_package/bin/attention_server" eval 'Code.require_file(System.fetch_env!("HARKLIGHT_REHEARSAL_PROBE"))'

if [[ "$prior_version" == "$candidate_version" ]]; then
  "$candidate_package/bin/harklight" store convert-stopped "$HARKLIGHT_REHEARSAL_DB" --from-release "v$prior_version"
  HARKLIGHT_REHEARSAL_FORMAT=31 HARKLIGHT_REHEARSAL_ACTION=read \
    "$candidate_package/bin/attention_server" eval 'Code.require_file(System.fetch_env!("HARKLIGHT_REHEARSAL_PROBE"))'
  printf 'PASS: published v%s is accepted as FORMAT30 input.\n' "$prior_version"
  exit 0
fi

printf 'synthetic fixture\n' > "$run_dir/snapshot/server.env"
printf 'synthetic private state\n' > "$run_dir/snapshot/fixture-marker"
printf '#!/bin/sh\nexit 0\n' > "$addressed_root/bin/start-server"
chmod 755 "$addressed_root/bin/start-server"
ln -s "releases/v$prior_version" "$addressed_root/current"
digest="$(sha256sum "$run_dir/snapshot/relay.sqlite3" | cut -d ' ' -f 1)"
cp -a "$run_dir/snapshot" "$addressed_root/state"
export HARKLIGHT_REHEARSAL_DB="$addressed_root/state/relay.sqlite3"

cat > "$run_dir/bin/systemctl" <<'SHIM'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == --user ]]
shift
active="$HARKLIGHT_REHEARSAL_ACTIVE"
case "$1" in
  is-active) [[ "$(<"$active")" == active ]] ;;
  show)
    case "$2" in
      --property=MainPID)
        if [[ "$(<"$active")" == active ]]; then echo 4321; else echo 0; fi ;;
      --property=ActiveState) cat "$active" ;;
      --property=ActiveState,SubState,MainPID)
        if [[ "$(<"$active")" == active ]]; then echo 'active running 4321'; else echo 'inactive dead 0'; fi ;;
      *) exit 90 ;;
    esac ;;
  stop) printf 'inactive\n' > "$active" ;;
  start)
    release="$("$HARKLIGHT_REHEARSAL_REAL_READLINK" -f "$HARKLIGHT_REHEARSAL_ROOT/current")"
    selected="${release##*/}"
    export HARKLIGHT_REHEARSAL_ACTION=read
    if [[ "$selected" == "$HARKLIGHT_REHEARSAL_CANDIDATE" ]]; then
      export HARKLIGHT_REHEARSAL_FORMAT=31
    else
      [[ "$selected" == "$HARKLIGHT_REHEARSAL_PRIOR" ]]
      export HARKLIGHT_REHEARSAL_FORMAT="$HARKLIGHT_REHEARSAL_PRIOR_FORMAT"
    fi
    "$release/bin/attention_server" eval 'Code.require_file(System.fetch_env!("HARKLIGHT_REHEARSAL_PROBE"))'
    if [[ "$selected" == "$HARKLIGHT_REHEARSAL_CANDIDATE" && "$HARKLIGHT_REHEARSAL_FAIL_READY" == 1 ]]; then
      printf 'candidate data\n' > "$HARKLIGHT_REHEARSAL_ROOT/state/candidate-marker"
      printf 'inactive\n' > "$active"
    else
      printf 'active\n' > "$active"
    fi ;;
  *) exit 91 ;;
esac
SHIM
cat > "$run_dir/bin/readlink" <<'SHIM'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${*: -1}" =~ ^/proc/[0-9]+/exe$ ]]; then
  active="$("$HARKLIGHT_REHEARSAL_REAL_READLINK" -f "$HARKLIGHT_REHEARSAL_ROOT/current")"
  printf '%s\n' "$active"/erts-*/bin/beam.smp
else
  exec "$HARKLIGHT_REHEARSAL_REAL_READLINK" "$@"
fi
SHIM
printf '#!/bin/sh\nexit 0\n' > "$run_dir/bin/sleep"
chmod 755 "$run_dir/bin/"*

run_install() {
  export HARKLIGHT_REHEARSAL_FAIL_READY="$1"
  printf 'active\n' > "$HARKLIGHT_REHEARSAL_ACTIVE"
  PATH="$run_dir/bin:$PATH" \
    HARKLIGHT_RELEASE_TAG="v$candidate_version" \
    HARKLIGHT_SOURCE_COMMIT="$source_commit" \
    HARKLIGHT_ARCHIVE_SHA256="$(cut -d ' ' -f 1 "$candidate_dist/SHA256SUMS")" \
    HARKLIGHT_ARCHIVE_PATH="$candidate_dist/$candidate_name.tar.gz" \
    HARKLIGHT_CHECKSUMS_PATH="$candidate_dist/SHA256SUMS" \
    HARKLIGHT_GUIDE_PATH="$candidate_package/SELF_HOST.md" \
    "$candidate_package/bin/harklight-release-install"
}

if run_install 1 > "$run_dir/failed-readiness.log" 2>&1; then
  printf 'Forced candidate readiness failure did not fail the installer\n' >&2
  exit 1
fi
cat "$run_dir/failed-readiness.log"
grep -q 'format=31 synthetic message, device and grant verified' "$run_dir/failed-readiness.log"
grep -q "format=$prior_format synthetic message, device and grant verified" "$run_dir/failed-readiness.log"
grep -q 'restored the pre-install mutable root' "$run_dir/failed-readiness.log"
[[ "$(readlink "$addressed_root/current")" == "releases/v$prior_version" ]]
[[ "$(sha256sum "$HARKLIGHT_REHEARSAL_DB" | cut -d ' ' -f 1)" == "$digest" ]]
[[ ! -e "$addressed_root/state/candidate-marker" ]]
printf 'PASS: FORMAT%s restore returned original database bytes and prior-reader access.\n' "$prior_format"

run_install 0 > "$run_dir/success.log" 2>&1
cat "$run_dir/success.log"
grep -q 'format=31 synthetic message, device and grant verified' "$run_dir/success.log"
grep -q 'is running under the selected package' "$run_dir/success.log"
[[ "$(readlink "$addressed_root/current")" == "releases/v$candidate_version" ]]
[[ -f "$addressed_root/state/fixture-marker" ]]
printf 'PASS: FORMAT%s packaged install selected the candidate and retained state.\n' "$prior_format"

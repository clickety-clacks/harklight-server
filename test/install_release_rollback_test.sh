#!/usr/bin/env bash
set -euo pipefail

installer="${1:?pass the install-release.sh path}"
mode="${2:-upgrade-failure}"
[[ -f "$installer" ]] || { printf 'installer is missing\n' >&2; exit 1; }
[[ "$mode" == upgrade-failure || "$mode" == optional-route ]] || {
  printf 'unknown test mode: %s\n' "$mode" >&2
  exit 1
}
source_store="${3:?pass a real pre-migration relay store}"
expected_store_sha="${4:?pass the pre-migration relay store SHA-256}"
app_ebin="${5:?pass the compiled application ebin directory}"
erl_libs="${6:?pass the compiled dependency library directory}"
migrator="${7:?pass the isolated store migration probe}"
[[ -f "$source_store" && "$expected_store_sha" =~ ^[0-9a-f]{64}$ &&
   -d "$app_ebin" && -d "$erl_libs" && -f "$migrator" ]] || {
  printf 'pre-migration store or compiled migration support is unavailable\n' >&2
  exit 1
}

scratch="$(mktemp -d)"
cleanup() { rm -rf -- "$scratch"; }
trap cleanup EXIT

fake_bin="$scratch/fake-bin"
home="$scratch/home"
runner_temp="$scratch/runner-temp"
root="$home/.local/share/harklight-server"
release_old="$root/releases/v0.1.10"
package_root="$scratch/installer-package"
archive_stage="$scratch/archive-stage/harklight-server-0.1.12-linux-x86_64"
archive="$scratch/harklight-server-0.1.12-linux-x86_64.tar.gz"
sums="$scratch/SHA256SUMS"
guide="$scratch/SELF_HOST.md"
mkdir -p "$fake_bin" "$runner_temp" "$root/state/tls" "$root/state/owner" \
  "$release_old/erts-15.2.7/bin" "$release_old/bin" \
  "$package_root/bin" "$archive_stage/erts-15.2.7/bin" "$archive_stage/bin"

cp "$source_store" "$root/state/relay.sqlite3"
printf 'ATTENTION_SERVER_JOIN_NAME=rollback-test\nATTENTION_SERVER_OWNER_PORT=12345\n' > "$root/state/server.env"
printf 'fixture\n' > "$root/state/tls/ca.pem"
printf 'fixture\n' > "$root/state/owner/identity.pem"
printf 'fixture\n' > "$root/state/owner/identity.key"
printf 'person-test\n' > "$root/state/person-id"
printf 'launcher\n' > "$root/bin/start-server"
chmod 755 "$root/bin/start-server"

printf 'version=0.1.10\nsource_commit=%040d\nsource_status=\n' 0 > "$release_old/BUILD_INFO"
printf 'beam\n' > "$release_old/erts-15.2.7/bin/beam.smp"
chmod 755 "$release_old/erts-15.2.7/bin/beam.smp"
ln -s releases/v0.1.10 "$root/current"

cat > "$release_old/bin/harklight" <<'OLD_HARKLIGHT'
#!/usr/bin/env bash
set -euo pipefail
actual="$(sha256sum "$HARKLIGHT_TEST_ROOT/state/relay.sqlite3" | cut -d ' ' -f 1)"
[[ "$actual" == "$HARKLIGHT_TEST_EXPECTED_STORE_SHA" ]]
OLD_HARKLIGHT
chmod 755 "$release_old/bin/harklight"

cat > "$archive_stage/bin/harklight" <<'CANDIDATE_HARKLIGHT'
#!/usr/bin/env bash
set -euo pipefail
exit 9
CANDIDATE_HARKLIGHT
chmod 755 "$archive_stage/bin/harklight"
printf 'beam\n' > "$archive_stage/erts-15.2.7/bin/beam.smp"
chmod 755 "$archive_stage/erts-15.2.7/bin/beam.smp"
printf 'server\n' > "$archive_stage/bin/attention_server"
chmod 755 "$archive_stage/bin/attention_server"
printf 'version=0.1.12\nsource_commit=0123456789abcdef0123456789abcdef01234567\nsource_status=\n' \
  > "$archive_stage/BUILD_INFO"
printf 'Harklight Server release guide\n' > "$archive_stage/SELF_HOST.md"
cp "$archive_stage/SELF_HOST.md" "$guide"
cp "$guide" "$package_root/SELF_HOST.md"
cp "$archive_stage/BUILD_INFO" "$package_root/BUILD_INFO"
cp "$installer" "$package_root/bin/harklight-release-install"
chmod 755 "$package_root/bin/harklight-release-install"

mkdir -p "$scratch/archive-stage"
tar -czf "$archive" -C "$scratch/archive-stage" harklight-server-0.1.12-linux-x86_64
archive_sha256="$(sha256sum "$archive" | cut -d ' ' -f 1)"
printf '%s  %s\n' "$archive_sha256" "$(basename "$archive")" > "$sums"

printf 'active\n' > "$scratch/service-state"
: > "$scratch/systemctl.log"
real_readlink="$(command -v readlink)"

cat > "$fake_bin/systemctl" <<'FAKE_SYSTEMCTL'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == --user ]] || exit 90
shift
action="${1:-}"
shift || true
printf '%s|%s|%s\n' "$action" "$HOME" "$RUNNER_TEMP" >> "$HARKLIGHT_TEST_LOG"
status="$(< "$HARKLIGHT_TEST_STATUS")"

case "$action" in
  is-active)
    [[ "$status" == active ]] || exit 3
    exit 0
    ;;
  show)
    property="${1#--property=}"
    case "$property" in
      MainPID) [[ "$status" == active ]] && printf '4321\n' || printf '0\n' ;;
      ActiveState) [[ "$status" == active ]] && printf 'active\n' || printf 'inactive\n' ;;
      ActiveState,SubState,MainPID)
        [[ "$status" == active ]] && printf 'active running 4321\n' || printf 'inactive dead 0\n'
        ;;
      *) exit 91 ;;
    esac
    ;;
  stop)
    printf 'inactive\n' > "$HARKLIGHT_TEST_STATUS"
    ;;
  start)
    current="$("$REAL_READLINK" -f -- "$HARKLIGHT_TEST_ROOT/current")"
    case "$current" in
      */releases/v0.1.12)
        shopt -s nullglob
        backups=("$HARKLIGHT_TEST_ROOT"/backups/*-v0.1.10-to-v0.1.12-*)
        shopt -u nullglob
        [[ ${#backups[@]} == 1 && -f "${backups[0]}/private-root.tar" ]] || {
          printf 'candidate started before its backup existed\n' >&2
          exit 92
        }
        # Exercise the real store opener after the installer backup exists.
        ERL_LIBS="$HARKLIGHT_TEST_ERL_LIBS" elixir \
          -pa "$HARKLIGHT_TEST_APP_EBIN" "$HARKLIGHT_TEST_MIGRATOR"
        grep -qx 'format=23' "$HARKLIGHT_TEST_MIGRATION_LOG"
        printf 'candidate-only\n' > "$HARKLIGHT_TEST_ROOT/state/candidate-write.txt"
        printf 'start:v0.1.12\n' >> "$HARKLIGHT_TEST_ACTIONS"
        if [[ "$HARKLIGHT_TEST_MODE" == upgrade-failure ]]; then
          printf 'inactive\n' > "$HARKLIGHT_TEST_STATUS"
          exit 0
        fi
        ;;
      */releases/v0.1.10)
        actual="$(sha256sum "$HARKLIGHT_TEST_ROOT/state/relay.sqlite3" | cut -d ' ' -f 1)"
        [[ "$actual" == "$HARKLIGHT_TEST_EXPECTED_STORE_SHA" ]] || {
          printf 'prior release started against a non-restored store\n' >&2
          exit 93
        }
        printf 'start:v0.1.10\n' >> "$HARKLIGHT_TEST_ACTIONS"
        ;;
      *)
        printf 'unexpected selected release: %s\n' "$current" >&2
        exit 94
        ;;
    esac
    printf 'active\n' > "$HARKLIGHT_TEST_STATUS"
    ;;
  *) exit 95 ;;
esac
FAKE_SYSTEMCTL
chmod 755 "$fake_bin/systemctl"

cat > "$fake_bin/readlink" <<'FAKE_READLINK'
#!/usr/bin/env bash
set -euo pipefail
last=""
for argument in "$@"; do last="$argument"; done
if [[ "$last" =~ ^/proc/[0-9]+/exe$ ]]; then
  current="$("$REAL_READLINK" -f -- "$HARKLIGHT_TEST_ROOT/current")"
  printf '%s/erts-15.2.7/bin/beam.smp\n' "$current"
else
  exec "$REAL_READLINK" "$@"
fi
FAKE_READLINK
chmod 755 "$fake_bin/readlink"

cat > "$fake_bin/sleep" <<'FAKE_SLEEP'
#!/usr/bin/env bash
exit 0
FAKE_SLEEP
chmod 755 "$fake_bin/sleep"

export HOME="$home"
export RUNNER_TEMP="$runner_temp"
export PATH="$fake_bin:$PATH"
export HARKLIGHT_TEST_ROOT="$root"
export HARKLIGHT_TEST_STATUS="$scratch/service-state"
export HARKLIGHT_TEST_LOG="$scratch/systemctl.log"
export HARKLIGHT_TEST_ACTIONS="$scratch/systemctl-actions.log"
export HARKLIGHT_TEST_MODE="$mode"
export HARKLIGHT_TEST_SOURCE_STORE="$source_store"
export HARKLIGHT_TEST_EXPECTED_STORE_SHA="$expected_store_sha"
export HARKLIGHT_TEST_APP_EBIN="$app_ebin"
export HARKLIGHT_TEST_ERL_LIBS="$erl_libs"
export HARKLIGHT_TEST_MIGRATOR="$migrator"
export HARKLIGHT_TEST_MIGRATION_LOG="$scratch/candidate-migration.log"
export REAL_READLINK="$real_readlink"
: > "$HARKLIGHT_TEST_ACTIONS"

set +e
output="$(
  HARKLIGHT_RELEASE_TAG=v0.1.12 \
  HARKLIGHT_SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567 \
  HARKLIGHT_ARCHIVE_SHA256="$archive_sha256" \
  HARKLIGHT_ARCHIVE_PATH="$archive" \
  HARKLIGHT_CHECKSUMS_PATH="$sums" \
  HARKLIGHT_GUIDE_PATH="$guide" \
    "$package_root/bin/harklight-release-install" 2>&1
)"
status=$?
set -e

if [[ "$mode" == upgrade-failure ]]; then
  [[ "$status" == 1 ]] || { printf 'expected installer exit 1, got %s\n%s\n' "$status" "$output" >&2; exit 1; }
  [[ "$output" == *'upgrade failed: candidate process did not become active under the selected package within 30 seconds'* ]] || {
    printf 'missing failed-upgrade report\n%s\n' "$output" >&2; exit 1;
  }
  [[ "$output" == *'restored the pre-install mutable root'* && "$output" == *'prior release v0.1.10, which is serving'* ]] || {
    printf 'missing restored prior-release report\n%s\n' "$output" >&2; exit 1;
  }
  [[ "$output" == *'candidate writes since the backup may have been lost'* ]] || {
    printf 'missing candidate-write loss disclosure\n%s\n' "$output" >&2; exit 1;
  }
  [[ "$("$real_readlink" -f -- "$root/current")" == "$release_old" ]]
  [[ "$(< "$scratch/service-state")" == active ]]
  restored_store_sha="$(sha256sum "$root/state/relay.sqlite3" | cut -d ' ' -f 1)"
  [[ "$restored_store_sha" == "$expected_store_sha" ]]
  [[ ! -e "$root/state/relay.sqlite3.uploads" ]]
  [[ ! -e "$root/state/candidate-write.txt" ]]
  [[ -d "$release_old" && -d "$root/releases/v0.1.12" ]]

  shopt -s nullglob
  backups=("$root"/backups/*-v0.1.10-to-v0.1.12-*)
  shopt -u nullglob
  [[ ${#backups[@]} == 1 && -f "${backups[0]}/private-root.tar" ]]
  tar -tf "${backups[0]}/private-root.tar" | grep -Fqx './state/relay.sqlite3'
  backup_store_sha="$(tar -xOf "${backups[0]}/private-root.tar" ./state/relay.sqlite3 | sha256sum | cut -d ' ' -f 1)"
  [[ "$backup_store_sha" == "$expected_store_sha" ]]
  grep -Fqx 'format=23' "$HARKLIGHT_TEST_MIGRATION_LOG"
  grep -Fqx 'start:v0.1.12' "$HARKLIGHT_TEST_ACTIONS"
  grep -Fqx 'start:v0.1.10' "$HARKLIGHT_TEST_ACTIONS"
  grep -Fq "|$home|$runner_temp" "$HARKLIGHT_TEST_LOG"

  printf 'PASS: candidate migration failure restored the pre-install mutable root and serving prior release\n'
  printf 'PASS: previous release files and candidate package remain; the candidate is no longer selected\n'
  printf 'PASS: inherited HOME and RUNNER_TEMP reached the service control calls unchanged\n'
else
  [[ "$status" == 0 ]] || { printf 'expected optional-route warning to continue, got %s\n%s\n' "$status" "$output" >&2; exit 1; }
  [[ "$output" == *'Warning: the authenticated owner agent-list route is unavailable'* ]] || {
    printf 'missing optional-route warning\n%s\n' "$output" >&2; exit 1;
  }
  [[ "$output" == *'is running under the selected package'* ]]
  [[ "$("$real_readlink" -f -- "$root/current")" == "$root/releases/v0.1.12" ]]
  [[ "$(< "$scratch/service-state")" == active ]]
  grep -Fqx 'format=23' "$HARKLIGHT_TEST_MIGRATION_LOG"
  [[ -d "$root/state/relay.sqlite3.uploads" ]]
  migrated_store_sha="$(sha256sum "$root/state/relay.sqlite3" | cut -d ' ' -f 1)"
  [[ "$migrated_store_sha" != "$expected_store_sha" ]]
  [[ -f "$root/state/candidate-write.txt" ]]
  [[ -d "$release_old" && -d "$root/releases/v0.1.12" ]]
  grep -Fq 'start:v0.1.12' "$HARKLIGHT_TEST_ACTIONS"
  ! grep -Fq 'start:v0.1.10' "$HARKLIGHT_TEST_ACTIONS"
  grep -Fq "|$home|$runner_temp" "$HARKLIGHT_TEST_LOG"

  printf 'PASS: unavailable owner agent-list route is reported without blocking other server functionality\n'
  printf 'PASS: the candidate remains selected and its migrated state remains intact\n'
  printf 'PASS: inherited HOME and RUNNER_TEMP reached the service control calls unchanged\n'
fi

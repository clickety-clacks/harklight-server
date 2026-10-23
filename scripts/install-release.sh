#!/usr/bin/env bash
set -euo pipefail
umask 077

fail() {
  printf 'Harklight installation failed: %s\n' "$1" >&2
  exit 1
}

required=(HARKLIGHT_RELEASE_TAG HARKLIGHT_SOURCE_COMMIT HARKLIGHT_ARCHIVE_SHA256
  HARKLIGHT_ARCHIVE_PATH HARKLIGHT_CHECKSUMS_PATH HARKLIGHT_GUIDE_PATH)
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || fail "selected release metadata is incomplete"
done

if [[ ! "$HARKLIGHT_RELEASE_TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  fail "selected release tag is not stable vX.Y.Z"
fi
release_version="${HARKLIGHT_RELEASE_TAG#v}"
[[ "$HARKLIGHT_SOURCE_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]] || fail "selected source commit is invalid"
[[ "$HARKLIGHT_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]] || fail "selected archive digest is invalid"

package_root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
archive_name="harklight-server-$release_version-linux-x86_64.tar.gz"
archive="$HARKLIGHT_ARCHIVE_PATH"
sums="$HARKLIGHT_CHECKSUMS_PATH"
guide="$HARKLIGHT_GUIDE_PATH"
for command in chmod cmp date flock grep ln mkdir mktemp mv python3 readlink rm sleep systemctl tar; do
  command -v "$command" >/dev/null || fail "required host command is unavailable"
done

[[ -f "$archive" && -f "$sums" && -f "$guide" ]] || fail "selected release assets are unavailable"
[[ -f "$package_root/BUILD_INFO" && -f "$package_root/SELF_HOST.md" ]] || fail "verified package metadata is unavailable"
cmp -s "$package_root/SELF_HOST.md" "$guide" || fail "package guide differs from its release asset"

python3 - "$archive" "$sums" "$archive_name" "$HARKLIGHT_ARCHIVE_SHA256" <<'PY'
import hashlib
import re
import sys

archive, sums_path, archive_name, expected = sys.argv[1:]
matches = []
with open(sums_path, encoding="ascii") as sums:
    for line in sums:
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)\n?", line)
        if match and match.group(2) == archive_name:
            matches.append(match.group(1))
if len(matches) != 1 or matches[0] != expected:
    raise SystemExit("selected release checksum metadata differs")
digest = hashlib.sha256()
with open(archive, "rb") as source:
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
if digest.hexdigest() != expected:
    raise SystemExit("selected release archive checksum differs")
PY

python3 - "$package_root/BUILD_INFO" "$release_version" "$HARKLIGHT_SOURCE_COMMIT" <<'PY'
import sys

path, version, commit = sys.argv[1:]
values = {}
with open(path, encoding="utf-8") as info:
    for line in info:
        if "=" in line:
            key, value = line.rstrip("\n").split("=", 1)
            values[key] = value
if values.get("version") != version:
    raise SystemExit("package BUILD_INFO version differs from selected tag")
if values.get("source_commit", "").lower() != commit.lower():
    raise SystemExit("package BUILD_INFO source differs from selected tag")
if values.get("source_status") != "":
    raise SystemExit("package BUILD_INFO records a dirty source tree")
PY

root="$HOME/.local/share/harklight-server"
state="$root/state"
releases="$root/releases"
unit="harklight-server.service"
[[ -d "$state" && -f "$state/relay.sqlite3" && -f "$state/server.env" ]] || fail "existing Harklight state and configuration are required"
[[ -x "$root/bin/start-server" && -L "$root/current" ]] || fail "existing Harklight launcher and current release are required"

was_active=0
switched=0
staging=""
backup_tmp=""
current_link=""

mkdir -p "$releases" "$root/backups"
exec 9>"$root/.release-install.lock"
flock 9

current_path="$(readlink -f -- "$root/current")" || fail "current release is unavailable"
case "$current_path" in
  "$releases"/v*) ;;
  *) fail "current release points outside the Harklight releases directory" ;;
esac
current_tag="${current_path##*/}"
[[ "$current_tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || fail "current release tag is invalid"
[[ -d "$current_path" && -f "$current_path/BUILD_INFO" ]] || fail "current release package is incomplete"

is_newer_version() {
  python3 - "$1" "$2" <<'PY'
import re
import sys

def parse(value):
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value):
        raise SystemExit("release version is invalid")
    return tuple(int(part) for part in value[1:].split("."))

print("yes" if parse(sys.argv[1]) > parse(sys.argv[2]) else "no")
PY
}

if [[ "$(is_newer_version "$current_tag" "$HARKLIGHT_RELEASE_TAG")" == yes ]]; then
  fail "selected release is older than the current package; automatic downgrade is refused"
fi

target="$releases/$HARKLIGHT_RELEASE_TAG"
install_info_matches() {
  local path="$1"
  [[ -f "$path/INSTALL_INFO" ]] || return 1
  grep -Fqx "tag=$HARKLIGHT_RELEASE_TAG" "$path/INSTALL_INFO" &&
    grep -Fqx "source_commit=$HARKLIGHT_SOURCE_COMMIT" "$path/INSTALL_INFO" &&
    grep -Fqx "archive_sha256=$HARKLIGHT_ARCHIVE_SHA256" "$path/INSTALL_INFO"
}

release_tree_matches() {
  local path="$1"
  [[ -x "$path/bin/attention_server" && -x "$path/bin/harklight" && -f "$path/BUILD_INFO" &&
     -f "$path/SELF_HOST.md" ]] || return 1
  cmp -s "$path/SELF_HOST.md" "$guide" || return 1
  python3 - "$path/BUILD_INFO" "$release_version" "$HARKLIGHT_SOURCE_COMMIT" <<'PY'
import sys

path, version, commit = sys.argv[1:]
values = {}
with open(path, encoding="utf-8") as info:
    for line in info:
        if "=" in line:
            key, value = line.rstrip("\n").split("=", 1)
            values[key] = value
if values.get("version") != version or values.get("source_commit", "").lower() != commit.lower():
    raise SystemExit("installed BUILD_INFO differs from the selected release")
if values.get("source_status") != "":
    raise SystemExit("installed package records a dirty source tree")
PY
}

process_uses_release() {
  local path="$1" pid executable
  pid="$(systemctl --user show --property=MainPID --value "$unit" 2>/dev/null || true)"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  executable="$(readlink -f -- "/proc/$pid/exe" 2>/dev/null || true)"
  [[ "$executable" == "$path"/erts-*/bin/beam.smp ]]
}

owner_api_ready() {
  local path="$1"
  (
    set +e
    set +u
    set -a
    if ! source "$state/server.env" >/dev/null 2>&1; then exit 1; fi
    set +a
    set -euo pipefail

    local address name port person
    address="${ATTENTION_SERVER_OWNER_BIND:-127.0.0.1}"
    case "$address" in
      0.0.0.0) address=127.0.0.1 ;;
      ::) address=::1 ;;
    esac
    name="${ATTENTION_SERVER_JOIN_NAME:-}"
    port="${ATTENTION_SERVER_OWNER_PORT:-}"
    [[ -n "$name" && "$port" =~ ^[0-9]+$ ]] || exit 1
    [[ -f "$state/tls/ca.pem" && -f "$state/owner/identity.pem" &&
       -f "$state/owner/identity.key" && -f "$state/person-id" ]] || exit 1
    person="$(< "$state/person-id")"
    [[ -n "$person" ]] || exit 1

    export ATTENTION_SERVER_ENABLE=0
    "$path/bin/harklight" owner agent-list \
      --connect "$address" \
      --name "$name" \
      --port "$port" \
      --ca "$state/tls/ca.pem" \
      --cert "$state/owner/identity.pem" \
      --key "$state/owner/identity.key" \
      --person "$person" >/dev/null 2>&1
  )
}

package_ready() {
  local path="$1"
  systemctl --user is-active --quiet "$unit" &&
    process_uses_release "$path" &&
    owner_api_ready "$path"
}

service_condition() {
  systemctl --user show --property=ActiveState,SubState,MainPID --value "$unit" 2>/dev/null ||
    printf 'unknown'
}

service_is_stopped() {
  local active_state pid
  active_state="$(systemctl --user show --property=ActiveState --value "$unit" 2>/dev/null || true)"
  pid="$(systemctl --user show --property=MainPID --value "$unit" 2>/dev/null || true)"
  [[ "$pid" == 0 ]] &&
    [[ "$active_state" == inactive || "$active_state" == failed ]]
}

previous_resume_attempted=0
resume_previous() {
  previous_resume_attempted=1
  systemctl --user start "$unit" || return 1
  local ready=0
  for _ in {1..30}; do
    if package_ready "$current_path"; then
      ready=1
      break
    fi
    sleep 1
  done
  if ((ready == 0)); then
    systemctl --user stop "$unit" && service_is_stopped || return 1
    return 1
  fi
  return 0
}

cleanup() {
  local result=$?
  if [[ -n "$staging" && -d "$staging" ]]; then rm -rf -- "$staging"; fi
  if [[ -n "$backup_tmp" && -d "$backup_tmp" ]]; then rm -rf -- "$backup_tmp"; fi
  if [[ -n "$current_link" && -L "$current_link" ]]; then rm -f -- "$current_link"; fi
  if ((was_active && !switched && !previous_resume_attempted)) && service_is_stopped; then
    if ! resume_previous; then
      printf 'Previous Harklight service could not be restarted and verified.\n' >&2
    fi
  fi
  return "$result"
}

trap cleanup EXIT

if [[ -e "$target" ]]; then
  install_info_matches "$target" || fail "bytes for an existing release tag cannot be replaced"
  release_tree_matches "$target" || fail "installed release contents do not match their recorded tuple"
else
  staging="$(mktemp -d "$releases/.staging-$HARKLIGHT_RELEASE_TAG.XXXXXX")"
  tar --no-same-owner --strip-components=1 -xzf "$archive" -C "$staging"
  [[ -f "$staging/BUILD_INFO" && -x "$staging/bin/attention_server" &&
     -x "$staging/bin/harklight" ]] || fail "staged package is incomplete"
  cmp -s "$staging/SELF_HOST.md" "$guide" || fail "staged guide differs from release metadata"
  python3 - "$staging/BUILD_INFO" "$release_version" "$HARKLIGHT_SOURCE_COMMIT" <<'PY'
import sys

path, version, commit = sys.argv[1:]
values = {}
with open(path, encoding="utf-8") as info:
    for line in info:
        if "=" in line:
            key, value = line.rstrip("\n").split("=", 1)
            values[key] = value
if values.get("version") != version or values.get("source_commit", "").lower() != commit.lower():
    raise SystemExit("staged BUILD_INFO differs from the selected release")
if values.get("source_status") != "":
    raise SystemExit("staged package was built from a dirty source tree")
PY
  cat >"$staging/INSTALL_INFO" <<EOF
repository=clickety-clacks/harklight-server
tag=$HARKLIGHT_RELEASE_TAG
source_commit=$HARKLIGHT_SOURCE_COMMIT
archive_sha256=$HARKLIGHT_ARCHIVE_SHA256
EOF
  chmod 600 "$staging/INSTALL_INFO"
  mv -- "$staging" "$target"
  staging=""
fi

if [[ "$current_tag" == "$HARKLIGHT_RELEASE_TAG" ]] && install_info_matches "$target"; then
  if systemctl --user is-active --quiet "$unit"; then
    ready=0
    for _ in {1..30}; do
      if package_ready "$target"; then
        ready=1
        break
      fi
      sleep 1
    done
    ((ready == 1)) || fail "selected release application and authenticated owner endpoint did not confirm; no install success was reported"
    printf 'Harklight release tuple %s@%s source=%s archive_sha256=%s is already running; no restart performed.\n' \
      'clickety-clacks/harklight-server' "$HARKLIGHT_RELEASE_TAG" "$HARKLIGHT_SOURCE_COMMIT" "$HARKLIGHT_ARCHIVE_SHA256"
    exit 0
  fi

  if ! systemctl --user stop "$unit" || ! service_is_stopped; then
    fail "current release recovery could not confirm the prior process stopped; service state: $(service_condition); latest state was retained"
  fi

  if ! systemctl --user start "$unit"; then
    if ! systemctl --user stop "$unit" || ! service_is_stopped; then
      fail "current release recovery start failed and stop was not confirmed; service state: $(service_condition); latest state was retained"
    fi
    fail "current release recovery start failed; selected package and latest state were retained"
  fi

  ready=0
  for _ in {1..30}; do
    if package_ready "$target"; then
      ready=1
      break
    fi
    sleep 1
  done
  if ((ready == 0)); then
    if ! systemctl --user stop "$unit" || ! service_is_stopped; then
      fail "current release recovery readiness did not confirm and stop was not confirmed; service state: $(service_condition); latest state was retained"
    fi
    fail "current release recovery application and authenticated owner endpoint did not confirm; selected package and latest state were retained"
  fi

  printf 'Harklight release tuple %s@%s source=%s archive_sha256=%s was restarted under the selected package.\n' \
    'clickety-clacks/harklight-server' "$HARKLIGHT_RELEASE_TAG" "$HARKLIGHT_SOURCE_COMMIT" "$HARKLIGHT_ARCHIVE_SHA256"
  exit 0
fi

initial_service_state="$(systemctl --user show --property=ActiveState --value "$unit" 2>/dev/null)" ||
  fail "server service state is unavailable; release was not switched"
initial_service_pid="$(systemctl --user show --property=MainPID --value "$unit" 2>/dev/null)" ||
  fail "server service process is unavailable; release was not switched"
[[ "$initial_service_pid" =~ ^[0-9]+$ ]] || fail "server service process identity is unavailable; release was not switched"
if [[ "$initial_service_state" != inactive && "$initial_service_state" != failed ]] ||
   [[ "$initial_service_pid" != 0 ]]; then
  was_active=1
fi

if ! systemctl --user stop "$unit" || ! service_is_stopped; then
  fail "server service stop was not confirmed; release was not switched; service state: $(service_condition)"
fi

backup_tmp="$(mktemp -d "$root/backups/.harklight-backup.XXXXXX")"
backup_id="${backup_tmp##*.}"
backup_final="$root/backups/$(date -u +%Y%m%dT%H%M%SZ)-$current_tag-to-$HARKLIGHT_RELEASE_TAG-$backup_id"
if ! tar --exclude='./releases' --exclude='./backups' --exclude='./.release-install.lock' \
  -cf "$backup_tmp/private-root.tar" -C "$root" .; then
  rm -rf -- "$backup_tmp"
  backup_tmp=""
  if ((was_active)) && ! resume_previous; then
    fail "full backup failed and previous service restart is unconfirmed; service state: $(service_condition)"
  fi
  fail "full private state backup failed; release was not switched"
fi
chmod 600 "$backup_tmp/private-root.tar"
cat >"$backup_tmp/INSTALL_INFO" <<EOF
from=$current_tag
to=$HARKLIGHT_RELEASE_TAG
source_commit=$HARKLIGHT_SOURCE_COMMIT
archive_sha256=$HARKLIGHT_ARCHIVE_SHA256
EOF
chmod 600 "$backup_tmp/INSTALL_INFO"
mv -- "$backup_tmp" "$backup_final"
backup_tmp=""

current_link="$root/.current-$HARKLIGHT_RELEASE_TAG.$$"
ln -s "releases/$HARKLIGHT_RELEASE_TAG" "$current_link"
mv -Tf -- "$current_link" "$root/current"
current_link=""
switched=1

if ! systemctl --user start "$unit"; then
  if ! systemctl --user stop "$unit" || ! service_is_stopped; then
    fail "candidate start failed and stop was not confirmed; service state: $(service_condition); packages and latest state were retained"
  fi
  fail "candidate start failed and candidate is stopped; packages, backup, and latest state were retained"
fi

ready=0
for _ in {1..30}; do
  if package_ready "$target"; then
    ready=1
    break
  fi
  sleep 1
done
if ((ready == 0)); then
  if ! systemctl --user stop "$unit" || ! service_is_stopped; then
    fail "candidate readiness did not confirm and stop was not confirmed; service state: $(service_condition); latest state was retained"
  fi
  fail "candidate application and authenticated owner endpoint did not confirm; candidate is stopped and latest state was retained"
fi

printf 'Harklight release tuple %s@%s source=%s archive_sha256=%s is running under the selected package.\n' \
  'clickety-clacks/harklight-server' "$HARKLIGHT_RELEASE_TAG" "$HARKLIGHT_SOURCE_COMMIT" "$HARKLIGHT_ARCHIVE_SHA256"
printf 'State backup retained at %s\n' "$backup_final"

#!/usr/bin/env bash
# Tests the installer's migrate orchestration against a contract-conforming
# test double, a private loopback PostgreSQL and the real schema files.
# Synthetic data only. Usage: migrate-test.sh [PATH/TO/harklight-postgres]
set -euo pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
tool="${1:-$here/../harklight-postgres.sh}"
scratch="$(mktemp -d)"; chmod 700 "$scratch"
root="$scratch/root"; state="$root/state"
pass=0
pg_bin="${HARKLIGHT_PG_BIN:-$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -t/ -k5 -n | tail -n 1)}"
[[ -x "$pg_bin/postgres" ]] || { echo "PostgreSQL programs not found" >&2; exit 1; }
args=(--root "$root" --pg-bin "$pg_bin")
cleanup() { "$pg_bin/pg_ctl" -D "$root/postgres/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$scratch"; }
trap cleanup EXIT
ok() { pass=$((pass + 1)); printf 'ok %d - %s\n' "$pass" "$1"; }
bad() { printf 'FAIL - %s\n' "$1" >&2; exit 1; }
expect_fail() { local what=$1 text=$2 out; shift 2; if out="$("$@" 2>&1)"; then bad "$what: succeeded"; fi; [[ "$out" == *"$text"* ]] || bad "$what: expected '$text' in: $out"; ok "$what"; }
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"

mkdir -p "$state/tls" "$state/owner" "$scratch/ext"; chmod 700 "$root" "$state"
printf 'synthetic sqlite bytes\n' > "$state/relay.sqlite3"
printf 'synthetic external key\n' > "$scratch/ext/server.key"
printf 'export ATTENTION_SERVER_ENABLE=1\nexport ATTENTION_SERVER_KEYFILE=%q\n' "$scratch/ext/server.key" > "$state/server.env"
echo person > "$state/person-id"; echo ca > "$state/tls/ca.pem"; echo id > "$state/owner/identity.key"
envsha() { sha256sum "$state/server.env" | cut -d' ' -f1; }
before_env="$(envsha)"

"$tool" setup "${args[@]}" --no-service --port "$port" >/dev/null
fake="$here/fake-migration.sh"
export FAKE_DIR="$scratch/fake" FAKE_PSQL="$pg_bin/psql"; mkdir "$FAKE_DIR"
m() { PGPASSWORD="$(< "$root/postgres/harklight_migrator.password")" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "postgres://harklight_migrator@127.0.0.1:$port/harklight?sslmode=disable" "$@"; }
workdirs() { ls -d "$root"/migration/*/ 2>/dev/null; }

expect_fail "migrate refuses when the packaged importer is not available" "not available in this release" \
  "$tool" migrate "${args[@]}" --migration-bin "$scratch/missing"

mkdir "$scratch/fakebin"; printf '#!/bin/sh\nexit 0\n' > "$scratch/fakebin/systemctl"; chmod +x "$scratch/fakebin/systemctl"
expect_fail "migrate refuses while Harklight Server is running" "is running" \
  env PATH="$scratch/fakebin:$PATH" "$tool" migrate "${args[@]}" --migration-bin "$fake"

printf 'wal' > "$state/relay.sqlite3-wal"
expect_fail "migrate refuses an un-checkpointed store" "not checkpointed" "$tool" migrate "${args[@]}" --migration-bin "$fake"
rm "$state/relay.sqlite3-wal"
[[ -z "$(workdirs)" ]] || bad "a refused migration created a work directory"

# A failure after import leaves everything recoverable and activates nothing.
export FAKE_FAIL_AT=validate
expect_fail "migrate stops at a failed validation with resume guidance" "migrate --resume" "$tool" migrate "${args[@]}" --migration-bin "$fake"
wd="$(workdirs | head -n 1)"; wd="${wd%/}"
[[ "$(m -c 'select phase from harklight.import_state')" == loading ]] || bad "phase moved on after a failed validate"
[[ ! -e "$wd/done.activate" && "$(envsha)" == "$before_env" ]] || bad "failure activated or edited server.env"
[[ "$(stat -c %a "$wd")" == 700 && "$(stat -c %a "$wd/backup/private-root.tar")" == 600 ]] || bad "work directory modes"
tar -tf "$wd/backup/private-root.tar" | grep -q 'relay.sqlite3' || bad "backup lacks the store"
[[ "$(sha256sum "$wd/backup/private-root.tar" | cut -d' ' -f1)" == "$(cat "$wd/backup/SHA256")" ]] || bad "backup digest"
ok "failed validation leaves the destination loading, server.env unchanged, backup intact"

# Resume continues, reruns the idempotent steps, and activates only after grants.
sums="$(sha256sum "$wd/backup/private-root.tar")"
rm "$scratch/ext/server.key"   # the verified staged copy must put it back
: > "$FAKE_DIR/calls.log"
"$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd" >"$scratch/resume.out"
[[ "$(tr '\n' ' ' < "$FAKE_DIR/calls.log")" == "import validate activate status " ]] || bad "unexpected resume call order: $(tr '\n' ' ' < "$FAKE_DIR/calls.log")"
[[ "$(m -c 'select phase from harklight.import_state')" == active ]] || bad "not active after resume"
[[ "$(sha256sum "$wd/backup/private-root.tar")" == "$sums" ]] || bad "the backup changed on resume"
[[ "$(envsha)" == "$before_env" ]] || bad "server.env changed"
grep -q 'server.env was not changed' "$scratch/resume.out" || bad "missing final guidance"
[[ "$(cat "$scratch/ext/server.key")" == "synthetic external key" && "$(stat -c %a "$scratch/ext/server.key")" == 600 ]] || bad "verified material was not installed"
grep -q 'identity material verified: installed 1 file' "$scratch/resume.out" || bad "material install not reported"
"$tool" verify-roles "${args[@]}" >/dev/null
ok "resume runs import, validate, grants, activate and status in order and keeps the backup identical"

python3 - "$wd/material-map.json" "$state" "$scratch/ext/server.key" <<'PY' || bad "material map"
import json, sys
m = json.load(open(sys.argv[1])); state, key = sys.argv[2:4]
assert m["server.env"] == state + "/server.env" and m["person-id"] == state + "/person-id"
assert m["tls/ca.pem"] == state + "/tls/ca.pem" and m["owner/identity.key"] == state + "/owner/identity.key"
assert m["external" + key] == key and not any("relay.sqlite3" in k for k in m)
PY
ok "material map names server.env, person-id, tls and owner files and external key files"

# Material that fails verification blocks the finish, even though the database is already active.
staged="$wd/staging/root/server.env"
cp "$staged" "$scratch/staged.keep"; echo tampered >> "$staged"
expect_fail "migrate refuses staged material whose hash differs" "ACTIVE but the importer's identity material could not be verified" \
  "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
cp "$scratch/staged.keep" "$staged"
cp "$wd/staging/COMPLETE.json" "$scratch/complete.keep"; echo ' ' >> "$wd/staging/COMPLETE.json"
expect_fail "migrate refuses a COMPLETE.json that does not match its digest" "does not match its recorded digest" \
  "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
cp "$scratch/complete.keep" "$wd/staging/COMPLETE.json"
echo changed > "$scratch/ext/server.key"
expect_fail "migrate never overwrites an existing file that differs from the staged copy" "was not overwritten" \
  "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
[[ "$(cat "$scratch/ext/server.key")" == changed ]] || bad "a differing file was overwritten"
printf 'synthetic external key\n' > "$scratch/ext/server.key"
"$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd" >/dev/null
[[ "$(envsha)" == "$before_env" ]] || bad "server.env changed"
ok "after the material problems are fixed, resume completes again"

# The runtime role cannot touch the lifecycle row the importer activated.
q() { PGPASSWORD="$(< "$root/postgres/harklight.password")" "$pg_bin/psql" --no-psqlrc -qtA "postgres://harklight@127.0.0.1:$port/harklight?sslmode=disable" -c "$1"; }
q "select phase from harklight.import_state" >/dev/null
if q "update harklight.import_state set phase = 'loading'" >/dev/null 2>&1; then bad "runtime role could rewrite import_state"; fi
ok "runtime role can read but not change the activated import state"

expect_fail "migrate refuses a destination that already holds data" "empty, unpublished destination" "$tool" migrate "${args[@]}" --migration-bin "$fake"
printf 'all %d checks passed\n' "$pass"

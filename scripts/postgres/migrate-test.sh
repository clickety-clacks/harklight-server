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
chmod 600 "$state/server.env" "$state/person-id" "$state/tls/ca.pem" "$state/owner/identity.key" "$scratch/ext/server.key"; chmod 700 "$state/tls" "$state/owner"
envsha() { sha256sum "$state/server.env" | cut -d' ' -f1; }
before_env="$(envsha)"

"$tool" setup "${args[@]}" --no-service --port "$port" >/dev/null
fake="$here/fake-migration.sh"
export FAKE_DIR="$scratch/fake" FAKE_PSQL="$pg_bin/psql"; mkdir "$FAKE_DIR"
m() { PGPASSWORD="$(< "$root/postgres/harklight_migrator.password")" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "postgres://harklight_migrator@127.0.0.1:$port/harklight?sslmode=disable" "$@"; }
workdirs() { ls -d "$root"/migration/*/ 2>/dev/null; }

# A systemctl shim whose unit state the test controls: "ACTIVESTATE MAINPID".
mkdir "$scratch/shim"
cat > "$scratch/shim/systemctl" <<'SH'
#!/bin/sh
read state pid < "$UNIT_STATE"
case "$*" in
  *is-active*) [ "$state" = active ] ;;
  *LoadState*) echo "${LOAD_STATE:-loaded}" ;;
  *ActiveState*) echo "$state" ;;
  *MainPID*) echo "$pid" ;;
  *) exit 0 ;;
esac
SH
chmod +x "$scratch/shim/systemctl"
export UNIT_STATE="$scratch/unit.state"; echo "inactive 0" > "$UNIT_STATE"
export PATH="$scratch/shim:$PATH"
calls() { tr '\n' ' ' < "$FAKE_DIR/calls.log"; }
: > "$FAKE_DIR/calls.log"

expect_fail "migrate refuses when the importer is missing" "is not available" "$tool" migrate "${args[@]}" --migration-bin "$scratch/missing"
# The shipped importer today: status answers a missing receipt but nothing is ready.
printf '#!/bin/sh\ncase "$1" in status) echo "Migration refused: receipt_missing" >&2 ;; *) echo "Migration refused: invalid_arguments" >&2 ;; esac\nexit 2\n' > "$scratch/statusonly"; chmod +x "$scratch/statusonly"
expect_fail "a status that answers for a missing receipt does not authorize migration" "does not report that the migration pipeline is ready" \
  "$tool" migrate "${args[@]}" --migration-bin "$scratch/statusonly"
expect_fail "migrate refuses an importer that reports migration_ready false" "does not report that the migration pipeline is ready" \
  env FAKE_READY=false "$tool" migrate "${args[@]}" --migration-bin "$fake"
printf '#!/bin/sh\necho "{\\"contract_version\\":1,\\"command\\":\\"capabilities\\",\\"commands\\":[\\"export\\",\\"verify\\",\\"status\\"],\\"migration_ready\\":true}"\n' > "$scratch/partial"; chmod +x "$scratch/partial"
expect_fail "migrate refuses an importer missing a pipeline command" "does not report that the migration pipeline is ready" \
  "$tool" migrate "${args[@]}" --migration-bin "$scratch/partial"
[[ -z "$(workdirs)" ]] || bad "an unavailable importer left a work directory"

for st in "active 123" "failed 123" "activating 0"; do
  echo "$st" > "$UNIT_STATE"
  expect_fail "migrate refuses a Server not proven stopped ($st)" "not proven stopped" "$tool" migrate "${args[@]}" --migration-bin "$fake"
done
echo "inactive 0" > "$UNIT_STATE"
expect_fail "migrate refuses a Server unit that is not loaded, even though it looks inactive" "not a loaded unit" \
  env LOAD_STATE=not-found "$tool" migrate "${args[@]}" --migration-bin "$fake"
SAVE_PATH="$PATH"; export PATH="/usr/bin:/bin"; command -v systemctl >/dev/null && NOSYS=0 || NOSYS=1
if [[ "$NOSYS" == 1 ]]; then
  expect_fail "migrate refuses when it cannot observe the Server at all" "cannot prove" "$tool" migrate "${args[@]}" --migration-bin "$fake"
fi
export PATH="$SAVE_PATH"
[[ -z "$(workdirs)" ]] || bad "a refused migration created a work directory"

printf 'wal' > "$state/relay.sqlite3-wal"
expect_fail "migrate refuses an un-checkpointed store" "not checkpointed" "$tool" migrate "${args[@]}" --migration-bin "$fake"
rm "$state/relay.sqlite3-wal"

# An external path the backup cannot capture stops the migration before export.
cp "$state/server.env" "$scratch/server.env.keep"
printf 'export HK_TEST_DIR=%q\n' "$scratch/ext" >> "$state/server.env"
expect_fail "migrate refuses when an external configured path cannot be captured" "cannot be captured" "$tool" migrate "${args[@]}" --migration-bin "$fake"
[[ "$(calls)" != *export* ]] || bad "export ran despite an incomplete material map"
rm -rf "$root"/migration; cp "$scratch/server.env.keep" "$state/server.env"; : > "$FAKE_DIR/calls.log"
before_env="$(envsha)"

# A failure after import leaves everything recoverable and activates nothing.
export FAKE_FAIL_AT=validate
expect_fail "migrate stops at a failed validation with resume guidance" "migrate --resume" "$tool" migrate "${args[@]}" --migration-bin "$fake"
wd="$(workdirs | head -n 1)"; wd="${wd%/}"
[[ "$(m -c 'select phase from harklight.import_state')" == loading ]] || bad "phase moved on after a failed validate"
[[ "$(envsha)" == "$before_env" ]] || bad "failure edited server.env"
[[ "$(stat -c %a "$wd")" == 700 && "$(stat -c %a "$wd/backup/private-root.tar")" == 600 ]] || bad "work directory modes"
tar -tf "$wd/backup/private-root.tar" | grep -q 'relay.sqlite3' || bad "backup lacks the store"
ok "failed validation leaves the destination loading, server.env unchanged, backup intact"

# A changed source, or one held open by a writer, is refused on resume before any importer call.
cp "$state/relay.sqlite3" "$scratch/sqlite.keep"; echo more >> "$state/relay.sqlite3"
: > "$FAKE_DIR/calls.log"
expect_fail "resume refuses a source whose main file changed" "source store changed" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
cp "$scratch/sqlite.keep" "$state/relay.sqlite3"
printf 'shm' > "$state/relay.sqlite3-shm"
expect_fail "resume refuses a source that gained a SHM file" "source store changed" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
rm "$state/relay.sqlite3-shm"
( exec 7<"$state/relay.sqlite3"; sleep 20 ) & holder=$!
sleep 1
expect_fail "resume refuses a source held open by a running process" "has the source store open" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
kill "$holder" 2>/dev/null || true; wait "$holder" 2>/dev/null || true
[[ "$(calls)" == "capabilities capabilities capabilities " || "$(calls)" == "capabilities capabilities " ]] || [[ "$(calls)" != *import* ]] || bad "importer ran against a changed source"

# Resume validates, applies grants, installs material, and does NOT activate.
sums="$(sha256sum "$wd/backup/private-root.tar")"
rm "$scratch/ext/server.key"   # the verified staged copy must put it back
: > "$FAKE_DIR/calls.log"
"$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd" >"$scratch/resume.out"
[[ "$(calls)" == "capabilities import validate " ]] || bad "unexpected resume call order: $(calls)"
[[ "$(m -c 'select phase from harklight.import_state')" == validated ]] || bad "migrate must leave the destination validated, not active"
[[ ! -e "$wd/done.activate" && "$(sha256sum "$wd/backup/private-root.tar")" == "$sums" && "$(envsha)" == "$before_env" ]] || bad "backup or server.env changed"
[[ "$(cat "$scratch/ext/server.key")" == "synthetic external key" && "$(stat -c %a "$scratch/ext/server.key")" == 600 ]] || bad "verified material was not installed"
grep -q 'NOT been activated' "$scratch/resume.out" || bad "missing guidance"
"$tool" verify-roles "${args[@]}" >/dev/null
ok "resume imports, validates, applies grants and installs verified material without activating"

python3 - "$wd/material-map.json" "$state" "$scratch/ext/server.key" <<'PY' || bad "material map"
import json, sys
m = json.load(open(sys.argv[1])); state, key = sys.argv[2:4]
assert m["server.env"] == state + "/server.env" and m["person-id"] == state + "/person-id"
assert m["tls/ca.pem"] == state + "/tls/ca.pem" and m["owner/identity.key"] == state + "/owner/identity.key"
assert m["external" + key] == key and not any("relay.sqlite3" in k for k in m)
PY
ok "material map names server.env, person-id, tls and owner files and external key files"

# Resume with the Server not proven stopped runs nothing.
echo "failed 123" > "$UNIT_STATE"; : > "$FAKE_DIR/calls.log"
expect_fail "resume refuses a Server not proven stopped" "not proven stopped" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
[[ "$(calls)" == "capabilities " ]] || bad "importer commands ran with the Server not proven stopped: $(calls)"
echo "inactive 0" > "$UNIT_STATE"
expect_fail "resume refuses a Server unit that is not loaded" "not a loaded unit" \
  env LOAD_STATE=not-found "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"

# Material that fails verification blocks activation.
staged="$wd/staging/root/server.env"
cp "$staged" "$scratch/staged.keep"; echo tampered >> "$staged"
expect_fail "resume refuses staged material whose hash differs" "could not be verified" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
[[ "$(m -c 'select phase from harklight.import_state')" == validated ]] || bad "activated despite bad material"
cp "$scratch/staged.keep" "$staged"
cp "$wd/staging/COMPLETE.json" "$scratch/complete.keep"; echo ' ' >> "$wd/staging/COMPLETE.json"
expect_fail "resume refuses a COMPLETE.json that does not match its digest" "does not match its recorded digest" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
cp "$scratch/complete.keep" "$wd/staging/COMPLETE.json"
echo changed > "$scratch/ext/server.key"; chmod 600 "$scratch/ext/server.key"
expect_fail "material install never overwrites a differing file" "was not overwritten" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
printf 'synthetic external key\n' > "$scratch/ext/server.key"; chmod 644 "$scratch/ext/server.key"
expect_fail "material install refuses an identical file that is not private" "not private" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
chmod 600 "$scratch/ext/server.key"
chmod 644 "$staged"
expect_fail "material install refuses staged files that are not private" "is not private" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
chmod 600 "$staged"
chmod 755 "$wd/staging"
expect_fail "material install refuses a staging directory that is not private" "is not private" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
chmod 700 "$wd/staging"
nested="$(dirname -- "$(find "$wd/staging/root" -type f | grep external | head -n 1)")"
chmod 755 "$nested"
expect_fail "material install refuses a nested staging directory that is not private" "is not private" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
chmod 700 "$nested"
chmod 644 "$wd/staging/COMPLETE.json"
expect_fail "material install refuses a COMPLETE.json that is not private" "is not private" "$tool" migrate "${args[@]}" --migration-bin "$fake" --resume "$wd"
chmod 600 "$wd/staging/COMPLETE.json"
[[ "$(m -c 'select phase from harklight.import_state')" == validated ]] || bad "activated despite material problems"
ok "material problems are refused and the destination stays validated"

q() { PGPASSWORD="$(< "$root/postgres/harklight.password")" "$pg_bin/psql" --no-psqlrc -qtA "postgres://harklight@127.0.0.1:$port/harklight?sslmode=disable" -c "$1"; }
q "select phase from harklight.import_state" >/dev/null
if q "update harklight.import_state set phase = 'active'" >/dev/null 2>&1; then bad "runtime role could activate the import"; fi
[[ "$(m -c 'select phase from harklight.import_state')" == validated ]] || bad "phase is not validated at the end"
ok "runtime role can read but not change the import state; nothing in this slice activates it"

grep -q activate "$FAKE_DIR/calls.log" && bad "activate was called" || true
expect_fail "migrate refuses a destination that already holds data" "empty, unpublished destination" "$tool" migrate "${args[@]}" --migration-bin "$fake"
printf 'all %d checks passed\n' "$pass"

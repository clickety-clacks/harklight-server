#!/usr/bin/env bash
# Throwaway self-test for harklight-postgres: version and disk refusals, local
# setup, backup and restore round trip, and the operator-supplied remote path.
# Uses only a private PostgreSQL cluster created in mode-0700 scratch on
# loopback, and synthetic data. Usage: selftest.sh [PATH/TO/harklight-postgres]
set -euo pipefail
here="$(cd -- "$(dirname -- "$0")" && pwd)"
tool="${1:-$here/../harklight-postgres.sh}"
scratch="$(mktemp -d)"
chmod 700 "$scratch"
root="$scratch/root"
state="$root/state"
pass=0
args=(--root "$root" --no-service)

cleanup() {
  if [[ -x "${pg_bin:-/nonexistent}/pg_ctl" && -d "$root/postgres/data" ]]; then
    "$pg_bin/pg_ctl" -D "$root/postgres/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  rm -rf -- "$scratch"
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok %d - %s\n' "$pass" "$1"; }
bad() { printf 'FAIL - %s\n' "$1" >&2; exit 1; }

# expect_fail DESCRIPTION EXPECTED-TEXT COMMAND...
expect_fail() {
  local what=$1 text=$2 out; shift 2
  if out="$("$@" 2>&1)"; then bad "$what: command succeeded"; fi
  [[ "$out" == *"$text"* ]] || bad "$what: expected '$text' in: $out"
  ok "$what"
}

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])
PY
}
port="$(free_port)"

mkdir -p "$state/tls" "$state/uploads"
chmod 700 "$root" "$state"
printf 'export ATTENTION_SERVER_ENABLE=1\n' > "$state/server.env"
printf 'synthetic ca\n' > "$state/tls/ca.pem"
head -c 1048576 /dev/urandom > "$state/uploads/blob.bin"
statehash() { (cd "$state" && find . -type f ! -name postgres.env -print0 | sort -z | xargs -0 sha256sum); }
statehash > "$scratch/state.sha"
pg_bin="${HARKLIGHT_PG_BIN:-}"
if [[ -z "$pg_bin" ]]; then
  pg_bin="$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -t/ -k5 -n | tail -n 1)"
fi
[[ -x "$pg_bin/postgres" ]] || { echo "PostgreSQL programs not found; set HARKLIGHT_PG_BIN" >&2; exit 1; }
args+=(--pg-bin "$pg_bin")

# Version refusal with a stub older than the minimum.
fake="$scratch/fake-pg"
mkdir "$fake"
for t in postgres initdb psql pg_dump pg_restore pg_isready pg_ctl; do
  printf '#!/bin/sh\necho "postgres (PostgreSQL) 15.4"\n' > "$fake/$t"
  chmod +x "$fake/$t"
done
expect_fail "preflight refuses PostgreSQL older than 16" "older than the required" \
  "$tool" preflight "${args[@]}" --pg-bin "$fake" --port "$port"

# Disk refusal.
expect_fail "preflight refuses insufficient free disk" "not enough free disk space" \
  "$tool" preflight "${args[@]}" --port "$port" --min-free-mb 999999999

# Port refusal.
python3 -c "
import socket, time, sys
s = socket.socket(); s.bind(('127.0.0.1', $port)); s.listen(1); time.sleep(30)" &
holder=$!
sleep 1
expect_fail "preflight refuses a busy port" "already in use" "$tool" preflight "${args[@]}" --port "$port"
kill "$holder" 2>/dev/null || true
wait "$holder" 2>/dev/null || true

# Setup.
"$tool" setup "${args[@]}" --port "$port" >"$scratch/setup.out"
grep -q 'privilege boundary verified' "$scratch/setup.out" || bad "setup did not verify the privilege boundary"
read_var() { (set +u; source "$1" >/dev/null 2>&1; printf '%s' "${!2}"); }
url="$(read_var "$state/postgres.env" ATTENTION_SERVER_POSTGRES_URL)"
pwfile="$(read_var "$state/postgres.env" ATTENTION_SERVER_POSTGRES_PASSWORD_FILE)"
murl="$(read_var "$root/postgres/migrator.env" HARKLIGHT_MIGRATOR_URL)"
mpwfile="$(read_var "$root/postgres/migrator.env" HARKLIGHT_MIGRATOR_PASSWORD_FILE)"
[[ "$url" == "postgres://harklight@127.0.0.1:$port/harklight?sslmode=disable" ]] || bad "unexpected url $url"
[[ "$murl" == "postgres://harklight_migrator@127.0.0.1:$port/harklight?sslmode=disable" ]] || bad "unexpected migrator url"
[[ "$(stat -c %a "$pwfile")" == 600 && "$(stat -c %a "$root/postgres")" == 700 && "$(stat -c %a "$root/postgres/migrator.env")" == 600 ]] || bad "permissions"
! grep -qi 'migrator' "$state/postgres.env" || bad "migrator details leaked into the runtime configuration"
ok "setup creates a loopback cluster, two roles and private files, and verifies the boundary"

q() { PGPASSWORD="$(< "$pwfile")" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$url" "$@"; }
m() { PGPASSWORD="$(< "$mpwfile")" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$murl" "$@"; }
admin() { "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 -h "$root/postgres/run" -p "$port" -U harklight_admin -d "${1:-postgres}" "${@:2}"; }

expect_fail "setup refuses an existing data location" "never reused or overwritten" "$tool" setup "${args[@]}" --port "$(free_port)"

if PGPASSWORD="$(< "$pwfile")" "$pg_bin/psql" -qtA -h "$(hostname -I | awk '{print $1}')" -p "$port" -U harklight -d harklight -c 'select 1' >/dev/null 2>&1; then
  bad "cluster accepted a non-loopback connection"
fi
ok "cluster refuses non-loopback connections"

# Real schema migration as the owner role, then grants for the runtime role.
m -f "$here/../../priv/postgres/001_initial.sql" >/dev/null
expect_fail "apply-grants refuses a missing schema" "does not exist yet" "$tool" apply-grants "${args[@]}" --schema nosuchschema
"$tool" apply-grants "${args[@]}" >/dev/null
q -c "select count(*) from harklight.import_state" >/dev/null
denied() { if q >/dev/null 2>&1 -c "$1"; then bad "runtime role was allowed to: $1"; fi; }
denied "truncate harklight.server_identity"
denied "truncate harklight.import_state"
denied "alter table harklight.import_state disable trigger all"
denied "alter table harklight.import_state disable trigger import_state_transition"
denied "alter table harklight.import_state add column z int"
denied "insert into harklight.import_state(import_id, source_sha256, schema_sha256, phase) values ('x', decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex'), 'active')"
denied "update harklight.schema_migrations set version = 9"
denied "delete from harklight.server_identity"
denied "create table harklight.intruder(i int)"
denied "drop table harklight.schema_migrations"
ok "runtime role cannot TRUNCATE, ALTER, disable the import trigger, write lifecycle tables or create objects"

# Application data created by the owner is usable by the runtime role.
m -c "create table harklight.synth_messages(id bigint primary key, body text not null);
      create table harklight.synth_receipts(op text primary key, item bigint references harklight.synth_messages(id))" >/dev/null
q -c "insert into harklight.synth_messages values (0,'runtime write')" >/dev/null
denied "truncate harklight.synth_messages"
ok "tables created later give the runtime role data access only"

# verify-roles refuses when the runtime role is over-privileged.
admin harklight -c "grant truncate on harklight.synth_messages to harklight" >/dev/null
expect_fail "verify-roles refuses a runtime role that can TRUNCATE" "can TRUNCATE" "$tool" verify-roles "${args[@]}"
admin harklight -c "revoke truncate on harklight.synth_messages from harklight" >/dev/null
admin harklight -c "alter table harklight.synth_messages owner to harklight" >/dev/null
expect_fail "verify-roles refuses a runtime role that owns a table" "owns or can act as the owner" "$tool" verify-roles "${args[@]}"
admin harklight -c "alter table harklight.synth_messages owner to harklight_migrator" >/dev/null
admin postgres -c "alter role harklight createrole" >/dev/null
expect_fail "verify-roles refuses a runtime role with a privileged attribute" "privileged attribute" "$tool" verify-roles "${args[@]}"
admin postgres -c "alter role harklight nocreaterole" >/dev/null
admin harklight -c "grant create on schema harklight to harklight" >/dev/null
expect_fail "verify-roles refuses CREATE on the schema" "can create in schema" "$tool" verify-roles "${args[@]}"
admin harklight -c "revoke create on schema harklight from harklight" >/dev/null
"$tool" verify-roles "${args[@]}" >/dev/null
ok "verify-roles passes again once privileges are restored"

# Synthetic data.
m -c "insert into harklight.synth_messages select g, md5(g::text) from generate_series(1,200000) g on conflict do nothing;
      insert into harklight.synth_receipts select 'op' || g, g from generate_series(1,50000) g" >/dev/null
digest() { m -c "select md5(string_agg(id::text || body, ',' order by id)) from harklight.synth_messages" ; }
before="$(digest)"
dbefore="$(m -c "select count(*) from harklight.server_identity") $(m -c "select count(*) from harklight.schema_migrations")"

mkdir "$scratch/fakebin"
printf '#!/bin/sh\nexit 0\n' > "$scratch/fakebin/systemctl"; chmod +x "$scratch/fakebin/systemctl"
expect_fail "backup refuses while Harklight Server is running" "is running" \
  env PATH="$scratch/fakebin:$PATH" "$tool" backup "${args[@]}" "$scratch/never.tar"

start=$(date +%s.%N)
"$tool" backup "${args[@]}" "$scratch/backup.tar" >/dev/null
printf 'backup seconds: %s\n' "$(echo "$(date +%s.%N) - $start" | bc)"
[[ "$(stat -c %a "$scratch/backup.tar")" == 600 ]] || bad "backup mode"
expect_fail "backup never overwrites" "already exists" "$tool" backup "${args[@]}" "$scratch/backup.tar"
ok "backup written"

m -c "truncate harklight.synth_receipts; delete from harklight.synth_messages where id % 2 = 0; create table harklight.stray(x int)" >/dev/null
echo damaged > "$state/uploads/blob.bin"; rm "$state/tls/ca.pem"
expect_fail "restore refuses a populated database without --replace" "pass --replace" "$tool" restore "${args[@]}" "$scratch/backup.tar"
start=$(date +%s.%N)
"$tool" restore "${args[@]}" --replace "$scratch/backup.tar" >/dev/null
printf 'restore seconds: %s\n' "$(echo "$(date +%s.%N) - $start" | bc)"
[[ "$(digest)" == "$before" ]] || bad "restored messages differ"
[[ "$(m -c 'select count(*) from harklight.synth_receipts')" == 50000 ]] || bad "receipts differ"
[[ "$(m -c "select count(*) from harklight.server_identity") $(m -c "select count(*) from harklight.schema_migrations")" == "$dbefore" ]] || bad "schema rows differ"
[[ "$(m -c "select count(*) from pg_tables where tablename = 'stray'")" == 0 ]] || bad "stray table survived"
[[ "$(statehash)" == "$(cat "$scratch/state.sha")" ]] || bad "state files differ after restore"
[[ -f "$state/postgres.env" ]] || bad "postgres.env was lost"
ok "restore round trip: database rows, receipts and state bytes identical"
q -c "insert into harklight.synth_messages values (-1,'after restore')" >/dev/null
denied "truncate harklight.synth_messages"
"$tool" verify-roles "${args[@]}" >/dev/null
ok "restore re-applies the runtime role's grants and keeps the boundary"

cp "$scratch/backup.tar" "$scratch/bad.tar"
mkdir "$scratch/x" && tar -C "$scratch/x" -xf "$scratch/bad.tar" && echo x >> "$scratch/x/database.dump" &&
  tar -C "$scratch/x" -cf "$scratch/bad.tar" MANIFEST database.dump state.tar
expect_fail "restore refuses a damaged archive" "checksum differs" "$tool" restore "${args[@]}" --replace "$scratch/bad.tar"

# Operator-supplied remote PostgreSQL (a second database on the same private cluster).
pw1="$(openssl rand -hex 12)"; pw2="$(openssl rand -hex 12)"; pw3="$(openssl rand -hex 12)"
admin postgres >/dev/null <<SQL
create role rmig login password '$pw1';
create role rrun login password '$pw2';
create role rsuper login superuser password '$pw3';
create database remotedb owner rmig template template0;
revoke all on database remotedb from public;
grant connect on database remotedb to rrun, rsuper;
SQL
printf 'host remotedb rmig,rrun,rsuper 127.0.0.1/32 scram-sha-256\n' >> "$root/postgres/data/pg_hba.conf"
"$pg_bin/pg_ctl" -D "$root/postgres/data" reload >/dev/null
printf '%s\n' "$pw1" > "$scratch/rmig.pw"; printf '%s\n' "$pw2" > "$scratch/rrun.pw"; printf '%s\n' "$pw3" > "$scratch/rsuper.pw"
state2="$scratch/state2"
mkdir -m 700 "$state2"; echo remote-config > "$state2/server.env"
r=(--root "$scratch/root2" --state "$state2" --pg-bin "$pg_bin")
mkdir -m 700 "$scratch/root2"
base="127.0.0.1:$port/remotedb"
expect_fail "remote setup refuses a database that already holds data" "never repurposes" \
  "$tool" setup "${r[@]}" --remote-url "postgres://harklight@127.0.0.1:$port/harklight" --password-file "$pwfile" \
    --migrator-url "$murl" --migrator-password-file "$mpwfile"
expect_fail "remote setup refuses a password in the URL" "do not put a password" \
  "$tool" setup "${r[@]}" --remote-url "postgres://rrun:x@$base" --password-file "$scratch/rrun.pw" --migrator-url "postgres://rmig@$base" --migrator-password-file "$scratch/rmig.pw"
expect_fail "remote setup requires a separate owner role" "separate owner role" \
  "$tool" setup "${r[@]}" --remote-url "postgres://rrun@$base" --password-file "$scratch/rrun.pw"
expect_fail "remote setup refuses the same role for runtime and owner" "must be different roles" \
  "$tool" setup "${r[@]}" --remote-url "postgres://rmig@$base" --password-file "$scratch/rmig.pw" --migrator-url "postgres://rmig@$base" --migrator-password-file "$scratch/rmig.pw"
expect_fail "remote setup refuses a superuser runtime role" "privileged attribute" \
  "$tool" setup "${r[@]}" --remote-url "postgres://rsuper@$base" --password-file "$scratch/rsuper.pw" --migrator-url "postgres://rmig@$base" --migrator-password-file "$scratch/rmig.pw"
[[ ! -e "$scratch/root2/postgres" && ! -e "$state2/postgres.env" ]] || bad "a refused remote setup left configuration behind"
"$tool" setup "${r[@]}" --remote-url "postgres://rrun@$base" --password-file "$scratch/rrun.pw" --migrator-url "postgres://rmig@$base" --migrator-password-file "$scratch/rmig.pw" >/dev/null
url2="postgres://rmig@$base"
q2() { PGPASSWORD="$pw1" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$url2" "$@"; }
q2 -c "create schema harklight; create table harklight.t(id int primary key, v text); insert into harklight.t select g, md5(g::text) from generate_series(1,1000) g" >/dev/null
"$tool" apply-grants "${r[@]}" >/dev/null
sum2="$(q2 -c "select md5(string_agg(v, ',' order by id)) from harklight.t")"
"$tool" backup "${r[@]}" "$scratch/remote.tar" >/dev/null
q2 -c "delete from harklight.t; create table harklight.extra(x int)" >/dev/null
"$tool" restore "${r[@]}" --replace "$scratch/remote.tar" >/dev/null
[[ "$(q2 -c "select md5(string_agg(v, ',' order by id)) from harklight.t")" == "$sum2" ]] || bad "remote restore differs"
[[ "$(q2 -c "select count(*) from pg_tables where tablename = 'extra'")" == 0 ]] || bad "remote extra table survived"
PGPASSWORD="$pw2" "$pg_bin/psql" -qtA "postgres://rrun@$base" -c "select count(*) from harklight.t" >/dev/null || bad "runtime role lost access after restore"
if PGPASSWORD="$pw2" "$pg_bin/psql" -qtA "postgres://rrun@$base" -c "truncate harklight.t" >/dev/null 2>&1; then bad "remote runtime role can truncate"; fi
ok "operator-supplied database: boundary checked, backs up and restores identically"

"$tool" status "${args[@]}" >/dev/null
ok "status reports working connections and a clean role boundary"
printf 'all %d checks passed\n' "$pass"

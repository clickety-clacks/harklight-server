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
. <(set +u; source "$state/postgres.env"; printf 'url=%q pwfile=%q\n' "$ATTENTION_SERVER_POSTGRES_URL" "$ATTENTION_SERVER_POSTGRES_PASSWORD_FILE")
[[ "$url" == "postgres://harklight@127.0.0.1:$port/harklight" ]] || bad "unexpected url $url"
[[ "$(stat -c %a "$pwfile")" == 600 && "$(stat -c %a "$root/postgres")" == 700 ]] || bad "permissions"
ok "setup creates a loopback cluster with private files"

q() { PGPASSWORD="$(< "$pwfile")" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$url" "$@"; }

expect_fail "setup refuses an existing data location" "never reused or overwritten" "$tool" setup "${args[@]}" --port "$(free_port)"

# A non-loopback connection must not be possible.
if PGPASSWORD="$(< "$pwfile")" "$pg_bin/psql" -qtA -h "$(hostname -I | awk '{print $1}')" -p "$port" -U harklight -d harklight -c 'select 1' >/dev/null 2>&1; then
  bad "cluster accepted a non-loopback connection"
fi
ok "cluster refuses non-loopback connections"

# Synthetic data.
q -c "create table messages(id bigint primary key, body text not null, at timestamptz not null);
      insert into messages select g, md5(g::text), now() from generate_series(1,200000) g;
      create table receipts(op text primary key, item bigint references messages(id));
      insert into receipts select 'op' || g, g from generate_series(1,50000) g" >/dev/null
digest() { q -c "select md5(string_agg(id::text || body, ',' order by id)) from messages" ; }
before="$(digest)"

# Backup refuses while the server runs.
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

# Damage the data and the state, then restore.
q -c "delete from receipts; delete from messages where id % 2 = 0; create table stray(x int)" >/dev/null
echo damaged > "$state/uploads/blob.bin"; rm "$state/tls/ca.pem"
expect_fail "restore refuses a populated database without --replace" "pass --replace" "$tool" restore "${args[@]}" "$scratch/backup.tar"
start=$(date +%s.%N)
"$tool" restore "${args[@]}" --replace "$scratch/backup.tar" >/dev/null
printf 'restore seconds: %s\n' "$(echo "$(date +%s.%N) - $start" | bc)"
[[ "$(digest)" == "$before" ]] || bad "restored messages differ"
[[ "$(q -c 'select count(*) from receipts')" == 50000 ]] || bad "receipts differ"
[[ "$(q -c "select count(*) from pg_tables where tablename = 'stray'")" == 0 ]] || bad "stray table survived"
[[ "$(statehash)" == "$(cat "$scratch/state.sha")" ]] || bad "state files differ after restore"
[[ -f "$state/postgres.env" ]] || bad "postgres.env was lost"
ok "restore round trip: database rows, receipts and state bytes identical"

# Corrupt archive refusal.
cp "$scratch/backup.tar" "$scratch/bad.tar"
mkdir "$scratch/x" && tar -C "$scratch/x" -xf "$scratch/bad.tar" && echo x >> "$scratch/x/database.dump" &&
  tar -C "$scratch/x" -cf "$scratch/bad.tar" MANIFEST database.dump state.tar
expect_fail "restore refuses a damaged archive" "checksum differs" "$tool" restore "${args[@]}" --replace "$scratch/bad.tar"

# Operator-supplied remote PostgreSQL (a second database on the same private cluster).
pw2="$(openssl rand -hex 12)"
"$pg_bin/psql" --no-psqlrc -q -h "$root/postgres/run" -p "$port" -U harklight_admin -d postgres <<SQL >/dev/null
create role remoteuser login password '$pw2';
create database remotedb owner remoteuser template template0;
SQL
printf 'host remotedb remoteuser 127.0.0.1/32 scram-sha-256\n' >> "$root/postgres/data/pg_hba.conf"
"$pg_bin/pg_ctl" -D "$root/postgres/data" reload >/dev/null
printf '%s\n' "$pw2" > "$scratch/remote.pw"
state2="$scratch/state2"
mkdir -m 700 "$state2"; echo remote-config > "$state2/server.env"
r=(--root "$scratch/root2" --state "$state2" --pg-bin "$pg_bin")
mkdir -m 700 "$scratch/root2"
expect_fail "remote setup refuses a database that already holds data" "never repurposes" \
  "$tool" setup "${r[@]}" --remote-url "postgres://harklight@127.0.0.1:$port/harklight" --password-file "$pwfile"
expect_fail "remote setup refuses a password in the URL" "do not put a password" \
  "$tool" setup "${r[@]}" --remote-url "postgres://remoteuser:x@127.0.0.1:$port/remotedb" --password-file "$scratch/remote.pw"
"$tool" setup "${r[@]}" --remote-url "postgres://remoteuser@127.0.0.1:$port/remotedb" --password-file "$scratch/remote.pw" >/dev/null
url2="postgres://remoteuser@127.0.0.1:$port/remotedb"
q2() { PGPASSWORD="$pw2" "$pg_bin/psql" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$url2" "$@"; }
q2 -c "create table t(id int primary key, v text); insert into t select g, md5(g::text) from generate_series(1,1000) g" >/dev/null
sum2="$(q2 -c "select md5(string_agg(v, ',' order by id)) from t")"
"$tool" backup "${r[@]}" "$scratch/remote.tar" >/dev/null
q2 -c "delete from t; create table extra(x int)" >/dev/null
"$tool" restore "${r[@]}" --replace "$scratch/remote.tar" >/dev/null
[[ "$(q2 -c "select md5(string_agg(v, ',' order by id)) from t")" == "$sum2" ]] || bad "remote restore differs"
[[ "$(q2 -c "select count(*) from pg_tables where tablename = 'extra'")" == 0 ]] || bad "remote extra table survived"
ok "operator-supplied database backs up and restores identically"

"$tool" status "${args[@]}" >/dev/null
ok "status reports a working connection"
printf 'all %d checks passed\n' "$pass"

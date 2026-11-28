#!/usr/bin/env bash
# Harklight Server self-host PostgreSQL management: preflight, setup, status,
# backup and restore. Packaged as bin/harklight-postgres.
#
# The PostgreSQL storage backend is opt-in and off by default. Nothing here is
# run by the package, the release installer or Server start; this command only
# acts when an operator invokes it, and it never edits server.env.
set -euo pipefail
umask 077

MIN_PG_MAJOR=16
TESTED_PG_MAJOR=16
DEFAULT_PORT=15432
ROLE=harklight
ADMIN_ROLE=harklight_admin
DBNAME=harklight
UNIT=harklight-postgres.service
SERVER_UNIT=harklight-server.service
BACKUP_FORMAT=1

usage() {
  cat >&2 <<'EOF'
usage: harklight-postgres COMMAND [options]

  preflight              check platform, PostgreSQL version, disk, port and data location
  setup                  create one dedicated local PostgreSQL cluster, role and database
  setup --remote-url URL --password-file FILE
                         use an operator-supplied empty PostgreSQL database instead
  status                 report the configured database, service and connection
  backup ARCHIVE         write one archive: database dump, state directory and checksums
  restore ARCHIVE        restore that archive (add --replace to overwrite existing data)

options:
  --root DIR             install root (default $HOME/.local/share/harklight-server)
  --state DIR            state directory (default ROOT/state)
  --pg-bin DIR           PostgreSQL binaries (default: pg_config, then /usr/lib/postgresql/*/bin)
  --port N               local cluster loopback port (default 15432)
  --min-free-mb N        override the disk requirement for preflight and setup
  --ca-file FILE         with --remote-url: server CA certificate
  --no-service           run the local cluster with pg_ctl instead of a user service
                         (containers and tests; not supervised)
  --online               backup or restore while Harklight Server is running (not advised)
  --replace              restore over a database that already holds data
EOF
  exit 2
}

fail() {
  printf 'harklight-postgres: %s\n' "$1" >&2
  exit 1
}

note() { printf '%s\n' "$*"; }

cmd=${1:-}
[[ -n "$cmd" ]] || usage
shift

root="${HARKLIGHT_ROOT:-$HOME/.local/share/harklight-server}"
state=""
pg_bin="${HARKLIGHT_PG_BIN:-}"
port=$DEFAULT_PORT
min_free_mb=""
remote_url=""
password_file=""
ca_file=""
use_service=1
online=0
replace=0
positional=()
while (($#)); do
  case "$1" in
    --root) root=${2:?}; shift 2 ;;
    --state) state=${2:?}; shift 2 ;;
    --pg-bin) pg_bin=${2:?}; shift 2 ;;
    --port) port=${2:?}; shift 2 ;;
    --min-free-mb) min_free_mb=${2:?}; shift 2 ;;
    --remote-url) remote_url=${2:?}; shift 2 ;;
    --password-file) password_file=${2:?}; shift 2 ;;
    --ca-file) ca_file=${2:?}; shift 2 ;;
    --no-service) use_service=0; shift ;;
    --online) online=1; shift ;;
    --replace) replace=1; shift ;;
    -h|--help) usage ;;
    --*) fail "unknown option $1" ;;
    *) positional+=("$1"); shift ;;
  esac
done

[[ "$root" = /* ]] || fail "--root must be an absolute path"
state=${state:-$root/state}
[[ "$state" = /* ]] || fail "--state must be an absolute path"
[[ "$port" =~ ^[0-9]+$ ]] && ((port >= 1024 && port <= 65535)) || fail "--port must be 1024-65535"
[[ -z "$min_free_mb" || "$min_free_mb" =~ ^[0-9]+$ ]] || fail "--min-free-mb must be a whole number"

pgroot="$root/postgres"
data_dir="$pgroot/data"
run_dir="$pgroot/run"
local_password_file="$pgroot/$ROLE.password"
env_file="$state/postgres.env"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

# ---------------------------------------------------------------- helpers

find_pg_bin() {
  local dir candidate
  if [[ -n "$pg_bin" ]]; then
    [[ -x "$pg_bin/postgres" ]] || fail "no postgres executable in $pg_bin"
    return
  fi
  if command -v pg_config >/dev/null 2>&1; then
    dir="$(pg_config --bindir 2>/dev/null || true)"
    if [[ -n "$dir" && -x "$dir/postgres" ]]; then pg_bin=$dir; return; fi
  fi
  for candidate in $(printf '%s\n' /usr/lib/postgresql/*/bin 2>/dev/null | sort -t/ -k5 -n -r); do
    if [[ -x "$candidate/postgres" ]]; then pg_bin=$candidate; return; fi
  done
  fail "PostgreSQL $MIN_PG_MAJOR or newer server and client programs were not found; on Ubuntu install the postgresql package, or use --remote-url with a PostgreSQL you operate"
}

pg_major() {
  local out
  out="$("$pg_bin/postgres" --version 2>/dev/null)" || fail "cannot run $pg_bin/postgres"
  [[ "$out" =~ ([0-9]+)(\.[0-9]+)* ]] || fail "cannot read the PostgreSQL version from: $out"
  printf '%s' "${BASH_REMATCH[1]}"
}

require_tools() {
  local tool
  for tool in "$@"; do
    [[ -x "$pg_bin/$tool" ]] || fail "$tool is missing from $pg_bin; install the PostgreSQL server and client packages"
  done
}

check_version() {
  local major
  major="$(pg_major)"
  ((major >= MIN_PG_MAJOR)) || fail "PostgreSQL $major is older than the required $MIN_PG_MAJOR; install PostgreSQL $MIN_PG_MAJOR or newer, or use --remote-url"
  if ((major != TESTED_PG_MAJOR)); then
    note "notice: PostgreSQL $major is newer than the tested version $TESTED_PG_MAJOR."
  fi
}

nearest_existing() {
  local path=$1
  while [[ ! -e "$path" && "$path" != / ]]; do path=$(dirname -- "$path"); done
  printf '%s' "$path"
}

free_mb() { df --output=avail -BM -- "$(nearest_existing "$1")" | tail -n 1 | tr -d "M "; }

tree_mb() {
  local path total=0 size
  for path in "$@"; do
    [[ -e "$path" ]] || continue
    size="$(du -sm -- "$path" | cut -f1)"
    total=$((total + size))
  done
  printf '%s' "$total"
}

port_in_use() {
  python3 - "$1" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(0)
sys.exit(1)
PY
}

systemctl_user() { systemctl --user "$@"; }

server_running() {
  command -v systemctl >/dev/null 2>&1 || return 1
  systemctl_user is-active --quiet "$SERVER_UNIT" 2>/dev/null
}

load_config() {
  [[ -f "$env_file" ]] || fail "no PostgreSQL configuration at $env_file; run setup first"
  # shellcheck disable=SC1090
  pg_url="$(set +u; source "$env_file" >/dev/null 2>&1; printf '%s' "${ATTENTION_SERVER_POSTGRES_URL:-}")"
  pg_pwfile="$(set +u; source "$env_file" >/dev/null 2>&1; printf '%s' "${ATTENTION_SERVER_POSTGRES_PASSWORD_FILE:-}")"
  pg_ca="$(set +u; source "$env_file" >/dev/null 2>&1; printf '%s' "${ATTENTION_SERVER_POSTGRES_CA_FILE:-}")"
  [[ -n "$pg_url" && -f "$pg_pwfile" ]] || fail "$env_file is incomplete; run setup again on a clean install"
}

# Run a client program against the configured database. The password goes in
# the child's environment, never on a command line.
with_db() {
  local program=$1; shift
  PGPASSWORD="$(< "$pg_pwfile")" PGSSLROOTCERT="${pg_ca:-}" PGCONNECT_TIMEOUT=10 PGOPTIONS="-c client_min_messages=warning" \
    "$pg_bin/$program" "$@"
}

psql_db() { with_db psql --no-psqlrc --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 "$pg_url" "$@"; }

psql_admin() {
  "$pg_bin/psql" --no-psqlrc --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 \
    -h "$run_dir" -p "$port" -U "$ADMIN_ROLE" -d "${1:-postgres}"
}

user_table_count() {
  psql_db -c "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r','p','v','m') and n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'"
}

wait_ready() {
  local _
  for _ in $(seq 1 60); do
    if "$pg_bin/pg_isready" -q -h "$run_dir" -p "$port"; then return 0; fi
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------- preflight

required_mb() {
  if [[ -n "$min_free_mb" ]]; then printf '%s' "$min_free_mb"; return; fi
  local existing
  existing="$(tree_mb "$state")"
  local need=$((1024 + 3 * existing))
  printf '%s' $((need < 2048 ? 2048 : need))
}

do_preflight() {
  local mode=${1:-local}
  [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || fail "this package supports Linux x86_64 only"
  command -v python3 >/dev/null || fail "python3 is required"
  find_pg_bin
  require_tools postgres initdb psql pg_dump pg_restore pg_isready
  check_version
  note "PostgreSQL programs: $pg_bin (major $(pg_major))"
  [[ -d "$state" ]] || fail "state directory $state does not exist; bootstrap Harklight first"
  if [[ "$mode" == local ]]; then
    [[ ! -e "$pgroot" ]] || fail "$pgroot already exists; it is never reused or overwritten. Move it away deliberately or choose another --root"
    ((${#run_dir} <= 90)) || fail "the socket directory path is too long for PostgreSQL; use a shorter --root"
    if port_in_use "$port"; then fail "loopback port $port is already in use; choose another with --port"; fi
    if ((use_service)); then
      command -v systemctl >/dev/null || fail "systemctl is required for service supervision (or pass --no-service)"
      systemctl_user show-environment >/dev/null 2>&1 || fail "the systemd user manager is unavailable; log in with a user session (or enable linger) or pass --no-service"
    fi
    local need have
    need="$(required_mb)"
    have="$(free_mb "$root")"
    ((have >= need)) || fail "not enough free disk space under $root: ${have} MB free, ${need} MB required"
    note "disk: ${have} MB free under $root, ${need} MB required"
  fi
  note "preflight passed"
}

# ---------------------------------------------------------------- setup

write_env_file() {
  local url=$1 pwfile=$2 ca=${3:-}
  {
    printf '# Written by harklight-postgres setup. PostgreSQL backend connection.\n'
    printf '# The backend stays off until you also set ATTENTION_SERVER_STORE_BACKEND=postgres in server.env.\n'
    printf 'export ATTENTION_SERVER_POSTGRES_URL=%q\n' "$url"
    printf 'export ATTENTION_SERVER_POSTGRES_PASSWORD_FILE=%q\n' "$pwfile"
    if [[ -n "$ca" ]]; then printf 'export ATTENTION_SERVER_POSTGRES_CA_FILE=%q\n' "$ca"; fi
  } > "$env_file.tmp"
  mv -- "$env_file.tmp" "$env_file"
}

created_pgroot=0
work=""
setup_cleanup() {
  local status=$?
  if ((status != 0 && created_pgroot)); then
    printf 'setup failed; removing the cluster this run created\n' >&2
    if ((use_service)); then
      systemctl_user disable --now "$UNIT" >/dev/null 2>&1 || true
      rm -f -- "$unit_dir/$UNIT" "$unit_dir/$SERVER_UNIT.d/postgres.conf"
      rmdir "$unit_dir/$SERVER_UNIT.d" 2>/dev/null || true
      systemctl_user daemon-reload >/dev/null 2>&1 || true
    else
      "$pg_bin/pg_ctl" -D "$data_dir" -m immediate stop >/dev/null 2>&1 || true
    fi
    rm -rf -- "$pgroot"
    rm -f -- "$env_file.tmp"
  fi
}

do_setup_local() {
  do_preflight local
  [[ ! -e "$env_file" ]] || fail "$env_file already exists; PostgreSQL is already configured for this state"
  require_tools pg_ctl
  trap setup_cleanup EXIT
  mkdir -m 700 -p -- "$root"
  mkdir -m 700 -- "$pgroot"
  created_pgroot=1
  mkdir -m 700 -- "$run_dir"

  local password
  password="$(openssl rand -hex 24)"
  printf '%s\n' "$password" > "$local_password_file"
  chmod 600 "$local_password_file"

  "$pg_bin/initdb" -D "$data_dir" -U "$ADMIN_ROLE" --auth-local=peer --auth-host=reject \
    --encoding=UTF8 --locale=C.UTF-8 --data-checksums >/dev/null
  cat > "$data_dir/pg_hba.conf" <<EOF
# Harklight dedicated cluster: loopback only, one role, one database.
local   all        $ADMIN_ROLE   peer map=harklight
host    $DBNAME    $ROLE       127.0.0.1/32   scram-sha-256
EOF
  cat >> "$data_dir/postgresql.conf" <<EOF

# Harklight dedicated cluster settings.
listen_addresses = '127.0.0.1'
port = $port
unix_socket_directories = '$run_dir'
unix_socket_permissions = 0700
password_encryption = scram-sha-256
EOF
  printf 'harklight   %s   %s\n' "$(id -un)" "$ADMIN_ROLE" > "$data_dir/pg_ident.conf"
  chmod 600 "$data_dir/pg_hba.conf" "$data_dir/pg_ident.conf"

  if ((use_service)); then
    mkdir -p -- "$unit_dir"
    cat > "$unit_dir/$UNIT" <<EOF
[Unit]
Description=Harklight Server PostgreSQL (dedicated cluster)
After=network.target

[Service]
Type=simple
ExecStart=$pg_bin/postgres -D $data_dir
KillSignal=SIGINT
TimeoutStopSec=120
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
    mkdir -p -- "$unit_dir/$SERVER_UNIT.d"
    cat > "$unit_dir/$SERVER_UNIT.d/postgres.conf" <<EOF
[Unit]
Wants=$UNIT
After=$UNIT
EOF
    systemctl_user daemon-reload
    systemctl_user enable --now "$UNIT" >/dev/null 2>&1
  else
    "$pg_bin/pg_ctl" -D "$data_dir" -l "$pgroot/postgres.log" -w start >/dev/null
  fi
  wait_ready || fail "the new PostgreSQL cluster did not become ready; see $pgroot/postgres.log or journalctl --user -u $UNIT"

  psql_admin postgres >/dev/null <<EOF
CREATE ROLE $ROLE LOGIN PASSWORD '$password';
CREATE DATABASE $DBNAME OWNER $ROLE ENCODING 'UTF8' TEMPLATE template0;
REVOKE ALL ON DATABASE $DBNAME FROM PUBLIC;
EOF
  local url="postgres://$ROLE@127.0.0.1:$port/$DBNAME"
  write_env_file "$url" "$local_password_file"
  load_config
  [[ "$(psql_db -c 'select current_user')" == "$ROLE" ]] || fail "the new role cannot connect over loopback"
  trap - EXIT
  note "PostgreSQL cluster created: data $data_dir, loopback port $port, role $ROLE, database $DBNAME."
  note "Connection settings: $env_file"
  note "To use it, add this line to $state/server.env and restart Harklight Server (the backend is opt-in):"
  note "  export ATTENTION_SERVER_STORE_BACKEND=postgres"
  note "  source $env_file"
}

do_setup_remote() {
  [[ -n "$password_file" && -f "$password_file" ]] || fail "--password-file must name an existing file"
  find_pg_bin
  require_tools psql pg_dump pg_restore
  [[ -d "$state" ]] || fail "state directory $state does not exist; bootstrap Harklight first"
  [[ ! -e "$env_file" ]] || fail "$env_file already exists; PostgreSQL is already configured for this state"
  [[ "$remote_url" =~ ^postgres(ql)?:// ]] || fail "--remote-url must be a postgres:// URL without a password"
  if [[ "$remote_url" =~ ://[^/@]*:[^/@]*@ ]]; then fail "do not put a password in the URL; use --password-file"; fi
  pg_url=$remote_url pg_pwfile=$password_file pg_ca=$ca_file
  local num count major
  num="$(psql_db -c 'show server_version_num')" || fail "cannot connect to the remote database"
  major=$((num / 10000))
  ((major >= MIN_PG_MAJOR)) || fail "the remote PostgreSQL is version $major; $MIN_PG_MAJOR or newer is required"
  count="$(user_table_count)"
  [[ "$count" == 0 ]] || fail "the remote database already holds $count objects; Harklight never repurposes an existing database. Create an empty database and a role dedicated to Harklight"
  psql_db -c 'create table harklight_setup_probe(x int); drop table harklight_setup_probe' >/dev/null ||
    fail "the role cannot create tables in the remote database"
  write_env_file "$remote_url" "$password_file" "$ca_file"
  note "Remote PostgreSQL recorded in $env_file (server version $major, database empty)."
  note "To use it, add this line to $state/server.env and restart Harklight Server (the backend is opt-in):"
  note "  export ATTENTION_SERVER_STORE_BACKEND=postgres"
  note "  source $env_file"
}

do_status() {
  find_pg_bin
  load_config
  note "configuration: $env_file"
  note "database url:  $pg_url"
  if command -v systemctl >/dev/null 2>&1 && [[ -f "$unit_dir/$UNIT" ]]; then
    note "service:       $(systemctl_user is-active "$UNIT" 2>/dev/null || true)"
  fi
  if psql_db -c 'select 1' >/dev/null 2>&1; then
    note "connection:    ok, server $(psql_db -c 'show server_version'), $(user_table_count) tables, $(psql_db -c 'select pg_size_pretty(pg_database_size(current_database()))')"
  else
    note "connection:    FAILED"
    exit 1
  fi
  note "free disk:     $(free_mb "$root") MB under $root"
}

# ------------------------------------------------------- backup / restore

guard_server_stopped() {
  if ((!online)) && server_running; then
    fail "Harklight Server is running; stop it first so the database and state files match (or pass --online to accept an inconsistent copy)"
  fi
}

sha() { sha256sum -- "$1" | cut -d' ' -f1; }

do_backup() {
  local out=${positional[0]:-}
  [[ -n "$out" ]] || fail "usage: harklight-postgres backup ARCHIVE"
  [[ "$out" = /* ]] || out="$PWD/$out"
  [[ ! -e "$out" ]] || fail "$out already exists; a backup is never overwritten"
  find_pg_bin
  require_tools pg_dump psql
  load_config
  [[ -d "$state" ]] || fail "state directory $state does not exist"
  guard_server_stopped
  psql_db -c 'select 1' >/dev/null || fail "cannot connect to the database"

  local dbmb statemb need have
  dbmb="$(psql_db -c 'select (pg_database_size(current_database()) / 1048576) + 1')"
  statemb="$(tree_mb "$state")"
  need=$(((dbmb + statemb) * 2 + 16))
  have="$(free_mb "$(dirname -- "$out")")"
  ((have >= need)) || fail "not enough free disk space for the backup: ${have} MB free, about ${need} MB required"

  work="$(mktemp -d "$(dirname -- "$out")/.harklight-backup.XXXXXX")"
  trap 'rm -rf -- "$work"' EXIT
  with_db pg_dump --format=custom --no-owner --no-privileges --file "$work/database.dump" "$pg_url"
  tar -C "$state" --exclude=./postgres.env --exclude='./*.password' -cf "$work/state.tar" .
  {
    printf 'format=%s\n' "$BACKUP_FORMAT"
    printf 'created=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'postgres_server=%s\n' "$(psql_db -c 'show server_version')"
    printf 'database_sha256=%s\n' "$(sha "$work/database.dump")"
    printf 'state_sha256=%s\n' "$(sha "$work/state.tar")"
    printf 'tables=%s\n' "$(user_table_count)"
  } > "$work/MANIFEST"
  tar -C "$work" -cf "$work/archive.tar" MANIFEST database.dump state.tar
  chmod 600 "$work/archive.tar"
  mv -- "$work/archive.tar" "$out"
  note "backup written: $out ($(du -h -- "$out" | cut -f1)); it contains private keys: keep it protected"
}

do_restore() {
  local archive=${positional[0]:-}
  [[ -n "$archive" && -f "$archive" ]] || fail "usage: harklight-postgres restore ARCHIVE"
  find_pg_bin
  require_tools pg_restore psql
  load_config
  guard_server_stopped

  work="$(mktemp -d "$root/.harklight-restore.XXXXXX")"
  trap 'rm -rf -- "$work"' EXIT
  local listing
  listing="$(tar -tf "$archive")" || fail "the archive cannot be read"
  [[ "$(printf '%s\n' "$listing" | sort | tr '\n' ' ')" == "MANIFEST database.dump state.tar " ]] ||
    fail "the archive does not have the expected contents"
  tar -C "$work" -xf "$archive" MANIFEST database.dump state.tar
  grep -Fqx "format=$BACKUP_FORMAT" "$work/MANIFEST" || fail "unsupported backup format"
  grep -Fqx "database_sha256=$(sha "$work/database.dump")" "$work/MANIFEST" || fail "database dump checksum differs; the archive is damaged"
  grep -Fqx "state_sha256=$(sha "$work/state.tar")" "$work/MANIFEST" || fail "state checksum differs; the archive is damaged"
  if tar -tf "$work/state.tar" | grep -Eq '(^|/)\.\.(/|$)|^/'; then fail "the state archive has unsafe paths"; fi

  psql_db -c 'select 1' >/dev/null || fail "cannot connect to the database"
  local existing
  existing="$(user_table_count)"
  if [[ "$existing" != 0 ]] && ((!replace)); then
    fail "the database already holds $existing objects; pass --replace to overwrite it (take a backup first)"
  fi

  if [[ "$existing" != 0 ]]; then
    if [[ -d "$data_dir" ]] && [[ "$pg_url" == *"127.0.0.1:$port/$DBNAME" ]]; then
      psql_admin postgres >/dev/null <<EOF
DROP DATABASE $DBNAME WITH (FORCE);
CREATE DATABASE $DBNAME OWNER $ROLE ENCODING 'UTF8' TEMPLATE template0;
REVOKE ALL ON DATABASE $DBNAME FROM PUBLIC;
EOF
    else
      psql_db -c 'drop owned by current_user cascade' >/dev/null
      psql_db -c 'create schema if not exists public' >/dev/null
    fi
  fi
  with_db pg_restore --no-owner --no-privileges --exit-on-error --single-transaction \
    --dbname "$pg_url" "$work/database.dump"

  local expected got
  expected="$(sed -n 's/^tables=//p' "$work/MANIFEST")"
  got="$(user_table_count)"
  [[ "$expected" == "$got" ]] || fail "restored $got tables but the backup recorded $expected"

  # Swap the state directory only after the database restored cleanly.
  local staged="$work/state" previous=""
  mkdir -m 700 -- "$staged"
  tar --no-same-owner -C "$staged" -xf "$work/state.tar"
  if [[ -e "$state" ]]; then
    previous="$state.pre-restore-$(date -u +%Y%m%dT%H%M%SZ)"
    mv -- "$state" "$previous"
    [[ ! -f "$previous/postgres.env" ]] || cp -p -- "$previous/postgres.env" "$staged/postgres.env"
  fi
  mv -- "$staged" "$state"
  chmod 700 "$state"
  note "restored $got tables and the state directory to $state"
  [[ -z "$previous" ]] || note "the previous state directory was kept at $previous"
}

case "$cmd" in
  preflight) do_preflight local ;;
  setup)
    if [[ -n "$remote_url" ]]; then do_setup_remote; else do_setup_local; fi ;;
  status) do_status ;;
  backup) do_backup ;;
  restore) do_restore ;;
  *) usage ;;
esac

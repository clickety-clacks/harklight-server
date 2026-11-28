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
MIGRATOR=harklight_migrator
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
  setup --remote-url URL --password-file FILE --migrator-url URL --migrator-password-file FILE
                         use an operator-supplied empty PostgreSQL database instead
  apply-grants           give the runtime role its privileges on the migrated schema
  verify-roles           check the runtime role cannot own, alter or truncate anything
  status                 report the configured database, service and connection
  migrate [--resume DIR] copy the stopped FORMAT32 store into the empty PostgreSQL database,
                         validate it and stage identity material; never activates
  backup ARCHIVE         write one archive: database dump, state directory and checksums
  restore ARCHIVE        restore that archive (add --replace to overwrite existing data)

options:
  --root DIR             install root (default $HOME/.local/share/harklight-server)
  --state DIR            state directory (default ROOT/state)
  --pg-bin DIR           PostgreSQL binaries (default: pg_config, then /usr/lib/postgresql/*/bin)
  --port N               local cluster loopback port (default 15432)
  --min-free-mb N        override the disk requirement for preflight and setup
  --ca-file FILE         with --remote-url: server CA certificate
  --schema NAME          schema for apply-grants (default harklight)
  --no-service           run the local cluster with pg_ctl instead of a user service
                         (containers and tests; not supervised)
  --online               backup or restore while Harklight Server is running (not advised)
  --replace              restore over a database that already holds data
  --resume DIR           with migrate: continue an interrupted migration in its work directory
  --migration-bin PATH   with migrate: the importer executable (default: harklight-migration beside this command)
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
migrator_url=""
migrator_password_file=""
schema=harklight
ca_file=""
use_service=1
online=0
replace=0
resume_dir=""
migration_bin=""
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
    --migrator-url) migrator_url=${2:?}; shift 2 ;;
    --migrator-password-file) migrator_password_file=${2:?}; shift 2 ;;
    --schema) schema=${2:?}; shift 2 ;;
    --no-service) use_service=0; shift ;;
    --online) online=1; shift ;;
    --replace) replace=1; shift ;;
    --resume) resume_dir=${2:?}; shift 2 ;;
    --migration-bin) migration_bin=${2:?}; shift 2 ;;
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
migrator_env="$pgroot/migrator.env"
migrator_password="$pgroot/$MIGRATOR.password"
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

read_env_var() { (set +u; source "$1" >/dev/null 2>&1; printf '%s' "${!2:-}"); }

load_config() {
  [[ -f "$env_file" ]] || fail "no PostgreSQL configuration at $env_file; run setup first"
  [[ -f "$migrator_env" ]] || fail "no migration-role configuration at $migrator_env; run setup first"
  pg_url="$(read_env_var "$env_file" ATTENTION_SERVER_POSTGRES_URL)"
  pg_pwfile="$(read_env_var "$env_file" ATTENTION_SERVER_POSTGRES_PASSWORD_FILE)"
  pg_ca="$(read_env_var "$env_file" ATTENTION_SERVER_POSTGRES_CA_FILE)"
  mg_url="$(read_env_var "$migrator_env" HARKLIGHT_MIGRATOR_URL)"
  mg_pwfile="$(read_env_var "$migrator_env" HARKLIGHT_MIGRATOR_PASSWORD_FILE)"
  [[ -n "$pg_url" && -f "$pg_pwfile" && -n "$mg_url" && -f "$mg_pwfile" ]] ||
    fail "PostgreSQL configuration is incomplete; run setup again on a clean install"
  if [[ -f "$pgroot/cluster.port" ]]; then port="$(< "$pgroot/cluster.port")"; fi
  detect_roles
}

detect_roles() {
  rt_name="$(psql_rt -c 'select current_user')" || fail "cannot connect as the runtime role"
  mg_name="$(psql_mg -c 'select current_user')" || fail "cannot connect as the migration role"
}

# Run a client program as the runtime or the migration role. Passwords go in
# the child's environment, never on a command line.
# The Server verifies TLS for any non-loopback host, and for a loopback host
# when a CA file is configured, unless the URL says sslmode=disable. Connect
# the same way so a setup or backup never silently uses an unencrypted link.
url_host() {
  local rest="${1#*@}"
  if [[ "$rest" == \[* ]]; then printf '%s' "${rest%%]*}]"; else printf '%s' "${rest%%[:/?]*}"; fi
}
ssl_default() {
  local url=$1 host
  [[ "$url" == *sslmode=* ]] && return 0
  host="$(url_host "$url")"
  case "$host" in
    127.0.0.1|localhost|::1|\[::1\]) [[ -z "${pg_ca:-}" ]] || printf 'verify-full' ;;
    *) printf 'verify-full' ;;
  esac
}
db_run() {
  local url=$1 pwfile=$2 mode; shift 2
  local program=$1; shift
  mode="$(ssl_default "$url")"
  PGPASSWORD="$(< "$pwfile")" PGSSLROOTCERT="${pg_ca:-}" PGCONNECT_TIMEOUT=10 PGOPTIONS="-c client_min_messages=warning" \
    env ${mode:+PGSSLMODE=$mode} "$pg_bin/$program" "$@"
}
rt_run() { db_run "$pg_url" "$pg_pwfile" "$@"; }
mg_run() { db_run "$mg_url" "$mg_pwfile" "$@"; }
psql_flags=(--no-psqlrc --quiet --tuples-only --no-align --set ON_ERROR_STOP=1)
psql_rt() { rt_run psql "${psql_flags[@]}" "$pg_url" "$@"; }
psql_mg() { mg_run psql "${psql_flags[@]}" "$mg_url" "$@"; }

psql_admin() {
  "$pg_bin/psql" "${psql_flags[@]}" -h "$run_dir" -p "$port" -U "$ADMIN_ROLE" -d "${1:-postgres}"
}

user_table_count() {
  psql_mg -c "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r','p','v','m') and n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'"
}

# Privileges the runtime role gets on one schema. Run as the migration role.
# Objects the migration role creates later get the same data privileges by
# default; the three lifecycle tables are read-only for the runtime role and
# are re-restricted whenever grants are applied.
grants_sql() {
  local sch=$1
  cat <<SQL
GRANT USAGE ON SCHEMA $sch TO "$rt_name";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA $sch TO "$rt_name";
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA $sch TO "$rt_name";
ALTER DEFAULT PRIVILEGES FOR ROLE "$mg_name" IN SCHEMA $sch GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "$rt_name";
ALTER DEFAULT PRIVILEGES FOR ROLE "$mg_name" IN SCHEMA $sch GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO "$rt_name";
DO \$\$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['schema_migrations', 'import_state', 'server_identity'] LOOP
    IF to_regclass(format('%I.%I', '$sch', t)) IS NOT NULL THEN
      EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON %I.%I FROM "$rt_name"', '$sch', t);
    END IF;
  END LOOP;
END \$\$;
SQL
}

# Catalog check that the runtime role cannot own, alter, truncate, disable
# triggers on or create anything. Prints one problem per line; empty = pass.
role_problems() {
  psql_mg <<SQL
select 'runtime role has a privileged attribute' from pg_roles
  where rolname = '$rt_name' and (rolsuper or rolcreatedb or rolcreaterole or rolreplication or rolbypassrls);
select 'runtime role is a member of ' || r.rolname from pg_auth_members m
  join pg_roles r on r.oid = m.roleid join pg_roles u on u.oid = m.member where u.rolname = '$rt_name';
select 'runtime role owns or can act as the owner of ' || n.nspname || '.' || c.relname
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'
    and c.relkind in ('r','p','v','m','S') and pg_has_role('$rt_name', c.relowner, 'USAGE');
select 'runtime role can ' || p || ' ' || n.nspname || '.' || c.relname
  from pg_class c join pg_namespace n on n.oid = c.relnamespace, unnest(array['TRUNCATE','REFERENCES','TRIGGER']) p
  where n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'
    and c.relkind in ('r','p') and has_table_privilege('$rt_name', c.oid, p);
select 'runtime role can write lifecycle table ' || n.nspname || '.' || c.relname
  from pg_class c join pg_namespace n on n.oid = c.relnamespace, unnest(array['INSERT','UPDATE','DELETE']) p
  where n.nspname = '$schema' and c.relname in ('schema_migrations','import_state','server_identity')
    and has_table_privilege('$rt_name', c.oid, p);
select 'runtime role can create in schema ' || n.nspname from pg_namespace n
  where n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'
    and (has_schema_privilege('$rt_name', n.oid, 'CREATE') or pg_has_role('$rt_name', n.nspowner, 'USAGE'));
select 'runtime role can create schemas in the database'
  where has_database_privilege('$rt_name', current_database(), 'CREATE');
select 'runtime role and migration role are the same' where '$rt_name' = '$mg_name';
SQL
}

check_roles() {
  local problems
  problems="$(role_problems)" || fail "could not check the runtime role's privileges"
  if [[ -n "$problems" ]]; then
    printf '%s\n' "$problems" >&2
    fail "the runtime role has more privilege than allowed; refusing"
  fi
}

# Exercise the privilege boundary for real in a throwaway schema.
probe_boundary() {
  local sch=hk_probe_$$
  if ! ( probe_body "$sch" ); then
    psql_mg -c "drop schema if exists $sch cascade" >/dev/null 2>&1 || true
    exit 1
  fi
  psql_mg -c "drop schema if exists $sch cascade" >/dev/null
  note "privilege boundary verified: runtime role cannot truncate, alter, drop, create, disable triggers or write lifecycle tables"
}

probe_body() {
  local sch=$1 sql
  psql_mg >/dev/null <<SQL
create schema $sch;
create table $sch.t(id int primary key, v text);
create function $sch.noop() returns trigger language plpgsql as \$\$ begin return new; end \$\$;
create trigger t_noop before insert on $sch.t for each row execute function $sch.noop();
create table $sch.schema_migrations(version int);
create table $sch.import_state(x int);
create table $sch.server_identity(x int);
$(grants_sql "$sch")
SQL
  psql_rt >/dev/null -c "insert into $sch.t values (1,'a'); update $sch.t set v='b'; select * from $sch.t; delete from $sch.t" ||
    fail "the runtime role cannot use its granted data privileges"
  psql_rt >/dev/null -c "select * from $sch.import_state; select * from $sch.schema_migrations; select * from $sch.server_identity" ||
    fail "the runtime role cannot read the lifecycle tables"
  local -a denied=(
    "truncate $sch.t"
    "alter table $sch.t add column z int"
    "alter table $sch.t disable trigger all"
    "alter table $sch.t disable trigger t_noop"
    "alter table $sch.t owner to \"$rt_name\""
    "drop table $sch.t"
    "create table $sch.extra(i int)"
    "create schema hk_runtime_schema_$$"
    "insert into $sch.import_state values (1)"
    "update $sch.schema_migrations set version = 1"
    "delete from $sch.server_identity"
    "drop trigger t_noop on $sch.t"
  )
  for sql in "${denied[@]}"; do
    if psql_rt >/dev/null 2>&1 -c "$sql"; then
      fail "boundary failure: the runtime role was allowed to: $sql"
    fi
  done
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
      [[ ! -e "$unit_dir/$UNIT" ]] || fail "$unit_dir/$UNIT already exists; one supervised local cluster per user. Remove that install deliberately or use --no-service"
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
    printf '# Written by harklight-postgres setup. Runtime (non-owner) PostgreSQL connection.\n'
    printf '# The backend stays off until you also set ATTENTION_SERVER_STORE_BACKEND=postgres in server.env.\n'
    printf 'export ATTENTION_SERVER_POSTGRES_URL=%q\n' "$url"
    printf 'export ATTENTION_SERVER_POSTGRES_PASSWORD_FILE=%q\n' "$pwfile"
    if [[ -n "$ca" ]]; then printf 'export ATTENTION_SERVER_POSTGRES_CA_FILE=%q\n' "$ca"; fi
  } > "$env_file.tmp"
  mv -- "$env_file.tmp" "$env_file"
}

write_migrator_env() {
  local url=$1 pwfile=$2
  {
    printf '# Migration/owner role. Never source this into server.env or the Server process.\n'
    printf 'export HARKLIGHT_MIGRATOR_URL=%q\n' "$url"
    printf 'export HARKLIGHT_MIGRATOR_PASSWORD_FILE=%q\n' "$pwfile"
  } > "$migrator_env"
  chmod 600 "$migrator_env"
}

created_pgroot=0
unit_created=0
work=""
setup_cleanup() {
  local status=$?
  if ((status != 0 && created_pgroot)); then
    printf 'setup failed; removing what this run created\n' >&2
    if [[ -d "$data_dir" ]]; then
      if ((use_service && unit_created)); then
        systemctl_user disable --now "$UNIT" >/dev/null 2>&1 || true
        rm -f -- "$unit_dir/$UNIT" "$unit_dir/$SERVER_UNIT.d/postgres.conf"
        rmdir "$unit_dir/$SERVER_UNIT.d" 2>/dev/null || true
        systemctl_user daemon-reload >/dev/null 2>&1 || true
      else
        "$pg_bin/pg_ctl" -D "$data_dir" -m immediate stop >/dev/null 2>&1 || true
      fi
    fi
    rm -rf -- "$pgroot"
    rm -f -- "$env_file" "$env_file.tmp"
  fi
}

new_password() { openssl rand -hex 24; }

do_setup_local() {
  do_preflight local
  [[ ! -e "$env_file" ]] || fail "$env_file already exists; PostgreSQL is already configured for this state"
  require_tools pg_ctl
  trap setup_cleanup EXIT
  mkdir -m 700 -p -- "$root"
  mkdir -m 700 -- "$pgroot"
  created_pgroot=1
  mkdir -m 700 -- "$run_dir"

  local rt_pw mg_pw
  rt_pw="$(new_password)"; mg_pw="$(new_password)"
  printf '%s\n' "$rt_pw" > "$local_password_file"
  printf '%s\n' "$mg_pw" > "$migrator_password"
  chmod 600 "$local_password_file" "$migrator_password"

  "$pg_bin/initdb" -D "$data_dir" -U "$ADMIN_ROLE" --auth-local=peer --auth-host=reject \
    --encoding=UTF8 --locale=C.UTF-8 --data-checksums >/dev/null
  cat > "$data_dir/pg_hba.conf" <<EOF
# Harklight dedicated cluster: loopback only, two roles, one database.
local   all        $ADMIN_ROLE   peer map=harklight
host    $DBNAME    $ROLE         127.0.0.1/32   scram-sha-256
host    $DBNAME    $MIGRATOR     127.0.0.1/32   scram-sha-256
EOF
  printf 'harklight   %s   %s\n' "$(id -un)" "$ADMIN_ROLE" > "$data_dir/pg_ident.conf"
  chmod 600 "$data_dir/pg_hba.conf" "$data_dir/pg_ident.conf"
  cat >> "$data_dir/postgresql.conf" <<EOF

# Harklight dedicated cluster settings.
listen_addresses = '127.0.0.1'
port = $port
unix_socket_directories = '$run_dir'
unix_socket_permissions = 0700
password_encryption = scram-sha-256
EOF
  printf '%s\n' "$port" > "$pgroot/cluster.port"

  if ((use_service)); then
    mkdir -p -- "$unit_dir"
    unit_created=1
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
CREATE ROLE $MIGRATOR LOGIN PASSWORD '$mg_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE $ROLE LOGIN PASSWORD '$rt_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE DATABASE $DBNAME OWNER $MIGRATOR ENCODING 'UTF8' TEMPLATE template0;
REVOKE ALL ON DATABASE $DBNAME FROM PUBLIC;
GRANT CONNECT ON DATABASE $DBNAME TO $ROLE;
EOF
  write_env_file "postgres://$ROLE@127.0.0.1:$port/$DBNAME?sslmode=disable" "$local_password_file"
  write_migrator_env "postgres://$MIGRATOR@127.0.0.1:$port/$DBNAME?sslmode=disable" "$migrator_password"
  load_config
  psql_mg -c 'revoke create on schema public from public' >/dev/null
  [[ "$(psql_rt -c 'select current_user')" == "$ROLE" ]] || fail "the runtime role cannot connect over loopback"
  [[ "$(psql_mg -c 'select current_user')" == "$MIGRATOR" ]] || fail "the migration role cannot connect over loopback"
  check_roles
  probe_boundary
  trap - EXIT
  note "PostgreSQL cluster created: data $data_dir, loopback port $port, database $DBNAME."
  note "Roles: $MIGRATOR owns the schema and runs migrations (credentials in $pgroot, outside the state directory); $ROLE is the non-owner runtime login."
  note "Runtime connection settings: $env_file"
  note "To use it, add these lines to $state/server.env and restart Harklight Server (the backend is opt-in):"
  note "  export ATTENTION_SERVER_STORE_BACKEND=postgres"
  note "  source $env_file"
  note "After the schema is migrated, run: harklight-postgres apply-grants && harklight-postgres verify-roles"
}

# The Server accepts only these runtime URL forms; the migration URL is held to the same rule: a user, no password, a
# database, and either no query, sslmode=verify-full, or sslmode=disable on a
# loopback host. Anything else would be rejected at Server start.
validate_role_url() {
  python3 -I - "$1" "${ca_file:-}" <<'PY' || exit 1
import sys
from urllib.parse import urlsplit, parse_qsl, unquote
u = urlsplit(sys.argv[1])
def bad(msg):
    sys.stderr.write("harklight-postgres: " + msg + "\n"); sys.exit(1)
if not u.hostname or not u.username or u.password is not None or u.fragment:
    bad("each database URL needs a host and a user, and no password")
db = unquote(u.path.lstrip("/"))
if not db or "/" in db or ":" in unquote(u.username):
    bad("each database URL needs a plain database name and user")
q = parse_qsl(u.query, keep_blank_values=True)
loop = u.hostname in ("127.0.0.1", "localhost", "::1")
if q == [] or q == [("sslmode", "verify-full")]:
    pass
elif q == [("sslmode", "disable")]:
    if not loop:
        bad("sslmode=disable is only accepted for a loopback host; use sslmode=verify-full")
    if sys.argv[2]:
        bad("sslmode=disable cannot be combined with a CA file; the Server rejects that combination")
else:
    bad("a database URL may carry only sslmode=verify-full (or sslmode=disable on loopback); other query parameters, including any that carry credentials, are refused")
PY
}

do_setup_remote() {
  [[ "$password_file" = /* && "${migrator_password_file:-/}" = /* ]] || fail "password files must be absolute paths"
  [[ -n "$password_file" && -f "$password_file" ]] || fail "--password-file must name the runtime role's password file"
  [[ -n "$migrator_url" && -n "$migrator_password_file" && -f "$migrator_password_file" ]] ||
    fail "a remote database needs --migrator-url and --migrator-password-file for a separate owner role"
  find_pg_bin
  require_tools psql pg_dump pg_restore
  [[ -d "$state" ]] || fail "state directory $state does not exist; bootstrap Harklight first"
  [[ ! -e "$env_file" && ! -e "$pgroot" ]] || fail "PostgreSQL is already configured for this install"
  local u
  for u in "$remote_url" "$migrator_url"; do
    [[ "$u" =~ ^postgres(ql)?:// ]] || fail "database URLs must be postgres:// URLs without a password"
    if [[ "$u" =~ ://[^/@]*:[^/@]*@ ]]; then fail "do not put a password in a URL; use password files"; fi
  done
  [[ -z "$ca_file" || "$ca_file" = /* ]] || fail "--ca-file must be an absolute path"
  [[ -z "$ca_file" || -f "$ca_file" ]] || fail "--ca-file does not exist"
  validate_role_url "$remote_url"
  validate_role_url "$migrator_url"
  [[ "$remote_url" != "$migrator_url" ]] || fail "the runtime and migration roles must be different roles"
  pg_url=$remote_url pg_pwfile=$password_file pg_ca=$ca_file
  mg_url=$migrator_url mg_pwfile=$migrator_password_file
  local num count major
  num="$(psql_mg -c 'show server_version_num')" || fail "cannot connect to the remote database as the migration role"
  psql_rt -c 'select 1' >/dev/null || fail "cannot connect to the remote database as the runtime role"
  [[ "$(psql_rt -c 'select current_user')" != "$(psql_mg -c 'select current_user')" ]] || fail "the runtime and migration roles must be different roles"
  major=$((num / 10000))
  ((major >= MIN_PG_MAJOR)) || fail "the remote PostgreSQL is version $major; $MIN_PG_MAJOR or newer is required"
  count="$(user_table_count)"
  [[ "$count" == 0 ]] || fail "the remote database already holds $count objects; Harklight never repurposes an existing database. Create an empty database with a dedicated owner and runtime role"
  detect_roles
  check_roles
  probe_boundary
  mkdir -m 700 -p -- "$pgroot"
  write_migrator_env "$migrator_url" "$migrator_password_file"
  write_env_file "$remote_url" "$password_file" "$ca_file"
  note "Remote PostgreSQL recorded (server version $major, database empty, runtime role verified non-owner)."
  note "Runtime connection settings: $env_file; migration role: $migrator_env"
  note "To use it, add these lines to $state/server.env and restart Harklight Server (the backend is opt-in):"
  note "  export ATTENTION_SERVER_STORE_BACKEND=postgres"
  note "  source $env_file"
  note "After the schema is migrated, run: harklight-postgres apply-grants && harklight-postgres verify-roles"
}

do_apply_grants() {
  find_pg_bin
  load_config
  [[ "$schema" =~ ^[a-z_][a-z0-9_]*$ ]] || fail "invalid schema name"
  [[ "$(psql_mg -c "select count(*) from pg_namespace where nspname = '$schema'")" == 1 ]] ||
    fail "schema $schema does not exist yet; run the migration first"
  grants_sql "$schema" | psql_mg >/dev/null
  note "runtime role privileges applied to schema $schema"
  check_roles
  note "runtime role verified: non-owner, no TRUNCATE, no ALTER, no trigger control, lifecycle tables read-only"
}

do_verify_roles() {
  find_pg_bin
  load_config
  check_roles
  probe_boundary
  note "runtime role verified"
}

do_status() {
  find_pg_bin
  load_config
  note "runtime configuration:   $env_file"
  note "runtime database url:    $pg_url"
  if command -v systemctl >/dev/null 2>&1 && [[ -f "$unit_dir/$UNIT" ]]; then
    note "service:                 $(systemctl_user is-active "$UNIT" 2>/dev/null || true)"
  fi
  if psql_rt -c 'select 1' >/dev/null 2>&1 && psql_mg -c 'select 1' >/dev/null 2>&1; then
    note "connections:             ok (runtime and migration roles), server $(psql_mg -c 'show server_version'), $(user_table_count) tables, $(psql_mg -c 'select pg_size_pretty(pg_database_size(current_database()))')"
  else
    note "connections:             FAILED"
    exit 1
  fi
  local problems
  problems="$(role_problems 2>/dev/null || true)"
  if [[ -n "$problems" ]]; then note "role boundary:           PROBLEMS"; printf '%s\n' "$problems"; exit 1; fi
  note "role boundary:           ok"
  note "free disk:               $(free_mb "$root") MB under $root"
}

# ----------------------------------------------------------- migrate

json_field() {
  python3 -I -c 'import json,sys
d = json.loads(sys.argv[1]); v = d.get(sys.argv[2])
print("" if v is None else v)' "$1" "$2"
}

migrate_failed() {
  local step=$1 work_dir=$2
  cat >&2 <<EOF
harklight-postgres: migration step "$step" failed.
Nothing was activated and no backend setting was changed. Keep the old Server stopped.
Do not delete the destination, rerun bootstrap, or touch the backup in $work_dir/backup.
Check progress: run the importer's status command with receipt $work_dir/receipt.json.
Fix the reported problem and continue with: harklight-postgres migrate --resume $work_dir
Or abandon this unpublished destination and restore the original backup; that rollback
is only valid before PostgreSQL has accepted any new writes.
EOF
  exit 2
}

# Run one importer step. Exit 0 and a JSON object on stdout are required.
migration_step() {
  local step=$1; shift
  local out
  if ! out="$("$migration_bin" "$@" 2> "$work_dir/$step.err")"; then migrate_failed "$step" "$work_dir"; fi
  case "$step" in
    export|verify)   # their existing output shape is preserved by the contract
      [[ "$(json_field "$out" result 2>/dev/null || true)" == ok ]] || migrate_failed "$step (unexpected output)" "$work_dir" ;;
    *)
      [[ "$(json_field "$out" contract_version 2>/dev/null || true)" == 1 ]] || migrate_failed "$step (unexpected output)" "$work_dir" ;;
  esac
  printf '%s\n' "$out" > "$work_dir/$step.json"
  printf '%s' "$out"
}

# Verify the importer's staged identity material against its COMPLETE.json and
# install only the listed files at their mapped locations. Fail closed.
install_material() {
  python3 -I - "$work_dir/validate.json" "$work_dir/material-map.json" "$work_dir" <<'PY' || return 1
import hashlib, json, os, shutil, sys
vpath, mpath, work = sys.argv[1:4]
def die(msg):
    sys.stderr.write("harklight-postgres: material check failed: " + msg + "\n"); sys.exit(1)
def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
v = json.load(open(vpath)); mapping = json.load(open(mpath))
mat = v.get("material")
if not isinstance(mat, dict) or not all(k in mat for k in ("root", "manifest_sha256", "files")):
    die("validate output has no complete material section")
root = os.path.realpath(mat["root"]); parent = os.path.dirname(root)
if os.path.commonpath([os.path.realpath(work), root]) != os.path.realpath(work):
    die("staging root is outside the migration work directory")
def private(path, what):
    if os.stat(path).st_mode & 0o077:
        die(what + " is not private (mode " + oct(os.stat(path).st_mode & 0o777) + "): " + path)
d = root
while True:
    private(d, "a staging directory")
    if d == os.path.realpath(work):
        break
    d = os.path.dirname(d)
complete = os.path.join(parent, "COMPLETE.json")
if not os.path.isfile(complete) or sha(complete) != mat["manifest_sha256"]:
    die("COMPLETE.json is missing or does not match its recorded digest")
c = json.load(open(complete))
for k in ("import_id", "source_sha256", "schema_sha256"):
    if c.get(k) != v.get(k):
        die("COMPLETE.json identity differs from the validated import (" + k + ")")
listed = {}
for f in mat["files"]:
    rel = f.get("path", "")
    if not rel or rel.startswith("/") or "//" in rel or ".." in rel.split("/"):
        die("unsafe staged path " + repr(rel))
    src = os.path.realpath(os.path.join(root, rel))
    if os.path.commonpath([root, src]) != root or not os.path.isfile(src):
        die("staged file missing or outside the staging root: " + rel)
    private(src, "a staged file")
    if os.path.getsize(src) != f.get("size") or sha(src) != f.get("sha256"):
        die("staged file length or hash differs: " + rel)
    listed[rel] = src
missing = sorted(set(mapping) - set(listed))
if missing:
    die("required material was not staged: " + ", ".join(missing))
extra = sorted(set(listed) - set(mapping))
if extra:
    die("staged files outside the export map: " + ", ".join(extra))
plan = []
for rel, src in sorted(listed.items()):
    dest = mapping[rel]
    if os.path.exists(dest):
        if sha(dest) != sha(src):
            die("the existing file differs from the verified staged copy and was not overwritten: " + dest)
        if os.stat(dest).st_mode & 0o077:
            die("the existing identity file is identical but not private (mode " + oct(os.stat(dest).st_mode & 0o777) + "); fix its mode and resume: " + dest)
        continue
    plan.append((src, dest))
for src, dest in plan:
    os.makedirs(os.path.dirname(dest), mode=0o700, exist_ok=True)
    tmp = dest + ".hk-install"
    shutil.copyfile(src, tmp); os.chmod(tmp, 0o600); os.replace(tmp, dest)
print("installed %d file(s); %d already identical" % (len(plan), len(listed) - len(plan)))
PY
}

# Prove the old Server is stopped on every invocation, resume included:
# the unit must be inactive or failed with no main process.
require_stopped() {
  command -v systemctl >/dev/null 2>&1 || fail "cannot prove Harklight Server is stopped (no systemctl); stop it by hand and use a systemd user session for the migration"
  local active pid
  active="$(systemctl_user show --property=ActiveState --value "$SERVER_UNIT" 2>/dev/null)" ||
    fail "cannot read the state of $SERVER_UNIT; refusing to migrate"
  pid="$(systemctl_user show --property=MainPID --value "$SERVER_UNIT" 2>/dev/null)" ||
    fail "cannot read the process of $SERVER_UNIT; refusing to migrate"
  [[ "$pid" == 0 && ( "$active" == inactive || "$active" == failed ) ]] ||
    fail "Harklight Server is not proven stopped ($SERVER_UNIT is $active, main process $pid); stop it and keep it stopped"
}

# The importer must really implement its read-only status command before any
# side effect. An unimplemented command refuses with invalid_arguments; an
# implemented one reports a different stable code for a missing receipt.
require_importer() {
  [[ -x "$migration_bin" ]] || fail "the packaged importer ($migration_bin) is not available, so the store cannot be migrated. Nothing was changed."
  local err
  err="$("$migration_bin" status --receipt "$root/migration/no-such-receipt.json" 2>&1 >/dev/null || true)"
  if [[ -z "$err" || "$err" == *invalid_arguments* ]]; then
    fail "this release's importer does not implement import, validate and activate yet, so the store cannot be migrated. Nothing was changed."
  fi
}

migrate_setup() {
  find_pg_bin
  load_config
  local self_dir
  self_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
  migration_bin="${migration_bin:-$self_dir/harklight-migration}"
  require_importer
  [[ -f "$state/relay.sqlite3" ]] || fail "no FORMAT32 store at $state/relay.sqlite3"
  require_stopped
  export HARKLIGHT_MIGRATOR_URL="$mg_url" HARKLIGHT_MIGRATOR_PASSWORD_FILE="$mg_pwfile"
  if [[ -n "${pg_ca:-}" ]]; then export HARKLIGHT_MIGRATOR_CA_FILE="$pg_ca"; fi
}

material_failed() {
  cat >&2 <<EOF
harklight-postgres: the importer's identity material could not be verified and installed.
The destination has NOT been activated by this command. Do not start Harklight Server.
Fix the cause and run: harklight-postgres migrate --resume $work_dir
EOF
  exit 2
}

do_migrate() {
  migrate_setup
  if [[ -s "$state/relay.sqlite3-wal" || -s "$state/relay.sqlite3-journal" ]]; then
    fail "the stopped store is not checkpointed; start the current release once and stop it cleanly, then retry"
  fi
  if [[ -n "$resume_dir" ]]; then
    work_dir="$resume_dir"
    [[ -d "$work_dir/backup" ]] || fail "$work_dir is not a migration work directory"
  else
    [[ "$(user_table_count)" == 0 ]] || fail "the PostgreSQL database already holds objects; a migration needs an empty, unpublished destination"
    mkdir -m 700 -p -- "$root/migration"
    work_dir="$(mktemp -d "$root/migration/$(date -u +%Y%m%dT%H%M%SZ).XXXXXX")"
    chmod 700 "$work_dir"
  fi
  note "migration work directory: $work_dir"
  local bundle="$work_dir/bundle" receipt="$work_dir/receipt.json" out validation_sha externals

  if [[ ! -f "$work_dir/done.backup" ]]; then
    mkdir -m 700 -p -- "$work_dir/backup"
    tar --exclude=./releases --exclude=./backups --exclude=./postgres --exclude=./migration \
      --exclude=./.release-install.lock -cf "$work_dir/backup/private-root.tar" -C "$root" . || migrate_failed backup "$work_dir"
    chmod 600 "$work_dir/backup/private-root.tar"
    sha "$work_dir/backup/private-root.tar" > "$work_dir/backup/SHA256"
    : > "$work_dir/done.backup"
    note "immutable backup written: $work_dir/backup/private-root.tar"
  fi

  if [[ ! -f "$work_dir/done.export" ]]; then
    externals="$(external_refs)" || migrate_failed "material map (an external path cannot be captured)" "$work_dir"
    python3 -I - "$state" "$work_dir/material-map.json" "$externals" <<'PY' || migrate_failed "material map" "$work_dir"
import json, os, sys
state, out, externals = sys.argv[1:4]
def walk_error(err):
    raise err
m = {}
for name in ("server.env", "person-id"):
    path = os.path.join(state, name)
    if os.path.isfile(path):
        m[name] = path
for sub in ("tls", "owner"):
    base = os.path.join(state, sub)
    if not os.path.isdir(base):
        continue
    for dirpath, _, files in os.walk(base, onerror=walk_error):
        for f in files:
            p = os.path.join(dirpath, f)
            if os.path.isfile(p):
                m[os.path.relpath(p, state)] = p
for line in externals.splitlines():
    if "\t" in line:
        _, p = line.split("\t", 1)
        m["external" + p] = p
with open(out, "w") as fh:
    json.dump(m, fh, indent=1, sort_keys=True)
os.chmod(out, 0o600)
PY
    rm -rf -- "$bundle"
    migration_step export export --source "$state/relay.sqlite3" --output "$bundle" --unit "$SERVER_UNIT" --material-map "$work_dir/material-map.json" >/dev/null
    migration_step verify verify "$bundle" >/dev/null
    : > "$work_dir/done.export"
    note "export and verify passed"
  fi

  if [[ ! -f "$work_dir/done.validate" ]]; then
    migration_step import import --bundle "$bundle" --receipt "$receipt" >/dev/null
    note "import completed"
    out="$(migration_step validate validate --bundle "$bundle" --receipt "$receipt")"
    validation_sha="$(json_field "$out" validation_sha256)"
    [[ "$validation_sha" =~ ^[0-9a-f]{64}$ ]] || migrate_failed "validate (no validation digest)" "$work_dir"
    : > "$work_dir/done.validate"
    note "validation passed ($validation_sha)"
  fi
  "$0" apply-grants --root "$root" --state "$state" --pg-bin "$pg_bin" >/dev/null || migrate_failed "apply-grants" "$work_dir"
  "$0" verify-roles --root "$root" --state "$state" --pg-bin "$pg_bin" >/dev/null || migrate_failed "verify-roles" "$work_dir"
  if ! out="$(install_material)"; then material_failed; fi
  note "identity material verified: $out"
  note "The destination is imported and validated. It has NOT been activated; server.env was not changed and Harklight Server was not started."
  note "Activation is not part of this command and is not available yet."
}

# ------------------------------------------------------- backup / restore

guard_server_stopped() {
  if ((!online)) && server_running; then
    fail "Harklight Server is running; stop it first so the database and state files match (or pass --online to accept an inconsistent copy)"
  fi
}

sha() { sha256sum -- "$1" | cut -d' ' -f1; }

# Files and directories server.env points at outside the state directory.
# Regular files are captured into the archive; anything else is refused so a
# backup never silently lacks bytes the Server needs. Prints NAME<TAB>path.
external_refs() {
  [[ -f "$state/server.env" ]] || return 0
  python3 -I - "$state" "$root" "$state/server.env" <<'PY'
import os, subprocess, sys
state, root, envfile = sys.argv[1:4]
def dump(cmd):
    out = subprocess.run(["env", "-i", "HOME=" + os.environ["HOME"], "PATH=" + os.environ["PATH"],
                          "bash", "-c", cmd, "x", envfile], capture_output=True).stdout
    return dict(kv.split(b"=", 1) for kv in out.split(b"\0") if b"=" in kv)
base = dump("env -0")
new = dump('set -a; source "$1" >/dev/null 2>&1; env -0')
inside = [os.path.realpath(p) for p in
          (state, root + "/releases", root + "/current", root + "/bin", root + "/postgres", root + "/backups")]
refused, found = [], []
for name, value in sorted(new.items()):
    if base.get(name) == value or name in (b"PWD", b"SHLVL", b"_", b"OLDPWD"):
        continue
    name, value = name.decode(), value.decode(errors="replace")
    if not value.startswith("/") or name.endswith(("POSTGRES_PASSWORD_FILE", "POSTGRES_CA_FILE")):
        continue
    if "\t" in value or "\n" in value or "/../" in value + "/":
        refused.append(name + "=" + value + " (unsupported characters)"); continue
    real = os.path.realpath(value)
    if any(real == d or real.startswith(d + "/") for d in inside):
        continue
    try:
        st = os.stat(real)
    except FileNotFoundError:
        continue
    except OSError as err:
        refused.append(name + "=" + value + " (cannot be inspected: " + err.strerror + ")"); continue
    import stat as statmod
    if statmod.S_ISREG(st.st_mode) and os.access(real, os.R_OK):
        found.append((name, value))
    else:
        refused.append(name + "=" + value + " (not a readable regular file)")
if refused:
    sys.stderr.write("harklight-postgres: server.env refers to paths outside the state directory that a backup cannot capture:\n  "
                     + "\n  ".join(refused) + "\nMove them into the state directory or make them regular files; refusing to write an incomplete backup.\n")
    sys.exit(3)
for name, value in found:
    print(name + "\t" + value)
PY
}

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
  psql_mg -c 'select 1' >/dev/null || fail "cannot connect to the database"

  local dbmb statemb need have
  dbmb="$(psql_mg -c 'select (pg_database_size(current_database()) / 1048576) + 1')"
  statemb="$(tree_mb "$state")"
  need=$(((dbmb + statemb) * 2 + 16))
  have="$(free_mb "$(dirname -- "$out")")"
  ((have >= need)) || fail "not enough free disk space for the backup: ${have} MB free, about ${need} MB required"

  work="$(mktemp -d "$(dirname -- "$out")/.harklight-backup.XXXXXX")"
  trap 'rm -rf -- "$work"' EXIT
  mg_run pg_dump --format=custom --no-owner --no-privileges --file "$work/database.dump" "$mg_url"
  tar -C "$state" --exclude=./postgres.env -cf "$work/state.tar" .
  local refs ext_n=0 name path
  refs="$(external_refs)" || exit 1
  mkdir -m 700 "$work/external"
  : > "$work/external/MAP"
  while IFS=$'\t' read -r name path; do
    [[ -n "$name" ]] || continue
    cp -p -- "$path" "$work/external/f$ext_n"
    printf 'f%s\t%s\t%s\t%s\n' "$ext_n" "$path" "$(sha "$work/external/f$ext_n")" "$(stat -c %a -- "$path")" >> "$work/external/MAP"
    ext_n=$((ext_n + 1))
  done <<< "$refs"
  tar -C "$work/external" -cf "$work/external.tar" .
  {
    printf 'format=%s\n' "$BACKUP_FORMAT"
    printf 'created=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'postgres_server=%s\n' "$(psql_mg -c 'show server_version')"
    printf 'database_sha256=%s\n' "$(sha "$work/database.dump")"
    printf 'state_sha256=%s\n' "$(sha "$work/state.tar")"
    printf 'external_sha256=%s\n' "$(sha "$work/external.tar")"
    printf 'external_files=%s\n' "$ext_n"
    printf 'tables=%s\n' "$(user_table_count)"
  } > "$work/MANIFEST"
  tar -C "$work" -cf "$work/archive.tar" MANIFEST database.dump state.tar external.tar
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
  [[ "$(printf '%s\n' "$listing" | sort | tr '\n' ' ')" == "MANIFEST database.dump external.tar state.tar " ]] ||
    fail "the archive does not have the expected contents"
  tar -C "$work" -xf "$archive" MANIFEST database.dump state.tar external.tar
  grep -Fqx "format=$BACKUP_FORMAT" "$work/MANIFEST" || fail "unsupported backup format"
  grep -Fqx "database_sha256=$(sha "$work/database.dump")" "$work/MANIFEST" || fail "database dump checksum differs; the archive is damaged"
  grep -Fqx "state_sha256=$(sha "$work/state.tar")" "$work/MANIFEST" || fail "state checksum differs; the archive is damaged"
  grep -Fqx "external_sha256=$(sha "$work/external.tar")" "$work/MANIFEST" || fail "external files checksum differs; the archive is damaged"
  mkdir -m 700 "$work/external"
  tar --no-same-owner -C "$work/external" -xf "$work/external.tar"
  local ext_conflicts="" eid edest esha emode
  while IFS=$'\t' read -r eid edest esha emode; do
    [[ -n "$eid" ]] || continue
    [[ "$eid" =~ ^f[0-9]+$ && "$edest" = /* && "/$edest/" != *"/../"* ]] || fail "the archive's external file map is unsafe"
    [[ "$(sha "$work/external/$eid")" == "$esha" ]] || fail "an external file in the archive is damaged"
    if [[ -e "$edest" && "$(sha "$edest" 2>/dev/null || true)" != "$esha" ]]; then ext_conflicts+=" $edest"; fi
  done < "$work/external/MAP"
  if [[ -n "$ext_conflicts" ]] && ((!replace)); then
    fail "these files outside the state directory already exist with different content (pass --replace to keep them aside as .pre-restore and restore the archived ones):$ext_conflicts"
  fi
  if tar -tf "$work/state.tar" | grep -Eq '(^|/)\.\.(/|$)|^/'; then fail "the state archive has unsafe paths"; fi

  psql_mg -c 'select 1' >/dev/null || fail "cannot connect to the database"
  local existing
  existing="$(user_table_count)"
  if [[ "$existing" != 0 ]] && ((!replace)); then
    fail "the database already holds $existing objects; pass --replace to overwrite it (take a backup first)"
  fi

  if [[ "$existing" != 0 ]]; then
    if [[ -f "$pgroot/cluster.port" && -d "$data_dir" ]]; then
      psql_admin postgres >/dev/null <<EOF
DROP DATABASE $DBNAME WITH (FORCE);
CREATE DATABASE $DBNAME OWNER $MIGRATOR ENCODING 'UTF8' TEMPLATE template0;
REVOKE ALL ON DATABASE $DBNAME FROM PUBLIC;
GRANT CONNECT ON DATABASE $DBNAME TO $ROLE;
EOF
      psql_mg -c 'revoke create on schema public from public' >/dev/null
    else
      psql_mg -c 'drop owned by current_user cascade' >/dev/null
      psql_mg -c 'create schema if not exists public' >/dev/null
    fi
  fi
  mg_run pg_restore --no-owner --no-privileges --exit-on-error --single-transaction \
    --dbname "$mg_url" "$work/database.dump"

  local expected got
  expected="$(sed -n 's/^tables=//p' "$work/MANIFEST")"
  got="$(user_table_count)"
  [[ "$expected" == "$got" ]] || fail "restored $got tables but the backup recorded $expected"
  # Grants are not part of the dump; give the runtime role its privileges back.
  if [[ "$(psql_mg -c "select count(*) from pg_namespace where nspname = '$schema'")" == 1 ]]; then
    grants_sql "$schema" | psql_mg >/dev/null
  fi
  check_roles

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
  while IFS=$'\t' read -r eid edest esha emode; do
    [[ -n "$eid" ]] || continue
    if [[ -e "$edest" ]]; then
      [[ "$(sha "$edest")" != "$esha" ]] || continue
      mv -- "$edest" "$edest.pre-restore-$(date -u +%Y%m%dT%H%M%SZ)"
    fi
    mkdir -p -m 700 -- "$(dirname -- "$edest")"
    install -m "$emode" -- "$work/external/$eid" "$edest"
  done < "$work/external/MAP"
  note "restored $got tables and the state directory to $state"
  [[ -z "$previous" ]] || note "the previous state directory was kept at $previous"
}

case "$cmd" in
  preflight) do_preflight local ;;
  setup)
    if [[ -n "$remote_url" ]]; then do_setup_remote; else do_setup_local; fi ;;
  print-tls-mode) pg_ca=${ca_file:-}; ssl_default "${positional[0]:?}" ;;
  status) do_status ;;
  apply-grants) do_apply_grants ;;
  verify-roles) do_verify_roles ;;
  migrate) do_migrate ;;
  backup) do_backup ;;
  restore) do_restore ;;
  *) usage ;;
esac

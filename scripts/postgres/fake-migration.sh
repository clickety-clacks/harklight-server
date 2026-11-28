#!/usr/bin/env bash
# Test double for bin/harklight-migration that follows installer-command-contract v1
# against a real private PostgreSQL and the real schema files. It carries no
# FORMAT32 logic: it exists so the installer's orchestration can be tested.
# Env: FAKE_DIR (scratch), FAKE_PSQL (psql path), FAKE_FAIL_AT (command to fail once).
set -euo pipefail
cmd=${1:?}; shift
echo "$cmd" >> "$FAKE_DIR/calls.log"
fail() { printf '%s\n' "$1" >&2; exit 2; }
if [[ "${FAKE_FAIL_AT:-}" == "$cmd" && ! -e "$FAKE_DIR/failed.$cmd" ]]; then : > "$FAKE_DIR/failed.$cmd"; fail "fake_injected_failure"; fi
m() { PGPASSWORD="$(< "$HARKLIGHT_MIGRATOR_PASSWORD_FILE")" "$FAKE_PSQL" --no-psqlrc -qtA -v ON_ERROR_STOP=1 "$HARKLIGHT_MIGRATOR_URL" "$@"; }
json() { printf '{"contract_version":1,"command":"%s","phase":"%s","import_id":"imp-1","source_sha256":"%s","schema_sha256":"%s","validation_sha256":%s}\n' "$cmd" "$1" "$(printf 'a%.0s' {1..64})" "$(printf 'b%.0s' {1..64})" "$2"; }
vsha=$(printf 'c%.0s' {1..64})
arg() { local k=$1; shift; while (($#)); do if [[ "$1" == "$k" ]]; then printf '%s' "$2"; return; fi; shift; done; }
case "$cmd" in
  export)
    out="$(arg --output "$@")"; [[ ! -e "$out" ]] || fail "output_exists"
    [[ -f "$(arg --source "$@")" && -f "$(arg --material-map "$@")" && -n "$(arg --unit "$@")" ]] || fail "bad_args"
    mkdir -m 700 "$out"; echo manifest > "$out/MANIFEST"; cp "$(arg --material-map "$@")" "$out/material-map.json"
    python3 -I - "$out" <<'PY'
import json, os, shutil, sys
out = sys.argv[1]
for rel, src in json.load(open(out + "/material-map.json")).items():
    dst = os.path.join(out, "material", rel); os.makedirs(os.path.dirname(dst), exist_ok=True); shutil.copyfile(src, dst)
PY
    json exported null ;;
  verify) [[ -f "$1/MANIFEST" ]] || fail "no_bundle"; json verified null ;;
  import)
    [[ -f "$(arg --bundle "$@")/MANIFEST" && -d "$(dirname -- "$(arg --receipt "$@")")" ]] || fail "bad_args"
    if [[ "$(m -c "select count(*) from pg_namespace where nspname = 'harklight'")" == 0 ]]; then
      for f in "$(dirname -- "$0")"/../../priv/postgres/*.sql; do m -f "$f" >/dev/null; done
    fi
    if [[ "$(m -c "select count(*) from harklight.import_state")" == 0 ]]; then
      m -c "insert into harklight.import_state(import_id, source_sha256, schema_sha256, phase) values ('imp-1', decode(repeat('aa',32),'hex'), decode(repeat('bb',32),'hex'), 'loading')" >/dev/null
    fi
    (umask 077; echo '{"import_id":"imp-1"}' > "$(arg --receipt "$@")")
    json loading null ;;
  status)
    [[ -f "$(arg --receipt "$@")" ]] || fail "no_receipt"
    phase="$(m -c "select phase from harklight.import_state")"; json "$phase" null ;;
  validate)
    m -c "update harklight.import_state set phase = 'validated', manifest_sha256 = decode('$vsha','hex') where phase = 'loading'" >/dev/null
    stage="$(dirname -- "$(arg --receipt "$@")")/staging"; rm -rf "$stage"; mkdir -p "$stage/root"
    material="$(python3 -I - "$(arg --bundle "$@")" "$stage" "${FAKE_BAD_MATERIAL:-}" <<'PY'
import hashlib, json, os, shutil, sys
bundle, stage, bad = sys.argv[1:4]
files = []
for rel in sorted(json.load(open(bundle + "/material-map.json"))):
    src = os.path.join(bundle, "material", rel)
    dst = os.path.join(stage, "root", rel); os.makedirs(os.path.dirname(dst), exist_ok=True); shutil.copyfile(src, dst)
    d = open(dst, "rb").read()
    files.append({"path": rel, "size": len(d), "sha256": hashlib.sha256(d).hexdigest()})
if bad == "drop" and files:
    files.pop()
complete = {"import_id": "imp-1", "source_sha256": "a" * 64, "schema_sha256": "b" * 64, "files": files}
cp = os.path.join(stage, "COMPLETE.json"); json.dump(complete, open(cp, "w"))
print(json.dumps({"root": os.path.join(stage, "root"), "manifest_sha256": hashlib.sha256(open(cp, "rb").read()).hexdigest(), "files": files}))
PY
)"
    printf '{"contract_version":1,"command":"validate","phase":"validated","import_id":"imp-1","source_sha256":"%s","schema_sha256":"%s","validation_sha256":"%s","material":%s}\n' "$(printf 'a%.0s' {1..64})" "$(printf 'b%.0s' {1..64})" "$vsha" "$material" ;;
  activate)
    [[ "$(arg --validation-sha256 "$@")" == "$vsha" ]] || fail "digest_mismatch"
    [[ "$(m -c "select has_schema_privilege('harklight', 'harklight', 'USAGE')")" == t ]] || fail "grants_not_applied_before_activation"
    m -c "update harklight.import_state set phase = 'active' where phase = 'validated' and manifest_sha256 = decode('$vsha','hex')" >/dev/null
    json active "\"$vsha\"" ;;
  *) fail "unknown_command" ;;
esac

#!/usr/bin/env python3
"""Private synthetic CI package smoke; emits counts/timings, never source values."""
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from urllib.parse import urlparse

package = Path(sys.argv[1]).resolve(strict=True)
root = Path(sys.argv[2]).resolve()
assert not root.exists(), 'requires fresh scratch'
url = urlparse(os.environ['HARKLIGHT_MIGRATOR_URL'])
assert url.hostname in ('127.0.0.1', 'localhost') and url.path == '/migration_package'
root.mkdir(mode=0o700)
os.umask(0o077)
env = dict(os.environ, ATTENTION_SERVER_ENABLE='0', ERL_FLAGS='+S 2:2')
timings = {}


def command(name, args):
    start = time.monotonic()
    result = subprocess.run([str(a) for a in args], env=env, capture_output=True, timeout=600)
    timings[name] = round((time.monotonic() - start) * 1000)
    # Raw diagnostics stay private, including on failure.
    (root / (name + '.stdout')).write_bytes(result.stdout)
    (root / (name + '.stderr')).write_bytes(result.stderr)
    if result.returncode:
        raise RuntimeError(f'{name} failed with exit {result.returncode}; private log retained')
    return json.loads(result.stdout.splitlines()[-1])


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def inventory(path):
    return {str(p.relative_to(path)): (p.stat().st_size, digest(p))
            for p in path.rglob('*') if p.is_file()}


def size(path):
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


source = root / 'source'
compatibility = package / 'fixtures/compatibility/run.sh'
assert command('seed', [compatibility, package, source, 'seed'])['result'] == 'pass'
baseline = json.loads((source / 'manifest.json').read_text())
assert baseline['schema'] == 2
certs = source / 'certs'
(source / 'person-id').write_text(baseline['people'][0] + '\n')
(source / 'server.env').write_text('\n'.join(
    f"ATTENTION_SERVER_{key}='{certs / filename}'"
    for key, filename in [('CERTFILE', 'server.pem'), ('KEYFILE', 'server.key'), ('CACERTFILE', 'ca.pem')]))
material = {'server.env': str(source / 'server.env'), 'person-id': str(source / 'person-id'),
            'owner/identity.pem': str(certs / 'expired.pem'), 'owner/identity.key': str(certs / 'expired.key')}
material.update({f'tls/{name}': str(certs / name) for name in ('server.pem', 'server.key', 'ca.pem', 'ca.key')})
material_file = root / 'material-map.json'
material_file.write_text(json.dumps(material))
backup = root / 'backup'
shutil.copytree(source, backup)
original = inventory(backup)
cli = package / 'bin/harklight-migration'
bundle = root / 'bundle'
receipt = root / 'receipt.json'
assert command('capabilities', [cli, 'capabilities'])['migration_ready'] is False
assert command('export', [cli, 'export', '--source', source / 'relay.sqlite3', '--output', bundle,
    '--supervisor', 'synthetic CI fixture', '--stopped-evidence', 'fixture process exited',
    '--material-map', material_file])['bundle_version'] == 2
assert command('verify', [cli, 'verify', bundle])['bundle_version'] == 2
args = ['--bundle', bundle, '--receipt', receipt]
# Prepare the same empty destination through the package's explicit schema API.
# The test-only trigger pauses the first people batch after raw facts committed.
env['HARKLIGHT_REHEARSAL_BUNDLE'] = str(bundle)
env['HARKLIGHT_REHEARSAL_RECEIPT'] = str(receipt)
prepare = '''
Application.ensure_all_started(:crypto)
Application.ensure_all_started(:postgrex)
{:ok, identity} = AttentionServer.Migration.ImportReceipt.open(
  System.fetch_env!("HARKLIGHT_REHEARSAL_RECEIPT"), System.fetch_env!("HARKLIGHT_REHEARSAL_BUNDLE"))
{:ok, options} = AttentionServer.Migration.Connection.options(System.get_env())
{:ok, _} = AttentionServer.Migration.PrepareSchema.run(options, identity)
IO.puts(AttentionServer.JSON.encode!(%{"result" => "prepared"}))
'''
assert command('prepare-interruption', [package / 'bin/attention_server', 'eval', prepare])['result'] == 'prepared'
container = os.environ['HARKLIGHT_REHEARSAL_PG_CONTAINER']
assert re.fullmatch(r'[a-f0-9]{12,64}', container)


def sql(statement):
    result = subprocess.run(['docker', 'exec', container, 'psql', '-X', '-At', '-v',
        'ON_ERROR_STOP=1', '-U', 'harklight_migrator', '-d', 'migration_package', '-c', statement],
        capture_output=True, timeout=20)
    if result.returncode:
        (root / 'interruption-sql.stderr').write_bytes(result.stderr)
        raise RuntimeError('private interruption SQL failed')
    return result.stdout.decode().strip()


sql('''CREATE FUNCTION public.migration_rehearsal_pause() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT count(*) FROM harklight.people) <> 1 OR
     NOT EXISTS (SELECT 1 FROM harklight.people WHERE id = NEW.id) THEN
    RAISE EXCEPTION 'rehearsal expected one transaction-local inserted person';
  END IF;
  PERFORM pg_sleep(12);
  RETURN NEW;
END $$;
CREATE TRIGGER migration_rehearsal_pause AFTER INSERT ON harklight.people
FOR EACH ROW EXECUTE FUNCTION public.migration_rehearsal_pause();''')
with (root / 'interrupted.stdout').open('wb') as out, (root / 'interrupted.stderr').open('wb') as err:
    process = subprocess.Popen([str(cli), 'import', *map(str, args)], env=env,
                               stdout=out, stderr=err, start_new_session=True)
    try:
        deadline = time.monotonic() + 120
        while True:
            if process.poll() is not None:
                raise RuntimeError('import exited before the controlled interruption')
            if sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='PgSleep'") == '1':
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('import did not reach the bounded interruption point')
            time.sleep(0.1)
        # PgSleep is reached only after the AFTER INSERT trigger has asserted
        # the importing transaction sees its newly written row. Other sessions
        # must still see zero, both here and after the killed batch rolls back.
        committed = int(sql("SELECT last_ordinal FROM harklight.import_checkpoints WHERE family='records'"))
        assert committed > 0
        assert sql('SELECT count(*) FROM harklight.people') == '0'
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=15)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=15)
# Wait only for this killed test process's server transaction to roll back.
deadline = time.monotonic() + 20
while sql('SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') != '0':
    if time.monotonic() >= deadline:
        raise RuntimeError('interrupted transaction did not disconnect')
    time.sleep(0.1)
assert int(sql("SELECT last_ordinal FROM harklight.import_checkpoints WHERE family='records'")) == committed
assert sql('SELECT count(*) FROM harklight.people') == '0'
assert sql("SELECT phase || ':' || (manifest_sha256 IS NULL)::text FROM harklight.import_state") == 'loading:true'
sql('DROP TRIGGER migration_rehearsal_pause ON harklight.people; DROP FUNCTION public.migration_rehearsal_pause();')
loaded = command('import', [cli, 'import', *args])
assert loaded['phase'] == 'loading' and loaded['validation_sha256'] is None
assert command('import-repeat', [cli, 'import', *args]) == loaded
validated = command('validate', [cli, 'validate', *args])
assert validated['phase'] == 'validated' and len(validated['validation_sha256']) == 64
assert command('validate-repeat', [cli, 'validate', *args]) == validated
resumed = command('lost-response-retry', [cli, 'import', *args])
assert resumed['phase'] == 'validated' and resumed['validation_sha256'] == validated['validation_sha256']
assert command('status', [cli, 'status', '--receipt', receipt])['phase'] == 'validated'
assert inventory(source) == original and inventory(backup) == original
# Reopen only a new restored copy, keeping the backup and captured source intact.
restored = root / 'restored'
shutil.copytree(backup, restored)
assert command('rollback-history', [compatibility, package, restored, 'verify'])['result'] == 'pass'
assert inventory(backup) == original
build = (package / 'BUILD_INFO').read_text()
commit = next(line.split('=', 1)[1] for line in build.splitlines() if line.startswith('source_commit='))
assert 'source_status=\n' in build
report = {'result': 'pass', 'scope': 'packaged-staged-import-and-format32-rollback',
          'source_commit': commit, 'manifest_schema': 2,
          'source_observations': len(baseline['observations']), 'phase': 'validated',
          'activation': False, 'timings_ms': timings,
          'interruption': {'signal': 'SIGKILL', 'committed_source_records': committed,
                           'people_inside_transaction': 1, 'people_after_rollback': 0, 'resumed': True},
          'file_bytes': {'source': size(source), 'backup': size(backup), 'bundle': size(bundle),
                         'material': size(Path(str(receipt) + '.material'))},
          'limits': ['synthetic small-history measurement, not a capacity forecast',
                     'PostgreSQL client-history parity is a separate gate']}
(root / 'safe-report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))

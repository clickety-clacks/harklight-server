#!/usr/bin/env python3
"""Private synthetic CI package smoke; emits counts/timings, never source values."""
import hashlib
import json
import os
from pathlib import Path
import shutil
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
          'file_bytes': {'source': size(source), 'backup': size(backup), 'bundle': size(bundle),
                         'material': size(Path(str(receipt) + '.material'))},
          'limits': ['synthetic small-history measurement, not a capacity forecast',
                     'PostgreSQL client-history parity and interrupted-process rehearsal are separate']}
(root / 'safe-report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))

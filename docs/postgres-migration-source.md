# FORMAT32 migration source inventory

The PostgreSQL migration is under development and is not an upgrade route yet.
FORMAT32 remains the default backend. The internal migration helpers supply stopped-source capture and a
read-only source inventory; it does not import, activate PostgreSQL, change
configuration, or start a listener.

`AttentionServer.Migration.Format32.preflight(path, provenance: receipt,
scratch_root: root)` accepts only a **copy carrying a trusted stopped-source
export receipt**. A bare `stopped: true` acknowledgement is refused. The
source-facing exporter must collect supervisor and process-marker observations
before and after copying, plus size, modification time and SHA-256 of the main
file and WAL/SHM/journal files. The supervisor must report the same inactive or failed
unit with MainPID zero; the process marker must be absent or confirmed stale;
the file observations must be unchanged and WAL/journal must be absent or
empty. The consumer binds the receipt's main-file hash and size to this copy.
Missing, incomplete, inconsistent or mismatched receipts are refused.

The receipt is trusted output from the operator's export step, not an
independent proof manufactured by this copy reader. `AttentionServer.Migration.Source.capture/3` gathers
that evidence without opening, lock-probing or checkpointing the original
through SQLite. The packaged export command uses these observations directly. The caller
must stop all Server writers, background work and provisioning, and retain the
original backup offline. Synthetic consumer tests use explicit producer
fixtures; they are not evidence that a real service was inspected.

The function creates a private disposable copy under the existing scratch
root, opens only that copy with SQLite in read-only mode, and deletes the
copy and its SQLite sidecars on completion. It refuses nonempty source WAL
or journal files, invalid anchor/checksum/codec/generation, unknown tables or
columns, and damaged record checksums. Hashes before and after inspection
detect changes to the source file. The source itself is never opened with
SQLite and no bootstrap or normal Store startup runs.

The returned inventory contains table names, column names, counts and
SHA-256 digests, plus the format, codec, generations and source size/hash.
It contains no record values or cursor secrets. Record digests use decoded
FORMAT32 addresses and values, encoded deterministically with length framing;
side-table digests preserve ordered SQL column values, including binary values.
The schema catalog deliberately includes auth sessions, operation receipts,
feed heads, page snapshots and upload staging, as well as the Engine records.
A new durable column needs an explicit disposition instead of silently being
omitted. Column order is significant in this version of the catalog.

This inventory is **not final migration validation**: referential closure,
attachment content, private configuration and identity files, full export,
checkpointed import and destination field-level comparison follow in later
slices. A passing inventory must never authorize activation by itself.
The scanner streams rows, but SQLite sorting and integrity checking still
need scratch space; no calibrated capacity or downtime estimate is claimed.

## Documented bridge chain

The following describes tagged source and package guides, not completed
PostgreSQL migration rehearsals:

- v0.1.45 writes FORMAT29 snapshots. The v0.2.16 guide supports FORMAT29 and
  supported FORMAT27 snapshots (through FORMAT28 and FORMAT29).
- A published v0.2.16–v0.2.20 bridge writes FORMAT30. Moving from protocol 0.1
  also needs compatible protocol 0.2 clients, as that package guide explains.
- v0.2.26 introduced FORMAT31 keyed records. The packaged stopped-store
  converter handles supported FORMAT30 snapshots with capacity checks.
- v0.2.34 introduced FORMAT32. Its converter handles FORMAT31 anchors and
  the documented FORMAT30 conversion. v0.2.36 also writes FORMAT32.

The new importer will accept FORMAT32 only. For an older supported source,
use the documented bridge/converter on a protected copy with its matching
uploads, configuration and keys. Do not infer format solely from release
version: the actual marker, codec and generation must pass validation.
Unknown formats require an identified supported upgrade path or a readable
backup. Never rerun bootstrap over existing or restored state.

Keep the full immutable pre-upgrade backup. Before accepting new writes,
rollback can return to the original stopped stack. After new writes,
restoring the old backup loses them and cannot undo external deliveries;
a binary or symlink change is not a data downgrade.

## Codec mismatch recovery

`source_otp_mismatch` means the keyed source was written by another Erlang/OTP
major version. Keep the backup unchanged and run the exporter from a verified
package using the source codec's OTP major. If that package is unavailable,
obtain a supported rekey/bridge procedure against a separate copy; do not edit
the anchor or downgrade the running binary to force admission. No cross-OTP
migration compatibility is claimed by this inventory.

## Stopped-source capture

The internal `Source.capture(path, new_directory, options)` helper creates a
mode-0700 directory with a mode-0600 byte copy and a typed provenance receipt.
It refuses an existing destination. Use `supervisor: {:systemd, unit}` for the
self-host install's user service. Admission requires a loaded unit whose
ActiveState is inactive or failed and MainPID is zero, before and after copying.
An optional `pidfile: path` must be absent or name a process confirmed absent
through Linux procfs. The installer lock is not a Server activity marker.

For a deployment managed by another supervisor, the explicit option is
`supervisor: {:operator, supervisor_name, stopped_evidence}`. Both strings must
be nonempty and the operator must provide the actual stop evidence; this mode
is never the default. Keep secrets out of the evidence string. The receipt
records these observations, not a claim that systemd was queried.

When available, `fuser` or `lsof` checks that no process holds the main, WAL or
SHM files open. An unavailable tool is recorded; an open file or failed
observation refuses capture. Main, WAL, SHM and journal size, modification time
and SHA-256 must remain unchanged throughout the copy. Nonempty WAL or journal
files are refused. Stop the original Server normally so it checkpoints its own
store; the migration helper never checkpoints it.

A failed capture can leave a private incomplete directory, but never publishes
a successful receipt. Keep it separate from the immutable original backup.
`Source.confirm/3` repeats the observations after subsequent export work.
A capture alone is not an importable or activated PostgreSQL store.

Typed intermediate files use length-delimited deterministic terms with a
checksum per record and a whole-file count, size and checksum. `Frames.fold/4`
validates the complete expected stream before calling its consumer, so a
truncated file cannot be accepted as a shorter export. These files can contain
identity and cursor material: keep their enclosing directory private and do
not attach them to public reports.

Capture uses ordinary file reads, which may update source access times; content,
size and modification time are verified unchanged.

## Packaged export and verification (development)

This command prepares a protected export. Import, relational validation and
activation are not implemented by this command yet. It does not change the
backend or start the Server. Do not use it as an upgrade route until the full
migration release is accepted.

Stop the Server and all provisioning/background writers first. Preserve the
complete original backup. Create a private JSON material map naming every
configuration and identity file needed to restore this installation. Its keys
are relative destination paths and values are absolute source paths, for example
`{"server.env":"/private/backup/server.env","owner/identity.pem":"/private/backup/owner/identity.pem"}`.
The command does not discover credentials, execute configuration, or print
material values. The deployment-specific inventory remains the operator's
responsibility; `{}` is explicit confirmation that there are no external
material files. A future installer wrapper will supply its required inventory.

```sh
bin/harklight-migration export --source /private/backup/relay.sqlite3 \
  --output /private/new-export --unit harklight.service \
  --material-map /private/material.json
bin/harklight-migration verify /private/new-export
```

For a different supervisor, replace `--unit` with both `--supervisor NAME` and
`--stopped-evidence TEXT`. Supply actual stopped-state evidence without secrets.
Use `--pidfile PATH` only if the deployment already has an external Server
pidfile. Source and output must be distinct, and output must not already exist.

The export contains decoded typed records, every inventoried SQL side table,
referenced upload/large-data/staged bytes, the explicit private material, a
source byte copy, and a manifest. Completion is written last. Both export and
verify compare every stream's count and decoded content digest to the source
inventory, verify per-frame and whole-file checksums, and hash every copied
file. Missing completion, truncation, changed bytes, unknown record families
or table columns are refused. Treat the entire directory as secret material;
only the command's count/digest summary is suitable for evidence.

Keep the successful export immutable. A failed export has no successful final
marker and must never be imported; retry into a fresh directory after resolving
the refusal. Do not delete or overwrite the original backup. The later import
will own restart checkpoints and require relational closure, target readback
and history comparison before activation. `verify` here establishes export
integrity, not that final activation gate.

For the standard self-host state directory, inventory `server.env`, `person-id`,
and all regular files under `tls/` and `owner/`, preserving their relative
paths. Include external files referenced by configuration explicitly. Do not
include the new PostgreSQL cluster or its passwords in the source material map.

## Unpublished import staging

`ImportSession` claims an empty, already migrated database under the schema's
advisory lock. It binds an import ID to the export-manifest and exact DDL digests.
An occupied database, a different import identity or a phase other than loading
is refused. It performs no DDL and never changes the runtime backend.

`ImportFacts.run/3` is the internal first staging pass. It writes each addressed
Engine fact using the shared typed attributes codec. Batches of at most 256
facts and their ordinal/digest checkpoint commit in the same transaction.
After interruption, the importer checks the original bundle and its consumed
prefix before continuing from the stored checkpoint. A stale checkpoint cannot
write again. The source bundle stays unchanged.

This pass preserves facts for later normalized projection and comparison; it
is not a complete import or an exposed command. Side tables, normalized rows,
upload chunks, relational closure and retained-client-history validation must
finish before a later validation/activation slice can publish the database.
The staging functions always leave phase `loading` and expose no activation
operation. Their dedicated CI test uses a fresh disposable PostgreSQL service,
kills a writer after uncommitted rows, checks rollback and resume, and compares
every staged fact with its original typed value.

`ImportAccounts.run/3` projects server identity and cursor epoch, people and
profiles/settings, current and revoked recorded principals, sender grants,
origin sessions, and the independent accepted/free-order/conversation highs.
It preserves the full typed principal/session attributes, and keeps absent
settings distinct from explicit values. Each projection has its own source
prefix checkpoint and uses the same atomic batch mechanism. Missing identity,
invalid epoch, person references or unsupported account facts refuse the stage.
Historical principals referenced only by later message facts are handled with
that later projection; no principal is invented by this stage. It still leaves
the database in `loading` and is not a complete import or activation command.

An empty FORMAT32 epoch table has effective epoch 1 in the current Server;
the import preserves that value. An explicitly stored positive epoch is copied
unchanged. No new epoch is allocated during conversion.

`ImportOrganizations.run/3` adds Hosted accounts, local and Hosted organizations,
organization rosters, groups, mute state and pending invitations after the account
stage. A missing local roster retains the current effective rule: local owners
excluding Hosted accounts. An explicit roster is copied exactly, including an
empty one. The local organization's typed attributes retain the original roster
or `nil` for that implicit case. Hosted organization/account and group attributes
preserve the entire original typed record. A Hosted group's normalized
`hosted_ref` is the group component of its `{business, group}` reference; the
business is the organization's `hosted_ref`, and the full tuple remains in the
group attributes. Hosted groups retain a null manager. Existing Hosted group
version records and deletion tombstones remain in the lossless facts staging.
Every organization/group phase has a checkpoint and remains unpublished.

`ImportTables.run/3` stages durable SQL side rows as typed compatibility facts.
Each family is the source table name. Its address is the single primary-key
value, or a tuple of values in the declared primary-key column order for a
composite key. Its body is the list of exact source values in the column order
recorded by `SourceSchema`. The shared typed codec encodes both key and body.
For example, `harklight_auth_schema` uses key `1` and body
`[1, minimum_generation, device_certificates_disabled]`, matching the runtime
certificate reader. Binary
hashes, integers and nulls are not converted to strings. The explicit table
catalog rejects unknown tables and separates durable rows from rebuildable
SQL indexes and source format/generation witnesses. Audit ordinals and the
SQLite sequence are retained. Each table has an independent atomic checkpoint.

This preserves pending authentication, receipt, baseline and cursor material
without making it actionable. Normalized runtime projections, device-progress
overlay application and referential/field validation remain required before
activation. Derived index tables are verified in the export but are not copied
as durable target facts. The original typed Engine facts remain separately
addressed, and the source bundle stays immutable.

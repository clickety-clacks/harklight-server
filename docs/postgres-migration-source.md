# FORMAT32 migration source inventory

The PostgreSQL migration is under development and is not an upgrade route yet.
FORMAT32 remains the default backend. This first slice supplies an internal,
read-only source inventory; it does not import, activate PostgreSQL, change
configuration, or start a listener.

`AttentionServer.Migration.Format32.preflight(path, stopped: true,
scratch_root: root)` inspects a **stopped, checkpointed backup**. The caller
must stop all Server writers, background work and provisioning before taking
that backup. The `stopped` option acknowledges that prerequisite; it cannot
stop another process or fence writers. Keep the original backup offline.

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

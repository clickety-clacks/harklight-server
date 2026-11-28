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

`ImportAuth.run/3` indexes browser and native sessions after account and SQL-fact
staging. Browser token hashes remain the original 32 bytes, with the source
owner key and revision. Native hexadecimal hashes become the equivalent 32
bytes, using the source device ID and person. Native attributes carry boolean
`pending`, original public key and sign-in timestamp; enrollment revision is
recorded but is not a native authentication gate. Neither source session type
has a server expiry, so the target expiry remains null.

Sessions linked to existing principals are retained even if currently revoked
or at a different revision; the runtime's existing predicates decide whether
they authenticate. Sessions referring to a principal that no longer exists
remain only in the exact SQL facts. Import does not invent a principal or grant
authority. Both session stages have checkpoints and leave the database loading.

`ImportStreams.run/3` copies explicit person/Agent heads, floors and seed
revisions without allocating positions. Full resource IDs and feed revisions
are preserved. Person feed kinds stay exactly as stored; revision-index kinds
are canonical by resource identity, so an `item_removed` feed row has an `item`
revision-index row. Availability-writer markers remain revision-only. Agent
resource kinds follow the existing decoder, and both Agent SQL tables preserve
their positions. Agent rows with null timestamps enter only the revision index.
Original person resource creation timestamps are copied to that index. Unknown
kinds, missing stream heads and revisions beyond a recorded head refuse the
stage. Checkpointed batches remain in the unpublished loading phase.

A person with no explicit change-log metadata retains FORMAT32's effective
bounds: the current epoch when messages, conversation entries, bounce settings
or alert settings exist, otherwise zero. Independent allocation counters do not
substitute for those retained facts. Explicit metadata always wins. When source
reconciliation has removed revision rows but kept creation timestamps, the
projection retains each timestamp with the source stream's effective seed
revision. It does not create a feed event or advance the head. Dedicated fixtures
exercise implicit bounds at epochs 1 and 17 and the actual source reconciliation
path, then compare saved-cursor reads with the PostgreSQL stream reader.

`ImportReceipts.run/3` projects `client_idempotency` and
`harklight_app_receipts` into distinct namespaces with their original operation
keys. Person scopes use an empty origin; existing encoded Agent scopes decode
to their original person and origin. Lowercase digest text becomes the same
32 bytes, and JSON receipt bodies use the shared typed response codec. Recent
receipts retain expiry at their original timestamp plus 30 days and have
`original_response=false`; retained receipts have no expiry and preserve their
source flag. Already-expired but still-stored rows are copied, leaving retention
decisions to the reader. Full source bytes remain in SQL facts. Repeated import
uses checkpoints and does not allocate new receipts or publish the target.

`ImportMessages.run/3` projects ordinary, principal-backed message records and
all retained revision bodies, followed by the original recipient item IDs.
Internal message IDs combine the person and accepted item position; public item
IDs, sender/message IDs, session, ordering, terminal state and supplemental
attributes remain unchanged. The current PostgreSQL item reader is exercised
against these rows while runtime admission still rejects the loading database.

This intermediate stage refuses a historical revision whose timestamp is not
retained by FORMAT32. FORMAT32 records the first and latest timestamps, but can
omit intermediate ones; no timestamp is invented. A schema follow-up must allow
that absence and represent historical/system origins without creating login
authority. Shared messages, seen state and conversation/answer projections also
remain later stages. This is not a full-history conversion or activation command.

The ordinary-record stage checks both origin and recipient item readers,
including wrong-origin and wrong-person rejection and exact attachment-bearing
record attributes. It does not populate attachment chunks or attachment link
rows, per-recipient seen state, conversations or answers. Those stages and their
field-level reader comparisons are required before import validation and
activation; this stage's success alone is not a complete history import.

`ImportFeeds.run/3` projects origin and advisory allocation highs independently
of change-log positions. Entries retain their exact sequence, session and typed
body; origin delivery acknowledgements retain their stage map and advisory
receipts retain their IDs. Highs are copied before entries and receipts, using
separate checkpoints. Entries beyond the recorded high, acknowledgements without
an origin entry and receipts without an advisory entry refuse the stage. This
preserves feed facts without dispatching deliveries or activating the target.

`ImportBytes.run/5` copies an already-verified bundle file into a matching target
upload manifest one 1 MiB chunk at a time. Each chunk and source-prefix checkpoint
commit atomically. Restart rereads and verifies the committed prefix before
continuing; a changed prefix refuses, and final byte length and SHA-256 must
match. Empty files produce no chunks. Completed uploads can only be rechecked
when their checkpoint already covers the file; this routine cannot append to a
completed upload. It does not mark uploads complete, create attachment links or
activate the database. The caller must finish those stages and validate target
content and references before activation.

`ImportUploads.run/3` stages published Engine and retained staged-file manifests,
transfers and verifies their chunks, and links ordinary message revisions using
each original file ordinal and typed descriptor. UUIDs and staged `fs` IDs stay
distinct; person, uploader/owner, size and SHA-256 are preserved. Full source
metadata is retained through the typed codec. Published Engine uploads use their
original `last_activity_ms` as the import publication marker, not a reconstructed
completion time. Staged files use their original `at_ms`, with the original
30-day expiry recorded as numeric top-level `reservation_expires_at_ms` for the
runtime cleanup reader. Engine uploads acquire no invented expiry.

Completion verifies target chunk count, exact bytes and hash before publishing
that upload's marker. Per-file completion and message-link checkpoints make the
stage repeatable. Inline file descriptors remain in their original message
record. Shared-message and conversation-entry links and large-data references
remain distinct follow-up projections; full closure and retained-history checks
are still required before database validation and activation.

Upload byte completion means that the exported bytes have been copied and their
length and digest verified; it does not mean that a staged reservation was linked
to a message. Staged reservations retain their original expiry. Upload cleanup
preserves every manifest and chunk while an import is loading or validated, so
cleanup cannot remove a committed chunk prefix before resume. Once active, the
ordinary cleanup policy again removes expired, unlinked reservations, including
those whose bytes have been fully transferred. Linked attachments remain retained.
`ImportBytes` is an internal chunk primitive; the `ImportUploads` entry point
binds the verified bundle to the import's source digest before calling it.
Schema 002 lets ordinary history retain unknown intermediate revision acceptance
times as SQL NULL. Known first and latest acceptance times remain exact. Hosted
notices retain their reserved origin and sender without manufacturing a principal.
The import still stays in `loading`; these record projections do not validate or
activate a destination. Fresh imports pin the digest of migrations 001 and 002 in
order. Resuming an already claimed import requires its original identity and
schema digest, even after the destination applies migration 002.

`ImportConversations.run/3` creates ordinary conversation containers using the
message's internal ID and copies exact per-person read positions. Positive read
positions must resolve to the same source conversation's origin reply. This phase
does not yet project conversation entries, seen revisions, answers or shared
conversation history, and cannot validate or activate a destination.

`ImportMaterial.run/3` copies the verified bundle's declared configuration and
identity files into a separate private staging directory bound to the original
import identity. It never writes to the original state paths. Copies use bounded
reads, private permissions, a partial file followed by rename, and size/digest
readback. A retry verifies finished files and resumes unfinished copies; an
existing mismatched file or identity is refused. The returned material inventory
contains relative restore paths and digests, never file contents. It does not by
itself establish that every required configuration or identity file was declared;
that completeness check remains part of final migration validation.

The planned `validate` result uses a `material` object with `root` (absolute path
to the verified staged files), `manifest_sha256`, and `files` (each containing
`path`, `size`, and `sha256`). Paths retain the export material map's relative
restore names, including `external/...`; those external entries require the
installer's explicit original destination mapping. The installer must verify the
inventory and bytes before installing them. `server.env` remains source material;
the installer applies the explicit backend configuration change separately after
successful activation. Export and verify include additive `contract_version: 1`
and `command` fields; their existing result and digest fields remain unchanged.

Material staging now refuses permission drift on resumed roots, intermediate
directories, files and owner/completion markers. Standard owner material requires
`person-id`, `owner/identity.pem` and `owner/identity.key`: the private key's public
SPKI fingerprint must match the certificate and the selected person's current
owner fingerprint in the decoded source records. A valid certificate/key pair
from another identity is refused. TLS continuity rests on stopped-source provenance, not a store fingerprint.
Standard self-host staging requires the owner files and all three configured TLS files.
The export observes `server.env` and every declared material file before and
after copying within the stopped-source window. Its configured `CERTFILE`,
`KEYFILE` and `CACERTFILE` paths must identify the exact captured files. The
parser accepts literal installer shell quoting without executing the file;
expand computed paths to explicit absolute literals before stopping/exporting.
The importer checks the server certificate/private-key pair, the optional
`tls/ca.key` pair, and the server certificate chain to that captured CA. These
are continuity checks, not certificate renewal; existing validity dates stay
unchanged. Every retained guided-join invitation's `ca_sha256`, where present,
must equal the captured CA's DER SHA-256. With no retained pin, TLS relies on
the captured source configuration alone. A foreign key, CA, configured path,
changed source file or pin mismatch refuses staging. Unknown material kinds
remain refused until they have an explicit typed continuity mapping. Final
material completeness and activation are still separate unfinished gates.

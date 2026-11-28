# Harklight PostgreSQL schema

The schema is for isolated PostgreSQL opt-in development. FORMAT32 SQLite is
the default. Applying this migration alone exposes no HTTP routes and selects
no backend. Run the explicit migration job before starting an opted-in node.
Version 1 installs into an unpublished database in one transaction under the
transaction-scoped advisory lock `(1212240455, 1)`. A failed run rolls back.
A second bootstrap fails without modifying the installed schema. Application
startup must check the installed version and performs no DDL.

The explicit migration job applies
[`001_initial.sql`](../priv/postgres/001_initial.sql),
[`002_message_history.sql`](../priv/postgres/002_message_history.sql), and
[`003_large_text.sql`](../priv/postgres/003_large_text.sql) in order.
They use the same advisory lock. For a fresh import, the schema digest is
SHA-256 over the ordered files' concatenated bytes. When resuming an import
claimed under an earlier version, keep its original pinned digest.
Version 2 represents message history, and version 3 adds shared large text.

- Keep person IDs and principal keys as opaque text. Preserve the server
  instance ID, cursor signing bytes and integer epoch exactly. Preserve
  historical principals referenced by messages even when revoked.
- Use a stable internal message ID in `messages.id`. Keep the existing
  person/sender/message tuple on ordinary records and all public item IDs
  in `message_recipients.item_id`. Preserve accepted positions and ordering
  values. Conversation entry IDs are scoped to their conversation.
- Store each original revision as a separate row. Keep exact wire values and
  legacy fields needed for projection in the versioned JSON bodies or
  attributes. Canonical digests are stored as bytes without rehashing during
  import. Use the empty origin key for person-scoped receipts and streams.
  Keep the operation receipt namespace distinct for recent, retained, and
  domain-level receipts.
- Keep the recorded first and latest message revision acceptance times.
  Store null for intermediate revision acceptance times absent from FORMAT32.
  Hosted notices retain `app:harklight/notices` for both origin and sender.
  They belong to a person and have no origin session. Their generated
  `principal_origin_key` is null. Other messages keep the scoped origin
  principal foreign key through that generated column. Importers supply the
  original origin key. The database generates the credential reference.
- A person/Agent stream is keyed by `(person_id, origin_key)`. Preserve head,
  retained floor, seed revision, first-seen timestamps and the original
  revisions of retained changes and live resources. Gaps are valid. Allocate
  new revisions by incrementing this row under lock, in the same transaction
  as the content and operation receipt. Acquire multiple stream head locks
  in ascending key order using C collation. Baselines use an expiring ordered
  snapshot of versioned resources.
- Person changes retain the FORMAT32 kind and encoded resource ID. In the
  revision index, item removals share the `item` identity. Shared items and
  entries have kind `item` and `entry`, and all three counts IDs share the
  `counts` kind. Keep `availability-writer:` markers under `availability_writer`
  in the revision index only. Agent changes retain the full resource ID and
  use the Agent decoder kind as text. Both advisory prefixes have kind
  `advisory`. Rows with a null source timestamp are revision-only.
- Import upload sizes and SHA-256 bytes unchanged, with zero-based chunks
  of up to 1 MiB. A non-null completion timestamp means the total size and
  digest have been validated. An empty file has zero chunks. Writers may
  only link complete files. Keep each attachment ordinal and its wire
  attributes from the selected message revision.
- PostgreSQL admissions reserve the declared bytes against one person,
  across upload owners. A new manifest adds the integer
  `reservation_expires_at_ms` key beside its typed attributes envelope.
  Remove this key when decoding the envelope. Exact scoped retries match the
  declared size, digest and typed metadata and retain the original expiration.
  Cleanup takes bounded batches of expired, unlinked reservations under
  manifest locks. Imported manifests without a reservation timestamp are
  excluded.
- Store externalized large text in `large_texts` and `large_text_chunks`.
  A typed resource holds a `Repository.Postgres.LargeText.Ref` with the
  lowercase SHA-256 hex string and byte length. SQL keys use the raw
  32-byte digest. Chunks start at zero and use 1 MiB except for a shorter
  final chunk. Empty content has no chunks. Exact staged retries leave
  stored bytes intact. Publish only after verifying the entire length and
  hash, using bounded reads. Resolve references only from authorized parent
  resources. The database is the authoritative copy. Preserve the source
  marker and side facts separately during import.
- Claims are scoped to one job, with an attempt count, unique claim token
  and expiry. A worker may record success while its token still matches an
  unfinished job, including after lease expiry. The fifth failed or expired
  attempt ends the job. Exhaustion sets `jobs.completed_at_ms` and stores a
  typed `compatibility_facts` row in the same transaction. The family is
  `job_exhaustion`, the address is `Attributes.key({kind, id})`, and the
  body records the attempt count, time and reason. The original job body
  remains available.

Importer order follows the foreign keys. Insert identity and people before
principals and memberships, then origin sessions and messages, then message
facts and attachments, then stream metadata, retained rows and jobs. Keep
unfinished imports unpublished. Field-level legacy mappings and readiness
validation remain part of importer and backend integration.

## FORMAT32 import ownership and readiness

Use a canonical digest of the stopped source manifest and the exact schema
bytes. The importer must claim an empty destination by inserting the singleton
`import_state` row in phase `loading`, before writing history. Resume only
with the same import ID, source digest and schema digest. Checkpoint each family
in `import_checkpoints` with the last fully written ordinal and cumulative
digest. Write the rows and their checkpoint in the same transaction after
locking the import state row. Checkpoint ordinal zero means no frame consumed.

After verifying the rows, referenced content, counts and closure, record the
validated manifest digest and transition `loading` to `validated`. The
installer activates in a separate transaction after verification, with the same
import identity and manifest digest. Guard the update with the import ID,
source digest, schema digest, validated manifest digest, and prior phase
`validated`. Require exactly one matched row. The database requires inserts
to begin in `loading`, rejects skipped or reversed phase transitions, and
prevents replacing the source, schema, import identity or validated manifest.
Restarting from scratch requires a new unpublished destination. An opted-in
runtime must require both
schema version 3 and `phase = 'active'` plus the server identity row. The
identity row alone cannot publish partial imports. An isolated fresh database
uses the same lifecycle with a manifest describing the newly provisioned
synthetic data.

## FORMAT32 record coverage

The stable typed envelope is `Repository.Postgres.Attributes.encode/1`.
It writes deterministic, uncompressed ETF minor version 2 as base64 in the
JSON object with keys `encoding` and `data`. Use it for facts with atoms,
binary data, tuple keys, sets, or exact numeric lexemes. Read with
`Attributes.decode/1` and compare decoded values. Public wire JSON must still be
rendered by the existing wire codecs. Compatibility fact keys are
`Attributes.key/1` of the original RecordCodec address, or of the ordered
tuple of the original side-table primary-key values. Family names use the
original engine field or source table name. One row represents one addressed
fact. No record contains a whole Engine or unbounded feed.

| Source records | Destination |
| --- | --- |
| `relay_instance_id`, `agent_cursor_key` | `server_identity` with epoch from `client_change_log_epoch` |
| `persons`, `free_order_highs`, `conversation_highs` | `people` and independent `allocation_highs` rows for accepted, free order, and conversation positions. Use empty origin and handle. Preserve highs even above the greatest retained row. |
| `people_profiles`, `delivery_states`, `algorithm_choices`, `alert_choices`, `bounce_levels` | `people` profile, state, and settings, preserving absent vs explicit values |
| `owners`, `devices`, `agents`, `sender_grants` | `principals` and `sender_grants` |
| `hosted_accounts`, `hosted_organizations`, `local_organizations`, `groups` | Hosted refs, organizations and normalized membership/invitations. Local group has a manager; Hosted group has an org-scoped Hosted reference and no manager. |
| `hosted_group_refs` | Preserve all records as compatibility facts, including tombstones without a present group. |
| `messages`, `shared_messages`, `shared_recipients`, `shared_seen` | Normalized messages, revisions, recipients and typed projection facts. Preserve typed historic/shared Seen facts separately when the current recipient row cannot represent them. |
| `events`, `conversation_events`, `shared_entries`, `shared_answers`, `chat_read_positions` | Conversation entries, answers and positions. Retain delivery event facts independently from conversation entries using compatibility facts keyed by their accepted-event address. |
| `receipts`, `origin_receipts`, `origin_actions` | Operation receipts, handoff receipts, and lossless typed action facts where no receipt is involved |
| `origin_sessions` | `origin_sessions` |
| `origin_feed` high, entry, delivery ack | `allocation_highs` at person/origin with empty handle, `origin_feed_entries`, `origin_delivery_acks`. Preserve ACK stages and first-serve data separately. |
| `advisory` high, entry, receipt | `allocation_highs` at person/origin/handle, `advisory_feed_entries`, and `advisory_feed_receipts` |
| `uploads` and large-data references | `uploads` and `upload_chunks`, streamed and verified by length/digest; preserve original metadata and references |
| `push_bindings`, `item_availability` | Retain the per-binding and per-item facts; project due work into `jobs`. Keep reminder records addressed by their owning recipient/item. |
| `retired_owner_keys`, `pending_enrollments`, `pending_history`, `join_invitations`, `browser_sign_ins`, `browser_sign_in_clock_high_water`, `pending_clock_high_water`, `maintainer_grants`, `source_report_grants`, `enrollment_read_grants`, `subscriptions`, `cursors`, `conversation_cursors`, `conversation_opened`, `reply_group_history`, `jev_endpoint`, `jev_model`, `bounce_revisions`, `opened_revisions`, `bounce_waiting_routes` | Preserve addressed typed rows, members and scalar facts under the named compatibility families. |
| `RecordCodec.derived_fields/0` and nested feed lookup indexes | Rebuild from durable records. |

## FORMAT32 side-table coverage

Certificate authority reads these typed `compatibility_facts` by primary key.
The family is the SQLite table name and the address is
`Attributes.key(primary_key)`. The body encodes the ordered source columns.

| Family | Primary key | Body term |
| --- | --- | --- |
| `harklight_auth_schema` | `1` | `[1, minimum_generation, device_certificates_disabled]` with an integer disable flag |
| `harklight_migration_roster` | device key | `[device_id, person_id, phase]` |

An absent auth schema fact represents a pre-generation source. An absent
roster fact for an enrolled device follows the sender-grant and native-session
overlap rules. The same committed read checks these facts and the normalized
principal, sender grants and native session rows.

- Import authoritative person and Agent heads, floors and seed revisions from
  `client_change_log_meta` and `agent_change_log_meta`. Readable source rows
  from `client_changes`, `agent_changes` and `agent_item_advisory_changes`
  become stream changes. Agent rows with NULL timestamps are revision-only
  facts after seed or expiry and enter only `stream_resource_revisions`.
  Carry `client_resource_revs` and `client_resource_created` into the same
  index, including legacy non-feed markers. Never increase the source head
  to synthesize changes.
- Preserve `client_rank_keys`, `client_ranks`, and `client_rank_state`.
  The SQL-only `client_change_log_state` and `agent_change_log_state`
  generation witnesses are FORMAT32 preflight checks, not target counters.
- Map `client_idempotency` and `harklight_app_receipts` to separate
  `operation_receipts` namespaces named for those source tables. Person
  receipts use an empty origin key and raw SHA-256 digest bytes. Recent rows
  expire 30 days after their original timestamp and have
  `original_response = false`. Retained rows have no expiry and preserve the
  source flag. Keep `harklight_app_reply_intents` as its own typed family.
- Preserve `harklight_auth_schema`, including device certificate disablement,
  and typed rows from `harklight_browser_sessions`, `harklight_browser_grants`,
  `harklight_migration_roster`, `harklight_migration_grants`, `harklight_passkeys`,
  `harklight_passkey_challenges`, `harklight_owner_codes`,
  `harklight_native_transactions` and `harklight_device_sessions`.
  Index currently actionable sessions in `auth_sessions` where the principal
  already exists. For a browser session, copy the raw 32-byte session hash
  into `token_hash` with kind `browser`, the owner key and its pinned revision.
  FORMAT32 browser sessions have no server expiry, so import a null expiry.
  Pending challenges do not need an invented principal.
- For native sessions, decode the FORMAT32 lowercase hexadecimal
  `session_hash` into its 32 digest bytes and store it as `token_hash`.
  Use kind `native`, the current device principal, a null server expiry,
  and typed session attributes with a boolean `:pending`. Preserve the exact
  source row separately. Session resolution follows the active device and
  returns the original hexadecimal identity. Replace a device's prior native
  session in the same transaction as the new credential. Certificate
  eligibility still depends on the migration roster, auth schema and grants.
- Stream `client_staged_files` into upload manifests and chunks, preserving
  associated `client_staged_file_owners` and `client_discarded_files` facts.
- Keep the exact typed records from `relay_inbox_snapshots`,
  `relay_inbox_item_pages`, `relay_inbox_receipt_sequences`,
  `relay_inbox_operation_receipts`, `relay_inbox_batches` and
  `relay_inbox_receipt_pages`. Where a snapshot represents a stream
  baseline, index it by the addressed scope and preserve its item order.
- Keep `relay_data_migrations` and `relay_audit`; retain audit ordinals
  and the relevant `sqlite_sequence` value. If `relay_device_progress`
  contains an applicable overlay, apply it to the durable cursor facts.
- `relay_state`, `relay_record_generation`, and `relay_records` supply
  validated FORMAT32 facts and format identity. `relay_large_data_refs`
  supplies referenced payload validation. The `DerivedIndex` SQL tables
  are rebuildable and do not translate to durable imported rows.
- Reject unknown durable fields, tables or columns until the mapping is
  reviewed. The compatibility families preserve facts one row at a time;
  acceptance still requires explicit runtime readback of imported facts.

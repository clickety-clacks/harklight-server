# Harklight PostgreSQL schema v1

The schema is for isolated PostgreSQL opt-in development. FORMAT32 SQLite is
the default. Applying this migration alone exposes no HTTP routes and selects
no backend. Run the explicit migration job before starting an opted-in node.
Version 1 installs into an unpublished database in one transaction under the
transaction-scoped advisory lock `(1212240455, 1)`. A failed run rolls back.
A second bootstrap fails without modifying the installed schema. Application
startup must check the installed version and performs no DDL.

The authoritative columns and constraints live in
[`priv/postgres/001_initial.sql`](../priv/postgres/001_initial.sql).

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
- A person/Agent stream is keyed by `(person_id, origin_key)`. Preserve head,
  retained floor, seed revision, first-seen timestamps and the original
  revisions of retained changes and live resources. Gaps are valid. Allocate
  new revisions by incrementing this row under lock, in the same transaction
  as the content and operation receipt. Acquire multiple stream head locks
  in ascending key order using C collation. Baselines use an expiring ordered
  snapshot of versioned resources.
- Import upload sizes and SHA-256 bytes unchanged, with zero-based chunks
  of up to 1 MiB. A non-null completion timestamp means the total size and
  digest have been validated. An empty file has zero chunks. Writers may
  only link complete files. Keep each attachment ordinal and its wire
  attributes from the selected message revision.
- Claims are scoped to one job, with an attempt count, unique claim token
  and expiry. A late worker must match its claim token to finish. Preserve
  resumable work, receipt distinctions and attempt history in job attributes
  where the backend relies on them.

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
import identity and manifest digest. An opted-in runtime must require both
schema version 1 and `phase = 'active'` plus the server identity row. The
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
  `operation_receipts` namespaces and preserve the retained original-response
  flag. Keep `harklight_app_reply_intents` as its own typed family.
- Preserve `harklight_auth_schema`, including device certificate disablement,
  and typed rows from `harklight_browser_sessions`, `harklight_browser_grants`,
  `harklight_migration_roster`, `harklight_migration_grants`, `harklight_passkeys`,
  `harklight_passkey_challenges`, `harklight_owner_codes`,
  `harklight_native_transactions` and `harklight_device_sessions`.
  Index currently actionable sessions in `auth_sessions` where the principal
  already exists. Pending challenges do not need an invented principal.
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

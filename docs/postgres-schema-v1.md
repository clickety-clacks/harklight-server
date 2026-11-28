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

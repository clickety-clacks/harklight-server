-- Run with psql -X -v ON_ERROR_STOP=1 against an unpublished database.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

CREATE SCHEMA harklight;
SET LOCAL search_path = harklight, pg_catalog;

CREATE TABLE schema_migrations (
  version integer PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE server_identity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  instance_id text NOT NULL,
  cursor_signing_key bytea NOT NULL,
  cursor_epoch bigint NOT NULL CHECK (cursor_epoch > 0)
);

CREATE TABLE import_state (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  import_id text NOT NULL UNIQUE,
  source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256) = 32),
  schema_sha256 bytea NOT NULL CHECK (octet_length(schema_sha256) = 32),
  phase text NOT NULL CHECK (phase IN ('loading', 'validated', 'active')),
  manifest_sha256 bytea CHECK (octet_length(manifest_sha256) = 32),
  CHECK ((phase = 'loading') = (manifest_sha256 IS NULL))
);

CREATE TABLE import_checkpoints (
  import_id text REFERENCES import_state (import_id),
  family text,
  last_ordinal bigint NOT NULL CHECK (last_ordinal >= 0),
  cumulative_sha256 bytea NOT NULL CHECK (octet_length(cumulative_sha256) = 32),
  PRIMARY KEY (import_id, family)
);

CREATE TABLE people (
  id text PRIMARY KEY,
  delivery_state text,
  profile jsonb NOT NULL DEFAULT '{}',
  settings jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE principals (
  key text PRIMARY KEY,
  person_id text NOT NULL REFERENCES people,
  kind text NOT NULL CHECK (kind IN ('owner', 'device', 'agent')),
  revision bigint NOT NULL CHECK (revision >= 0),
  revoked boolean NOT NULL DEFAULT false,
  attributes jsonb NOT NULL DEFAULT '{}',
  UNIQUE (person_id, key)
);

CREATE TABLE sender_grants (
  principal_key text REFERENCES principals,
  pattern text NOT NULL,
  PRIMARY KEY (principal_key, pattern)
);

CREATE TABLE auth_sessions (
  token_hash bytea PRIMARY KEY,
  person_id text NOT NULL,
  principal_key text NOT NULL,
  kind text NOT NULL,
  principal_revision bigint NOT NULL,
  expires_at_ms bigint,
  attributes jsonb NOT NULL DEFAULT '{}',
  FOREIGN KEY (person_id, principal_key) REFERENCES principals (person_id, key)
);
CREATE INDEX auth_sessions_principal_idx ON auth_sessions (person_id, principal_key);

CREATE TABLE hosted_accounts (
  external_ref text PRIMARY KEY,
  person_id text NOT NULL UNIQUE REFERENCES people,
  attributes jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE organizations (
  id text PRIMARY KEY,
  hosted_ref text UNIQUE,
  attributes jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE organization_members (
  organization_id text REFERENCES organizations,
  person_id text REFERENCES people,
  PRIMARY KEY (organization_id, person_id)
);
CREATE INDEX organization_members_person_idx ON organization_members (person_id, organization_id);

CREATE TABLE groups (
  id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations,
  manager_id text REFERENCES people,
  name text NOT NULL,
  hosted_ref text,
  attributes jsonb NOT NULL DEFAULT '{}',
  UNIQUE (organization_id, hosted_ref),
  CHECK ((manager_id IS NULL) <> (hosted_ref IS NULL))
);
CREATE INDEX groups_organization_idx ON groups (organization_id, name);

CREATE TABLE group_members (
  group_id text REFERENCES groups,
  person_id text REFERENCES people,
  muted boolean NOT NULL DEFAULT false,
  attributes jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (group_id, person_id)
);
CREATE INDEX group_members_person_idx ON group_members (person_id, group_id);

CREATE TABLE group_invitations (
  id text PRIMARY KEY,
  group_id text NOT NULL REFERENCES groups,
  person_id text NOT NULL REFERENCES people,
  state text NOT NULL,
  attributes jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX group_invitations_person_idx ON group_invitations (person_id, state);

CREATE TABLE origin_sessions (
  person_id text REFERENCES people,
  origin_key text,
  handle text,
  attributes jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (person_id, origin_key, handle),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

CREATE TABLE allocation_highs (
  person_id text REFERENCES people,
  origin_key text,
  session_handle text,
  kind text CHECK (kind IN ('accepted', 'free_order', 'conversation', 'origin_feed', 'advisory_feed')),
  high bigint NOT NULL CHECK (high >= 0),
  PRIMARY KEY (person_id, origin_key, session_handle, kind),
  CHECK ((kind IN ('origin_feed', 'advisory_feed')) = (origin_key <> '')),
  CHECK ((kind = 'advisory_feed') = (session_handle <> ''))
);

CREATE TABLE origin_feed_entries (
  person_id text,
  origin_key text,
  session_handle text,
  position bigint CHECK (position > 0),
  body jsonb NOT NULL,
  PRIMARY KEY (person_id, origin_key, position),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

CREATE TABLE origin_delivery_acks (
  person_id text,
  origin_key text,
  position bigint CHECK (position > 0),
  body jsonb NOT NULL,
  PRIMARY KEY (person_id, origin_key, position),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

CREATE TABLE advisory_feed_entries (
  person_id text,
  origin_key text,
  session_handle text,
  position bigint CHECK (position > 0),
  body jsonb NOT NULL,
  PRIMARY KEY (person_id, origin_key, session_handle, position),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

CREATE TABLE advisory_feed_receipts (
  person_id text,
  origin_key text,
  session_handle text,
  advisory_id text,
  PRIMARY KEY (person_id, origin_key, session_handle, advisory_id),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

-- One row per independently addressed fact. These supplementary rows carry
-- durable FORMAT32 fields that do not belong in a principal or wire resource.
CREATE TABLE compatibility_facts (
  family text,
  address bytea,
  body jsonb NOT NULL,
  PRIMARY KEY (family, address)
);

CREATE TABLE messages (
  id text PRIMARY KEY,
  person_id text NOT NULL,
  origin_key text NOT NULL,
  sender_id text NOT NULL,
  message_id text NOT NULL,
  session_handle text,
  group_id text REFERENCES groups,
  revision bigint NOT NULL CHECK (revision >= 1),
  accepted_at_ms bigint NOT NULL,
  accepted_position bigint,
  free_order bigint,
  terminal jsonb,
  attributes jsonb NOT NULL DEFAULT '{}',
  UNIQUE (person_id, sender_id, message_id),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key),
  FOREIGN KEY (person_id, origin_key, session_handle)
    REFERENCES origin_sessions (person_id, origin_key, handle)
);
CREATE INDEX messages_origin_session_idx ON messages (person_id, origin_key, session_handle);

CREATE TABLE message_revisions (
  message_id text REFERENCES messages,
  revision bigint CHECK (revision >= 1),
  accepted_at_ms bigint NOT NULL,
  body jsonb NOT NULL,
  PRIMARY KEY (message_id, revision)
);

CREATE TABLE message_recipients (
  message_id text REFERENCES messages,
  person_id text REFERENCES people,
  item_id text NOT NULL,
  seen_revision bigint,
  attributes jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (message_id, person_id),
  UNIQUE (person_id, item_id)
);

CREATE TABLE conversations (
  id text PRIMARY KEY,
  message_id text NOT NULL UNIQUE REFERENCES messages,
  attributes jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE conversation_entries (
  id text NOT NULL,
  conversation_id text NOT NULL REFERENCES conversations,
  position bigint NOT NULL CHECK (position >= 0),
  request_revision bigint NOT NULL,
  kind text NOT NULL,
  body jsonb NOT NULL,
  PRIMARY KEY (conversation_id, id),
  UNIQUE (conversation_id, position)
);

CREATE TABLE answers (
  id text PRIMARY KEY,
  message_id text NOT NULL REFERENCES messages,
  person_id text NOT NULL REFERENCES people,
  request_revision bigint NOT NULL,
  body jsonb NOT NULL
);
CREATE INDEX answers_message_idx ON answers (message_id, person_id);

CREATE TABLE read_positions (
  conversation_id text REFERENCES conversations,
  person_id text REFERENCES people,
  position bigint NOT NULL CHECK (position >= 0),
  PRIMARY KEY (conversation_id, person_id)
);

CREATE TABLE operation_receipts (
  person_id text REFERENCES people,
  origin_key text,
  namespace text,
  operation_key text,
  digest bytea NOT NULL,
  status integer NOT NULL,
  response jsonb NOT NULL,
  created_at_ms bigint NOT NULL,
  expires_at_ms bigint,
  original_response boolean NOT NULL,
  PRIMARY KEY (person_id, origin_key, namespace, operation_key)
);
CREATE INDEX operation_receipts_expiry_idx ON operation_receipts (expires_at_ms)
  WHERE expires_at_ms IS NOT NULL;

CREATE TABLE handoff_receipts (
  person_id text,
  origin_key text,
  turn_id text,
  operation_key text,
  body jsonb NOT NULL,
  PRIMARY KEY (person_id, origin_key, turn_id, operation_key),
  FOREIGN KEY (person_id, origin_key) REFERENCES principals (person_id, key)
);

CREATE TABLE stream_heads (
  person_id text REFERENCES people,
  origin_key text,
  head bigint NOT NULL DEFAULT 0,
  floor bigint NOT NULL DEFAULT 0,
  seed_revision bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (person_id, origin_key),
  CHECK (floor >= 0 AND floor <= head),
  CHECK (seed_revision >= 0 AND seed_revision <= head)
);

CREATE TABLE stream_changes (
  person_id text,
  origin_key text,
  revision bigint CHECK (revision > 0),
  resource_kind text NOT NULL,
  resource_id text NOT NULL,
  session_handle text,
  created_at_ms bigint NOT NULL,
  PRIMARY KEY (person_id, origin_key, revision),
  FOREIGN KEY (person_id, origin_key) REFERENCES stream_heads
);
CREATE INDEX stream_changes_expiry_idx ON stream_changes (created_at_ms);
CREATE INDEX stream_changes_session_idx ON stream_changes (person_id, origin_key, session_handle, revision);

CREATE TABLE stream_resource_revisions (
  person_id text,
  origin_key text,
  resource_kind text,
  resource_id text,
  revision bigint NOT NULL CHECK (revision > 0),
  first_seen_at_ms bigint,
  PRIMARY KEY (person_id, origin_key, resource_kind, resource_id),
  FOREIGN KEY (person_id, origin_key) REFERENCES stream_heads
);

CREATE TABLE baseline_snapshots (
  snapshot_id text PRIMARY KEY,
  person_id text NOT NULL,
  origin_key text NOT NULL,
  revision bigint NOT NULL,
  expires_at_ms bigint NOT NULL,
  attributes jsonb NOT NULL,
  FOREIGN KEY (person_id, origin_key) REFERENCES stream_heads
);
CREATE INDEX baseline_snapshots_expiry_idx ON baseline_snapshots (expires_at_ms);

CREATE TABLE baseline_snapshot_items (
  snapshot_id text REFERENCES baseline_snapshots ON DELETE CASCADE,
  position bigint CHECK (position >= 0),
  resource_kind text NOT NULL,
  resource_id text NOT NULL,
  revision bigint NOT NULL,
  body jsonb NOT NULL,
  PRIMARY KEY (snapshot_id, position)
);

CREATE TABLE uploads (
  id text PRIMARY KEY,
  person_id text NOT NULL REFERENCES people,
  owner_key text,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
  completed_at_ms bigint,
  attributes jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX uploads_owner_idx ON uploads (person_id, owner_key);

CREATE TABLE upload_chunks (
  upload_id text REFERENCES uploads ON DELETE CASCADE,
  chunk_number integer CHECK (chunk_number >= 0),
  bytes bytea NOT NULL CHECK (octet_length(bytes) BETWEEN 1 AND 1048576),
  PRIMARY KEY (upload_id, chunk_number)
);

CREATE TABLE message_uploads (
  message_id text,
  revision bigint,
  ordinal integer CHECK (ordinal >= 0),
  upload_id text NOT NULL REFERENCES uploads,
  attributes jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (message_id, revision, ordinal),
  FOREIGN KEY (message_id, revision) REFERENCES message_revisions
);
CREATE INDEX message_uploads_upload_idx ON message_uploads (upload_id);

CREATE TABLE jobs (
  kind text CHECK (kind IN ('push', 'reminder')),
  id text,
  person_id text NOT NULL REFERENCES people,
  ready_at_ms bigint NOT NULL,
  claim_token text,
  claim_until_ms bigint,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  completed_at_ms bigint,
  body jsonb NOT NULL,
  PRIMARY KEY (kind, id),
  CHECK ((claim_token IS NULL) = (claim_until_ms IS NULL))
);
CREATE INDEX jobs_ready_idx ON jobs (kind, ready_at_ms, id) WHERE completed_at_ms IS NULL;

INSERT INTO schema_migrations (version) VALUES (1);
COMMIT;

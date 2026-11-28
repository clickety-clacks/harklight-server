-- Run after 005_shared_message_history.sql in the explicit migration job.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

DO $$
BEGIN
  IF (SELECT max(version) FROM harklight.schema_migrations) IS DISTINCT FROM 5 THEN
    RAISE EXCEPTION 'conversation entry upload migration requires schema version 5';
  END IF;
END;
$$;

CREATE TABLE harklight.conversation_entry_uploads (
  conversation_id text,
  entry_id text,
  ordinal integer CHECK (ordinal >= 0),
  upload_id text NOT NULL REFERENCES harklight.uploads,
  attributes jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (conversation_id, entry_id, ordinal),
  FOREIGN KEY (conversation_id, entry_id) REFERENCES harklight.conversation_entries (conversation_id, id)
);

CREATE INDEX conversation_entry_uploads_upload_idx
  ON harklight.conversation_entry_uploads (upload_id);

INSERT INTO harklight.schema_migrations (version) VALUES (6);
COMMIT;

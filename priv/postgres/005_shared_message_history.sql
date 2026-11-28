-- Run after 004_conversation_read_positions.sql in the explicit migration job.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

DO $$
BEGIN
  IF (SELECT max(version) FROM harklight.schema_migrations) IS DISTINCT FROM 4 THEN
    RAISE EXCEPTION 'shared message history migration requires schema version 4';
  END IF;
END;
$$;

ALTER TABLE harklight.messages
  ADD COLUMN kind text NOT NULL DEFAULT 'ordinary' CHECK (kind IN ('ordinary', 'shared')),
  ADD CONSTRAINT messages_shared_identity CHECK (
    kind <> 'shared' OR (id = message_id AND session_handle IS NULL)
  ),
  DROP CONSTRAINT messages_person_id_sender_id_message_id_key,
  ADD CONSTRAINT messages_scoped_sender_message_key UNIQUE (person_id, sender_id, message_id, kind),
  DROP CONSTRAINT messages_group_id_fkey,
  ADD CONSTRAINT messages_group_id_fkey FOREIGN KEY (group_id) REFERENCES harklight.groups ON DELETE SET NULL,
  DROP CONSTRAINT messages_principal_origin_fkey,
  DROP COLUMN principal_origin_key;

-- A shared record keeps the original sender as historical provenance. The
-- current principal and its grants authorize sender requests separately.
ALTER TABLE harklight.messages
  ADD COLUMN principal_origin_key text
    GENERATED ALWAYS AS (
      CASE WHEN kind = 'ordinary' THEN nullif(origin_key, 'app:harklight/notices') END
    ) STORED,
  ADD CONSTRAINT messages_principal_origin_fkey
    FOREIGN KEY (person_id, principal_origin_key) REFERENCES harklight.principals (person_id, key);

INSERT INTO harklight.schema_migrations (version) VALUES (5);
COMMIT;

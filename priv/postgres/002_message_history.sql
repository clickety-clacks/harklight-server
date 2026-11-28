-- Run after 001_initial.sql in the explicit migration job.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

DO $$
BEGIN
  IF (SELECT max(version) FROM harklight.schema_migrations) IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'message history migration requires schema version 1';
  END IF;
END;
$$;

-- Hosted notices have a stable system origin, independent of credentials.
-- Every other origin retains the original person-scoped principal check.
ALTER TABLE harklight.messages
  ADD COLUMN principal_origin_key text
    GENERATED ALWAYS AS (nullif(origin_key, 'app:harklight/notices')) STORED,
  ADD CONSTRAINT messages_person_id_fkey FOREIGN KEY (person_id) REFERENCES harklight.people (id),
  ADD CONSTRAINT messages_hosted_notice_origin CHECK (
    origin_key <> 'app:harklight/notices' OR
    (sender_id = 'app:harklight/notices' AND session_handle IS NULL)
  ),
  DROP CONSTRAINT messages_person_id_origin_key_fkey,
  ADD CONSTRAINT messages_principal_origin_fkey
    FOREIGN KEY (person_id, principal_origin_key) REFERENCES harklight.principals (person_id, key);

-- FORMAT32 only records the original and latest acceptance times.
ALTER TABLE harklight.message_revisions ALTER COLUMN accepted_at_ms DROP NOT NULL;

INSERT INTO harklight.schema_migrations (version) VALUES (2);
COMMIT;

-- Run after 003_large_text.sql in the explicit migration job.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

DO $$
BEGIN
  IF (SELECT max(version) FROM harklight.schema_migrations) IS DISTINCT FROM 3 THEN
    RAISE EXCEPTION 'conversation read position migration requires schema version 3';
  END IF;
END;
$$;

-- FORMAT32 read-position facts never name a request revision.
ALTER TABLE harklight.conversation_entries
  ALTER COLUMN request_revision DROP NOT NULL,
  ADD CONSTRAINT conversation_entries_request_revision
    CHECK (request_revision IS NOT NULL OR kind = 'read_position');

INSERT INTO harklight.schema_migrations (version) VALUES (4);
COMMIT;

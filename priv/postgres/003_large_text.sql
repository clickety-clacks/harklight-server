-- Run after 002_message_history.sql in the explicit migration job.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(1212240455, 1);

DO $$
BEGIN
  IF (SELECT max(version) FROM harklight.schema_migrations) IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'large text migration requires schema version 2';
  END IF;
END;
$$;

CREATE TABLE harklight.large_texts (
  sha256 bytea PRIMARY KEY CHECK (octet_length(sha256) = 32),
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  verified boolean NOT NULL DEFAULT false
);

CREATE TABLE harklight.large_text_chunks (
  sha256 bytea REFERENCES harklight.large_texts ON DELETE CASCADE,
  chunk_number bigint CHECK (chunk_number >= 0),
  bytes bytea NOT NULL CHECK (octet_length(bytes) BETWEEN 1 AND 1048576),
  PRIMARY KEY (sha256, chunk_number)
);

INSERT INTO harklight.schema_migrations (version) VALUES (3);
COMMIT;

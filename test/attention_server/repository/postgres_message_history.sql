BEGIN;
SET LOCAL search_path = harklight, pg_catalog;

INSERT INTO people (id) VALUES ('history-one'), ('history-two');
INSERT INTO principals (key, person_id, kind, revision)
  VALUES ('fixture-principal', 'history-one', 'device', 1);

INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
VALUES
  ('hosted-one', 'history-one', 'app:harklight/notices', 'app:harklight/notices', 'notice-one', 3, 10),
  ('hosted-two', 'history-two', 'app:harklight/notices', 'app:harklight/notices', 'notice-two', 1, 12),
  ('ordinary-one', 'history-one', 'fixture-principal', 'app:fixture', 'ordinary-one', 1, 20);

INSERT INTO message_revisions (message_id, revision, accepted_at_ms, body)
VALUES ('hosted-one', 1, 10, '{}'), ('hosted-one', 2, NULL, '{}'), ('hosted-one', 3, 30, '{}');

DO $$
BEGIN
  IF (SELECT max(version) FROM schema_migrations) <> 2 OR
     (SELECT count(*) FROM messages WHERE principal_origin_key IS NULL) <> 2 OR
     EXISTS (SELECT 1 FROM principals WHERE key = 'app:harklight/notices') OR
     (SELECT array_agg(accepted_at_ms ORDER BY revision)
      FROM message_revisions WHERE message_id = 'hosted-one')
       IS DISTINCT FROM ARRAY[10, NULL, 30]::bigint[] THEN
    RAISE EXCEPTION 'message history was not preserved';
  END IF;

  BEGIN
    INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
    VALUES ('foreign', 'history-two', 'fixture-principal', 'app:fixture', 'foreign', 1, 10);
    RAISE EXCEPTION 'foreign principal was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;

  BEGIN
    INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
    VALUES ('missing', 'unknown-person', 'app:harklight/notices', 'app:harklight/notices', 'notice', 1, 10);
    RAISE EXCEPTION 'Hosted notice without a person was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;

  BEGIN
    INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
    VALUES ('spoof', 'history-one', 'app:harklight/notices', 'app:fixture', 'spoof', 1, 10);
    RAISE EXCEPTION 'ordinary sender was accepted for a Hosted notice origin';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END;
$$;

ROLLBACK;

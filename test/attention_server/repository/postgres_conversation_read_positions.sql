BEGIN;
SET LOCAL search_path TO harklight, pg_catalog;

INSERT INTO people (id) VALUES ('conversation-person');
INSERT INTO principals (key, person_id, kind, revision)
  VALUES ('conversation-origin', 'conversation-person', 'device', 1);
INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
  VALUES ('conversation-message', 'conversation-person', 'conversation-origin', 'app:fixture', 'message', 2, 10);
INSERT INTO conversations (id, message_id) VALUES ('conversation-message', 'conversation-message');

INSERT INTO conversation_entries (id, conversation_id, position, request_revision, kind, body) VALUES
  ('3', 'conversation-message', 3, 2, 'reply', '{"event_id":"7","request_revision":2}'),
  ('4', 'conversation-message', 4, NULL, 'read_position',
   '{"event_id":"read:4","read_through_conversation_seq":3}'),
  ('6', 'conversation-message', 6, 2, 'file_use', '{"event_id":"7","request_revision":2}');
INSERT INTO read_positions VALUES ('conversation-message', 'conversation-person', 3);

DO $$
BEGIN
  IF (SELECT max(version) FROM schema_migrations) <> 4 OR
     (SELECT count(*) FROM conversation_entries WHERE conversation_id = 'conversation-message') <> 3 OR
     NOT EXISTS (
       SELECT 1 FROM conversation_entries
       WHERE conversation_id = 'conversation-message' AND id = '4' AND position = 4
         AND request_revision IS NULL AND body = '{"event_id":"read:4","read_through_conversation_seq":3}'
     ) OR
     NOT EXISTS (SELECT 1 FROM read_positions WHERE conversation_id = 'conversation-message' AND position = 3)
  THEN
    RAISE EXCEPTION 'conversation positions were not preserved';
  END IF;

  BEGIN
    INSERT INTO conversation_entries (id, conversation_id, position, request_revision, kind, body)
      VALUES ('7', 'conversation-message', 7, NULL, 'reply', '{}');
    RAISE EXCEPTION 'non-read fact accepted a missing revision';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END;
$$;

ROLLBACK;

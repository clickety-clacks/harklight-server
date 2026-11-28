BEGIN;
SET LOCAL search_path TO harklight, pg_catalog;

INSERT INTO people (id) VALUES ('history-sender'), ('history-recipient');
INSERT INTO principals (key, person_id, kind, revision)
  VALUES ('history-device', 'history-sender', 'device', 1);
INSERT INTO organizations (id, hosted_ref) VALUES ('history-org', 'hosted-history-org');
INSERT INTO groups (id, organization_id, name, hosted_ref)
  VALUES ('history-group', 'history-org', 'Retained group name', 'hosted-history-group');

INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
  VALUES ('ordinary/history-sender/i1', 'history-sender', 'history-device', 'app:fixture', 'shared_fixture', 1, 10);
INSERT INTO messages
  (id, person_id, origin_key, sender_id, message_id, kind, group_id, revision, accepted_at_ms, attributes)
  VALUES ('shared_fixture', 'history-sender', 'historic-device', 'app:fixture', 'shared_fixture',
          'shared', 'history-group', 2, 11, '{"group_id":"history-group","group_name":"Retained group name"}');
INSERT INTO message_revisions VALUES ('shared_fixture', 1, 11, '{"body":"original"}'),
                                     ('shared_fixture', 2, 12, '{"body":"latest"}');
INSERT INTO message_recipients VALUES ('shared_fixture', 'history-recipient', 'shared_recipient_item', 1,
                                      '{"frozen":{"revision":1}}');
DELETE FROM groups WHERE id = 'history-group';

DO $$
BEGIN
  IF (SELECT max(version) FROM schema_migrations) <> 5 OR
     (SELECT count(*) FROM messages WHERE person_id = 'history-sender' AND message_id = 'shared_fixture') <> 2 OR
     NOT EXISTS (
       SELECT 1 FROM messages
       WHERE id = 'shared_fixture' AND kind = 'shared' AND origin_key = 'historic-device'
         AND principal_origin_key IS NULL AND group_id IS NULL
         AND attributes->>'group_name' = 'Retained group name' AND attributes->>'group_id' = 'history-group'
     ) OR
     NOT EXISTS (
       SELECT 1 FROM message_recipients
       WHERE message_id = 'shared_fixture' AND person_id = 'history-recipient' AND item_id = 'shared_recipient_item'
         AND seen_revision = 1 AND attributes->'frozen'->>'revision' = '1'
     ) OR EXISTS (SELECT 1 FROM principals WHERE key = 'historic-device')
  THEN
    RAISE EXCEPTION 'shared identities or retained history were not preserved';
  END IF;

  BEGIN
    INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
      VALUES ('ordinary/history-sender/i2', 'history-sender', 'history-device', 'app:fixture', 'shared_fixture', 1, 10);
    RAISE EXCEPTION 'duplicate ordinary identity was accepted';
  EXCEPTION WHEN unique_violation THEN NULL;
  END;

  BEGIN
    INSERT INTO messages (id, person_id, origin_key, sender_id, message_id, revision, accepted_at_ms)
      VALUES ('ordinary/history-recipient/i1', 'history-recipient', 'history-device', 'app:fixture', 'ordinary', 1, 10);
    RAISE EXCEPTION 'ordinary origin bypassed scoped principal reference';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;
END;
$$;

ROLLBACK;

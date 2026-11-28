BEGIN;
SET LOCAL search_path = harklight, pg_catalog;

INSERT INTO people (id) VALUES ('synthetic-person');
INSERT INTO principals (key, person_id, kind, revision)
  VALUES ('synthetic-origin', 'synthetic-person', 'device', 1);
INSERT INTO organizations (id) VALUES ('synthetic-local-org'), ('synthetic-hosted-org');
INSERT INTO groups (id, organization_id, manager_id, name, hosted_ref) VALUES
  ('local-group', 'synthetic-local-org', 'synthetic-person', 'Local', NULL),
  ('hosted-group', 'synthetic-hosted-org', NULL, 'Hosted', 'external-group');

INSERT INTO allocation_highs VALUES
  ('synthetic-person', '', '', 'accepted', 90),
  ('synthetic-person', '', '', 'free_order', 35),
  ('synthetic-person', '', '', 'conversation', 58),
  ('synthetic-person', 'synthetic-origin', '', 'origin_feed', 60),
  ('synthetic-person', 'synthetic-origin', 'synthetic-session', 'advisory_feed', 93);
INSERT INTO origin_feed_entries VALUES
  ('synthetic-person', 'synthetic-origin', 'synthetic-session', 4, '{"first_served":true}'),
  ('synthetic-person', 'synthetic-origin', 'synthetic-session', 9, '{}');
INSERT INTO origin_delivery_acks VALUES
  ('synthetic-person', 'synthetic-origin', 4, '{"ack1":22,"ack2":28}');
INSERT INTO advisory_feed_entries VALUES
  ('synthetic-person', 'synthetic-origin', 'synthetic-session', 7, '{}');
INSERT INTO advisory_feed_receipts VALUES
  ('synthetic-person', 'synthetic-origin', 'synthetic-session', 'synthetic-advisory');

DO $$
BEGIN
  INSERT INTO import_state (import_id, source_sha256, schema_sha256, phase, manifest_sha256)
    VALUES ('invalid', decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'),
            'active', decode(repeat('04', 32), 'hex'));
  RAISE EXCEPTION 'direct active insert was accepted';
EXCEPTION WHEN check_violation THEN NULL;
END;
$$;

INSERT INTO import_state (import_id, source_sha256, schema_sha256, phase)
  VALUES ('synthetic-import', decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'), 'loading');
DO $$
BEGIN
  UPDATE import_state SET import_id = 'replacement';
  RAISE EXCEPTION 'import identifier changed';
EXCEPTION WHEN check_violation THEN NULL;
END;
$$;
INSERT INTO import_checkpoints VALUES
  ('synthetic-import', 'origin_feed', 2, decode(repeat('03', 32), 'hex'));

DO $$
BEGIN
  IF (SELECT high FROM allocation_highs WHERE kind = 'origin_feed') <>
      60 OR (SELECT max(position) FROM origin_feed_entries) <> 9 THEN
    RAISE EXCEPTION 'origin allocation high lost';
  END IF;

  IF (SELECT count(*) FROM origin_feed_entries) <> 2 OR
     (SELECT body->>'ack2' FROM origin_delivery_acks WHERE position = 4) <> '28' THEN
    RAISE EXCEPTION 'pending entry/ack distinction lost';
  END IF;

  BEGIN
    INSERT INTO groups (id, organization_id, manager_id, name, hosted_ref)
      VALUES ('invalid', 'synthetic-hosted-org', 'synthetic-person', 'Invalid', 'external');
    RAISE EXCEPTION 'conflicting custody was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;

  BEGIN
    UPDATE import_state SET phase = 'active', manifest_sha256 = decode(repeat('04', 32), 'hex')
      WHERE import_id = 'synthetic-import';
    RAISE EXCEPTION 'unvalidated activation was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;

  BEGIN
    UPDATE import_state SET source_sha256 = decode(repeat('05', 32), 'hex');
    RAISE EXCEPTION 'import source identity changed';
  EXCEPTION WHEN check_violation THEN NULL;
  END;

  BEGIN
    UPDATE import_state SET schema_sha256 = decode(repeat('05', 32), 'hex');
    RAISE EXCEPTION 'import schema identity changed';
  EXCEPTION WHEN check_violation THEN NULL;
  END;

  BEGIN
    DELETE FROM import_state;
    RAISE EXCEPTION 'import ownership was removed';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END;
$$;

UPDATE import_state SET phase = 'validated', manifest_sha256 = decode(repeat('04', 32), 'hex');
DO $$
BEGIN
  UPDATE import_state SET phase = 'active', manifest_sha256 = decode(repeat('06', 32), 'hex');
  RAISE EXCEPTION 'activation substituted the validated manifest';
EXCEPTION WHEN check_violation THEN NULL;
END;
$$;
UPDATE import_state SET phase = 'active'
  WHERE import_id = 'synthetic-import'
    AND phase = 'validated'
    AND source_sha256 = decode(repeat('01', 32), 'hex')
    AND schema_sha256 = decode(repeat('02', 32), 'hex')
    AND manifest_sha256 = decode(repeat('04', 32), 'hex');
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM import_state WHERE phase = 'active') THEN
    RAISE EXCEPTION 'validated import did not activate';
  END IF;
END;
$$;

ROLLBACK;

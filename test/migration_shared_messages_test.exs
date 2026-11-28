if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.SharedMessagesTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, Engine, Groups, SharedMessages, Store}
    alias AttentionServer.ClientAPI.SharedCapability
    alias AttentionServer.Migration.{Bundle, ImportConversations, ImportEntries, ImportMessages, ImportSession, ImportSharedMessages, ImportSharedReaderFacts, ImportUploads}
    alias AttentionServer.Repository.Postgres.{Attributes, Schema}

    @sender "a1000000-0000-4000-8000-000000000001"
    @first "a1000000-0000-4000-8000-000000000002"
    @second "a1000000-0000-4000-8000-000000000003"
    @outside "a1000000-0000-4000-8000-000000000004"
    @origin AttentionServer.BrowserRecipient.key(@sender, "synthetic-retained-session", "a6000000-0000-4000-8000-000000000001")
    @upload "a5000000-0000-4000-8000-000000000001"
    @group "a2000000-0000-4000-8000-000000000001"
    @shared "shared_synthetic-collision"

    test "shared records retain historical authority and frozen revisions beside ordinary collisions" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      db = start_supervised!({Postgrex, [hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password), database: "migration_shared_history"]})
      root = Path.join(System.tmp_dir!(), "shared-import-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "source"))
      {:ok, relay} = CompatibilityScenarios.start(fixture)
      CompatibilityScenarios.provision(relay, fixture)
      GenServer.stop(relay)
      Agent.stop(fixture.clock)
      path = Path.join(fixture.root, "relay.sqlite3")
      source = seed(path)
      bundle = Path.join(root, "bundle")
      assert {:ok, summary} = Bundle.export(path, bundle,
        supervisor: {:operator, "synthetic fixture", "Relay stopped"}, material: %{})
      identity = %{id: "synthetic-shared-import", source: Base.decode16!(summary.sha256, case: :lower), schema: Schema.digest()}
      assert :ok = ImportMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_shared_recipient() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.item_id LIKE 'shared_%' THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_shared_recipient BEFORE INSERT ON harklight.message_recipients FOR EACH ROW EXECUTE FUNCTION harklight.fail_shared_recipient()", [])
      assert {:error, _} = ImportSharedMessages.run(db, identity, bundle)
      assert {:ok, %{ordinal: 1}} = ImportSession.checkpoint(db, identity, "shared/shared_messages/v1")
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "shared/shared_recipients/v1")
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_shared_recipient ON harklight.message_recipients", [])
      assert :ok = ImportSharedMessages.run(db, identity, bundle)
      assert :ok = ImportSharedMessages.run(db, identity, bundle)
      assert :ok = ImportUploads.run(db, identity, bundle)
      assert {:ok, %{rows: [upload_row]}} = ImportSession.query(db,
        "SELECT id,person_id,owner_key,size_bytes,sha256,completed_at_ms,attributes FROM harklight.uploads WHERE id=$1", [@upload])
      assert {:ok, %{rows: chunks}} = ImportSession.query(db,
        "SELECT chunk_number,bytes FROM harklight.upload_chunks WHERE upload_id=$1 ORDER BY chunk_number", [@upload])
      assert {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.uploads WHERE id=$1", [@upload])
      assert {:error, _} = ImportSharedReaderFacts.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.uploads(id,person_id,owner_key,size_bytes,sha256,completed_at_ms,attributes) VALUES($1,$2,$3,$4,$5,$6,$7)", upload_row)
      for [ordinal, bytes] <- chunks do
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.upload_chunks(upload_id,chunk_number,bytes) VALUES($1,$2,$3)", [@upload, ordinal, bytes])
      end
      for {column, bad, good} <- [{"completed_at_ms", nil, Enum.at(upload_row, 5)},
                                  {"owner_key", "unrelated-owner", Enum.at(upload_row, 2)},
                                  {"person_id", @outside, @sender}] do
        assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.uploads SET " <> column <> "=$2 WHERE id=$1", [@upload, bad])
        assert {:error, _} = ImportSharedReaderFacts.run(db, identity, bundle)
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "shared/reader-messages/v1")
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
          "SELECT count(*) FROM harklight.message_uploads WHERE message_id=$1", [@shared])
        assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.uploads SET " <> column <> "=$2 WHERE id=$1", [@upload, good])
      end
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_shared_scope() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.family='shared_revision_scope' THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_shared_scope BEFORE INSERT ON harklight.compatibility_facts FOR EACH ROW EXECUTE FUNCTION harklight.fail_shared_scope()", [])
      assert {:error, _} = ImportSharedReaderFacts.run(db, identity, bundle)
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "shared/reader-recipients/v1")
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family='shared_revision_scope'", [])
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_shared_scope ON harklight.compatibility_facts", [])
      assert :ok = ImportSharedReaderFacts.run(db, identity, bundle)
      assert :ok = ImportSharedReaderFacts.run(db, identity, bundle)
      for person <- Map.keys(source.persons) do
        expected_access = MapSet.size(SharedMessages.organizations(source, person)) > 0 or SharedMessages.retained_history?(source, person)
        assert read_fact(db, "shared_read_access", person) == expected_access
      end
      assert read_fact(db, "shared_read_access", @outside) == false
      assert read_fact(db, "shared_read_access", @sender) == true
      for {{id, person}, recipient} <- source.shared_recipients do
        record = Map.fetch!(source.shared_messages, id)
        assert read_fact(db, "shared_revision_scope", {person, recipient.item_id}) ==
          %{group?: Map.has_key?(record, :group_id), frozen_revision: get_in(recipient, [:frozen, :revision])}
      end
      assert {:ok, %{rows: [[@upload]]}} = ImportSession.query(db,
        "SELECT upload_id FROM harklight.message_uploads WHERE message_id=$1 AND revision=1 AND ordinal=0", [@shared])
      for revision <- [1, 2] do
        historical = SharedMessages.historical_record(source.shared_messages[@shared], revision)
        assert read_fact(db, "shared_revision_files", {@shared, revision}) == Map.get(historical, :files, [])
      end
      verify_link_drift(db, identity, bundle)
      alias AttentionServer.Migration.VerifySharedMessages
      assert {:ok, verified} = VerifySharedMessages.run(db, identity, bundle)
      assert verified == %{messages: 1, revisions: 2, recipients: 2}
      for {column, bad, good} <- [{"accepted_at_ms", 999, 100},
                                  {"origin_key", "changed-historical-origin", @origin}] do
        assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.messages SET " <> column <> "=$2 WHERE id=$1", [@shared, bad])
        assert {:error, {:shared_message_mismatch, :message, _}} = VerifySharedMessages.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.messages SET " <> column <> "=$2 WHERE id=$1", [@shared, good])
        assert {:ok, ^verified} = VerifySharedMessages.run(db, identity, bundle)
      end
      assert {:ok, %{rows: [[original_recipient]]}} = ImportSession.query(db,
        "SELECT attributes FROM harklight.message_recipients WHERE message_id=$1 AND person_id=$2", [@shared, @first])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET attributes='{}'::jsonb WHERE message_id=$1 AND person_id=$2", [@shared, @first])
      assert {:error, {:shared_message_mismatch, :recipient, _}} = VerifySharedMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET attributes=$3 WHERE message_id=$1 AND person_id=$2", [@shared, @first, original_recipient])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_revisions SET accepted_at_ms=999 WHERE message_id=$1 AND revision=1", [@shared])
      assert {:error, {:shared_message_mismatch, {:revision, 1}, _}} = VerifySharedMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_revisions SET accepted_at_ms=100 WHERE message_id=$1 AND revision=1", [@shared])
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.message_revisions(message_id,revision,body) SELECT message_id,99,body FROM harklight.message_revisions WHERE message_id=$1 AND revision=1", [@shared])
      assert {:error, {:shared_count_mismatch, :revisions}} = VerifySharedMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.message_revisions WHERE message_id=$1 AND revision=99", [@shared])
      assert {:ok, ^verified} = VerifySharedMessages.run(db, identity, bundle)
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
      alias AttentionServer.Migration.ImportSharedConversations
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_shared_entry() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.id='shared-retained-turn' THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_shared_entry BEFORE INSERT ON harklight.conversation_entries FOR EACH ROW EXECUTE FUNCTION harklight.fail_shared_entry()", [])
      assert {:error, _} = ImportSharedConversations.run(db, identity, bundle)
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "shared/conversations-shared_entries/v1")
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_shared_entry ON harklight.conversation_entries", [])
      assert :ok = ImportSharedConversations.run(db, identity, bundle)
      assert :ok = ImportSharedConversations.run(db, identity, bundle)
      assert map_size(source.shared_entries) > 0
      assert map_size(source.shared_answers) > 0
      for {id, entry} <- source.shared_entries do
        assert {:ok, %{rows: [[position, revision, "reply", body]]}} = ImportSession.query(db,
          "SELECT position,request_revision,kind,body FROM harklight.conversation_entries WHERE conversation_id=$1 AND id=$2", [@shared, id])
        assert position == entry.position
        assert revision == entry.request_revision
        assert {:ok, ^entry} = Attributes.decode(body)
      end
      for {id, answer} <- source.shared_answers do
        assert {:ok, %{rows: [[person, revision, body]]}} = ImportSession.query(db,
          "SELECT person_id,request_revision,body FROM harklight.answers WHERE message_id=$1 AND id=$2", [@shared, id])
        assert person == answer.person
        assert revision == answer.request_revision
        assert {:ok, ^answer} = Attributes.decode(body)
      end
      expected_read = source.shared_recipients[{@shared, @second}].read_position
      assert {:ok, %{rows: [[^expected_read]]}} = ImportSession.query(db,
        "SELECT position FROM harklight.read_positions WHERE conversation_id=$1 AND person_id=$2", [@shared, @second])
      alias AttentionServer.Migration.VerifySharedConversations
      assert {:ok, conversation_counts} = VerifySharedConversations.run(db, identity, bundle)
      assert conversation_counts.entries == map_size(source.shared_entries)
      assert conversation_counts.answers == map_size(source.shared_answers)
      for {field, table, column, bad} <- [
        {:conversations, "conversations", "attributes", "'{}'::jsonb"},
        {:entries, "conversation_entries", "body", "'{}'::jsonb"},
        {:entries, "conversation_entries", "position", "position+100"},
        {:entries, "conversation_entries", "request_revision", "2"},
        {:answers, "answers", "body", "'{}'::jsonb"},
        {:read_positions, "read_positions", "position", "position+100"}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        scope = case field do
          :conversations -> "id"
          :answers -> "message_id"
          _ -> "conversation_id"
        end
        assert {:ok, %{num_rows: changed}} = ImportSession.query(tx,
          "UPDATE harklight." <> table <> " SET " <> column <> "=" <> bad <> " WHERE " <> scope <> "=$1", [@shared])
        assert changed > 0
        assert {:error, {:shared_conversation_mismatch, ^field, _}} = VerifySharedConversations.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^conversation_counts} = VerifySharedConversations.run(db, identity, bundle)
      end
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.conversation_entries(id,conversation_id,position,request_revision,kind,body) SELECT 'extra-entry',conversation_id,position+100,request_revision,kind,body FROM harklight.conversation_entries WHERE conversation_id=$1 LIMIT 1", [@shared])
      assert {:error, {:shared_conversation_count_mismatch, :entries}} = VerifySharedConversations.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.conversation_entries WHERE conversation_id=$1 AND id='extra-entry'", [@shared])
      assert {:ok, ^conversation_counts} = VerifySharedConversations.run(db, identity, bundle)
      # Both ordinary tuple consumers must still resolve the ordinary row.
      assert :ok = ImportConversations.run(db, identity, bundle)
      assert :ok = ImportEntries.run(db, identity, bundle)
      assert {:ok, %{rows: [[2]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.messages WHERE person_id=$1 AND sender_id='app:shared-fixture' AND message_id=$2", [@sender, @shared])
      assert {:ok, %{rows: [[nil, nil, nil, nil, attrs]]}} = ImportSession.query(db,
        "SELECT group_id,session_handle,accepted_position,principal_origin_key,attributes FROM harklight.messages WHERE id=$1 AND kind='shared'", [@shared])
      expected = source.shared_messages[@shared]
      assert {:ok, ^expected} = Attributes.decode(attrs)
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.principals WHERE key=$1", [@origin])
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.groups WHERE id=$1", [@group])
      assert {:ok, %{rows: rows}} = ImportSession.query(db,
        "SELECT person_id,attributes FROM harklight.message_recipients WHERE message_id=$1 ORDER BY person_id", [@shared])
      recipients = Map.new(rows, fn [person, body] ->
        assert {:ok, recipient} = Attributes.decode(body)
        assert recipient == source.shared_recipients[{@shared, person}]
        {{@shared, person}, recipient}
      end)
      restored = %{source | shared_messages: %{@shared => expected}, shared_recipients: recipients}
      for person <- Map.keys(source.persons), groups? <- [false, true], revision <- [1, 2, 3] do
        assert visible_revision(restored, person, groups?, revision) == visible_revision(source, person, groups?, revision)
      end
      assert {:ok, %{"request_revision" => 1}} = visible_revision(restored, @first, true, 1)
      assert :not_found = visible_revision(restored, @first, true, 2)
      assert {:ok, %{"request_revision" => 2}} = visible_revision(restored, @second, true, 2)
      assert :not_found = visible_revision(restored, @second, false, 1)
      for revision <- [1, 2] do
        historical = SharedMessages.historical_record(expected, revision)
        assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
          "SELECT body FROM harklight.message_revisions WHERE message_id=$1 AND revision=$2", [@shared, revision])
        assert {:ok, message} = Attributes.decode(body)
        assert message == historical.message
      end
    end

    defp verify_link_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyMessageUploads
      assert {:ok, verified} = VerifyMessageUploads.run(db, identity, bundle, :shared)
      assert verified.links > 0
      for mutation <- [
        "UPDATE harklight.message_uploads SET attributes='{}'::jsonb WHERE message_id=$1",
        "DELETE FROM harklight.message_uploads WHERE message_id=$1",
        "UPDATE harklight.message_uploads SET ordinal=ordinal+10 WHERE message_id=$1"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, mutation, [@shared])
          assert n > 0
          assert {:error, {:message_upload_mismatch, :shared, _, _, _}} = VerifyMessageUploads.run(tx, identity, bundle, :shared)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^verified} = VerifyMessageUploads.run(db, identity, bundle, :shared)
      end
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx,
          "INSERT INTO harklight.message_uploads(message_id,revision,ordinal,upload_id,attributes) SELECT message_id,revision,ordinal+100,upload_id,attributes FROM harklight.message_uploads WHERE message_id=$1", [@shared])
        assert {:error, {:message_upload_count_mismatch, :shared}} = VerifyMessageUploads.run(tx, identity, bundle, :shared)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^verified} = VerifyMessageUploads.run(db, identity, bundle, :shared)
    end

    defp read_fact(db, family, key) do
      assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
        "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, Attributes.key(key)])
      assert {:ok, value} = Attributes.decode(body)
      value
    end

    defp visible_revision(engine, person, groups?, revision) do
      with true <- MapSet.size(SharedMessages.organizations(engine, person)) > 0 or SharedMessages.retained_history?(engine, person),
           %{item_id: item} <- engine.shared_recipients[{@shared, person}],
           {:ok, record, recipient} <- SharedMessages.recipient_message(engine, person, item),
           true <- SharedCapability.visible?(record, %{groups?: groups?}),
           historical = SharedMessages.historical_record(record, get_in(recipient, [:frozen, :revision])),
           true <- revision <= historical.revision do
        {:ok, SharedMessages.revision(SharedMessages.historical_record(historical, revision))}
      else
        _ -> :not_found
      end
    end

    defp seed(path) do
      {:ok, store} = Store.open_relay(path)
      bytes = "shared retained attachment"
      metadata = %{id: @upload, person: @sender, uploader: @origin,
        uploader_kind: "client", op_id: "a5000000-0000-4000-8000-000000000002",
        sender_id: "app:shared-fixture", message_id: @shared, revision: 1,
        name: "retained.txt", size: byte_size(bytes), sha256: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
        last_activity_ms: 100, content_type: "text/plain"}
      File.mkdir_p!(store.upload_dir)
      File.write!(Path.join(store.upload_dir, @upload), bytes)
      files = AttentionServer.ClientAPI.SharedAttachments.references([{metadata, nil}])
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
        engine = Engine.put_row(engine, :uploads, @upload, metadata)
        engine = Enum.reduce(Enum.with_index([@sender, @first, @second, @outside]), engine, fn {person, n}, acc ->
          {:ok, next, _} = Engine.hosted_account_create(acc, "shared-fixture-#{n}", false, person, String.duplicate(Integer.to_string(n + 1), 64))
          next
        end)
        {:ok, engine} = Engine.replace_hosted_organization(engine, "shared-business", [@sender, @first, @second], "a3000000-0000-4000-8000-000000000001")
        {:ok, engine, _} = Groups.replace_hosted(engine, {"shared-business", "team"}, @group, "Retained group", [@sender, @first, @second], 1)
        attrs = %{id: @shared, sender_person: @sender, sender_key: @origin,
          sender_id: "app:shared-fixture", message: %{"title" => "First shared revision"},
          answer_policy: :everyone, files: files, group_id: @group, group_name: "Retained group"} |> Map.put("created_at_ms", 100)
        {:ok, engine, record, _} = SharedMessages.create(engine, attrs, [@first, @second], nil)
        entry = %{id: "shared-retained-turn", at_ms: 101, text: "Retained discussion",
          speaker: %{"kind" => "person", "id" => @first}}
        {:ok, engine, _} = SharedMessages.reply(engine, record, {:person, @first}, entry)
        record = engine.shared_messages[@shared]
        {:ok, engine} = SharedMessages.read_position(engine, record, @second,
          engine.shared_recipients[{@shared, @second}], SharedMessages.recipient_entry_id(entry.id, @second))
        {:ok, engine, _} = AttentionServer.SharedAnswers.answer(engine, record,
          engine.shared_recipients[{@shared, @first}], @first, 1, {:comment, "Retained answer"}, 102)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"shared-business", "team"}, @group, "Retained group", [@sender, @second], 2)
        {:ok, engine} = SharedMessages.revise(engine, record, 1, %{"title" => "Second shared revision"}, [], 200)
        {:ok, engine} = Groups.delete_hosted(engine, {"shared-business", "team"})
        {:ok, engine} = Engine.delete_hosted_organization(engine, "shared-business")
        {:ok, engine} = Engine.setup_device(engine, @sender, "ordinary-live-origin")
        {:ok, engine} = Engine.setup_sender(engine, "ordinary-live-origin", "app:shared-fixture")
        {:ok, engine} = Engine.setup_device(engine, @sender, "ordinary-viewer")
        frame = %{"type" => "forward", "op_id" => "a4000000-0000-4000-8000-000000000001",
          "session_handle" => "a4000000-0000-4000-8000-000000000002",
          "message" => %{"sender_id" => "app:shared-fixture", "message_id" => @shared,
            "created_at" => 1, "title" => "Ordinary collision", "message_type" => "fyi",
            "answer_ui" => AttentionServer.Message.seen_descriptor()}}
        {engine, {:ok, _}} = Engine.request_command(engine, "ordinary-live-origin", frame, 300)
        discussion = %{"type" => "discussion", "sender_id" => "app:shared-fixture", "message_id" => @shared,
          "request_revision" => 1, "text" => "Discussion", "op_id" => "a4000000-0000-4000-8000-000000000003"}
        {engine, {:ok, turn}} = Engine.request_command(engine, "ordinary-viewer", discussion, 301)
        reply = %{"type" => "conversation_reply", "sender_id" => "app:shared-fixture", "message_id" => @shared,
          "request_revision" => 1, "in_reply_to" => String.to_integer(turn["event_id"]), "text" => "Reply",
          "op_id" => "a4000000-0000-4000-8000-000000000004"}
        {engine, {:ok, result}} = Engine.request_command(engine, "ordinary-live-origin", reply, 302)
        read = %{"type" => "conversation_read", "sender_id" => "app:shared-fixture", "message_id" => @shared,
          "read_through_conversation_seq" => result["seq"], "op_id" => "a4000000-0000-4000-8000-000000000005"}
        {engine, {:ok, _}} = Engine.request_command(engine, "ordinary-viewer", read, 303)
        {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: @sender, actor: "fixture", action: "shared_history"})
      source = store.engine
      :ok = Store.close(store)
      source
    end
  end
end

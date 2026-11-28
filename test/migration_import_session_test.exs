if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.ImportSessionTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON, Relay}
    alias AttentionServer.Migration.{Bundle, Frames, ImportFacts, ImportOrganizations, ImportSession, ImportTables, SourceTables, ImportAuth, ImportStreams, ImportReceipts, ImportMessages, ImportFeeds, ImportUploads}
    alias AttentionServer.Repository.Postgres.{Attributes, Operations, Repo, Transaction}
    alias AttentionServer.ClientAPI.{Idempotency, Writes}

    @receipt_at 1_700_000_000_000
    @ordinary_entry_upload "97000000-0000-4000-8000-000000000001"
    @original_json ~s({ "synthetic" : true })

    for epoch <- [1, 17] do
      # Full source/import/restore equivalence over >256 records and conversation
      # entries, including repeated read-only mutation probes, is deliberately
      # bounded separately from the smaller migration cases below.
      @tag timeout: 300_000, epoch: epoch
      test "interrupted migration and authority readback with epoch #{epoch}", %{epoch: epoch} do
        uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
        assert uri.host in ["localhost", "127.0.0.1"]
        [user, password] = String.split(uri.userinfo, ":", parts: 2)

        config = [
          hostname: uri.host,
          port: uri.port,
          username: URI.decode(user),
          password: URI.decode(password),
          database:
            if(epoch == 17,
              do: "migration_explicit_epoch",
              else: String.trim_leading(uri.path, "/")
            )
        ]

        db = start_supervised!({Postgrex, config})

        root =
          Path.join(System.tmp_dir!(), "import-history-#{System.unique_integer([:positive])}")

        File.mkdir!(root)
        File.chmod!(root, 0o700)
        on_exit(fn -> File.rm_rf!(root) end)
        fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
        {:ok, relay} = CompatibilityScenarios.start(fixture)

        try do
          CompatibilityScenarios.run(relay, fixture)
          {person, %{key: owner}} = Enum.at(:sys.get_state(relay).store.engine.owners, 0)
          device = Base.encode16(:crypto.hash(:sha256, "synthetic revoked device"), case: :lower)
          agent = Base.encode16(:crypto.hash(:sha256, "synthetic revoked agent"), case: :lower)
          assert :ok = Relay.setup_device(relay, person, device)
          assert :ok = Relay.setup_sender(relay, device, "app:device-scope")
          assert {:ok, subscription_reply} = Relay.request_line(relay, device, JSON.encode!(%{
            "v" => 1, "type" => "subscribe_remote",
            "op_id" => "95000000-0000-4000-8000-000000000001", "revision" => 7,
            "sender_ids" => ["app:device-scope", "app:retained-subscription"]
          }) <> "\n")
          assert {:ok, %{"revision" => 7}} = JSON.decode_line(subscription_reply)
          assert :ok = Relay.owner_revoke_device(relay, person, owner, device)

          assert :ok =
                   Relay.owner_register_agent(
                     relay,
                     person,
                     owner,
                     "30000000-0000-4000-8000-000000000001",
                     agent
                   )

          assert :ok =
                   Relay.owner_replace_agent_grants(
                     relay,
                     person,
                     owner,
                     agent,
                     1,
                     ["app:read"],
                     ["app:respond"]
                   )

          assert :ok =
                   Relay.owner_replace_agent_grants(
                     relay,
                     person,
                     owner,
                     agent,
                     2,
                     ["app:read", "app:read-more"],
                     ["app:respond"]
                   )

          assert :ok = Relay.owner_revoke_agent(relay, person, owner, agent)
        after
          GenServer.stop(relay)
        end

        # Synthetic setup: allocation high deliberately exceeds retained history.
        source_path = Path.join(fixture.root, "relay.sqlite3")
        seed_advisory(source_path)
        seed_historical_messages(source_path)
        seed_long_conversation(source_path)
        expected_organizations = seed_organizations(source_path, epoch)
        expected_recipients = source_recipient_expectations(source_path)
        expected_subscriptions = source_subscription_expectations(source_path)
        expected_chats = source_chat_expectations(source_path)
        {:ok, sqlite} = Exqlite.Sqlite3.open(source_path)

        {:ok, rows} =
          AttentionServer.Store.RecordTable.select(
            sqlite,
            "SELECT key,payload,checksum FROM relay_records"
          )

        {session_person, session_owner} = Enum.find_value(rows, fn [key, payload, hash] ->
          case AttentionServer.Store.RecordCodec.decode_row({key, payload, hash}) do
            {:ok, {:row, :owners, person}, owner} -> {person, owner}
            _ -> nil
          end
        end)
        {:ok, _} = AttentionServer.Store.AuthSchema.initialize(sqlite)
        :ok = AttentionServer.BrowserAuth.add_session(sqlite,
          :crypto.hash(:sha256, "synthetic migration browser session"),
          session_person, session_owner.key, session_owner.revision, "fixture")
        :ok = AttentionServer.BrowserAuth.add_session(sqlite,
          :crypto.hash(:sha256, "synthetic stale browser session"),
          session_person, session_owner.key, session_owner.revision + 5, "fixture")
        :ok = AttentionServer.BrowserAuth.add_session(sqlite,
          :crypto.hash(:sha256, "synthetic retired owner session"),
          session_person, String.duplicate("0", 64), 1, "fixture")
        devices = Enum.flat_map(rows, fn [key, payload, hash] ->
          case AttentionServer.Store.RecordCodec.decode_row({key, payload, hash}) do
            {:ok, {:row, :devices, id}, device} -> [{id, device}]
            _ -> []
          end
        end)
        active = devices |> Enum.reject(fn {_, d} -> d.revoked end) |> Enum.take(2)
        [revoked] = Enum.filter(devices, fn {_, d} -> d.revoked end)
        assert length(active) == 2
        for {{id, device}, index} <- Enum.with_index(active ++ [revoked]) do
          hash = :crypto.hash(:sha256, "synthetic native session #{index}") |> Base.encode16(case: :lower)
          :ok = AttentionServer.NativeSignIn.add_session(sqlite, hash, id, device.person,
            String.duplicate("a", 64), 1_700_000_000_000 + index, index == 0)
        end

        {address, _} =
          Enum.find_value(rows, fn [key, payload, hash] ->
            case AttentionServer.Store.RecordCodec.decode_row({key, payload, hash}) do
              {:ok, {:row, :persons, _} = address, value} -> {address, value}
              _ -> nil
            end
          end)

        {key, payload, hash} = AttentionServer.Store.RecordCodec.encode_row(address, 1_000_000)

        :ok =
          AttentionServer.Store.RecordTable.run(
            sqlite,
            "UPDATE relay_records SET payload=?,checksum=? WHERE key=?",
            [{:blob, payload}, {:blob, hash}, {:blob, key}]
          )

        for number <- 1..300 do
          {key, payload, hash} =
            AttentionServer.Store.RecordCodec.encode_row(
              {:row, :persons, "synthetic-extra-#{number}"},
              10_000 + number
            )

          :ok =
            AttentionServer.Store.RecordTable.run(
              sqlite,
              "INSERT INTO relay_records(key,payload,checksum,account) VALUES (?,?,?,1)",
              [{:blob, key}, {:blob, payload}, {:blob, hash}]
            )
        end

        # A removed resource may have no surviving item, but its cursor event
        # and canonical revision must survive. Independent highs may exceed it.
        {:ok, [[stream_person, head]]} = AttentionServer.Store.RecordTable.select(sqlite,
          "SELECT person_id,head FROM client_change_log_meta ORDER BY person_id LIMIT 1")
        removed_id = AttentionServer.ChangeLog.item_resource({stream_person, "app:synthetic", "removed-migration"})
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "UPDATE client_change_log_meta SET head=? WHERE person_id=?", [head + 20, stream_person])
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "INSERT INTO client_changes VALUES (?,?,?,?,?)", [stream_person, head + 1, "item_removed", removed_id, 1_700_000_000_000])
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "INSERT INTO client_resource_revs VALUES (?,?,?)", [stream_person, removed_id, head + 1])
        {:ok, scopes} = AttentionServer.Store.RecordTable.select(sqlite,
          "SELECT person_id,agent_key,head FROM agent_change_log_meta LIMIT 1")
        [[agent_person, agent_key, agent_head]] = scopes
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "UPDATE agent_change_log_meta SET head=? WHERE person_id=? AND agent_key=?", [agent_head + 20, agent_person, agent_key])
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "INSERT INTO agent_changes VALUES (?,?,?,?,?,NULL)", [agent_person, agent_key, "advisory:synthetic-expired", agent_head + 1, "synthetic-session"])
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "INSERT INTO agent_item_advisory_changes VALUES (?,?,?,?,?,?)", [agent_person, agent_key, "item-advisory:synthetic-current:" <> removed_id, agent_head + 2, "synthetic-session", 1_700_000_000_001])

        # Same key in the two receipt namespaces remains two distinct rows.
        receipt_digest = AttentionServer.ClientAPI.Idempotency.digest("POST", ["synthetic"], %{})
        for {table, original} <- [{"client_idempotency", nil}, {"harklight_app_receipts", 1}] do
          columns = if is_nil(original), do: "(person_id,idempotency_key,digest,status,body,at_ms)", else: "(person_id,idempotency_key,digest,status,body,at_ms,original_response)"
          digest = if original, do: receipt_digest, else: Idempotency.digest("POST", ["another"], %{})
          values = [session_person, "synthetic-same-key", digest, 201, @original_json, @receipt_at]
          values = if is_nil(original), do: values, else: values ++ [original]
          placeholders = Enum.map_join(values, ",", fn _ -> "?" end)
          :ok = AttentionServer.Store.RecordTable.run(sqlite,
            "INSERT INTO " <> table <> columns <> " VALUES (" <> placeholders <> ")", values)
        end
        :ok = AttentionServer.Store.RecordTable.run(sqlite,
          "INSERT INTO client_idempotency VALUES (?,?,?,?,?,?)",
          [Idempotency.agent_scope(agent_person, agent_key), "synthetic-agent-key", receipt_digest, 200,
           JSON.encode!(%{"file" => %{"synthetic" => "agent"}}), @receipt_at])

        for {table, key, body, flags} <- [
              {"client_idempotency", "synthetic-recent", %{"settings" => true}, []},
              {"harklight_app_receipts", "synthetic-compact", %{"item_id" => "i1"}, [0]}
            ] do
          values = [session_person, key, receipt_digest, 200, JSON.encode!(body), @receipt_at] ++ flags
          placeholders = Enum.map_join(values, ",", fn _ -> "?" end)
          :ok = AttentionServer.Store.RecordTable.run(sqlite, "INSERT INTO #{table} VALUES (#{placeholders})", values)
        end

        if epoch == 17 do
          :ok =
            AttentionServer.Store.RecordTable.run(
              sqlite,
              "INSERT INTO client_change_log_epoch(id,epoch) VALUES(1,17) ON CONFLICT(id) DO UPDATE SET epoch=17",
              []
            )
        end

        :ok = Exqlite.Sqlite3.close(sqlite)
        bundle = Path.join(root, "export")

        assert {:ok, summary} =
                 Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
                   supervisor: {:operator, "synthetic fixture", "seed Relay stopped"},
                   material: %{}
                 )

        {:ok, manifest, _} = Bundle.verify(bundle)

        receipt = Path.join(root, "import-receipt.json")
        assert {:ok, identity} = AttentionServer.Migration.ImportReceipt.open(receipt, bundle)
        assert identity.source == Base.decode16!(summary.sha256, case: :lower)
        assert {:ok, %{"phase" => "unclaimed", "checkpoint_rows" => 0}} =
          AttentionServer.Migration.ImportStatus.read(db, identity)

        # Refuse an already populated target rather than claiming it.
        {:ok, _} =
          ImportSession.query(db, "INSERT INTO harklight.people(id) VALUES ('occupied')", [])

        assert {:error, :destination_not_empty} = ImportSession.claim(db, identity)
        {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.people WHERE id='occupied'", [])
        assert {:ok, :claimed} = ImportSession.claim(db, identity)
        assert {:ok, :resumed} = ImportSession.claim(db, identity)

        assert {:error, :import_identity_mismatch} =
                 ImportSession.claim(db, %{identity | id: "other"})

        {:ok, zero} = ImportSession.checkpoint(db, identity, "interruption")
        parent = self()

        interrupted =
          Task.async(fn ->
            {:ok, own_db} = Postgrex.start_link(config)

            ImportSession.batch(own_db, identity, "interruption", zero, [:before_failure], fn tx,
                                                                                              _ ->
              {:ok, _} =
                ImportSession.query(tx, "INSERT INTO harklight.people(id) VALUES ('partial')", [])

              send(parent, :uncommitted_rows_written)

              receive do
                :never -> :ok
              end
            end)
          end)

        assert_receive :uncommitted_rows_written, 5_000
        Task.shutdown(interrupted, :brutal_kill)
        assert {:ok, ^zero} = ImportSession.checkpoint(db, identity, "interruption")

        assert {:ok, %{rows: [[0]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.people", [])

        assert {:ok, one} =
                 ImportSession.batch(db, identity, "interruption", zero, [:committed], fn tx, _ ->
                   case ImportSession.query(
                          tx,
                          "INSERT INTO harklight.people(id) VALUES ('committed')",
                          []
                        ) do
                     {:ok, _} -> :ok
                     error -> error
                   end
                 end)

        assert one.ordinal == 1
        assert {:ok, status} = AttentionServer.Migration.ImportStatus.read(db, identity)
        assert status["phase"] == "loading"
        assert status["checkpoint_families"] == 1 and status["checkpoint_rows"] == 1
        assert status["validation_sha256"] == nil
        assert {:error, :import_identity_mismatch} =
          AttentionServer.Migration.ImportStatus.read(db, %{identity | schema: :crypto.hash(:sha256, "wrong schema")})
        password_file = Path.join(root, "synthetic-db-password")
        File.write!(password_file, config[:password])
        File.chmod!(password_file, 0o600)
        env = %{"HARKLIGHT_MIGRATOR_URL" => "postgres://#{config[:username]}@127.0.0.1:#{config[:port]}/#{config[:database]}",
          "HARKLIGHT_MIGRATOR_PASSWORD_FILE" => password_file}
        previous = Map.new(Map.keys(env), &{&1, System.get_env(&1)})
        try do
          System.put_env(env)
          assert {:ok, ^status} = AttentionServer.Migration.CLI.run(["status", "--receipt", receipt])
        after
          Enum.each(previous, fn {key, value} ->
            if is_nil(value), do: System.delete_env(key), else: System.put_env(key, value)
          end)
        end

        assert {:error, :checkpoint_changed} =
                 ImportSession.batch(db, identity, "interruption", zero, [:stale], fn _, _ ->
                   flunk("stale checkpoint must not write")
                 end)

        # Fail the second batch after the first checkpoint has committed. This
        # trigger exists only in this dedicated throwaway database.
        {:ok, _} =
          ImportSession.query(
            db,
            """
            CREATE FUNCTION harklight.synthetic_interrupt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF (SELECT count(*) FROM harklight.compatibility_facts) >= 256 THEN
                RAISE EXCEPTION 'synthetic interruption after committed batch';
              END IF;
              RETURN NEW;
            END $$
            """,
            []
          )

        {:ok, _} =
          ImportSession.query(
            db,
            """
            CREATE TRIGGER synthetic_interrupt BEFORE INSERT ON harklight.compatibility_facts
            FOR EACH ROW EXECUTE FUNCTION harklight.synthetic_interrupt()
            """,
            []
          )

        assert {:error, _} = ImportFacts.run(db, identity, bundle)

        assert {:ok, %{ordinal: 256} = boundary} =
                 ImportSession.checkpoint(db, identity, "records")

        assert {:ok, %{rows: [[256]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        {:ok, _} =
          ImportSession.query(
            db,
            "DROP TRIGGER synthetic_interrupt ON harklight.compatibility_facts",
            []
          )

        {:ok, _} =
          ImportSession.query(
            db,
            "UPDATE harklight.import_checkpoints SET cumulative_sha256=$1 WHERE family='records'",
            [:binary.copy(<<0>>, 32)]
          )

        assert {:error, :checkpoint_source_mismatch} = ImportFacts.run(db, identity, bundle)

        assert {:ok, %{rows: [[256]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        {:ok, _} =
          ImportSession.query(
            db,
            "UPDATE harklight.import_checkpoints SET cumulative_sha256=$1 WHERE family='records'",
            [boundary.digest]
          )

        assert {:ok, complete} = ImportFacts.run(db, identity, bundle)
        assert complete.ordinal == manifest.data.records.count
        assert {:ok, ^complete} = ImportFacts.run(db, identity, bundle)
        assert {:ok, ^complete} = ImportSession.checkpoint(db, identity, "records")

        assert {:ok, %{rows: [[count]]}} =
                 ImportSession.query(db, "SELECT count(*) FROM harklight.compatibility_facts", [])

        assert count == complete.ordinal

        assert {:ok, :ok} =
                 Frames.fold(
                   Path.join(bundle, "records.frames"),
                   manifest.data.records,
                   :ok,
                   fn {address, expected}, :ok ->
                     {:ok, %{rows: [[text]]}} =
                       ImportSession.query(
                         db,
                         "SELECT body::text FROM harklight.compatibility_facts WHERE address = $1",
                         [Attributes.key(address)]
                       )

                     {:ok, envelope} = JSON.decode_line(text <> "\n")
                     assert {:ok, ^expected} = Attributes.decode(envelope)
                     {:ok, :ok}
                   end
                 )

        assert {:ok, %{rows: [["loading", nil]]}} =
                 ImportSession.query(
                   db,
                   "SELECT phase, manifest_sha256 FROM harklight.import_state",
                   []
                 )

        {:ok, _} =
          ImportSession.query(db, "DELETE FROM harklight.people WHERE id='committed'", [])

        assert :ok = ImportOrganizations.run(db, identity, bundle)
        assert :ok = ImportOrganizations.run(db, identity, bundle)
        verify_organizations(db, expected_organizations)
        verify_organization_drift(db, identity, bundle)
        assert :ok = ImportTables.run(db, identity, bundle)
        assert :ok = ImportTables.run(db, identity, bundle)
        verify_tables(db, bundle, manifest)
        assert :ok = ImportAuth.run(db, identity, bundle)
        assert :ok = ImportAuth.run(db, identity, bundle)
        verify_auth(db, bundle, manifest)
        verify_auth_drift(db, identity, bundle)
        assert :ok = ImportStreams.run(db, identity, bundle)
        assert :ok = ImportStreams.run(db, identity, bundle)
        assert {:ok, _} = AttentionServer.Migration.VerifyStreamPositions.run(db, identity, bundle)
        assert {:ok, %{epoch: ^epoch}} = AttentionServer.Migration.VerifyStreamHeads.run(db, identity, bundle)
        verify_streams(db, bundle, manifest)
        verify_stream_position_drift(db, identity, bundle)
        verify_stream_head_drift(db, identity, bundle)
        assert :ok = ImportReceipts.run(db, identity, bundle)
        assert :ok = ImportReceipts.run(db, identity, bundle)
        verify_receipts(db, bundle, manifest)
        verify_receipt_drift(db, identity, bundle)
        assert :ok = ImportFeeds.run(db, identity, bundle)
        assert :ok = ImportFeeds.run(db, identity, bundle)
        verify_feeds(db, bundle, manifest)
        assert :ok = ImportMessages.run(db, identity, bundle)
        assert :ok = ImportMessages.run(db, identity, bundle)
        start_supervised!({Repo, config})
        assert {:error, :postgres_not_active} = Transaction.verify_ready()
        assert :ok = AttentionServer.Migration.ImportRecipients.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportRecipients.run(db, identity, bundle)
        for {id, person, seen, metadata} <- expected_recipients do
          assert {:ok, %{rows: [[^seen, actual]]}} = ImportSession.query(db,
            "SELECT seen_revision,attributes FROM harklight.message_recipients WHERE message_id=$1 AND person_id=$2", [id, person])
          assert {:ok, ^metadata} = Attributes.decode(actual)
        end
        assert {:ok, _} = ImportSession.query(db, """
          CREATE FUNCTION harklight.fail_subscription_head() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.family='device_subscription_heads' THEN RAISE EXCEPTION 'synthetic interruption'; END IF;
            RETURN NEW;
          END $$
          """, [])
        assert {:ok, _} = ImportSession.query(db, """
          CREATE TRIGGER fail_subscription_head BEFORE INSERT ON harklight.compatibility_facts
          FOR EACH ROW EXECUTE FUNCTION harklight.fail_subscription_head()
          """, [])
        assert {:error, _} = AttentionServer.Migration.ImportSubscriptions.run(db, identity, bundle)
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
          "SELECT count(*) FROM harklight.compatibility_facts WHERE family IN ('device_subscription_heads','device_subscription_senders')", [])
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "devices/subscriptions/v1")
        assert {:ok, _} = ImportSession.query(db,
          "DROP TRIGGER fail_subscription_head ON harklight.compatibility_facts", [])
        assert :ok = AttentionServer.Migration.ImportSubscriptions.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportSubscriptions.run(db, identity, bundle)
        for {person, device, revision, senders} <- expected_subscriptions do
          assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
            "SELECT body FROM harklight.compatibility_facts WHERE family='device_subscription_heads' AND address=$1",
            [Attributes.key({person, device})])
          assert {:ok, ^revision} = Attributes.decode(body)
          for sender <- senders do
            assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
              "SELECT body FROM harklight.compatibility_facts WHERE family='device_subscription_senders' AND address=$1",
              [Attributes.key({person, device, revision, sender})])
            assert {:ok, true} = Attributes.decode(body)
          end
          assert {:ok, %{rows: []}} = ImportSession.query(db,
            "SELECT body FROM harklight.compatibility_facts WHERE family='device_subscription_senders' AND address=$1",
            [Attributes.key({person, device, revision, "app:never-subscribed"})])
        end
        verify_subscription_drift(db, identity, bundle, expected_subscriptions)
        verify_messages(db, bundle, manifest)
        assert {:ok, _} = ImportSession.query(db, """
          CREATE FUNCTION harklight.fail_answer() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'synthetic answer interruption'; END $$
          """, [])
        assert {:ok, _} = ImportSession.query(db,
          "CREATE TRIGGER fail_answer BEFORE INSERT ON harklight.answers FOR EACH ROW EXECUTE FUNCTION harklight.fail_answer()", [])
        assert {:error, _} = AttentionServer.Migration.ImportAnswers.run(db, identity, bundle)
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "answers/ordinary/v1")
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.answers", [])
        assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_answer ON harklight.answers", [])
        assert :ok = AttentionServer.Migration.ImportAnswers.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportAnswers.run(db, identity, bundle)
        assert {:ok, answer_count} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
          {{:row, :messages, {person, _, _} = key}, %{terminal: %{state: "answered"}} = record}, count ->
            message = ImportMessages.id(key, record)
            revision = Map.get(record, :revision, 1)
            assert {:ok, %{rows: [[id, body]]}} = ImportSession.query(db,
              "SELECT id,body FROM harklight.answers WHERE message_id=$1 AND person_id=$2 AND request_revision=$3",
              [message, person, revision])
            assert id == AttentionServer.Migration.ImportAnswers.id(message, revision)
            assert {:ok, %{terminal: terminal, item_id: item_id}} = Attributes.decode(body)
            assert terminal == record.terminal
            assert item_id == AttentionServer.ClientAPI.Resources.item_id(record)
            assert AttentionServer.ClientAPI.Resources.answer_outcome(record, terminal) ==
              AttentionServer.ClientAPI.Resources.answer_outcome(record, record.terminal)
            {:ok, count + 1}
          _, count -> {:ok, count}
        end)
        assert answer_count > 0
        assert {:ok, %{rows: [[^answer_count]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.answers", [])

        verify_receipt_replay(session_person, {agent_person, agent_key}, receipt_digest)
        wrong_source = %{identity | source: :crypto.hash(:sha256, "different export")}
        assert {:error, :import_source_mismatch} = ImportUploads.run(db, wrong_source, bundle)
        assert :ok = ImportUploads.run(db, identity, bundle)
        assert {:ok, %{rows: upload_people}} = ImportSession.query(db,
          "SELECT DISTINCT person_id FROM harklight.uploads", [])
        for [person] <- upload_people do
          assert {:ok, 0} = AttentionServer.Repository.Postgres.Uploads.cleanup(person, 9_000_000_000_000)
        end
        assert :ok = ImportUploads.run(db, identity, bundle)
        verify_uploads(db, bundle, manifest)
        # The relation test below selects the exact source row referenced by a
        # stored read position, rather than relying on arbitrary event order.
        assert {:ok, reference} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, nil, fn
          {{:row, :chat_read_positions, {person, _, _}}, position}, nil when position > 0 ->
            {:ok, Attributes.key({:row, :conversation_events, {person, position}})}
          _, found -> {:ok, found}
        end)
        assert is_binary(reference)
        assert {:ok, %{rows: [[saved_body]]}} = ImportSession.query(db,
          "SELECT body::text FROM harklight.compatibility_facts WHERE family='conversation_events' AND address=$1", [reference])
        assert {:ok, _} = ImportSession.query(db,
          "DELETE FROM harklight.compatibility_facts WHERE family='conversation_events' AND address=$1", [reference])
        assert {:error, :read_position_reference_mismatch} = AttentionServer.Migration.ImportConversations.run(db, identity, bundle)
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "conversations/read-positions/v1")
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.read_positions", [])
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES('conversation_events',$1,$2::text::jsonb)", [reference, saved_body])
        assert :ok = AttentionServer.Migration.ImportConversations.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportConversations.run(db, identity, bundle)
        verify_conversation_positions(db, bundle, manifest)
        assert {:ok, _} = ImportSession.query(db, """
          CREATE FUNCTION harklight.fail_entry() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'synthetic entry interruption'; END $$
          """, [])
        assert {:ok, _} = ImportSession.query(db,
          "CREATE TRIGGER fail_entry BEFORE INSERT ON harklight.conversation_entries FOR EACH ROW EXECUTE FUNCTION harklight.fail_entry()", [])
        assert {:error, _} = AttentionServer.Migration.ImportEntries.run(db, identity, bundle)
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "conversations/entries/v1")
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.conversation_entries", [])
        assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_entry ON harklight.conversation_entries", [])
        assert :ok = AttentionServer.Migration.ImportEntries.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportEntries.run(db, identity, bundle)
        assert {:ok, entry_count} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
          {{:row, :conversation_events, {person, position}}, %{key: {person, sender, external}, payload: payload} = event}, count ->
            assert {:ok, %{rows: [[id, ^position, revision, kind, body]]}} = ImportSession.query(db,
              "SELECT e.id,e.position,e.request_revision,e.kind,e.body FROM harklight.conversation_entries e JOIN harklight.conversations c ON c.id=e.conversation_id JOIN harklight.messages m ON m.id=c.message_id WHERE m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3 AND e.position=$4",
              [person, sender, external, position])
            assert id == Integer.to_string(position)
            assert revision == payload["request_revision"]
            assert kind == payload["kind"]
            assert {:ok, ^event} = Attributes.decode(body)
            {:ok, count + 1}
          _, count -> {:ok, count}
        end)
        assert :ok = AttentionServer.Migration.ImportChatSummary.run(db, identity, bundle)
        assert :ok = AttentionServer.Migration.ImportChatSummary.run(db, identity, bundle)
        for {id, expected} <- expected_chats do
          assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
            "SELECT attributes FROM harklight.conversations WHERE id=$1", [id])
          assert {:ok, attributes} = Attributes.decode(body)
          assert Map.take(attributes, [:chat_started, :latest_origin_reply_seq, :unread_replies]) == expected
        end
        assert entry_count > 0
        assert {:ok, %{rows: [[^entry_count]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.conversation_entries", [])

        assert {:ok, verified} = AttentionServer.Migration.VerifyFacts.run(db, identity, bundle)
        assert verified.rows > manifest.data.records.count
        assert {:error, :import_source_mismatch} = AttentionServer.Migration.VerifyFacts.run(db,
          %{identity | source: :crypto.hash(:sha256, "wrong source")}, bundle)
        address = Attributes.key({:field, :relay_instance_id})
        assert {:ok, %{rows: [[original]]}} = ImportSession.query(db,
          "SELECT body::text FROM harklight.compatibility_facts WHERE family='relay_instance_id' AND address=$1", [address])
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.compatibility_facts SET body=$2::text::jsonb WHERE family='relay_instance_id' AND address=$1",
          [address, JSON.encode!(Attributes.encode("changed synthetic identity"))])
        assert {:error, :source_fact_mismatch} = AttentionServer.Migration.VerifyFacts.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.compatibility_facts SET body=$2::text::jsonb WHERE family='relay_instance_id' AND address=$1", [address, original])
        assert {:ok, %{rows: [[session_address, session_body]]}} = ImportSession.query(db,
          "SELECT address,body::text FROM harklight.compatibility_facts WHERE family='harklight_browser_sessions' LIMIT 1", [])
        assert {:ok, _} = ImportSession.query(db,
          "DELETE FROM harklight.compatibility_facts WHERE family='harklight_browser_sessions' AND address=$1", [session_address])
        assert {:error, :source_fact_mismatch} = AttentionServer.Migration.VerifyFacts.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES('harklight_browser_sessions',$1,$2::text::jsonb)", [session_address, session_body])
        extra = Attributes.key({:row, :persons, "synthetic unexpected person"})
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES('persons',$1,$2::text::jsonb)",
          [extra, JSON.encode!(Attributes.encode(1))])
        assert {:error, :source_fact_count_mismatch} = AttentionServer.Migration.VerifyFacts.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "DELETE FROM harklight.compatibility_facts WHERE family='persons' AND address=$1", [extra])
        assert {:ok, ^verified} = AttentionServer.Migration.VerifyFacts.run(db, identity, bundle)
        assert {:ok, uploads_verified} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)
        assert uploads_verified.uploads > 0
        assert {:ok, %{rows: [[upload_id, ordinal, bytes]]}} = ImportSession.query(db,
          "SELECT upload_id,chunk_number,bytes FROM harklight.upload_chunks LIMIT 1", [])
        assert {:ok, _} = ImportSession.query(db,
          "DELETE FROM harklight.upload_chunks WHERE upload_id=$1 AND chunk_number=$2", [upload_id, ordinal])
        assert {:error, :upload_chunk_source_mismatch} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.upload_chunks(upload_id,chunk_number,bytes) VALUES($1,$2,$3)", [upload_id, ordinal, "wrong bytes"])
        assert {:error, :upload_chunk_source_mismatch} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.upload_chunks SET bytes=$3 WHERE upload_id=$1 AND chunk_number=$2", [upload_id, ordinal, bytes])
        assert {:ok, %{rows: [[source_size, chunk_count]]}} = ImportSession.query(db,
          "SELECT size_bytes,(SELECT count(*) FROM harklight.upload_chunks WHERE upload_id=$1) FROM harklight.uploads WHERE id=$1", [upload_id])
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.uploads SET size_bytes=size_bytes+1 WHERE id=$1", [upload_id])
        assert {:error, :upload_source_mismatch} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.uploads SET size_bytes=$2 WHERE id=$1", [upload_id, source_size])
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.upload_chunks(upload_id,chunk_number,bytes) VALUES($1,$2,$3)", [upload_id, chunk_count, "extra"])
        assert {:error, :upload_source_mismatch} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db,
          "DELETE FROM harklight.upload_chunks WHERE upload_id=$1 AND chunk_number=$2", [upload_id, chunk_count])
        assert {:ok, ^uploads_verified} = AttentionServer.Migration.VerifyUploads.run(db, identity, bundle)

        assert {:ok, ordinary_links} = AttentionServer.Migration.VerifyMessageUploads.run(db, identity, bundle, :ordinary)
        assert ordinary_links.links > 0
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, _} = ImportSession.query(tx,
            "UPDATE harklight.message_uploads SET attributes='{}'::jsonb WHERE message_id IN (SELECT id FROM harklight.messages WHERE kind='ordinary')", [])
          assert {:error, {:message_upload_mismatch, :ordinary, _, _, _}} = AttentionServer.Migration.VerifyMessageUploads.run(tx, identity, bundle, :ordinary)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^ordinary_links} = AttentionServer.Migration.VerifyMessageUploads.run(db, identity, bundle, :ordinary)
        verify_revision_files(db, identity, bundle, manifest)
        verify_item_facts(db, identity, bundle, manifest)
        verify_message_drift(db, identity, bundle)
        verify_recipient_drift(db, identity, bundle)
        verify_authority_drift(db, identity, bundle)
        verify_account_drift(db, identity, bundle)
        verify_allocation_bounds(db, root)
        assert {:ok, _} = AttentionServer.Migration.VerifyAccounts.run(db, identity, bundle)
        verify_feed_drift(db, identity, bundle)
        verify_conversation_drift(db, identity, bundle)
        verify_ordinary_entry_uploads(db, identity, bundle)
        assert {:ok, %{references: 0}} = AttentionServer.Migration.VerifyLargeText.run(db, identity, bundle)
        extra_hash = :crypto.hash(:sha256, "")
        assert {:ok, _} = ImportSession.query(db,
          "INSERT INTO harklight.large_texts(sha256,size_bytes,verified) VALUES($1,0,true)", [extra_hash])
        assert {:error, :large_text_hash_count_mismatch} = AttentionServer.Migration.VerifyLargeText.run(db, identity, bundle)
        assert {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.large_texts WHERE sha256=$1", [extra_hash])
        assert {:ok, %{references: 0}} = AttentionServer.Migration.VerifyLargeText.run(db, identity, bundle)
        stop_supervised!(Repo)

        assert {:ok, :ok} =
                 Frames.fold(
                   Path.join(bundle, "records.frames"),
                   manifest.data.records,
                   :ok,
                   fn entry, :ok ->
                     verify_account(db, entry)
                     {:ok, :ok}
                   end
                 )

        assert {:ok, %{rows: [["loading"]]}} =
                 ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])

        assert {:ok, %{rows: [[^epoch]]}} =
                 ImportSession.query(db, "SELECT cursor_epoch FROM harklight.server_identity", [])

        assert {:ok, %{rows: [[2]]}} =
                 ImportSession.query(
                   db,
                   "SELECT count(*) FROM harklight.principals WHERE revoked=true AND revision=2",
                   []
                 )

        assert {:ok, %{rows: [[text]]}} =
                 ImportSession.query(
                   db,
                   "SELECT attributes::text FROM harklight.principals WHERE kind='agent' AND revoked=true",
                   []
                 )

        assert {:ok, envelope} = JSON.decode_line(text <> "\n")
        assert {:ok, agent} = Attributes.decode(envelope)
        assert agent.grant_revision == 4
        assert agent.read_grants == MapSet.new(["app:read", "app:read-more"])
        assert agent.respond_grants == MapSet.new(["app:respond"])
        assert length(agent.grant_history) == 4
        Agent.stop(fixture.clock)
      end
    end

    @tag timeout: 120_000
    test "legacy source without local roster retains initialization membership" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      db = start_supervised!({Postgrex, [
        hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password),
        database: "migration_implicit_roster"
      ]})
      root = Path.join(System.tmp_dir!(), "import-legacy-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
      {:ok, relay} = CompatibilityScenarios.start(fixture, shared_messages: false, groups: false)
      expected = try do
        %{person: person, other: other} = CompatibilityScenarios.provision(relay, fixture)
        {:ok, hosted} = Relay.hosted_account_create(relay, "synthetic-legacy-hosted")
        engine = :sys.get_state(relay).store.engine
        %{instance: engine.relay_instance_id, members: [person, other], hosted: hosted}
      after
        GenServer.stop(relay)
      end
      # Modern provisioning initializes the roster even with features off.
      # Reconstruct the valid legacy state after stopping, with no group facts.
      alias AttentionServer.{Engine, Store}
      {:ok, legacy} = Store.open_relay(Path.join(fixture.root, "relay.sqlite3"))
      assert legacy.engine.groups == %{}
      {engine, :ok, touched} = Engine.with_touches(legacy.engine, fn engine ->
        {Engine.delete_row(engine, :local_organizations, :default), :ok}
      end)
      {:ok, legacy} = Store.commit(legacy, {engine, touched}, %{person: "fixture", actor: "fixture", action: "legacy_roster"})
      refute Map.has_key?(legacy.engine.local_organizations, :default)
      :ok = Store.close(legacy)
      bundle = Path.join(root, "export")
      assert {:ok, summary} = Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
        supervisor: {:operator, "synthetic fixture", "seed Relay stopped"}, material: %{})
      identity = %{id: "synthetic-legacy", source: Base.decode16!(summary.sha256, case: :lower),
        schema: AttentionServer.Repository.Postgres.Schema.digest()}
      assert :ok = ImportOrganizations.run(db, identity, bundle)
      assert :ok = ImportOrganizations.run(db, identity, bundle)
      assert :ok = ImportAuth.run(db, identity, bundle)
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.auth_sessions", [])
      assert {:ok, legacy_counts} = AttentionServer.Migration.VerifyOrganizations.run(db, identity, bundle)
      assert legacy_counts["organization_members"] == length(expected.members)
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, %{num_rows: 1}} = ImportSession.query(tx,
          "INSERT INTO harklight.organization_members(organization_id,person_id) SELECT $1,person_id FROM harklight.hosted_accounts LIMIT 1", [expected.instance])
        assert {:error, {:organization_count_mismatch, "organization_members"}} = AttentionServer.Migration.VerifyOrganizations.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^legacy_counts} = AttentionServer.Migration.VerifyOrganizations.run(db, identity, bundle)
      assert_members(db, expected.instance, expected.members)
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.organization_members WHERE person_id=$1", [expected.hosted])
      assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
        "SELECT attributes::text FROM harklight.organizations WHERE id=$1", [expected.instance])
      assert_typed(text, nil)
      Agent.stop(fixture.clock)
    end

    for mode <- [:implicit, :reset], epoch <- [1, 17] do
      @tag timeout: 120_000, mode: mode, epoch: epoch
      test "source stream fallback #{mode} at epoch #{epoch}", %{mode: mode, epoch: epoch} do
        alias AttentionServer.{ChangeLog, Store}
        alias AttentionServer.Store.RecordTable
        alias AttentionServer.Repository.Postgres.Streams
        uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
        assert uri.host in ["localhost", "127.0.0.1"]
        [user, password] = String.split(uri.userinfo, ":", parts: 2)
        config = [hostname: uri.host, port: uri.port, username: URI.decode(user),
          password: URI.decode(password), database: "migration_#{mode}_#{epoch}"]
        db = start_supervised!({Postgrex, config})
        root = Path.join(System.tmp_dir!(), "stream-fallback-#{System.unique_integer([:positive])}")
        File.mkdir!(root)
        File.chmod!(root, 0o700)
        on_exit(fn -> File.rm_rf!(root) end)
        fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
        {:ok, relay} = CompatibilityScenarios.start(fixture)
        try do
          CompatibilityScenarios.run(relay, fixture)
        after
          GenServer.stop(relay)
        end
        path = Path.join(fixture.root, "relay.sqlite3")
        {:ok, store} = Store.open_relay(path)
        {{person, _, _}, _} = Enum.at(store.engine.messages, 0)
        assert ChangeLog.seeded?(store.engine, person)
        :ok = RecordTable.run(store.db,
          "INSERT INTO client_change_log_epoch VALUES(1,?) ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch", [epoch])

        if mode == :implicit do
          # Valid cut-over history: durable content exists before a person has
          # any change-log rows. Do not derive a head from allocation counters.
          for table <- ~w(client_change_log_meta client_changes client_resource_revs client_resource_created) do
            :ok = RecordTable.run(store.db, "DELETE FROM " <> table <> " WHERE person_id=?", [person])
          end
        else
          # Exercise the real reconciliation operation, which deliberately
          # clears revision rows but retains resource creation timestamps.
          {:ok, [[version]]} = RecordTable.select(store.db,
            "SELECT store_version FROM client_change_log_state WHERE id=1")
          :ok = ChangeLog.enable(store.db, version + 1)
          assert {:ok, [[0]]} = RecordTable.select(store.db, "SELECT count(*) FROM client_resource_revs")
        end

        source = %{store | change_log: true}
        assert {:ok, [[source_epoch]]} = RecordTable.select(store.db,
          "SELECT epoch FROM client_change_log_epoch WHERE id=1")
        assert {:ok, head} = ChangeLog.head(source, person)
        if mode == :implicit, do: assert(head == epoch)
        assert {:ok, [], false, ^head} = ChangeLog.read(source, person, head, 50)
        assert {:error, :cursor_expired} = ChangeLog.read(source, person, head - 1, 50)
        assert {:ok, created} = RecordTable.select(store.db,
          "SELECT person_id,resource_id,created_at_ms FROM client_resource_created")
        expected_created = Enum.map(created, fn [p, id, at] ->
          assert {:ok, rev} = ChangeLog.rev(source, p, id)
          {p, id, at, rev}
        end)
        if mode == :reset, do: assert(expected_created != [])
        :ok = Store.close(store)
        bundle = Path.join(root, "export")
        assert {:ok, summary} = Bundle.export(path, bundle,
          supervisor: {:operator, "synthetic fixture", "stopped fallback source"}, material: %{})
        identity = %{id: "synthetic-fallback", source: Base.decode16!(summary.sha256, case: :lower),
          schema: AttentionServer.Repository.Postgres.Schema.digest()}
        assert :ok = ImportStreams.run(db, identity, bundle)
        assert :ok = ImportStreams.run(db, identity, bundle)
        assert {:ok, _} = AttentionServer.Migration.VerifyStreamPositions.run(db, identity, bundle)
        assert {:ok, %{epoch: ^source_epoch}} = AttentionServer.Migration.VerifyStreamHeads.run(db, identity, bundle)
        start_supervised!({Repo, config})
        assert {:ok, [], false, ^head} = Streams.person_page(person, head, 50)
        assert {:error, :cursor_expired} = Streams.person_page(person, head - 1, 50)
        stop_supervised!(Repo)
        for {p, id, at, rev} <- expected_created do
          {:ok, kind} = ImportStreams.person_kind(id)
          assert {:ok, %{rows: [[^rev, ^at]]}} = ImportSession.query(db,
            "SELECT revision,first_seen_at_ms FROM harklight.stream_resource_revisions WHERE person_id=$1 AND origin_key='' AND resource_kind=$2 AND resource_id=$3", [p, kind, id])
        end
        assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
        Agent.stop(fixture.clock)
      end
    end

    defp verify_receipt_replay(person, {agent_person, agent}, hex_digest) do
      digest = Base.decode16!(hex_digest, case: :lower)
      other = :crypto.hash(:sha256, "another operation")
      expires = @receipt_at + Idempotency.retention_ms()
      no_write = fn -> flunk("a live imported receipt must prevent execution") end
      no_project = fn _ -> flunk("literal responses bypass projection") end
      run = fn scope, hash, now, write, project ->
        Transaction.run([], fn _ -> Operations.execute_client(scope, hash, now, write, project) end)
      end

      original = {person, "", "synthetic-same-key"}
      assert {:ok, {201, {:encoded_json, @original_json}, true}} =
               run.(original, digest, expires, no_write, no_project)
      response = Writes.respond(Plug.Test.conn(:post, "/"), {:ok, 201, {:encoded_json, @original_json}, true})
      assert response.resp_body == @original_json
      assert Plug.Conn.get_resp_header(response, "idempotent-replayed") == ["true"]
      assert {:error, :idempotency_conflict} = run.(original, other, expires + 1, no_write, no_project)

      compact = {person, "", "synthetic-compact"}
      assert {:ok, {200, %{"item" => "projected"}, true}} =
               run.(compact, digest, expires, no_write, fn %{"item_id" => "i1"} ->
                 {:ok, %{"item" => "projected"}}
               end)
      assert {:ok, {202, %{"code" => "accepted_reconcile"}, true}} =
               run.(compact, digest, expires, no_write, fn _ -> {:error, :not_found} end)
      assert {:error, :idempotency_conflict} =
               run.(compact, digest, expires, no_write, fn _ -> {:error, :idempotency_conflict} end)

      assert {:ok, {200, %{"synthetic" => "agent"}, true}} =
               run.({agent_person, agent, "synthetic-agent-key"}, digest, expires, no_write, no_project)

      recent = {person, "", "synthetic-recent"}
      project = fn %{"settings" => true} -> {:ok, %{"settings" => "current"}} end
      assert {:ok, {200, %{"settings" => "current"}, true}} =
               run.(recent, digest, expires, no_write, project)
      assert {:error, :idempotency_conflict} = run.(recent, other, expires, no_write, project)
      assert {:error, :not_found} =
               run.(recent, digest, expires, no_write, fn _ -> {:error, :not_found} end)

      assert {:error, :rejected} =
               run.(recent, other, expires + 1, fn ->
                 assert {:ok, _} = Transaction.query("INSERT INTO harklight.people(id) VALUES ('rolled-back-receipt')", [])
                 {:error, :rejected}
               end, project)
      assert {:ok, %{rows: []}} =
               Transaction.query("SELECT id FROM harklight.people WHERE id = 'rolled-back-receipt'", [])
      assert {:ok, {200, %{"settings" => "current"}, true}} =
               run.(recent, digest, expires, no_write, project)

      assert {:ok, {201, %{"settings" => "current"}, false}} =
               run.(recent, other, expires + 1, fn -> {:ok, 201, %{"settings" => true}} end, project)
      assert {:error, :idempotency_conflict} = run.(recent, digest, expires + 1, no_write, project)
      assert {:ok, {201, %{"settings" => "current"}, true}} =
               run.(recent, other, expires + 1, no_write, project)
    end

    @tag timeout: 120_000
    test "upload chunks resume from a committed prefix and retain exact reader bytes" do
      alias AttentionServer.Migration.ImportBytes
      alias AttentionServer.Repository.Postgres.{Repo, Uploads}
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      config = [hostname: uri.host, port: uri.port, username: URI.decode(user),
        password: URI.decode(password), database: "migration_upload_bytes"]
      db = start_supervised!({Postgrex, config})
      identity = %{id: "synthetic-chunks", source: :crypto.hash(:sha256, "synthetic chunk source"),
        schema: AttentionServer.Repository.Postgres.Schema.digest()}
      assert {:ok, :claimed} = ImportSession.claim(db, identity)
      assert {:ok, _} = ImportSession.query(db, "INSERT INTO harklight.people(id) VALUES('synthetic-person')", [])
      bytes = :binary.copy("a", Uploads.chunk_bytes()) <> :binary.copy("b", Uploads.chunk_bytes()) <>
        :binary.copy("c", Uploads.chunk_bytes()) <> "last chunk"
      hash = :crypto.hash(:sha256, bytes)
      expected = %{size: byte_size(bytes), sha256: Base.encode16(hash, case: :lower)}
      root = Path.join(System.tmp_dir!(), "import-chunks-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      path = Path.join(root, "synthetic-upload")
      File.write!(path, bytes)
      File.chmod!(path, 0o600)
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.uploads(id,person_id,size_bytes,sha256) VALUES('synthetic-upload','synthetic-person',$1,$2)", [expected.size, hash])
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.chunk_number=1 THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_chunk BEFORE INSERT ON harklight.upload_chunks FOR EACH ROW EXECUTE FUNCTION harklight.fail_chunk()", [])
      assert {:error, _} = ImportBytes.run(db, identity, "synthetic-upload", path, expected)
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.upload_chunks", [])
      assert {:ok, %{ordinal: 1}} = ImportSession.checkpoint(db, identity, "upload-bytes/synthetic-upload/v1")
      # An expired source reservation must survive while import is unpublished,
      # including a partially transferred file whose checkpoint already exists.
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.uploads SET attributes=jsonb_build_object('reservation_expires_at_ms',1) WHERE id='synthetic-upload'", [])
      start_supervised!({Repo, config})
      assert {:ok, 0} = Uploads.cleanup("synthetic-person", 2)
      stop_supervised!(Repo)
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.upload_chunks", [])
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_chunk ON harklight.upload_chunks", [])
      <<_, tail::binary>> = bytes
      File.write!(path, <<?z, tail::binary>>)
      assert {:error, :checkpoint_source_mismatch} = ImportBytes.run(db, identity, "synthetic-upload", path, expected)
      assert {:ok, %{ordinal: 1}} = ImportSession.checkpoint(db, identity, "upload-bytes/synthetic-upload/v1")
      File.write!(path, bytes)
      assert {:ok, %{chunks: 4, bytes: size}} = ImportBytes.run(db, identity, "synthetic-upload", path, expected)
      assert size == expected.size
      assert {:ok, %{chunks: 4}} = ImportBytes.run(db, identity, "synthetic-upload", path, expected)
      assert {:ok, %{rows: [[4, max_size]]}} = ImportSession.query(db,
        "SELECT count(*),max(octet_length(bytes)) FROM harklight.upload_chunks", [])
      assert max_size == Uploads.chunk_bytes()
      start_supervised!({Repo, config})
      assert {:error, :not_found} = Uploads.read_chunk("synthetic-person", "synthetic-upload", 0)
      assert {:ok, "synthetic-upload"} = Uploads.complete({"synthetic-person", nil, "synthetic-upload"}, 123)
      assert {:ok, %{chunks: 4}} = ImportBytes.run(db, identity, "synthetic-upload", path, expected)
      readback = for ordinal <- 0..3, into: <<>> do
        assert {:ok, chunk} = Uploads.read_chunk("synthetic-person", "synthetic-upload", ordinal)
        chunk
      end
      assert readback == bytes
      assert :crypto.hash(:sha256, readback) == hash
      assert {:error, :not_found} = Uploads.read_chunk("wrong-person", "synthetic-upload", 0)
      empty = Path.join(root, "empty")
      File.write!(empty, "")
      empty_hash = :crypto.hash(:sha256, "")
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.uploads(id,person_id,size_bytes,sha256) VALUES('empty','synthetic-person',0,$1)", [empty_hash])
      assert {:ok, %{chunks: 0, bytes: 0}} = ImportBytes.run(db, identity, "empty", empty,
        %{size: 0, sha256: Base.encode16(empty_hash, case: :lower)})
      assert {:ok, "empty"} = Uploads.complete({"synthetic-person", nil, "empty"}, 124)
      assert {:error, :not_found} = Uploads.read_chunk("synthetic-person", "empty", 0)
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
      stop_supervised!(Repo)
    end

    @tag timeout: 120_000
    test "unknown delivery acknowledgement stage refuses the receipt checkpoint" do
      alias AttentionServer.Store.{RecordCodec, RecordTable}
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      db = start_supervised!({Postgrex, [hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password), database: "migration_invalid_ack"]})
      root = Path.join(System.tmp_dir!(), "invalid-ack-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "history"))
      {:ok, relay} = CompatibilityScenarios.start(fixture)
      try do
        CompatibilityScenarios.run(relay, fixture)
      after
        GenServer.stop(relay)
      end
      path = Path.join(fixture.root, "relay.sqlite3")
      {:ok, sqlite} = Exqlite.Sqlite3.open(path)
      {:ok, rows} = RecordTable.select(sqlite, "SELECT key,payload,checksum FROM relay_records")
      {address, stages} = Enum.find_value(rows, fn [key, payload, hash] ->
        case RecordCodec.decode_row({key, payload, hash}) do
          {:ok, {:origin_feed, _, {:delivery_ack, _}} = address, stages} -> {address, stages}
          _ -> nil
        end
      end)
      {key, payload, hash} = RecordCodec.encode_row(address, Map.put(stages, :unsupported_stage, 1))
      :ok = RecordTable.run(sqlite, "UPDATE relay_records SET payload=?,checksum=? WHERE key=?",
        [{:blob, payload}, {:blob, hash}, {:blob, key}])
      :ok = Exqlite.Sqlite3.close(sqlite)
      bundle = Path.join(root, "export")
      assert {:ok, summary} = Bundle.export(path, bundle,
        supervisor: {:operator, "synthetic fixture", "stopped invalid-stage fixture"}, material: %{})
      identity = %{id: "synthetic-invalid-ack", source: Base.decode16!(summary.sha256, case: :lower),
        schema: AttentionServer.Repository.Postgres.Schema.digest()}
      assert {:error, :unsupported_delivery_ack} = ImportFeeds.run(db, identity, bundle)
      assert {:error, :unsupported_delivery_ack} = ImportFeeds.run(db, identity, bundle)
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "feeds/receipts/v1")
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.origin_delivery_acks", [])
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
      Agent.stop(fixture.clock)
    end

    defp verify_conversation_positions(db, root, manifest) do
      assert {:ok, {containers, positions}} = Frames.fold(Path.join(root, "records.frames"),
        manifest.data.records, {0, 0}, fn
          {{:row, :messages, key}, record}, {containers, positions} ->
            id = ImportMessages.id(key, record)
            assert {:ok, %{rows: [[^id, text]]}} = ImportSession.query(db,
              "SELECT message_id,attributes::text FROM harklight.conversations WHERE id=$1", [id])
            assert_typed(text, %{source_key: key})
            {:ok, {containers + 1, positions}}
          {{:row, :chat_read_positions, {person, sender, external}}, position}, {containers, positions} ->
            assert {:ok, %{rows: [[^position]]}} = ImportSession.query(db,
              "SELECT r.position FROM harklight.read_positions r JOIN harklight.messages m ON m.id=r.conversation_id WHERE r.person_id=$1 AND m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3",
              [person, sender, external])
            {:ok, {containers, positions + 1}}
          _, counts -> {:ok, counts}
        end)
      assert containers > 0 and positions > 0
      assert {:ok, %{rows: [[^containers]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.conversations", [])
      assert {:ok, %{rows: [[^positions]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.read_positions", [])
    end

    defp verify_uploads(db, root, manifest) do
      alias AttentionServer.Repository.Postgres.Uploads
      assert {:ok, published} = Frames.fold(Path.join(root, "records.frames"), manifest.data.records, 0, fn
        {{:row, :uploads, id}, source}, count ->
          assert {:ok, %{rows: [[person, owner, size, hash, at, attrs]]}} = ImportSession.query(db,
            "SELECT person_id,owner_key,size_bytes,sha256,completed_at_ms,attributes::text FROM harklight.uploads WHERE id=$1", [id])
          assert {person, owner, size, hash, at} == {source.person, source.uploader, source.size,
            Base.decode16!(source.sha256, case: :lower), source.last_activity_ms}
          assert_typed(attrs, source)
          verify_upload_bytes(person, id, Path.join(root, "files/uploads/" <> id), source.size)
          {:ok, count + 1}
        _, count -> {:ok, count}
      end)
      staged = Map.fetch!(manifest.data.tables, "client_staged_files")
      assert {:ok, :ok} = Frames.fold(Path.join(root, staged.file), staged.summary, :ok, fn
        [id, person, _, _, size, hex, at] = source, :ok ->
          assert {:ok, %{rows: [[^person, ^size, hash, ^at, attrs, expiry]]}} = ImportSession.query(db,
            "SELECT person_id,size_bytes,sha256,completed_at_ms,attributes::text,(attributes->>'reservation_expires_at_ms')::bigint FROM harklight.uploads WHERE id=$1", [id])
          assert hash == Base.decode16!(hex, case: :lower)
          assert expiry == at + 30 * 86_400_000
          {:ok, envelope} = JSON.decode_line(attrs <> "\n")
          assert {:ok, ^source} = Attributes.decode(Map.delete(envelope, "reservation_expires_at_ms"))
          verify_upload_bytes(person, id, Path.join(root, "files/uploads/client-api-v2-staged/" <> id), size)
          {:ok, :ok}
      end)
      assert published > 0 and staged.summary.count > 0
      expected = published + staged.summary.count
      assert {:ok, %{rows: [[^expected]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.uploads", [])
      assert {:ok, links} = Frames.fold(Path.join(root, "records.frames"), manifest.data.records, 0, fn
        {{:row, :messages, key}, record}, total ->
          count = Enum.reduce(Map.get(record, :files_by_revision, %{}), total, fn {revision, files}, n ->
            Enum.reduce(Enum.with_index(files), n, fn {file, ordinal}, n ->
              if file["tier"] == "inline" do
                n
              else
                id = ImportMessages.id(key, record)
                upload = file["upload_id"]
                assert {:ok, %{rows: [[^upload, text]]}} = ImportSession.query(db,
                  "SELECT upload_id,attributes::text FROM harklight.message_uploads WHERE message_id=$1 AND revision=$2 AND ordinal=$3", [id, revision, ordinal])
                assert_typed(text, file)
                n + 1
              end
            end)
          end)
          {:ok, count}
        _, count -> {:ok, count}
      end)
      assert links > 0
      assert {:ok, %{rows: [[^links]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.message_uploads", [])
      assert {:error, :not_found} = Uploads.read_chunk("synthetic-wrong-person", "missing", 0)
    end

    defp verify_upload_bytes(person, id, path, size) do
      alias AttentionServer.Repository.Postgres.Uploads
      count = div(size + Uploads.chunk_bytes() - 1, Uploads.chunk_bytes())
      ordinals = if count == 0, do: [], else: 0..(count - 1)
      actual = for ordinal <- ordinals, into: <<>> do
        assert {:ok, bytes} = Uploads.read_chunk(person, id, ordinal)
        assert {:error, :not_found} = Uploads.read_chunk("synthetic-wrong-person", id, ordinal)
        bytes
      end
      # This fixture is deliberately small; production import streams chunks.
      assert actual == File.read!(path)
      assert byte_size(actual) == size
      assert {:error, :not_found} = Uploads.read_chunk(person, id, count)
    end

    defp seed_long_conversation(path) do
      alias AttentionServer.{Engine, Store}
      {:ok, store} = Store.open_relay(path)
      {key = {person, sender, external}, record} = Enum.find(store.engine.messages, fn {{_, _, id}, _} ->
        id == "synthetic-three-revisions"
      end)
      {actor, _} = Enum.find(store.engine.devices, fn {device, principal} ->
        principal.person == person and not principal.revoked and device != record.origin
      end)
      bytes = "synthetic ordinary entry attachment"
      metadata = %{id: @ordinary_entry_upload, person: person, uploader: actor,
        op_id: "96000000-0000-4000-8000-000000000001", sender_id: sender,
        message_id: external, revision: record.revision, name: "entry.txt",
        size: byte_size(bytes), sha256: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
        last_activity_ms: 1_700_000_000_000}
      File.write!(Path.join(store.upload_dir, @ordinary_entry_upload), bytes)
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
        engine = Engine.put_row(engine, :uploads, @ordinary_entry_upload, metadata)
        discussion = %{"type" => "discussion", "sender_id" => sender, "message_id" => external,
          "request_revision" => record.revision, "text" => "Synthetic multi-page conversation",
          "files" => [%{"name" => "inline.txt", "data_base64" => Base.encode64("inline")},
            %{"name" => metadata.name, "upload_id" => metadata.id, "size" => metadata.size, "sha256" => metadata.sha256}],
          "op_id" => "96000000-0000-4000-8000-000000000001"}
        {engine, {:ok, turn}} = Engine.request_command(engine, actor, discussion, record.accepted_at + 10)
        in_reply_to = String.to_integer(turn["event_id"])
        {engine, read_position, last_position} = Enum.reduce(1..260, {engine, nil, nil}, fn n, {current, read, _last} ->
          reply = %{"type" => "conversation_reply", "sender_id" => sender, "message_id" => external,
            "request_revision" => record.revision, "in_reply_to" => in_reply_to,
            "text" => "Synthetic reply #{n}",
            "op_id" => "96000000-0000-4000-8001-" <> String.pad_leading(Integer.to_string(n), 12, "0")}
          {next, {:ok, fields}} = Engine.request_command(current, record.origin, reply, record.accepted_at + 10 + n)
          {next, if(n == 128, do: fields["seq"], else: read), fields["seq"]}
        end)
        read = %{"type" => "conversation_read", "sender_id" => sender, "message_id" => external,
          "read_through_conversation_seq" => read_position, "op_id" => "96000000-0000-4000-8000-000000000002"}
        {engine, {:ok, _}} = Engine.request_command(engine, actor, read, record.accepted_at + 300)
        chat = AttentionServer.ClientAPI.Resources.item(engine, key, %{"key" => "synthetic-rank"})["chat"]
        assert chat["unread_replies"] == 132
        assert chat["last_origin_reply"] == "e#{last_position}"
        assert Enum.count(engine.conversation_events, fn {_, event} -> event.key == key end) > 256
        {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: person, actor: "fixture", action: "synthetic_long_chat"})
      :ok = Store.close(store)
    end

    defp source_chat_expectations(path) do
      {:ok, store} = AttentionServer.Store.open_relay(path)
      expected = Enum.map(store.engine.messages, fn {key, record} ->
        chat = AttentionServer.ClientAPI.Resources.item(store.engine, key, %{"key" => "synthetic-rank"})["chat"]
        last = case chat["last_origin_reply"] do
          nil -> nil
          "e" <> position -> String.to_integer(position)
        end
        {ImportMessages.id(key, record), %{chat_started: chat["started"],
          latest_origin_reply_seq: last, unread_replies: chat["unread_replies"]}}
      end)
      :ok = AttentionServer.Store.close(store)
      expected
    end

    defp verify_subscription_drift(db, identity, bundle, expected) do
      verifier = AttentionServer.Migration.VerifySubscriptions
      assert {:ok, counts} = verifier.run(db, identity, bundle)
      assert counts.heads == length(expected)
      assert counts.senders == Enum.sum(Enum.map(expected, fn {_, _, _, senders} -> length(senders) end))
      assert counts.senders > 0
      for family <- ~w(subscriptions device_subscription_heads device_subscription_senders) do
        for change <- [:body, :missing] do
          assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
            sql = if change == :body,
              do: "UPDATE harklight.compatibility_facts SET body='{}'::jsonb WHERE family=$1",
              else: "DELETE FROM harklight.compatibility_facts WHERE family=$1"
            assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [family])
            assert n > 0
            assert {:error, {:subscription_fact_mismatch, ^family, _}} = verifier.run(tx, identity, bundle)
            Postgrex.rollback(tx, :synthetic_drift)
          end)
          assert {:ok, ^counts} = verifier.run(db, identity, bundle)
        end
      end
      {person, device, revision, _} = hd(expected)
      for {family, key, value} <- [
        {"device_subscription_heads", {person, "synthetic-unexpected-device"}, 0},
        {"device_subscription_senders", {person, device, revision + 1000, "app:stale-revision"}, true},
        {"device_subscription_senders", {"synthetic-other-person", device, revision, "app:wrong-scope"}, true}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: 1}} = ImportSession.query(tx,
            "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)",
            [family, Attributes.key(key), JSON.encode!(Attributes.encode(value))])
          assert {:error, {:subscription_count_mismatch, ^family}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
    end

    defp source_subscription_expectations(path) do
      {:ok, store} = AttentionServer.Store.open_relay(path)
      expected = Enum.flat_map(store.engine.devices, fn {device, %{person: person} = principal} ->
        case Map.get(store.engine.subscriptions, {person, device}) do
          %{revision: revision, senders: senders} -> [{person, device, revision, Enum.sort(senders)}]
          nil -> if principal.revoked, do: [], else: [{person, device, 0, []}]
        end
      end)
      :ok = AttentionServer.Store.close(store)
      expected
    end

    defp source_recipient_expectations(path) do
      {:ok, store} = AttentionServer.Store.open_relay(path)
      expected = Enum.map(store.engine.messages, fn {{person, _, _} = key, record} ->
        raw = Map.get(record, :revision, 1)
        state = Map.get(store.engine.bounce_revisions, key, %{})
        substantive = Map.get(Map.get(state, :raw_to_substantive, %{}), raw,
          Map.get(state, :current_substantive_revision, raw))
        seen = AttentionServer.ClientAPI.Resources.seen?(store.engine, key, record)
        {ImportMessages.id(key, record), person, if(seen, do: substantive),
          %{visible: AttentionServer.BounceDecision.person_visible?(record),
            substantive_revision: substantive, origin_result: Map.get(record, :origin_result)}}
      end)
      :ok = AttentionServer.Store.close(store)
      expected
    end

    defp seed_historical_messages(path) do
      alias AttentionServer.{Engine, Store}
      {:ok, store} = Store.open_relay(path)
      {{person, sender, _}, sample} = Enum.find(store.engine.messages, fn {_, record} ->
        is_binary(Map.get(record, :session_handle))
      end)
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
        external_id = "synthetic-three-revisions"
        body = %{"sender_id" => sender, "message_id" => external_id,
          "created_at" => 1, "title" => "First historical revision", "body" => "Original content",
          "message_type" => "fyi", "answer_ui" => AttentionServer.Message.seen_descriptor()}
        frame = %{"type" => "forward", "op_id" => "95000000-0000-4000-8000-000000000020",
          "session_handle" => sample.session_handle, "message" => body}
        at = sample.accepted_at + 100
        {engine, reply} = Engine.request_command(engine, sample.origin, frame, at)
        assert {:ok, _} = reply
        engine = Enum.reduce(2..3, engine, fn revision, engine ->
          frame = %{"type" => "conversation_revise", "sender_id" => sender,
            "message_id" => external_id, "base_revision" => revision - 1,
            "op_id" => "95000000-0000-4000-8000-00000000002#{revision}",
            "message" => Map.put(body, "body", "Historical revision #{revision}")}
          {engine, reply} = Engine.request_command(engine, sample.origin, frame, at + revision)
          assert {:ok, _} = reply
          engine
        end)
        record = Map.fetch!(engine.messages, {person, sender, external_id})
        assert map_size(record.revisions) == 3
        assert record.accepted_at == at and record.revision_accepted_at == at + 3
        {:ok, engine, %{created: true}} = Engine.hosted_notice(engine, person,
          %{"title" => "Synthetic service notice", "body" => "Retained notice content", "key" => "migration-history"}, at + 4)
        {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: person, actor: "fixture", action: "synthetic_history"})
      :ok = Store.close(store)
    end

    defp seed_advisory(path) do
      alias AttentionServer.{BounceDecision, BouncePolicy, Engine, Store}
      {:ok, store} = Store.open_relay(path)
      {{person, sender, _}, sample} = Enum.find(store.engine.messages, fn {_, record} ->
        is_binary(Map.get(record, :session_handle))
      end)
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
      {:ok, levels} = BouncePolicy.normalize_levels([
        %{"name" => "Follow up", "lower_bound" => 0, "bounce_after_days" => 1}
      ])
      {:ok, engine, _} = BounceDecision.configure_levels(engine, person, levels, sample.accepted_at)
      external_id = "synthetic-migration-advisory"
      frame = %{
        "type" => "forward", "op_id" => "30000000-0000-4000-8000-000000000007",
        "session_handle" => sample.session_handle,
        "route" => "other", "cli_version" => "synthetic", "unsupported_reason" => nil,
        "advisory_v1" => true,
        "message" => %{"sender_id" => sender, "message_id" => external_id,
          "created_at" => 1, "title" => "Synthetic advisory", "message_type" => "fyi",
          "answer_ui" => %{"schema" => "attention-client.actions/1", "actions" => [
            %{"id" => "seen", "label" => "Seen", "kind" => "value", "value" => %{"acknowledgment" => "seen"}}
          ]}}
      }
      {engine, {:ok, _}} = Engine.request_command(engine, sample.origin, frame, sample.accepted_at + 1)
      record = Map.fetch!(engine.messages, {person, sender, external_id})
      scope = {person, record.origin, record.session_handle}
      engine = BounceDecision.record_scores(engine, person,
        [%{"sender_id" => sender, "message_id" => external_id, "score" => 0.9}], record.accepted_at)
      engine = BounceDecision.evaluate_due(engine, record.accepted_at + 86_400_001)
      feed = Map.fetch!(engine.advisory_feeds, scope)
      assert feed.high > 0
      engine = Enum.reduce(feed.entries, engine, fn {_, entry}, acc ->
        {:ok, next, _} = BounceDecision.advisory_ack(acc, record.origin, record.session_handle, entry["advisory_id"])
        next
      end)
      assert BounceDecision.valid_state?(engine)
      {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: person, actor: "fixture", action: "synthetic_advisory"})
      :ok = Store.close(store)
    end

    defp verify_feeds(db, bundle, manifest) do
      assert {:ok, counts} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, %{}, fn
        {{family, scope, part}, value}, counts when family in [:origin_feed, :advisory] ->
          {person, origin, handle} = case scope do
            {person, origin} -> {person, origin, ""}
            triple -> triple
          end
          {table, params, sql, typed} = case {family, part} do
            {family, :high} ->
              kind = if family == :origin_feed, do: "origin_feed", else: "advisory_feed"
              {"allocation_highs", [person, origin, handle, kind],
                "SELECT high FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND kind=$4", false}
            {:origin_feed, {:entry, position}} ->
              {"origin_feed_entries", [person, origin, position],
                "SELECT body::text FROM harklight.origin_feed_entries WHERE person_id=$1 AND origin_key=$2 AND position=$3", true}
            {:origin_feed, {:delivery_ack, position}} ->
              {"origin_delivery_acks", [person, origin, position],
                "SELECT body::text FROM harklight.origin_delivery_acks WHERE person_id=$1 AND origin_key=$2 AND position=$3", true}
            {:advisory, {:entry, position}} ->
              {"advisory_feed_entries", [person, origin, handle, position],
                "SELECT body::text FROM harklight.advisory_feed_entries WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND position=$4", true}
            {:advisory, {:receipt, id}} ->
              {"advisory_feed_receipts", [person, origin, handle, id],
                "SELECT true FROM harklight.advisory_feed_receipts WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND advisory_id=$4", false}
          end
          assert {:ok, %{rows: [[actual]]}} = ImportSession.query(db, sql, params)
          if typed, do: assert_typed(actual, value), else: assert(actual == value)
          {:ok, Map.update(counts, table, 1, &(&1 + 1))}
        _, counts -> {:ok, counts}
      end)
      for table <- ~w(origin_feed_entries origin_delivery_acks advisory_feed_entries advisory_feed_receipts) do
        expected = Map.fetch!(counts, table)
        assert expected > 0
        assert {:ok, %{rows: [[^expected]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, [])
      end
      expected = Map.fetch!(counts, "allocation_highs")
      assert {:ok, %{rows: [[^expected]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.allocation_highs WHERE kind IN ('origin_feed','advisory_feed')", [])
    end

    defp verify_messages(db, bundle, manifest) do
      alias AttentionServer.Repository.Postgres.Items
      alias AttentionServer.ClientAPI.Resources
      assert {:ok, {count, revisions}} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, {0, 0},
        fn
          {{:row, :messages, {person, sender, external_id} = key}, record}, {count, rev_count} ->
            id = ImportMessages.id(key, record)
            item = Resources.item_id(record)
            current = Map.get(record, :revision, 1)
            expected = Map.merge(record, %{revision: current, session_handle: Map.get(record, :session_handle), free_order: Map.get(record, :free_order)})
            assert {:ok, ^key, ^expected} = Items.agent_item({person, record.origin}, item)
            assert {:error, :not_found} = Items.agent_item({person, "synthetic-wrong-origin"}, item)
            assert {:error, :not_found} = Items.recipient_item("synthetic-wrong-person", item)
            if AttentionServer.BounceDecision.person_visible?(record) do
              assert {:ok, ^key, ^expected} = Items.recipient_item(person, item)
            else
              assert {:error, :not_found} = Items.recipient_item(person, item)
            end
            assert {:ok, %{rows: [[^person, ^sender, ^external_id, ^current]]}} = ImportSession.query(db,
              "SELECT person_id,sender_id,message_id,revision FROM harklight.messages WHERE id=$1", [id])
            versions = Map.get(record, :revisions, %{1 => record.message})
            for {rev, body} <- versions do
              assert {:ok, %{rows: [[at, text]]}} = ImportSession.query(db,
                "SELECT accepted_at_ms,body::text FROM harklight.message_revisions WHERE message_id=$1 AND revision=$2", [id, rev])
              expected_at = cond do
                rev == 1 -> record.accepted_at
                rev == current -> Map.get(record, :revision_accepted_at)
                true -> nil
              end
              assert at == expected_at
              assert_typed(text, body)
            end
            assert {:ok, %{rows: [[^person, ^item]]}} = ImportSession.query(db,
              "SELECT person_id,item_id FROM harklight.message_recipients WHERE message_id=$1", [id])
            {:ok, {count + 1, rev_count + map_size(versions)}}
          _, acc -> {:ok, acc}
        end)
      assert count > 0
      assert {:ok, %{rows: [[unknown_count]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.message_revisions WHERE accepted_at_ms IS NULL", [])
      assert unknown_count > 0
      assert {:ok, %{rows: [[notice_count]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.messages WHERE origin_key='app:harklight/notices' AND principal_origin_key IS NULL AND session_handle IS NULL", [])
      assert notice_count > 0
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.principals WHERE key='app:harklight/notices'", [])
      assert {:ok, attached} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0,
        fn
          {{:row, :messages, _}, record}, n ->
            files = record |> Map.get(:files_by_revision, %{}) |> Map.values() |> List.flatten()
            {:ok, n + length(files)}
          _, n -> {:ok, n}
        end)
      assert attached > 0
      for {table, expected} <- [{"messages", count}, {"message_recipients", count}, {"message_revisions", revisions}] do
        assert {:ok, %{rows: [[^expected]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, [])
      end
    end

    defp verify_receipts(db, bundle, manifest) do
      expected_count = Enum.reduce(~w(client_idempotency harklight_app_receipts), 0, fn table, total ->
        source = Map.fetch!(manifest.data.tables, table)
        assert {:ok, :ok} = Frames.fold(Path.join(bundle, source.file), source.summary, :ok,
          fn values, :ok ->
            [scope, key, digest, status, body, at | flags] = values
            {:ok, {person, origin}} = ImportReceipts.scope(scope)
            assert {:ok, %{rows: [[hash, ^status, text, ^at, expires, original]]}} = ImportSession.query(db,
              "SELECT digest,status,response::text,created_at_ms,expires_at_ms,original_response FROM harklight.operation_receipts WHERE person_id=$1 AND origin_key=$2 AND namespace=$3 AND operation_key=$4", [person, origin, table, key])
            assert hash == Base.decode16!(digest, case: :lower)
            {:ok, expected} = JSON.decode_line(body <> "\n")
            assert_typed(text, expected)
            if table == "client_idempotency" do
              assert expires == at + 30 * 24 * 60 * 60 * 1000
              refute original
            else
              assert expires == nil
              assert original == (flags == [1])
            end
            {:ok, :ok}
          end)
        total + source.summary.count
      end)
      assert {:ok, %{rows: [[^expected_count]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.operation_receipts", [])
      assert {:ok, %{rows: [[2]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.operation_receipts WHERE operation_key='synthetic-same-key'", [])
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.operation_receipts WHERE operation_key='synthetic-agent-key' AND origin_key<>''", [])
    end

    defp verify_receipt_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyReceipts
      assert {:ok, expected} = verifier.run(db, identity, bundle)
      assert Enum.all?(expected.receipts, fn {_, n} -> n > 0 end)
      for change <- [
        "digest=decode(repeat('00',32),'hex')", "status=status+1",
        "response='{}'::jsonb", "created_at_ms=created_at_ms+1",
        "expires_at_ms=coalesce(expires_at_ms,0)+1", "original_response=NOT original_response",
        "operation_key=operation_key || '-wrong'", "namespace=namespace || '-wrong'",
        "origin_key=origin_key || '-wrong'"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx,
            "UPDATE harklight.operation_receipts SET " <> change, [])
          assert n > 0
          assert {:error, {:receipt_state_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^expected} = verifier.run(db, identity, bundle)
      end
      for {sql, error} <- [
        {"DELETE FROM harklight.operation_receipts WHERE operation_key='synthetic-agent-key'", :state},
        {"UPDATE harklight.operation_receipts r SET person_id=(SELECT p.id FROM harklight.people p WHERE p.id<>r.person_id LIMIT 1) WHERE operation_key='synthetic-agent-key'", :state},
        {"UPDATE harklight.compatibility_facts SET body='{}'::jsonb WHERE family='harklight_app_receipts'", :state},
        {"INSERT INTO harklight.operation_receipts SELECT person_id,origin_key,namespace,operation_key || '-extra',digest,status,response,created_at_ms,expires_at_ms,original_response FROM harklight.operation_receipts LIMIT 1", :count}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          if error == :state do
            assert {:error, {:receipt_state_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          else
            assert {:error, {:receipt_count_mismatch, "operation_receipts"}} = verifier.run(tx, identity, bundle)
          end
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^expected} = verifier.run(db, identity, bundle)
      end
      snapshot = "INSERT INTO harklight.baseline_snapshots SELECT 'synthetic-baseline',person_id,origin_key,1,1,'{}'::jsonb FROM harklight.stream_heads LIMIT 1"
      for {table, statements} <- [
        {"handoff_receipts", ["INSERT INTO harklight.handoff_receipts SELECT person_id,key,'synthetic-turn','synthetic-operation','{}'::jsonb FROM harklight.principals LIMIT 1"]},
        {"jobs", ["INSERT INTO harklight.jobs(kind,id,person_id,ready_at_ms,body) SELECT 'push','synthetic-job',id,1,'{}'::jsonb FROM harklight.people LIMIT 1"]},
        {"baseline_snapshots", [snapshot]},
        {"baseline_snapshot_items", [snapshot, "INSERT INTO harklight.baseline_snapshot_items VALUES('synthetic-baseline',0,'item','synthetic-item',1,'{}'::jsonb)"]}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          for sql <- statements do
            assert {:ok, %{num_rows: 1}} = ImportSession.query(tx, sql, [])
          end
          assert {:error, {:receipt_count_mismatch, ^table}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^expected} = verifier.run(db, identity, bundle)
      end
    end

    defp verify_stream_head_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyStreamHeads
      source_file = Path.join(bundle, "source.sqlite3")
      before = AttentionServer.Migration.Source.file_observation(source_file)
      assert {:ok, heads} = VerifyStreamHeads.run(db, identity, bundle)
      assert heads.heads > 0
      for {sql, error} <- [
        {"UPDATE harklight.stream_heads SET head=head+1", :stream_head_mismatch},
        {"UPDATE harklight.stream_heads SET floor=floor+1,head=head+1", :stream_head_mismatch},
        {"UPDATE harklight.stream_heads SET seed_revision=seed_revision+1,head=head+1", :stream_head_mismatch},
        {"UPDATE harklight.server_identity SET cursor_epoch=cursor_epoch+1", :cursor_identity_mismatch},
        {"UPDATE harklight.server_identity SET cursor_signing_key=decode(repeat('00',32),'hex')", :cursor_identity_mismatch}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, _} = ImportSession.query(tx, sql, [])
          assert {:error, ^error} = VerifyStreamHeads.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^heads} = VerifyStreamHeads.run(db, identity, bundle)
      end
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx,
          "INSERT INTO harklight.stream_heads(person_id,origin_key,head,floor,seed_revision) SELECT person_id,'extra-synthetic-stream',head,floor,seed_revision FROM harklight.stream_heads LIMIT 1", [])
        assert {:error, :stream_head_count_mismatch} = VerifyStreamHeads.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^heads} = VerifyStreamHeads.run(db, identity, bundle)
      assert AttentionServer.Migration.Source.file_observation(source_file) == before
      # Mutate the disposable copied source after the verifier has read its epoch
      # and built the TEMP index. A read-only SQLite connection is not a seal.
      original_bytes = File.read!(source_file)
      changing_query = fn connection, sql, params ->
        result = ImportSession.query(connection, sql, params)
        if String.starts_with?(sql, "SELECT instance_id,cursor_signing_key,cursor_epoch") do
          assert {:ok, writer} = Exqlite.Sqlite3.open(source_file)
          try do
            assert :ok = Exqlite.Sqlite3.execute(writer,
              "INSERT INTO client_change_log_epoch(id,epoch) VALUES(1,999) ON CONFLICT(id) DO UPDATE SET epoch=999")
          after
            Exqlite.Sqlite3.close(writer)
          end
        end
        result
      end
      try do
        assert {:error, :stream_source_changed} = VerifyStreamHeads.run(db, identity, bundle, changing_query)
      after
        # Both SQLite connections are closed now. Restore only this disposable
        # negative-test copy, including any WAL retained by the read-only close.
        for suffix <- ["-wal", "-shm", "-journal"], do: File.rm(source_file <> suffix)
        File.write!(source_file, original_bytes)
      end
      assert {:ok, ^heads} = VerifyStreamHeads.run(db, identity, bundle)
    end

    defp verify_stream_position_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyStreamPositions
      assert {:ok, counts} = VerifyStreamPositions.run(db, identity, bundle)
      assert counts.revisions > 0 and counts.changes > 0
      for {sql, error} <- [
        {"UPDATE harklight.stream_resource_revisions SET revision=revision+100", :stream_revision_mismatch},
        {"UPDATE harklight.stream_resource_revisions SET first_seen_at_ms=coalesce(first_seen_at_ms,0)+100", :stream_revision_mismatch},
        {"UPDATE harklight.stream_changes SET created_at_ms=created_at_ms+100", :stream_change_mismatch},
        {"UPDATE harklight.stream_changes SET session_handle='changed-session'", :stream_change_mismatch}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, ^error} = VerifyStreamPositions.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = VerifyStreamPositions.run(db, identity, bundle)
      end
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx,
          "INSERT INTO harklight.stream_resource_revisions(person_id,origin_key,resource_kind,resource_id,revision) SELECT person_id,origin_key,'item','item:extra-synthetic',revision FROM harklight.stream_resource_revisions LIMIT 1", [])
        assert {:error, {:stream_count_mismatch, :revisions}} = VerifyStreamPositions.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^counts} = VerifyStreamPositions.run(db, identity, bundle)
    end

    defp verify_streams(db, bundle, manifest) do
      for table <- ~w(client_change_log_meta agent_change_log_meta client_resource_revs client_resource_created client_changes agent_changes agent_item_advisory_changes) do
        source = Map.fetch!(manifest.data.tables, table)
        assert {:ok, :ok} = Frames.fold(Path.join(bundle, source.file), source.summary, :ok,
          fn row, :ok ->
            verify_stream_row(db, table, row)
            {:ok, :ok}
          end)
      end
      count_rows = fn table -> Map.fetch!(manifest.data.tables, table).summary.count end
      expected_heads = count_rows.("client_change_log_meta") + count_rows.("agent_change_log_meta")
      expected_revisions = count_rows.("client_resource_revs") + count_rows.("agent_changes") + count_rows.("agent_item_advisory_changes")
      expected_changes = Enum.reduce(~w(agent_changes agent_item_advisory_changes), count_rows.("client_changes"), fn table, count ->
        source = Map.fetch!(manifest.data.tables, table)
        {:ok, total} = Frames.fold(Path.join(bundle, source.file), source.summary, count,
          fn [_, _, _, _, _, at], acc -> {:ok, acc + if(is_nil(at), do: 0, else: 1)} end)
        total
      end)
      for {table, expected} <- [{"stream_heads", expected_heads}, {"stream_resource_revisions", expected_revisions}, {"stream_changes", expected_changes}] do
        assert {:ok, %{rows: [[^expected]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, [])
      end
      assert {:ok, %{rows: [[count]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.stream_changes WHERE resource_kind='item_removed'", [])
      assert count > 0
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.stream_changes WHERE resource_id='advisory:synthetic-expired'", [])
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.stream_resource_revisions WHERE resource_id='advisory:synthetic-expired' AND resource_kind='advisory'", [])
      assert {:ok, %{rows: [[markers]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.stream_resource_revisions WHERE resource_kind='availability_writer'", [])
      assert markers > 0
    end

    defp verify_stream_row(db, "client_change_log_meta", [person, head, floor, seed]),
      do: assert_stream_head(db, person, "", head, floor, seed)
    defp verify_stream_row(db, "agent_change_log_meta", [person, agent, head, floor, seed]),
      do: assert_stream_head(db, person, agent, head, floor, seed)
    defp verify_stream_row(db, "client_resource_revs", [person, id, rev]) do
      {:ok, kind} = ImportStreams.person_kind(id)
      assert_stream_revision(db, person, "", kind, id, rev)
    end
    defp verify_stream_row(db, "client_resource_created", [person, id, at]) do
      {:ok, kind} = ImportStreams.person_kind(id)
      assert {:ok, %{rows: [[^at]]}} = ImportSession.query(db,
        "SELECT first_seen_at_ms FROM harklight.stream_resource_revisions WHERE person_id=$1 AND origin_key='' AND resource_kind=$2 AND resource_id=$3", [person, kind, id])
    end
    defp verify_stream_row(db, "client_changes", [person, rev, kind, id, at]),
      do: assert_stream_change(db, person, "", kind, id, rev, nil, at)
    defp verify_stream_row(db, table, [person, agent, id, rev, session, at])
         when table in ~w(agent_changes agent_item_advisory_changes) do
      {:ok, kind} = ImportStreams.agent_kind(id)
      assert_stream_revision(db, person, agent, kind, id, rev)
      if is_nil(at) do
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
          "SELECT count(*) FROM harklight.stream_changes WHERE person_id=$1 AND origin_key=$2 AND revision=$3", [person, agent, rev])
      else
        assert_stream_change(db, person, agent, kind, id, rev, session, at)
      end
    end
    defp assert_stream_head(db, person, origin, head, floor, seed) do
      assert {:ok, %{rows: [[^head, ^floor, ^seed]]}} = ImportSession.query(db,
        "SELECT head,floor,seed_revision FROM harklight.stream_heads WHERE person_id=$1 AND origin_key=$2", [person, origin])
    end
    defp assert_stream_revision(db, person, origin, kind, id, rev) do
      assert {:ok, %{rows: [[^rev]]}} = ImportSession.query(db,
        "SELECT revision FROM harklight.stream_resource_revisions WHERE person_id=$1 AND origin_key=$2 AND resource_kind=$3 AND resource_id=$4", [person, origin, kind, id])
    end
    defp assert_stream_change(db, person, origin, kind, id, rev, session, at) do
      assert {:ok, %{rows: [[^kind, ^id, ^session, ^at]]}} = ImportSession.query(db,
        "SELECT resource_kind,resource_id,session_handle,created_at_ms FROM harklight.stream_changes WHERE person_id=$1 AND origin_key=$2 AND revision=$3", [person, origin, rev])
    end

    defp verify_ordinary_entry_uploads(db, identity, bundle) do
      alias AttentionServer.Migration.ImportEntryUploads
      assert {:ok, %{rows: [[owner, person, attrs, completed]]}} = ImportSession.query(db,
        "SELECT owner_key,person_id,attributes,completed_at_ms FROM harklight.uploads WHERE id=$1", [@ordinary_entry_upload])
      assert {:ok, %{rows: [[other_person]]}} = ImportSession.query(db,
        "SELECT id FROM harklight.people WHERE id<>$1 LIMIT 1", [person])
      assert {:error, :synthetic_missing_upload} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx, "DELETE FROM harklight.uploads WHERE id=$1", [@ordinary_entry_upload])
        # The prerequisite byte importer rejects the missing manifest before
        # the entry-link scope query is reached.
        assert {:error, :upload_manifest_mismatch} = ImportEntryUploads.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_missing_upload)
      end)
      for {column, bad, good} <- [
        {"person_id", other_person, person},
        {"owner_key", "synthetic-wrong-owner", owner},
        {"attributes", %{}, attrs},
        {"completed_at_ms", nil, completed}
      ] do
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.uploads SET " <> column <> "=$2 WHERE id=$1", [@ordinary_entry_upload, bad])
        assert {:error, :ordinary_entry_upload_scope_mismatch} = ImportEntryUploads.run(db, identity, bundle)
        assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "ordinary/entry-uploads/v1")
        assert {:ok, %{rows: [[0]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.conversation_entry_uploads", [])
        assert {:ok, _} = ImportSession.query(db,
          "UPDATE harklight.uploads SET " <> column <> "=$2 WHERE id=$1", [@ordinary_entry_upload, good])
      end
      assert :ok = ImportEntryUploads.run(db, identity, bundle)
      assert :ok = ImportEntryUploads.run(db, identity, bundle)
      assert {:ok, %{rows: [[conversation_id, entry_id, 1, file, ^person, body]]}} = ImportSession.query(db,
        "SELECT u.conversation_id,u.entry_id,u.ordinal,u.attributes,m.person_id,e.body FROM harklight.conversation_entry_uploads u JOIN harklight.conversation_entries e ON e.conversation_id=u.conversation_id AND e.id=u.entry_id JOIN harklight.conversations c ON c.id=e.conversation_id JOIN harklight.messages m ON m.id=c.message_id WHERE u.upload_id=$1", [@ordinary_entry_upload])
      assert {:ok, event} = Attributes.decode(body)
      assert {:ok, descriptor} = Attributes.decode(file)
      assert descriptor == Enum.at(event.payload["files"], 1)
      assert hd(event.payload["files"])["tier"] == "inline"
      verifier = AttentionServer.Migration.VerifyEntryUploads
      assert {:ok, manifest, _} = Bundle.verify(bundle)
      assert {:ok, expected_total} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
        {{:row, :conversation_events, _}, source_event}, n ->
          files = Map.get(source_event.payload, "files", [])
          {:ok, n + Enum.count(files, &(&1["tier"] != "inline"))}
        _, n -> {:ok, n}
      end)
      # The compatibility history already has a file; the mixed inline/upload
      # fixture adds another. Count all source links, mutate only the target.
      assert expected_total >= 2
      assert {:ok, %{ordinary_entry_links: ^expected_total} = counts} = verifier.run(db, identity, bundle)
      target = [conversation_id, entry_id, 1]
      where = " WHERE conversation_id=$1 AND entry_id=$2 AND ordinal=$3"
      for statement <- [
        "UPDATE harklight.conversation_entry_uploads SET ordinal=0",
        "UPDATE harklight.conversation_entry_uploads SET attributes='{}'::jsonb",
        "DELETE FROM harklight.conversation_entry_uploads"
      ] do
        sql = statement <> where
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: 1}} = ImportSession.query(tx, sql, target)
          assert {:error, {:ordinary_entry_upload_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, %{num_rows: 1}} = ImportSession.query(tx,
          "INSERT INTO harklight.conversation_entry_uploads SELECT conversation_id,entry_id,ordinal+1,upload_id,attributes FROM harklight.conversation_entry_uploads" <> where, target)
        assert {:error, :ordinary_entry_upload_count_mismatch} = verifier.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^counts} = verifier.run(db, identity, bundle)
    end

    defp verify_auth_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyAuthSessions
      assert {:ok, %{auth_sessions: 5} = counts} = verifier.run(db, identity, bundle)
      for sql <- [
        "UPDATE harklight.auth_sessions SET principal_revision=principal_revision+1",
        "UPDATE harklight.auth_sessions SET expires_at_ms=1",
        "UPDATE harklight.auth_sessions SET attributes='{}'::jsonb",
        "UPDATE harklight.auth_sessions SET kind=CASE kind WHEN 'browser' THEN 'native' ELSE 'browser' END",
        "DELETE FROM harklight.auth_sessions WHERE kind='native'"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, {:auth_session_mismatch, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      for {hash, expected} <- [
        {:crypto.hash(:sha256, "synthetic retired owner session"), :orphan},
        {:crypto.hash(:sha256, "synthetic extra session"), :extra}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: 1}} = ImportSession.query(tx,
            "INSERT INTO harklight.auth_sessions(token_hash,person_id,principal_key,kind,principal_revision,expires_at_ms,attributes) SELECT $1,person_id,principal_key,kind,principal_revision,expires_at_ms,attributes FROM harklight.auth_sessions LIMIT 1", [hash])
          result = verifier.run(tx, identity, bundle)
          if expected == :orphan do
            assert {:error, {:auth_session_mismatch, _}} = result
          else
            assert {:error, :auth_session_count_mismatch} = result
          end
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
    end

    defp verify_auth(db, bundle, manifest) do
      for table <- ~w(harklight_browser_sessions harklight_device_sessions) do
        source = Map.fetch!(manifest.data.tables, table)
        assert {:ok, :ok} = Frames.fold(Path.join(bundle, source.file), source.summary, :ok,
          fn values, :ok ->
            {:ok, _, row} = SourceTables.fact(table, values)
            {hash, key, kind} = if table == "harklight_browser_sessions" do
              {row["session_hash"], row["owner_key"], "browser"}
            else
              {Base.decode16!(row["session_hash"], case: :lower), row["device_id"], "native"}
            end
            assert {:ok, %{rows: principals}} = ImportSession.query(db,
              "SELECT revision FROM harklight.principals WHERE key=$1 AND person_id=$2", [key, row["person_id"]])
            assert {:ok, %{rows: sessions}} = ImportSession.query(db,
              "SELECT person_id,principal_key,kind,principal_revision,expires_at_ms,attributes::text FROM harklight.auth_sessions WHERE token_hash=$1", [hash])
            if principals == [] do
              assert sessions == []
            else
              [[principal_revision]] = principals
              assert [[person, ^key, ^kind, revision, nil, text]] = sessions
              assert person == row["person_id"]
              if kind == "browser" do
                assert revision == row["owner_revision"]
                assert_typed(text, row)
              else
                assert revision == principal_revision
                assert_typed(text, %{pending: row["pending"] == 1,
                  public_key: row["public_key"], signed_in_at_ms: row["signed_in_at_ms"]})
              end
            end
            {:ok, :ok}
          end)
      end
      assert {:ok, %{rows: [[5]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.auth_sessions", [])
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.auth_sessions s JOIN harklight.principals p ON p.key=s.principal_key AND p.person_id=s.person_id WHERE s.kind='browser' AND p.kind='owner' AND NOT p.revoked AND p.revision=s.principal_revision", [])
      assert {:ok, %{rows: [[2]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.auth_sessions s JOIN harklight.principals p ON p.key=s.principal_key AND p.person_id=s.person_id WHERE s.kind='native' AND p.kind='device' AND NOT p.revoked", [])
    end

    defp verify_message_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyMessages
      assert {:ok, verified} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, %{rows: [[id, at, attributes, person, item]]}} = ImportSession.query(db,
        "SELECT m.id,m.accepted_at_ms,m.attributes,r.person_id,r.item_id FROM harklight.messages m JOIN harklight.message_recipients r ON m.id=r.message_id ORDER BY m.id LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.messages SET accepted_at_ms=$2 WHERE id=$1", [id, at + 1])
      assert {:error, {:ordinary_message_mismatch, :message, digest}} = VerifyMessages.run(db, identity, bundle)
      assert digest == Base.encode16(:crypto.hash(:sha256, id), case: :lower)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.messages SET accepted_at_ms=$2,attributes='{}'::jsonb WHERE id=$1", [id, at])
      assert {:error, {:ordinary_message_mismatch, :message, ^digest}} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.messages SET attributes=$2::text::jsonb WHERE id=$1", [id, JSON.encode!(attributes)])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET item_id='synthetic-wrong-correlation' WHERE message_id=$1 AND person_id=$2", [id, person])
      assert {:error, {:ordinary_message_mismatch, :message, ^digest}} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET item_id=$3 WHERE message_id=$1 AND person_id=$2", [id, person, item])
      assert {:ok, %{rows: [[group]]}} = ImportSession.query(db,
        "SELECT id FROM harklight.groups ORDER BY id LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.messages SET group_id=$2 WHERE id=$1", [id, group])
      assert {:error, {:ordinary_message_mismatch, :message, ^digest}} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.messages SET group_id=NULL WHERE id=$1", [id])
      assert {:ok, ^verified} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, %{rows: [[revision, revision_at, body]]}} = ImportSession.query(db,
        "SELECT revision,accepted_at_ms,body FROM harklight.message_revisions WHERE message_id=$1 ORDER BY revision LIMIT 1", [id])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_revisions SET body='{}'::jsonb WHERE message_id=$1 AND revision=$2", [id, revision])
      assert {:error, {:ordinary_message_mismatch, {:revision, ^revision}, ^digest}} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_revisions SET body=$3::text::jsonb WHERE message_id=$1 AND revision=$2", [id, revision, JSON.encode!(body)])
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.message_revisions(message_id,revision,accepted_at_ms,body) VALUES($1,999,$2,$3::text::jsonb)", [id, revision_at, JSON.encode!(body)])
      assert {:error, :ordinary_message_count_mismatch} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.message_revisions WHERE message_id=$1 AND revision=999", [id])
      assert {:ok, ^verified} = VerifyMessages.run(db, identity, bundle)
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
    end

    defp verify_conversation_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyConversations
      assert {:ok, result} = VerifyConversations.run(db, identity, bundle)
      assert result.entries > 256
      assert result.read_positions > 0
      assert {:ok, %{rows: [[id, conversation, body]]}} = ImportSession.query(db,
        "SELECT id,conversation_id,body FROM harklight.conversation_entries LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.conversation_entries SET body='{}'::jsonb WHERE conversation_id=$1 AND id=$2", [conversation, id])
      assert {:error, :conversation_entry_mismatch} = VerifyConversations.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.conversation_entries SET body=$3 WHERE conversation_id=$1 AND id=$2", [conversation, id, body])
      assert {:ok, %{rows: [[read_conversation, person, position]]}} = ImportSession.query(db,
        "SELECT conversation_id,person_id,position FROM harklight.read_positions LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.read_positions SET position=position+1 WHERE conversation_id=$1 AND person_id=$2", [read_conversation, person])
      assert {:error, :conversation_read_position_mismatch} = VerifyConversations.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.read_positions SET position=$3 WHERE conversation_id=$1 AND person_id=$2", [read_conversation, person, position])
      assert {:ok, %{rows: [[attrs]]}} = ImportSession.query(db,
        "SELECT attributes FROM harklight.conversations WHERE id=$1", [conversation])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.conversations SET attributes='{}'::jsonb WHERE id=$1", [conversation])
      assert {:error, :conversation_summary_mismatch} = VerifyConversations.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.conversations SET attributes=$2 WHERE id=$1", [conversation, attrs])
      assert {:ok, _} = ImportSession.query(db, """
        INSERT INTO harklight.conversation_entries(id,conversation_id,position,request_revision,kind,body)
        SELECT 'extra-entry',conversation_id,999999,request_revision,kind,body
        FROM harklight.conversation_entries WHERE conversation_id=$1 AND id=$2
        """, [conversation, id])
      assert {:error, {:conversation_count_mismatch, "conversation_entries"}} = VerifyConversations.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.conversation_entries WHERE conversation_id=$1 AND id='extra-entry'", [conversation])
      assert {:ok, ^result} = VerifyConversations.run(db, identity, bundle)
    end

    defp verify_recipient_drift(db, identity, bundle) do
      alias AttentionServer.Migration.VerifyRecipientState
      assert {:ok, result} = VerifyRecipientState.run(db, identity, bundle)
      assert result.ordinary_answers > 0
      assert {:ok, %{rows: [[message, person, seen, attrs]]}} = ImportSession.query(db,
        "SELECT r.message_id,r.person_id,r.seen_revision,r.attributes FROM harklight.message_recipients r JOIN harklight.messages m ON m.id=r.message_id WHERE m.kind='ordinary' LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET seen_revision=$3 WHERE message_id=$1 AND person_id=$2", [message, person, (seen || 0) + 100])
      assert {:error, {:recipient_state_mismatch, :recipient, _}} = VerifyRecipientState.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET seen_revision=$3,attributes='{}'::jsonb WHERE message_id=$1 AND person_id=$2", [message, person, seen])
      assert {:error, {:recipient_state_mismatch, :recipient, _}} = VerifyRecipientState.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.message_recipients SET attributes=$3 WHERE message_id=$1 AND person_id=$2", [message, person, attrs])
      assert {:ok, %{rows: [[answer, body]]}} = ImportSession.query(db,
        "SELECT a.id,a.body FROM harklight.answers a JOIN harklight.messages m ON m.id=a.message_id WHERE m.kind='ordinary' LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.answers SET body='{}'::jsonb WHERE id=$1", [answer])
      assert {:error, {:recipient_state_mismatch, :answer, _}} = VerifyRecipientState.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db, "UPDATE harklight.answers SET body=$2 WHERE id=$1", [answer, body])
      assert {:ok, ^result} = VerifyRecipientState.run(db, identity, bundle)
    end

    defp verify_item_facts(db, identity, bundle, manifest) do
      alias AttentionServer.{Engine, AgentAPI.FeedActivity, ClientAPI.Resources}
      alias AttentionServer.Migration.ImportItemFacts
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_item_facts() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.family='ordinary_item_availability' THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_item_facts BEFORE INSERT ON harklight.compatibility_facts FOR EACH ROW EXECUTE FUNCTION harklight.fail_item_facts()", [])
      assert {:error, _} = ImportItemFacts.run(db, identity, bundle)
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "messages/item-facts/v1")
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family IN ('ordinary_item_files','ordinary_item_availability')", [])
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER fail_item_facts ON harklight.compatibility_facts", [])
      assert :ok = ImportItemFacts.run(db, identity, bundle)
      assert :ok = ImportItemFacts.run(db, identity, bundle)
      assert {:ok, records} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, [],
        fn entry, entries -> {:ok, [entry | entries]} end)
      assert {:ok, source} = AttentionServer.Store.RecordCodec.decode(Enum.map(Enum.reverse(records), fn {address, value} ->
        AttentionServer.Store.RecordCodec.encode_row(address, value) end))
      source = Engine.rebuild_route_b_index(source)
      assert Enum.any?(source.messages, fn {_, record} -> is_binary(Map.get(record, :session_handle)) end)
      for {{person, _, _} = key, record} <- source.messages do
        expected = Resources.item(source, key, %{"key" => "synthetic-rank"})
        assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
          "SELECT body FROM harklight.compatibility_facts WHERE family='ordinary_item_files' AND address=$1",
          [Attributes.key({person, Resources.item_id(record), record.revision})])
        assert {:ok, files} = Attributes.decode(body)
        assert files == expected["files"]
        assert {:ok, %{rows: rows}} = ImportSession.query(db,
          "SELECT body FROM harklight.compatibility_facts WHERE family='ordinary_item_availability' AND address=$1",
          [Attributes.key({person, Resources.item_id(record)})])
        if is_binary(Map.get(record, :session_handle)) do
          assert [[body]] = rows
          assert {:ok, {status, stamp}} = Attributes.decode(body)
          assert FeedActivity.render_committed(status, stamp) == expected["availability"]
        else
          assert rows == []
          refute Map.has_key?(expected, "availability")
        end
      end
    end

    defp verify_revision_files(db, identity, bundle, manifest) do
      alias AttentionServer.{Engine, BounceDecision}
      alias AttentionServer.ClientAPI.Resources
      alias AttentionServer.Migration.ImportRevisionFiles
      alias AttentionServer.Repository.Postgres.Items
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.fail_revision_files() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.family='ordinary_revision_files' THEN RAISE EXCEPTION 'synthetic interruption'; END IF;
          RETURN NEW;
        END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER fail_revision_files BEFORE INSERT ON harklight.compatibility_facts FOR EACH ROW EXECUTE FUNCTION harklight.fail_revision_files()", [])
      assert {:error, _} = ImportRevisionFiles.run(db, identity, bundle)
      assert {:ok, %{rows: [[0]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family='ordinary_revision_files'", [])
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "messages/revision-files/v1")
      assert {:ok, _} = ImportSession.query(db,
        "DROP TRIGGER fail_revision_files ON harklight.compatibility_facts", [])
      assert :ok = ImportRevisionFiles.run(db, identity, bundle)
      assert :ok = ImportRevisionFiles.run(db, identity, bundle)
      assert {:ok, uploads} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, %{}, fn
        {{:row, :uploads, id}, source}, acc -> {:ok, Map.put(acc, id, source)}
        _, acc -> {:ok, acc}
      end)
      assert {:ok, {count, empty, attached}} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, {0, 0, 0}, fn
        {{:row, :messages, {person, _, _} = key}, record}, counts ->
          engine = %Engine{messages: %{key => record}, uploads: uploads}
          next = Enum.reduce(Map.keys(Map.get(record, :revisions, %{1 => record.message})), counts, fn revision, {n, e, a} ->
            assert {:ok, expected} = Resources.revision(engine, key, revision)
            item = Resources.item_id(record)
            assert {:ok, %{rows: [[body]]}} = ImportSession.query(db,
              "SELECT body FROM harklight.compatibility_facts WHERE family='ordinary_revision_files' AND address=$1",
              [Attributes.key({person, item, revision})])
            assert {:ok, files} = Attributes.decode(body)
            assert files == expected["files"]
            if BounceDecision.person_visible?(record) do
              assert {:ok, %{"revision" => ^expected}} = Items.recipient_revision(person, item, revision)
            else
              assert {:error, :not_found} = Items.recipient_revision(person, item, revision)
            end
            assert {:error, :not_found} = Items.recipient_revision("unrelated-person", item, revision)
            {n + 1, e + if(files == [], do: 1, else: 0), a + if(files == [], do: 0, else: 1)}
          end)
          {:ok, next}
        _, counts -> {:ok, counts}
      end)
      assert empty > 0
      assert attached > 0
      assert {:ok, %{rows: [[^count]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family='ordinary_revision_files'", [])
    end

    defp verify_tables(db, bundle, manifest) do
      for {table, source} <- manifest.data.tables do
        family = table
        assert {:ok, %{rows: [[count]]}} = ImportSession.query(db,
          "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family])
        case SourceTables.disposition(table) do
          {:durable, _} ->
            assert count == source.summary.count
            assert {:ok, :ok} = Frames.fold(Path.join(bundle, source.file), source.summary, :ok,
              fn values, :ok ->
                {:ok, address, _row} = SourceTables.fact(table, values)
                assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
                  "SELECT body::text FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
                  [family, Attributes.key(address)])
                assert_typed(text, values)
                {:ok, :ok}
              end)
          excluded when excluded in [:derived, :source_only] -> assert count == 0
        end
      end
      assert {:ok, %{rows: [[3]]}} = ImportSession.query(db,
        "SELECT count(*) FROM harklight.compatibility_facts WHERE family='harklight_browser_sessions'", [])
      assert {:ok, %{rows: [[text]]}} = ImportSession.query(db,
        "SELECT body::text FROM harklight.compatibility_facts WHERE family='harklight_auth_schema' AND address=$1", [Attributes.key(1)])
      {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, [1, 1, 0]} = Attributes.decode(envelope)
    end

    defp seed_organizations(path, epoch) do
      alias AttentionServer.{Engine, Groups, Store}
      {:ok, store} = Store.open_relay(path)
      {engine, :ok, touched} = Engine.with_touches(store.engine, fn engine ->
        [manager, invitee | _] = Enum.sort(Map.keys(engine.owners))
        extra = "50000000-0000-4000-8000-000000000001"
        hosted = "50000000-0000-4000-8000-000000000002"
        organization = "60000000-0000-4000-8000-000000000001"
        local_group = "70000000-0000-4000-8000-000000000001"
        hosted_group = "70000000-0000-4000-8000-000000000002"
        deleted_group = "70000000-0000-4000-8000-000000000003"
        invitation = "80000000-0000-4000-8000-000000000001"
        {:ok, engine} = Engine.operator_bootstrap(engine, extra, String.duplicate("e", 64))
        {:ok, engine, _} = Engine.hosted_account_create(engine, "synthetic-hosted", true, hosted, String.duplicate("f", 64))
        {:ok, engine} = Engine.replace_hosted_organization(engine, "synthetic-business", [hosted], organization)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"synthetic-business", "team"}, hosted_group, "Hosted team", [hosted], 3)
        {:ok, engine, _} = Groups.mute(engine, hosted_group, hosted, true)
        {:ok, engine, _} = Groups.replace_hosted(engine, {"synthetic-business", "deleted"}, deleted_group, "Deleted team", [hosted], 1)
        {:ok, engine} = Groups.delete_hosted(engine, {"synthetic-business", "deleted"})
        {:ok, engine, _} = Groups.create(engine, local_group, manager, "Pending invitations")
        {:ok, engine, _, ^invitation} = Groups.invite(engine, local_group, manager, invitee, invitation)
        {:ok, engine, _} = Groups.mute(engine, local_group, manager, true)
        engine = if epoch == 1 do
          {:ok, explicit} = Engine.set_local_organization_member(engine, extra, true)
          explicit
        else
          {:ok, explicit} = Engine.set_local_organization_member(engine, extra, false)
          explicit
        end
        {engine, :ok}
      end)
      {:ok, store} = Store.commit(store, {engine, touched}, %{person: "fixture", actor: "fixture", action: "groups"})
      :ok = Store.close(store)
      %{
        instance: engine.relay_instance_id,
        local: Map.get(engine.local_organizations, :default),
        owners: Map.keys(engine.owners),
        accounts: engine.hosted_accounts,
        organizations: engine.hosted_organizations,
        groups: engine.groups
      }
    end

    defp verify_organization_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyOrganizations
      assert {:ok, counts} = verifier.run(db, identity, bundle)
      assert Enum.all?(counts, fn {_, n} -> n > 0 end)
      for sql <- [
        "UPDATE harklight.hosted_accounts SET attributes='{}'::jsonb",
        "UPDATE harklight.organizations SET attributes='{}'::jsonb",
        "UPDATE harklight.groups SET name=name || '-changed'",
        "UPDATE harklight.groups SET manager_id=(SELECT id FROM harklight.people WHERE id<>groups.manager_id LIMIT 1) WHERE manager_id IS NOT NULL",
        "UPDATE harklight.groups SET hosted_ref=hosted_ref || '-changed' WHERE hosted_ref IS NOT NULL",
        "UPDATE harklight.group_members SET muted=NOT muted",
        "UPDATE harklight.group_members SET attributes='{\"changed\":true}'::jsonb",
        "UPDATE harklight.group_invitations SET state='accepted'",
        "UPDATE harklight.group_invitations SET attributes='{\"changed\":true}'::jsonb",
        "DELETE FROM harklight.organization_members"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, {:organization_state_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      for {table, sql} <- [
        {"organizations", "INSERT INTO harklight.organizations(id) VALUES('synthetic-extra-organization')"},
        {"group_members", "INSERT INTO harklight.group_members(group_id,person_id) SELECT g.id,p.id FROM harklight.groups g CROSS JOIN harklight.people p WHERE NOT EXISTS(SELECT 1 FROM harklight.group_members m WHERE m.group_id=g.id AND m.person_id=p.id) LIMIT 1"},
        {"group_invitations", "INSERT INTO harklight.group_invitations SELECT id || '-extra',group_id,person_id,state,attributes FROM harklight.group_invitations LIMIT 1"}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: 1}} = ImportSession.query(tx, sql, [])
          assert {:error, {:organization_count_mismatch, ^table}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
    end

    defp verify_organizations(db, expected) do
      assert {:ok, %{rows: [[organization_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.organizations", [])
      assert organization_count == map_size(expected.organizations) + 1
      assert {:ok, %{rows: [[group_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.groups", [])
      assert group_count == map_size(expected.groups)
      assert {:ok, %{rows: [[account_count]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.hosted_accounts", [])
      assert account_count == map_size(expected.accounts)
      hosted_people = MapSet.new(Map.values(expected.accounts), & &1.person)
      local_members = expected.local || MapSet.difference(MapSet.new(expected.owners), hosted_people)
      assert_members(db, expected.instance, local_members)
      assert {:ok, %{rows: [[nil, text]]}} =
        ImportSession.query(db, "SELECT hosted_ref,attributes::text FROM harklight.organizations WHERE id=$1", [expected.instance])
      assert_typed(text, expected.local)
      for {ref, value} <- expected.accounts do
        assert {:ok, %{rows: [[person, text]]}} =
          ImportSession.query(db, "SELECT person_id,attributes::text FROM harklight.hosted_accounts WHERE external_ref=$1", [ref])
        assert person == value.person
        assert_typed(text, value)
      end
      for {ref, value} <- expected.organizations do
        assert_members(db, value.id, value.members)
        assert {:ok, %{rows: [[^ref, text]]}} =
          ImportSession.query(db, "SELECT hosted_ref,attributes::text FROM harklight.organizations WHERE id=$1", [value.id])
        assert_typed(text, value)
      end
      for {id, group} <- expected.groups do
        assert {:ok, %{rows: [[org, manager, name, hosted_ref, text]]}} =
          ImportSession.query(db, "SELECT organization_id,manager_id,name,hosted_ref,attributes::text FROM harklight.groups WHERE id=$1", [id])
        assert org == group.organization_id and manager == group.manager and name == group.name
        assert hosted_ref == if(group.hosted_ref, do: elem(group.hosted_ref, 1), else: nil)
        assert_typed(text, group)
        assert {:ok, %{rows: members}} =
          ImportSession.query(db, "SELECT person_id,muted FROM harklight.group_members WHERE group_id=$1 ORDER BY person_id", [id])
        assert members == Enum.map(Enum.sort(group.members), fn {person, muted} -> [person, muted] end)
        assert {:ok, %{rows: invitations}} =
          ImportSession.query(db, "SELECT person_id,id,state FROM harklight.group_invitations WHERE group_id=$1 ORDER BY person_id", [id])
        assert invitations == Enum.map(Enum.sort(group.invitations), fn {person, invitation} -> [person, invitation, "pending"] end)
      end
      # Concrete nonempty fixtures ensure each normalized branch ran.
      assert {:ok, %{rows: [[1]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.groups WHERE manager_id IS NULL", [])
      assert {:ok, %{rows: [[2]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.group_members WHERE muted", [])
      assert {:ok, %{rows: [[1]]}} =
        ImportSession.query(db, "SELECT count(*) FROM harklight.group_invitations WHERE state='pending'", [])
    end

    defp assert_members(db, organization, expected) do
      assert {:ok, %{rows: members}} =
        ImportSession.query(db, "SELECT person_id FROM harklight.organization_members WHERE organization_id=$1 ORDER BY person_id", [organization])
      assert List.flatten(members) == Enum.sort(expected)
    end

    defp verify_feed_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyFeeds
      assert {:ok, counts} = verifier.run(db, identity, bundle)
      assert Enum.all?(counts, fn {_, count} -> count > 0 end)
      for table <- ~w(origin_feed_entries origin_delivery_acks advisory_feed_entries advisory_feed_receipts) do
        sql = if table == "advisory_feed_receipts", do: "DELETE FROM harklight." <> table,
          else: "UPDATE harklight." <> table <> " SET body='{}'::jsonb"
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, {:feed_state_mismatch, ^table, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx,
          "UPDATE harklight.origin_feed_entries SET session_handle='synthetic-wrong-session'", [])
        assert {:error, {:feed_state_mismatch, "origin_feed_entries", _}} = verifier.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
        assert {:ok, _} = ImportSession.query(tx,
          "INSERT INTO harklight.advisory_feed_receipts SELECT person_id,origin_key,session_handle,advisory_id || '-extra' FROM harklight.advisory_feed_receipts LIMIT 1", [])
        assert {:error, {:feed_count_mismatch, "advisory_feed_receipts"}} = verifier.run(tx, identity, bundle)
        Postgrex.rollback(tx, :synthetic_drift)
      end)
      assert {:ok, ^counts} = verifier.run(db, identity, bundle)
    end

    defp verify_account_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyAccounts
      assert {:ok, counts} = verifier.run(db, identity, bundle)
      assert counts.people >= 300 and counts.highs > counts.people
      for sql <- [
        "UPDATE harklight.people SET profile='{\"synthetic_drift\":true}'::jsonb",
        "UPDATE harklight.people SET settings='{\"synthetic_drift\":true}'::jsonb",
        "UPDATE harklight.people SET delivery_state='synthetic-changed'",
        "UPDATE harklight.allocation_highs SET high=high+1"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, {:account_state_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      for {sql, table} <- [
        {"INSERT INTO harklight.people(id) VALUES('synthetic-unexpected-person')", "people"},
        {"INSERT INTO harklight.allocation_highs(person_id,origin_key,session_handle,kind,high) SELECT id,'synthetic-extra-origin','','origin_feed',999 FROM harklight.people LIMIT 1", "allocation_highs"}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, _} = ImportSession.query(tx, sql, [])
          assert {:error, {:account_count_mismatch, ^table}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
    end

    defp verify_allocation_bounds(db, root) do
      alias AttentionServer.Migration.VerifyAllocationBounds
      assert {:ok, %{rows: [[person]]}} = ImportSession.query(db,
        "SELECT id FROM harklight.people ORDER BY id LIMIT 1", [])
      cases = [
        {"accepted", "", "", {:row, :persons, person},
         [{{:row, :messages, {person, "synthetic", "id"}}, %{accepted_seq: 2}}]},
        {"accepted", "", "", {:row, :persons, person},
         [{{:row, :events, {person, 2}}, %{}}]},
        {"free_order", "", "", {:row, :free_order_highs, person},
         [{{:row, :messages, {person, "synthetic", "id"}}, %{accepted_seq: 1, free_order: 2}}]},
        {"free_order", "", "", {:row, :free_order_highs, person},
         [{{:row, :conversation_events, {person, 1}}, %{free_order: 2}}]},
        {"conversation", "", "", {:row, :conversation_highs, person},
         [{{:row, :conversation_events, {person, 2}}, %{}}]},
        {"origin_feed", "synthetic-origin", "", {:origin_feed, {person, "synthetic-origin"}, :high},
         [{{:origin_feed, {person, "synthetic-origin"}, {:entry, 2}}, %{}}]},
        {"origin_feed", "synthetic-origin", "", {:origin_feed, {person, "synthetic-origin"}, :high},
         [{{:origin_feed, {person, "synthetic-origin"}, {:delivery_ack, 2}}, %{}}]},
        {"advisory_feed", "synthetic-origin", "synthetic-session",
         {:advisory, {person, "synthetic-origin", "synthetic-session"}, :high},
         [{{:advisory, {person, "synthetic-origin", "synthetic-session"}, {:entry, 2}}, %{}}]}
      ]
      for {kind, origin, handle, address, retained} <- cases do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          # Keep unrelated zero-bound allocators present; only the selected
          # counter is made inconsistent with its own retained value.
          for other <- ["accepted", "conversation"] do
            assert {:ok, _} = ImportSession.query(tx,
              "INSERT INTO harklight.allocation_highs VALUES($1,'','',$2,1000) ON CONFLICT(person_id,origin_key,session_handle,kind) DO UPDATE SET high=1000", [person, other])
          end
          params = [person, origin, handle, kind]
          assert {:ok, _} = ImportSession.query(tx,
            "INSERT INTO harklight.allocation_highs VALUES($1,$2,$3,$4,1) ON CONFLICT(person_id,origin_key,session_handle,kind) DO UPDATE SET high=1", params)
          # Real checksum-validated source frames also contain high=1: this is
          # not merely source/destination drift. Both copies agree but are low.
          file = Path.join(root, "low-high-#{System.unique_integer([:positive])}.frames")
          assert {:ok, low} = Frames.write(file, [{address, 1} | retained])
          assert {:error, {:allocation_bound_mismatch, ^kind, _}} = VerifyAllocationBounds.run(tx, file, low)
          File.rm!(file)
          assert {:ok, missing} = Frames.write(file, retained)
          assert {:ok, _} = ImportSession.query(tx,
            "DELETE FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND kind=$4", params)
          assert {:error, {:allocation_bound_mismatch, ^kind, _}} = VerifyAllocationBounds.run(tx, file, missing)
          File.rm!(file)
          assert {:ok, adequate} = Frames.write(file, [{address, 2} | retained])
          assert {:ok, _} = ImportSession.query(tx,
            "INSERT INTO harklight.allocation_highs VALUES($1,$2,$3,$4,2)", params)
          assert :ok = VerifyAllocationBounds.run(tx, file, adequate)
          File.rm!(file)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
      end
    end

    defp verify_authority_drift(db, identity, bundle) do
      verifier = AttentionServer.Migration.VerifyAuthority
      assert {:ok, counts} = verifier.run(db, identity, bundle)
      assert counts.principals > 0 and counts.grants > 0 and counts.sessions > 0
      for sql <- [
        "UPDATE harklight.principals SET revision=revision+1 WHERE revoked=true",
        "UPDATE harklight.principals SET revoked=false WHERE revoked=true",
        "UPDATE harklight.principals SET attributes='{}'::jsonb WHERE kind='agent'",
        "UPDATE harklight.sender_grants SET pattern=pattern || '/wrong'",
        "UPDATE harklight.origin_sessions SET attributes='{}'::jsonb"
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: n}} = ImportSession.query(tx, sql, [])
          assert n > 0
          assert {:error, {:authority_mismatch, _, _}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
      for {sql, field} <- [
        {"INSERT INTO harklight.principals(key,person_id,kind,revision) SELECT 'synthetic-extra-principal',id,'agent',1 FROM harklight.people LIMIT 1", :principals},
        {"INSERT INTO harklight.sender_grants(principal_key,pattern) SELECT key,'synthetic-extra-pattern' FROM harklight.principals LIMIT 1", :grants},
        {"INSERT INTO harklight.origin_sessions(person_id,origin_key,handle) SELECT person_id,key,'synthetic-extra-session' FROM harklight.principals LIMIT 1", :sessions}
      ] do
        assert {:error, :synthetic_drift} = Postgrex.transaction(db, fn tx ->
          assert {:ok, %{num_rows: 1}} = ImportSession.query(tx, sql, [])
          assert {:error, {:authority_count_mismatch, ^field}} = verifier.run(tx, identity, bundle)
          Postgrex.rollback(tx, :synthetic_drift)
        end)
        assert {:ok, ^counts} = verifier.run(db, identity, bundle)
      end
    end

    defp verify_account(db, {{:row, field, person}, high})
         when field in [:persons, :free_order_highs, :conversation_highs] do
      kind =
        %{persons: "accepted", free_order_highs: "free_order", conversation_highs: "conversation"}[
          field
        ]

      assert {:ok, %{rows: [[^high]]}} =
               ImportSession.query(
                 db,
                 "SELECT high FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key='' AND session_handle='' AND kind=$2",
                 [person, kind]
               )
    end

    defp verify_account(db, {{:row, :owners, person}, value}) do
      assert {:ok, %{rows: [[^person, "owner", revision, false, text]]}} =
               ImportSession.query(
                 db,
                 "SELECT person_id,kind,revision,revoked,attributes::text FROM harklight.principals WHERE key=$1",
                 [value.key]
               )

      assert revision == value.revision
      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, field, key}, value}) when field in [:devices, :agents] do
      person = value.person
      kind = if field == :devices, do: "device", else: "agent"

      assert {:ok, %{rows: [[^person, ^kind, revision, revoked, text]]}} =
               ImportSession.query(
                 db,
                 "SELECT person_id,kind,revision,revoked,attributes::text FROM harklight.principals WHERE key=$1",
                 [key]
               )

      assert revision == value.enrollment_revision and revoked == value.revoked
      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, :sender_grants, {_, key}}, patterns}) do
      assert {:ok, %{rows: rows}} =
               ImportSession.query(
                 db,
                 "SELECT pattern FROM harklight.sender_grants WHERE principal_key=$1 ORDER BY pattern",
                 [key]
               )

      assert List.flatten(rows) == Enum.sort(patterns)
    end

    defp verify_account(db, {{:row, :origin_sessions, {person, origin, handle}}, value}) do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(
                 db,
                 "SELECT attributes::text FROM harklight.origin_sessions WHERE person_id=$1 AND origin_key=$2 AND handle=$3",
                 [person, origin, handle]
               )

      assert_typed(text, value)
    end

    defp verify_account(db, {{:field, :relay_instance_id}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(db, "SELECT instance_id FROM harklight.server_identity", [])
    end

    defp verify_account(db, {{:field, :agent_cursor_key}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(
                 db,
                 "SELECT cursor_signing_key FROM harklight.server_identity",
                 []
               )
    end

    defp verify_account(db, {{:row, :people_profiles, person}, value}) do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(db, "SELECT profile::text FROM harklight.people WHERE id=$1", [
                 person
               ])

      assert_typed(text, value)
    end

    defp verify_account(db, {{:row, :delivery_states, person}, value}) do
      assert {:ok, %{rows: [[^value]]}} =
               ImportSession.query(
                 db,
                 "SELECT delivery_state FROM harklight.people WHERE id=$1",
                 [person]
               )
    end

    defp verify_account(db, {{:row, field, person}, value})
         when field in [:algorithm_choices, :alert_choices, :bounce_levels] do
      assert {:ok, %{rows: [[text]]}} =
               ImportSession.query(
                 db,
                 "SELECT settings::text FROM harklight.people WHERE id=$1",
                 [person]
               )

      assert {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, settings} = Attributes.decode(envelope)
      assert Map.fetch!(settings, field) == value
    end

    defp verify_account(_, _), do: :ok

    defp assert_typed(text, expected) do
      assert {:ok, envelope} = JSON.decode_line(text <> "\n")
      assert {:ok, ^expected} = Attributes.decode(envelope)
    end
  end
end

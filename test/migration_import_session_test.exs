if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.ImportSessionTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON}
    alias AttentionServer.Migration.{Bundle, Frames, ImportAccounts, ImportFacts, ImportSession}
    alias AttentionServer.Repository.Postgres.Attributes

    @tag timeout: 120_000
    test "empty admission, atomic interruption, resume and exact typed history readback" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)

      config = [
        hostname: uri.host,
        port: uri.port,
        username: URI.decode(user),
        password: URI.decode(password),
        database: String.trim_leading(uri.path, "/")
      ]

      {:ok, db} = Postgrex.start_link(config)
      on_exit(fn -> if Process.alive?(db), do: GenServer.stop(db) end)
      root = Path.join(System.tmp_dir!(), "import-history-#{System.unique_integer([:positive])}")
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

      # Synthetic setup: allocation high deliberately exceeds retained history.
      source_path = Path.join(fixture.root, "relay.sqlite3")
      {:ok, sqlite} = Exqlite.Sqlite3.open(source_path)

      {:ok, rows} =
        AttentionServer.Store.RecordTable.select(
          sqlite,
          "SELECT key,payload,checksum FROM relay_records"
        )

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

      :ok = Exqlite.Sqlite3.close(sqlite)
      bundle = Path.join(root, "export")

      assert {:ok, summary} =
               Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
                 supervisor: {:operator, "synthetic fixture", "seed Relay stopped"},
                 material: %{}
               )

      {:ok, manifest, _} = Bundle.verify(bundle)

      identity = %{
        id: "synthetic-import",
        source: Base.decode16!(summary.sha256, case: :lower),
        schema: :crypto.hash(:sha256, File.read!("priv/postgres/001_initial.sql"))
      }

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
      assert {:ok, %{ordinal: 256} = boundary} = ImportSession.checkpoint(db, identity, "records")

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

      {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.people WHERE id='committed'", [])
      assert :ok = ImportAccounts.run(db, identity, bundle)
      assert :ok = ImportAccounts.run(db, identity, bundle)

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

      assert {:ok, %{rows: [[1]]}} =
               ImportSession.query(db, "SELECT cursor_epoch FROM harklight.server_identity", [])

      Agent.stop(fixture.clock)
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

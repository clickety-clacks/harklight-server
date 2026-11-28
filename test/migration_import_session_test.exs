if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.ImportSessionTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON}
    alias AttentionServer.Migration.{Bundle, Frames, ImportFacts, ImportSession}
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

      Agent.stop(fixture.clock)
    end
  end
end

if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.LargeTextImportTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON, Relay}
    alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportSession}
    alias AttentionServer.Repository.Postgres.{LargeText, Schema}

    @tag timeout: 120_000
    test "real stopped large-text export resumes committed chunks and projects exact paths" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      db = start_supervised!({Postgrex, hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password), database: "migration_large_text"})
      root = Path.join(System.tmp_dir!(), "large-import-#{System.unique_integer([:positive])}")
      File.mkdir!(root)
      File.chmod!(root, 0o700)
      on_exit(fn -> File.rm_rf!(root) end)
      fixture = CompatibilityScenarios.fixture(Path.join(root, "source"))
      {:ok, relay} = CompatibilityScenarios.start(fixture)
      text = :binary.copy("x", LargeText.chunk_bytes() - 1) <> "💡" <>
        :binary.copy("y", LargeText.chunk_bytes()) <> "\"\\\n"
      assert byte_size(text) > 2 * 1024 * 1024
      try do
        %{person: person} = CompatibilityScenarios.provision(relay, fixture)
        origin = "synthetic-large-import-origin"
        assert :ok = Relay.setup_device(relay, person, origin)
        assert :ok = Relay.setup_sender(relay, origin, "app:large-import")
        for number <- [1, 2] do
          frame = %{"v" => 1, "type" => "forward",
            "op_id" => "73000000-0000-4000-8000-00000000000#{number}",
            "session_handle" => "73000000-0000-4000-8000-000000000010",
            "message" => %{"sender_id" => "app:large-import", "message_id" => "large-#{number}",
              "created_at" => 1, "title" => "Synthetic large history", "body" => text}}
          assert {:ok, reply} = Relay.request_line(relay, origin, JSON.encode!(frame) <> "\n")
          assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
        end
      after
        GenServer.stop(relay)
      end
      Agent.stop(fixture.clock)
      bundle = Path.join(root, "bundle")
      assert {:ok, summary} = Bundle.export(Path.join(fixture.root, "relay.sqlite3"), bundle,
        supervisor: {:operator, "synthetic fixture", "Relay stopped"}, material: %{})
      assert {:ok, manifest, ^summary} = Bundle.verify(bundle)
      identity = %{id: "synthetic-large-import", source: Base.decode16!(summary.sha256, case: :lower), schema: Schema.digest()}
      assert {:error, :import_source_mismatch} = ImportLargeText.run(db,
        %{identity | source: :crypto.hash(:sha256, "different bundle")}, bundle)
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.interrupt_large_import() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.chunk_number = 1 THEN RAISE EXCEPTION 'synthetic interruption'; END IF; RETURN NEW; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER interrupt_large_import BEFORE INSERT ON harklight.large_text_chunks FOR EACH ROW EXECUTE FUNCTION harklight.interrupt_large_import()", [])
      assert {:error, _} = ImportLargeText.run(db, identity, bundle)
      assert {:ok, %{rows: [[1]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.large_text_chunks", [])
      assert {:ok, %{rows: [[false]]}} = ImportSession.query(db, "SELECT verified FROM harklight.large_texts", [])
      hash = Base.encode16(:crypto.hash(:sha256, text), case: :lower)
      family = "large-text-bytes/" <> hash <> "/v1"
      assert {:ok, checkpoint} = ImportSession.checkpoint(db, identity, family)
      assert checkpoint.ordinal == 1
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER interrupt_large_import ON harklight.large_text_chunks", [])
      # A resume must validate the already committed source prefix.
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.import_checkpoints SET cumulative_sha256=$2 WHERE family=$1", [family, :crypto.hash(:sha256, "wrong prefix")])
      assert {:error, :checkpoint_source_mismatch} = ImportLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.import_checkpoints SET cumulative_sha256=$2 WHERE family=$1", [family, checkpoint.digest])
      assert {:ok, count} = ImportLargeText.run(db, identity, bundle)
      assert count >= 4
      assert {:ok, ^count} = ImportLargeText.run(db, identity, bundle)
      assert {:ok, %{rows: [[1, true]]}} = ImportSession.query(db,
        "SELECT count(*),bool_and(verified) FROM harklight.large_texts", [])
      assert {:ok, %{rows: [[3]]}} = ImportSession.query(db, "SELECT count(*) FROM harklight.large_text_chunks", [])
      query = &ImportSession.query(db, &1, &2)
      assert {:ok, 2} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
        {{:row, :messages, _} = address, record}, found ->
          assert {:ok, projected} = ImportLargeText.project(db, address, record)
          assert %LargeText.Ref{} = ref = projected.message["body"]
          bytes = Enum.map_join(0..2, fn ordinal ->
            assert {:ok, chunk} = LargeText.read_chunk(ref, ordinal, query)
            chunk
          end)
          assert bytes == text
          assert {:ok, ^record} = ImportLargeText.project(db, {:row, :messages, "unmapped literal"}, record)
          assert {:error, :large_text_projection_mismatch} = ImportLargeText.project(db, address,
            %{record | message: Map.put(record.message, "body", "different source value")})
          {:ok, found + 1}
        _, found -> {:ok, found}
      end)
      file = Path.join(bundle, "files/uploads/" <> hash)
      File.rename!(file, file <> ".saved")
      assert {:error, _} = ImportLargeText.run(db, identity, bundle)
      File.write!(file, :binary.copy("z", byte_size(text)))
      assert {:error, _} = ImportLargeText.run(db, identity, bundle)
      File.rm!(file)
      File.rename!(file <> ".saved", file)
      assert {:ok, ^count} = ImportLargeText.run(db, identity, bundle)
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db, "SELECT phase FROM harklight.import_state", [])
    end
  end
end

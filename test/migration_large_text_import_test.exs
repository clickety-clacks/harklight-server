if System.get_env("HARKLIGHT_MIGRATION_TEST_URL") do
  Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

  defmodule AttentionServer.Migration.LargeTextImportTest do
    use ExUnit.Case, async: false
    alias AttentionServer.{CompatibilityScenarios, JSON, Relay}
    alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportMessages, ImportSession, ImportTables, VerifyLargeText}
    alias AttentionServer.Repository.Postgres.{Items, LargeText, Repo, Schema}

    @tag timeout: 120_000
    test "real stopped large-text export resumes committed chunks and projects exact paths" do
      uri = URI.parse(System.fetch_env!("HARKLIGHT_MIGRATION_TEST_URL"))
      assert uri.host in ["localhost", "127.0.0.1"]
      [user, password] = String.split(uri.userinfo, ":", parts: 2)
      config = [hostname: uri.host, port: uri.port,
        username: URI.decode(user), password: URI.decode(password), database: "migration_large_text"]
      db = start_supervised!({Postgrex, config})
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
      # A committed ordinary phase from an earlier importer must resume into
      # the new reference phase, without resetting its source checkpoint.
      assert {:ok, _} = ImportSession.query(db, """
        CREATE FUNCTION harklight.interrupt_text_projection() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'synthetic projection interruption'; END $$
        """, [])
      assert {:ok, _} = ImportSession.query(db,
        "CREATE TRIGGER interrupt_text_projection BEFORE UPDATE ON harklight.message_revisions FOR EACH ROW EXECUTE FUNCTION harklight.interrupt_text_projection()", [])
      assert {:error, _} = ImportMessages.run(db, identity, bundle)
      assert {:ok, %{ordinal: 2}} = ImportSession.checkpoint(db, identity, "messages/ordinary/v1")
      assert {:ok, %{ordinal: 0}} = ImportSession.checkpoint(db, identity, "messages/large-text/v1")
      assert {:ok, _} = ImportSession.query(db, "DROP TRIGGER interrupt_text_projection ON harklight.message_revisions", [])
      assert :ok = ImportMessages.run(db, identity, bundle)
      assert :ok = ImportMessages.run(db, identity, bundle)
      assert {:ok, %{ordinal: 2}} = ImportSession.checkpoint(db, identity, "messages/large-text/v1")
      start_supervised!({Repo, config})
      query = &ImportSession.query(db, &1, &2)
      assert {:ok, 2} = Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
        {{:row, :messages, _} = address, record}, found ->
          assert {:ok, projected} = ImportLargeText.project(db, address, record)
          assert %LargeText.Ref{} = ref = projected.message["body"]
          {:row, :messages, {person, _, _} = key} = address
          item = AttentionServer.ClientAPI.Resources.item_id(record)
          assert {:ok, ^key, ^projected} = Items.recipient_item(person, item)
          assert {:error, :not_found} = Items.recipient_item("different-person", item)
          assert {:error, :not_found} = Items.agent_item({person, "different-origin"}, item)
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
      stop_supervised!(Repo)
      assert :ok = ImportTables.run(db, identity, bundle)
      verify_published_text(db, identity, bundle, count, hash)
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
    defp verify_published_text(db, identity, bundle, count, hex) do
      hash = Base.decode16!(hex, case: :lower)
      assert {:ok, %{references: ^count}} = VerifyLargeText.run(db, identity, bundle)
      extra_hash = :crypto.hash(:sha256, "")
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.large_texts(sha256,size_bytes,verified) VALUES($1,0,true)", [extra_hash])
      assert {:error, :large_text_hash_count_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db, "DELETE FROM harklight.large_texts WHERE sha256=$1", [extra_hash])
      assert {:error, :import_source_mismatch} = VerifyLargeText.run(db,
        %{identity | source: :crypto.hash(:sha256, "foreign source")}, bundle)
      assert {:ok, %{rows: checkpoints}} = ImportSession.query(db,
        "SELECT family,last_ordinal,cumulative_sha256 FROM harklight.import_checkpoints ORDER BY family", [])
      assert {:ok, %{rows: [[original]]}} = ImportSession.query(db,
        "SELECT bytes FROM harklight.large_text_chunks WHERE sha256=$1 AND chunk_number=0", [hash])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.large_text_chunks SET bytes=$2 WHERE sha256=$1 AND chunk_number=0", [hash, "changed"])
      assert {:error, :large_text_chunk_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.large_text_chunks WHERE sha256=$1 AND chunk_number=0", [hash])
      assert {:error, :large_text_chunk_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.large_text_chunks(sha256,chunk_number,bytes) VALUES($1,0,$2)", [hash, original])
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.large_text_chunks(sha256,chunk_number,bytes) VALUES($1,99,$2)", [hash, "extra"])
      assert {:error, :large_text_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.large_text_chunks WHERE sha256=$1 AND chunk_number=99", [hash])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.large_texts SET verified=false WHERE sha256=$1", [hash])
      assert {:error, :large_text_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.large_texts SET verified=true WHERE sha256=$1", [hash])
      assert {:ok, %{rows: [[family, address, body]]}} = ImportSession.query(db,
        "SELECT family,address,body FROM harklight.compatibility_facts WHERE family LIKE 'large-text-paths/%' ORDER BY family,address LIMIT 1", [])
      assert {:ok, _} = ImportSession.query(db,
        "UPDATE harklight.compatibility_facts SET body='{}'::jsonb WHERE family=$1 AND address=$2", [family, address])
      assert {:error, :large_text_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, address])
      assert {:error, :large_text_source_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)", [family, address, JSON.encode!(body)])
      assert {:ok, _} = ImportSession.query(db,
        "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES('large-text-paths/extra',$1,$2::text::jsonb)", [address, JSON.encode!(body)])
      assert {:error, :large_text_path_count_mismatch} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, _} = ImportSession.query(db,
        "DELETE FROM harklight.compatibility_facts WHERE family='large-text-paths/extra'", [])
      assert {:ok, %{references: ^count}} = VerifyLargeText.run(db, identity, bundle)
      assert {:ok, %{rows: ^checkpoints}} = ImportSession.query(db,
        "SELECT family,last_ordinal,cumulative_sha256 FROM harklight.import_checkpoints ORDER BY family", [])
      assert {:ok, %{rows: [["loading"]]}} = ImportSession.query(db,
        "SELECT phase FROM harklight.import_state", [])
    end
  end
end

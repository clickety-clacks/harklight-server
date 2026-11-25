defmodule AttentionServer.Store.KeyedRetainedUploadsTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, Relay, Store}
  alias AttentionServer.Store.KeyedMigration
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @handle "00000000-0000-4000-8000-000000000010"
  @discussion "00000000-0000-4000-8000-000000000011"
  @forward "00000000-0000-4000-8000-000000000012"
  @bytes <<0, 17, 250, 99>>

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)

    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "B", "reader-b")

    issue(relay, "origin", %{
      "type" => "forward",
      "op_id" => @forward,
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item",
        "created_at" => 1,
        "title" => "Fixture"
      }
    })

    attributes = %{
      "op_id" => @discussion,
      "sender_id" => "app:test",
      "message_id" => "item",
      "revision" => 1,
      "name" => "payload.bin"
    }

    source = Path.join(directory, "source.bin")
    File.write!(source, @bytes)
    size = byte_size(@bytes)
    digest = Base.encode16(:crypto.hash(:sha256, @bytes), case: :lower)
    assert {:ok, lease} = Relay.upload_begin(relay, "reader", attributes, size)

    assert {:ok, %{"upload_id" => id}} =
             Relay.upload_file(relay, "reader", attributes, source, size, digest, lease)

    assert :ok = Relay.upload_end(relay, lease)

    issue(relay, "reader", %{
      "type" => "discussion",
      "op_id" => @discussion,
      "sender_id" => "app:test",
      "message_id" => "item",
      "request_revision" => 1,
      "text" => "Attached",
      "files" => [
        %{"upload_id" => id, "name" => "payload.bin", "sha256" => digest, "size" => size}
      ]
    })

    assert {:ok, %{person: "A"}, file} = Relay.fetch_file(relay, "reader", id)
    assert {:error, :not_found} = Relay.fetch_file(relay, "reader-b", id)
    assert File.read!(file) == @bytes
    assert :ok = GenServer.stop(relay)

    %{path: path, upload_path: file, id: id}
  end

  test "missing retained bytes refuse conversion and permit retry after repair", %{
    path: path,
    upload_path: file
  } do
    assert {:ok, legacy} = Store.open(path)
    File.rm!(file)
    before = legacy_anchor(path)

    assert {:error, :retained_upload_unavailable} = convert_open_store(legacy)
    assert :ok = Store.close(legacy)
    assert legacy_anchor(path) == before
    assert_unconverted(path)

    File.write!(file, @bytes)
    assert {:ok, version} = convert(path)
    assert version == hd(before) + 1
  end

  test "same-size corrupt retained bytes refuse conversion", %{path: path, upload_path: file} do
    assert {:ok, legacy} = Store.open(path)
    File.write!(file, <<0, 17, 250, 98>>)
    before = legacy_anchor(path)

    assert {:error, :retained_upload_unavailable} = convert_open_store(legacy)
    assert :ok = Store.close(legacy)
    assert legacy_anchor(path) == before
    assert_unconverted(path)
  end

  test "missing retained upload metadata refuses before conversion and leaves bytes available", %{
    path: path,
    upload_path: file,
    id: id
  } do
    remove_upload_metadata(path, id)
    before = legacy_anchor(path)

    assert {:error, {:storage_unavailable, {:error, :invalid_state}}} = convert(path)
    assert legacy_anchor(path) == before
    assert_unconverted(path)
    assert File.read!(file) == @bytes
  end

  test "healthy conversion retains upload identity and person scope", %{
    path: path,
    upload_path: file,
    id: id
  } do
    assert {:ok, version} = convert(path)

    with_db(path, fn db ->
      assert {:ok, engine, ^version} = KeyedMigration.load(db)
      assert %{person: "A", id: ^id} = upload = engine.uploads[id]
      assert :ok = Store.verify_upload(file, upload)
      assert File.read!(file) == @bytes

      assert %{"files" => [%{"upload_id" => ^id}]} =
               engine.origin_feeds[{"A", "origin"}].entries[2]

      assert engine.origin_feeds[{"B", "reader-b"}] == nil
    end)
  end

  defp issue(relay, actor, frame) do
    assert {:ok, reply} =
             Relay.request_line(relay, actor, JSON.encode_line(Map.put(frame, "v", 1)))

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
  end

  defp convert(path) do
    with {:ok, legacy} <- Store.open(path) do
      try do
        convert_open_store(legacy)
      after
        Store.close(legacy)
      end
    end
  end

  defp convert_open_store(legacy) do
    KeyedMigration.convert_store(legacy, fn _, _, generation -> {:ok, generation} end)
  end

  defp legacy_anchor(path) do
    with_db(path, fn db ->
      assert [row] = query(db, "SELECT version, payload, checksum FROM relay_state WHERE id = 1")
      row
    end)
  end

  defp assert_unconverted(path) do
    with_db(path, fn db ->
      assert [] = query(db, "SELECT name FROM sqlite_schema WHERE name LIKE 'relay_record%'")
    end)
  end

  defp remove_upload_metadata(path, id) do
    with_db(path, fn db ->
      assert [[payload]] = query(db, "SELECT payload FROM relay_state WHERE id = 1")
      {:attention_relay_state, 30, engine} = :erlang.binary_to_term(payload, [:safe])
      engine = %{engine | uploads: Map.delete(engine.uploads, id)}
      next = :erlang.term_to_binary({:attention_relay_state, 30, engine})
      assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")

      try do
        assert :ok = Sqlite3.bind(stmt, [{:blob, next}, {:blob, :crypto.hash(:sha256, next)}])
        assert :done = Sqlite3.step(db, stmt)
      after
        :ok = Sqlite3.release(db, stmt)
      end
    end)
  end

  defp query(db, sql) do
    assert {:ok, stmt} = Sqlite3.prepare(db, sql)

    try do
      assert {:ok, rows} = Sqlite3.fetch_all(db, stmt)
      rows
    after
      :ok = Sqlite3.release(db, stmt)
    end
  end

  defp with_db(path, fun) do
    assert {:ok, db} = Sqlite3.open(path)

    try do
      fun.(db)
    after
      :ok = Sqlite3.close(db)
    end
  end
end

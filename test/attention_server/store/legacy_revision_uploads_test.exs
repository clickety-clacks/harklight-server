defmodule AttentionServer.Store.LegacyRevisionUploadsTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, Relay, Store}
  alias AttentionServer.Store.KeyedMigration
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @handle "00000000-0000-4000-8000-000000000010"
  @op "00000000-0000-4000-8000-000000000011"
  @orphan "00000000-0000-4000-8000-000000000012"
  @bytes <<0, 17, 250, 99>>

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    source = Path.join(directory, "source.bin")
    File.write!(source, @bytes)
    size = byte_size(@bytes)
    digest = Base.encode16(:crypto.hash(:sha256, @bytes), case: :lower)

    attributes = %{
      "op_id" => @op,
      "sender_id" => "app:test",
      "message_id" => "item",
      "revision" => 1,
      "name" => "payload.bin"
    }

    assert {:ok, lease} = Relay.upload_begin(relay, "origin", attributes, size)

    assert {:ok, %{"upload_id" => id}} =
             Relay.upload_file(relay, "origin", attributes, source, size, digest, lease)

    assert :ok = Relay.upload_end(relay, lease)

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => @op,
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item",
        "created_at" => 1,
        "title" => "Fixture"
      },
      "files" => [
        %{"upload_id" => id, "name" => "payload.bin", "sha256" => digest, "size" => size}
      ]
    }

    assert {:ok, response} = Relay.request_line(relay, "origin", JSON.encode_line(frame))
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)
    assert {:ok, %{id: ^id}, file_path} = Relay.fetch_file(relay, "reader", id)
    assert File.read!(file_path) == @bytes

    discard = Map.take(attributes, ~w(op_id sender_id message_id revision))

    assert {:ok, %{"state" => "committed", "discarded" => 0}} =
             Relay.discard_uploads(relay, "origin", discard)

    assert :ok = GenServer.stop(relay)
    %{path: path, file_path: file_path, id: id}
  end

  test "a legacy writer cannot retire metadata still owned by a revision", %{
    path: path,
    file_path: file_path,
    id: id
  } do
    store = open_revision_only(path)
    result = Store.delete_uploads(store, [id], audit())

    assert match?({:error, :storage_unavailable, ^store}, result)
    assert File.read!(file_path) == @bytes
    assert :ok = Store.close(store)
  end

  test "legacy reopen refuses an unregistered revision attachment without deleting the file", %{
    path: path,
    file_path: file_path,
    id: id
  } do
    rewrite_snapshot(path, fn engine ->
      engine = revision_only(engine)
      %{engine | uploads: Map.delete(engine.uploads, id)}
    end)

    result = open_result(path)

    assert {result, File.exists?(file_path)} ==
             {{:error, {:storage_unavailable, {:error, :invalid_state}}}, true}

    assert File.read!(file_path) == @bytes
  end

  test "missing retained bytes prevent ordinary orphan cleanup and conversion", %{
    path: path,
    file_path: file_path
  } do
    store = open_revision_only(path)
    assert :ok = Store.close(store)
    File.rm!(file_path)
    orphan = Path.join(Path.dirname(file_path), @orphan)
    File.write!(orphan, "orphan")

    assert {:ok, store} = Store.open(path)
    assert File.exists?(orphan)

    assert KeyedMigration.convert_store(store, fn _, _, generation -> {:ok, generation} end) ==
             {:error, :retained_upload_unavailable}

    assert {:error, :not_keyed} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)

    File.write!(file_path, @bytes)
    assert :ok = open_result(path)
    refute File.exists?(orphan)
  end

  test "conversion verifies same-size changes in a revision-only upload", %{
    path: path,
    file_path: file_path
  } do
    store = open_revision_only(path)
    File.write!(file_path, <<0, 17, 250, 98>>)

    result = KeyedMigration.convert_store(store, fn _, _, generation -> {:ok, generation} end)

    assert result == {:error, :retained_upload_unavailable}
    assert {:error, :not_keyed} = KeyedMigration.load(store.db)
    assert :ok = Store.close(store)
  end

  defp open_revision_only(path) do
    assert {:ok, store} = Store.open(path)
    assert {:ok, store} = Store.commit(store, revision_only(store.engine), audit())
    store
  end

  defp revision_only(engine) do
    update_in(engine.origin_feeds[{"A", "origin"}].entries[1], &Map.delete(&1, "files"))
  end

  defp audit, do: %{person: "A", actor: "origin", action: "retain_revision_upload"}

  defp open_result(path) do
    case Store.open(path) do
      {:ok, store} -> Store.close(store)
      error -> error
    end
  end

  defp rewrite_snapshot(path, change) do
    assert {:ok, db} = Sqlite3.open(path)

    try do
      assert {:ok, stmt} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
      assert {:ok, [[payload]]} = Sqlite3.fetch_all(db, stmt)
      assert :ok = Sqlite3.release(db, stmt)
      {:attention_relay_state, 29, engine} = :erlang.binary_to_term(payload, [:safe])
      data = :erlang.term_to_binary({:attention_relay_state, 29, change.(engine)})
      assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
      assert :ok = Sqlite3.bind(stmt, [{:blob, data}, {:blob, :crypto.hash(:sha256, data)}])
      assert :done = Sqlite3.step(db, stmt)
      assert :ok = Sqlite3.release(db, stmt)
    after
      assert :ok = Sqlite3.close(db)
    end
  end
end

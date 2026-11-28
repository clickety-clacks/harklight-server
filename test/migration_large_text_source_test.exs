defmodule AttentionServer.Migration.LargeTextSourceTest do
  use ExUnit.Case, async: true
  alias AttentionServer.{Engine, LargeData}
  alias AttentionServer.Migration.LargeTextSource
  alias AttentionServer.Store.{RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  test "large ordinary, shared and feed fields require exact path, marker and exported bytes" do
    root = Path.join(System.tmp_dir!(), "large-source-#{System.unique_integer([:positive])}")
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    on_exit(fn -> File.rm_rf!(root) end)
    bytes = :binary.copy("synthetic large text ", 130_000)
    assert byte_size(bytes) > LargeData.inline_limit()
    source = %Engine{
      messages: %{{"person", "app:test", "message"} => %{
        message: %{"body" => bytes}, revisions: %{1 => %{"body" => bytes}}}},
      shared_messages: %{"shared" => %{message: %{"body" => bytes}}},
      origin_feeds: %{{"person", "origin"} => %{high: 1, entries: %{1 => %{"body" => bytes}}}},
      advisory_feeds: %{{"person", "origin", "session"} => %{high: 1,
        entries: %{1 => %{"body" => bytes}}, receipts: MapSet.new()}}
    }
    {:ok, durable, _} = LargeData.externalize(source, root)
    rows = LargeData.persisted_references(durable)
    assert length(rows) == 5
    {:ok, db} = Sqlite3.open(Path.join(root, "copy.sqlite3"))
    try do
      assert :ok = RecordTable.seed(db, 0, RecordCodec.encode(durable, &LargeData.persist_refs/1))
      for row <- rows do
        assert {:ok, ref} = LargeTextSource.reference(db, row, root)
        assert ref.size == byte_size(bytes)
        assert ref.sha256 == Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
        assert File.read!(ref.file) == bytes
      end
      [path, marker, id, size, hash] = hd(rows)
      assert {:error, :invalid_large_text_source_reference} =
        LargeTextSource.reference(db, [path, marker <> "changed", id, size, hash], root)
      missing = :erlang.term_to_binary([{:map, :messages}, {:map, {"person", "app:test", "missing"}}, {:map, :message}, {:map, "body"}])
      assert {:error, :large_text_source_reference_mismatch} =
        LargeTextSource.reference(db, [missing, marker, id, size, hash], root)
      # Literal marker text with no matching reference row is never scanned or
      # rewritten. A claimed row pointing at different text is refused.
      address = {:row, :messages, {"person", "app:test", "literal"}}
      {key, payload, checksum} = RecordCodec.encode_row(address, %{message: %{"body" => marker <> " literal"}})
      assert :ok = RecordTable.run(db,
        "INSERT INTO relay_records(key,payload,checksum,account) VALUES(?,?,?,0)",
        [{:blob, key}, {:blob, payload}, {:blob, checksum}])
      literal = :erlang.term_to_binary([{:map, :messages}, {:map, {"person", "app:test", "literal"}}, {:map, :message}, {:map, "body"}])
      assert {:error, :large_text_source_reference_mismatch} =
        LargeTextSource.reference(db, [literal, marker, id, size, hash], root)
      assert {:ok, _} = LargeTextSource.reference(db, hd(rows), root)
      File.write!(Path.join(root, id), :binary.copy("x", byte_size(bytes)))
      assert {:error, :storage_unavailable} = LargeTextSource.reference(db, hd(rows), root)
      File.rm!(Path.join(root, id))
      assert {:error, :storage_unavailable} = LargeTextSource.reference(db, hd(rows), root)
    after
      Sqlite3.close(db)
    end
  end
end

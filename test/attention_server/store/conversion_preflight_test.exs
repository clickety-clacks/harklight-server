defmodule AttentionServer.Store.ConversionPreflightTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Store.ConversionPreflight
  alias Exqlite.Sqlite3

  test "checks the SQLite payload length before reading its prefix" do
    {:ok, db} = Sqlite3.open(":memory:")
    on_exit(fn -> Sqlite3.close(db) end)
    :ok = Sqlite3.execute(db, "CREATE TABLE relay_state (id INTEGER PRIMARY KEY, payload BLOB)")
    :ok = Sqlite3.execute(db, "INSERT INTO relay_state VALUES (1, zeroblob(8388608))")

    assert {:error, :fit_not_established} =
             ConversionPreflight.read(db, 8 * 1024 * 1024, 128 * 1024 * 1024)

    assert {:error, :fit_not_established} =
             ConversionPreflight.read(db, 128 * 1024 * 1024, 8 * 1024 * 1024)

    assert {:error, :unsupported_snapshot} =
             ConversionPreflight.read(db, 128 * 1024 * 1024, 128 * 1024 * 1024)

    payload = :erlang.term_to_binary({:attention_relay_state, 30, %{}}, compressed: 6)
    {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ? WHERE id = 1")
    :ok = Sqlite3.bind(stmt, [{:blob, payload}])
    :done = Sqlite3.step(db, stmt)
    :ok = Sqlite3.release(db, stmt)

    assert {:ok, %{format: 30, stored_bytes: length}} =
             ConversionPreflight.read(db, 128 * 1024 * 1024, 128 * 1024 * 1024)

    assert length == byte_size(payload)
  end

  test "reports the compressed snapshot length and expanded length from six bytes" do
    # A sentinel body ensures this test never passes by inflating the snapshot.
    payload = <<131, 80, 100_000_000::32, 0, 0, 0>>

    assert {:ok, %{stored_bytes: 9, expanded_bytes: 100_000_000}} =
             ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
  end

  test "uses the length of an uncompressed snapshot" do
    payload = :erlang.term_to_binary({:attention_relay_state, 30, %{}})
    length = byte_size(payload)

    assert {:ok, %{stored_bytes: ^length, expanded_bytes: ^length}} =
             ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
  end

  test "refuses an unreadable or incomplete ETF header" do
    for payload <- [<<>>, <<130, 104, 3>>, <<131, 80, 1>>, <<131, 80, 0::32>>] do
      assert {:error, :fit_not_established} =
               ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
    end
  end

  test "reads the revision from the snapshot's own compressed or uncompressed prefix" do
    for format <- [27, 29, 30], flags <- [[], [{:compressed, 6}]] do
      term = {:attention_relay_state, format, %{history: :binary.copy("synthetic", 10_000)}}
      payload = :erlang.term_to_binary(term, flags)
      assert {:ok, ^format} = ConversionPreflight.snapshot_format(payload)
    end
  end

  test "an envelope can refuse a format mismatch, an over-budget decode, or an out-of-shape store" do
    {:ok, db} = Sqlite3.open(":memory:")
    on_exit(fn -> Sqlite3.close(db) end)
    :ok = Sqlite3.execute(db, "CREATE TABLE relay_state (id INTEGER PRIMARY KEY, payload BLOB)")
    :ok = Sqlite3.execute(db, "CREATE TABLE agent_changes (id INTEGER PRIMARY KEY)")

    payload = :erlang.term_to_binary({:attention_relay_state, 30, %{body: :binary.copy("synthetic", 20_000)}}, compressed: 6)
    {:ok, stmt} = Sqlite3.prepare(db, "INSERT INTO relay_state VALUES (1, ?)")
    :ok = Sqlite3.bind(stmt, [{:blob, payload}])
    :done = Sqlite3.step(db, stmt)
    :ok = Sqlite3.release(db, stmt)

    mib = 1024 * 1024
    budget = %{memory: 128 * mib, disk: 128 * mib, uploads: 0}

    envelope = %{
      expanded_bytes: mib,
      memory_fixed: 64 * mib, memory_per_byte: 32,
      disk_fixed: 64 * mib, disk_per_byte: 3,
      revisions: 0, changes: 0, references: 0, uploads: 0
    }

    assert {:ok, %{format: 30}} = ConversionPreflight.check(db, budget, 30, %{30 => envelope})

    assert {:error, :source_format_mismatch} =
             ConversionPreflight.check(db, budget, 29, %{30 => envelope})

    assert {:error, :fit_not_established} =
             ConversionPreflight.check(db, %{budget | memory: 48 * mib}, 30, %{30 => envelope})

    :ok = Sqlite3.execute(db, "INSERT INTO agent_changes VALUES (1)")

    assert {:error, :fit_not_established} = ConversionPreflight.check(db, budget, 30, %{30 => envelope})
  end

  test "accepts the older atom tag encodings without loading an Engine" do
    tag = "attention_relay_state"
    size = byte_size(tag)

    for atom <- [
          <<100, size::16, tag::binary>>,
          <<118, size::16, tag::binary>>,
          <<119, size, tag::binary>>
        ] do
      payload = <<131, 104, 3, atom::binary, 97, 30, 116, 0::32>>
      assert {:ok, 30} = ConversionPreflight.snapshot_format(payload)
    end
  end

  test "skips a store that already has a FORMAT31 row anchor" do
    payload = :erlang.term_to_binary({:attention_relay_keyed_state, 31, {:fixture, 27}})

    assert {:already_converted, 31} = ConversionPreflight.snapshot_format(payload)
  end

  test "refuses a wrong anchor, unsupported revision, and truncated compressed prefix" do
    for payload <- [
          :erlang.term_to_binary({:attention_relay_keyed_state, 30, %{}}),
          :erlang.term_to_binary({:attention_relay_state, 28, %{}}),
          <<131, 80, 100_000_000::32, 0, 0, 0>>
        ] do
      assert {:error, :fit_not_established} = ConversionPreflight.snapshot_format(payload)
    end
  end
end

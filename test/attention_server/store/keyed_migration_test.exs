defmodule AttentionServer.Store.KeyedMigrationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Store
  alias AttentionServer.Store.{KeyedMigration, RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  setup do
    directory =
      Path.join(System.tmp_dir!(), "keyed-migration-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    path = Path.join(directory, "relay.sqlite3")
    assert {:ok, store} = Store.open(path)
    on_exit(fn -> Store.close(store) end)
    %{path: path, store: store}
  end

  test "writes a versioned anchor and reads the converted state after reopen", ctx do
    %{path: path, store: store} = ctx

    assert {:ok, generation} = KeyedMigration.convert(store)
    assert generation == store.version + 1
    assert :ok = Store.close(store)
    assert {:error, _reason} = Store.open(path)
    assert {:ok, db} = Sqlite3.open(path)

    try do
      assert {:ok, engine, ^generation} = KeyedMigration.load(db)
      assert engine == store.engine

      row = RecordCodec.encode_row({:field, :pending_clock_high_water}, 10)
      assert {:ok, nil} = RecordTable.commit(db, generation, [{:put, row}], fn -> {:ok, nil} end)
      assert {:ok, engine, advanced} = KeyedMigration.load(db)
      assert advanced == generation + 1
      assert engine.pending_clock_high_water == 10
    after
      Sqlite3.close(db)
    end
  end

  test "an anchor write failure leaves the legacy snapshot readable and no keyed schema", ctx do
    %{path: path, store: store} = ctx

    assert :ok =
             Sqlite3.execute(store.db, """
             CREATE TRIGGER fail_anchor BEFORE UPDATE ON relay_state
             BEGIN SELECT RAISE(ABORT, 'write failed'); END
             """)

    assert {:error, _} = KeyedMigration.convert(store)
    assert {:error, :not_keyed} = KeyedMigration.load(store.db)
    assert {:ok, legacy} = Store.open(path)
    assert legacy.version == store.version
    assert legacy.engine == store.engine
    assert :ok = Store.close(legacy)
  end
end

defmodule AttentionServer.Migration.Format32Test do
  use ExUnit.Case, async: true
  alias AttentionServer.{Store, Migration.Format32}
  alias Exqlite.Sqlite3

  setup do
    root =
      Path.join(System.tmp_dir!(), "migration-inventory-#{System.unique_integer([:positive])}")

    File.mkdir!(root)
    File.chmod!(root, 0o700)
    path = Path.join(root, "source.sqlite3")
    {:ok, store} = Store.open_relay(path)
    Store.close(store)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, path: path}
  end

  test "inventories a disposable copy and leaves the source and scratch unchanged", %{
    root: root,
    path: path
  } do
    before = File.read!(path)
    entries = File.ls!(root) |> Enum.sort()
    assert {:ok, manifest} = Format32.preflight(path, stopped: true, scratch_root: root)
    assert manifest.anchor.format == 32
    assert manifest.records.count > 0
    assert manifest.tables["relay_records"].count == manifest.records.count
    assert manifest.source_sha256 == Base.encode16(:crypto.hash(:sha256, before), case: :lower)
    assert File.read!(path) == before
    assert Enum.sort(File.ls!(root)) == entries
    assert {:ok, ^manifest} = Format32.preflight(path, stopped: true, scratch_root: root)
  end

  test "requires stopped-backup acknowledgement and checkpoint", %{root: root, path: path} do
    assert {:error, :stopped_backup_required} = Format32.preflight(path, scratch_root: root)
    File.write!(path <> "-wal", "unfinished")

    assert {:error, :store_not_checkpointed} =
             Format32.preflight(path, stopped: true, scratch_root: root)
  end

  test "refuses unknown durable tables and columns", %{root: root, path: path} do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, "CREATE TABLE future_state (id TEXT)")
    Sqlite3.close(db)

    assert {:error, :unknown_source_table} =
             Format32.preflight(path, stopped: true, scratch_root: root)

    {:ok, db} = Sqlite3.open(path)

    :ok =
      Sqlite3.execute(
        db,
        "DROP TABLE future_state; ALTER TABLE relay_state ADD COLUMN future TEXT"
      )

    Sqlite3.close(db)

    assert {:error, {:unsupported_source_columns, "relay_state"}} =
             Format32.preflight(path, stopped: true, scratch_root: root)
  end

  test "refuses damaged row checksums and never bootstraps a missing source", %{
    root: root,
    path: path
  } do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, "UPDATE relay_records SET checksum = x'00'")
    Sqlite3.close(db)

    assert {:error, :invalid_source_record} =
             Format32.preflight(path, stopped: true, scratch_root: root)

    absent = Path.join(root, "absent.sqlite3")

    assert {:error, :invalid_source_file} =
             Format32.preflight(absent, stopped: true, scratch_root: root)

    refute File.exists?(absent)
  end
end

Code.require_file("../support/migration_fixtures.ex", __DIR__)

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

    assert {:ok, manifest} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )

    assert manifest.anchor.format == 32
    assert manifest.records.count > 0
    assert manifest.tables["relay_records"].count == manifest.records.count
    assert manifest.source_sha256 == Base.encode16(:crypto.hash(:sha256, before), case: :lower)
    assert File.read!(path) == before
    assert Enum.sort(File.ls!(root)) == entries

    assert {:ok, ^manifest} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )
  end

  test "requires stopped-source provenance and checkpoint", %{root: root, path: path} do
    assert {:error, :stopped_provenance_required} = Format32.preflight(path, scratch_root: root)
    File.write!(path <> "-wal", "unfinished")

    assert {:error, :store_not_checkpointed} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )
  end

  test "refuses unknown durable tables and columns", %{root: root, path: path} do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, "CREATE TABLE future_state (id TEXT)")
    Sqlite3.close(db)

    assert {:error, :unknown_source_table} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )

    {:ok, db} = Sqlite3.open(path)

    :ok =
      Sqlite3.execute(
        db,
        "DROP TABLE future_state; ALTER TABLE relay_state ADD COLUMN future TEXT"
      )

    Sqlite3.close(db)

    assert {:error, {:unsupported_source_columns, "relay_state"}} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )
  end

  test "refuses damaged row checksums and never bootstraps a missing source", %{
    root: root,
    path: path
  } do
    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, "UPDATE relay_records SET checksum = x'00'")
    Sqlite3.close(db)

    assert {:error, :invalid_source_record} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )

    absent = Path.join(root, "absent.sqlite3")

    assert {:error, :invalid_source_file} =
             Format32.preflight(absent,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )

    refute File.exists?(absent)
  end

  test "refuses missing, unstable, active, live-marker, or mismatched provenance", %{
    root: root,
    path: path
  } do
    record = AttentionServer.MigrationFixtures.provenance(path)

    assert {:error, :stopped_provenance_required} =
             Format32.preflight(path, stopped: true, scratch_root: root)

    for invalid <- [
          %{},
          put_in(record.supervisor_before.active_state, "active"),
          put_in(record.supervisor_after.main_pid, 123),
          %{record | process_marker_before: %{state: :live}},
          %{record | process_marker_after: %{state: :stale, pid: 123, alive: true}},
          put_in(record.files_after.main.mtime, record.files_after.main.mtime + 1),
          put_in(record.files_after.shm, %{size: 1, mtime: 0, sha256: String.duplicate("a", 64)}),
          %{record | files_before: Map.delete(record.files_before, :wal)},
          %{
            record
            | files_before:
                Map.put(
                  record.files_before,
                  :main,
                  Map.put(record.files_before.main, :sha256, String.duplicate("a", 64))
                ),
              files_after:
                Map.put(
                  record.files_after,
                  :main,
                  Map.put(record.files_after.main, :sha256, String.duplicate("a", 64))
                )
          }
        ] do
      assert {:error, :invalid_stopped_provenance} =
               Format32.preflight(path, provenance: invalid, scratch_root: root)
    end
  end

  test "rejects correct-checksum unknown, derived and wrong-kind record addresses", %{
    root: root,
    path: path
  } do
    alias AttentionServer.Store.{RecordCodec, RecordTable}

    for address <- [
          {:field, :touched_keys},
          {:row, :future_family, "x"},
          {:field, :persons},
          {:member, :bounce_discussed_messages, "x"},
          {:origin_feed, {"p", "a"}, {:receipt, "x"}},
          {:advisory, {"p", "a", "s"}, {:delivery_ack, 1}}
        ] do
      {:ok, db} = Sqlite3.open(path)
      {key, payload, checksum} = RecordCodec.encode_row(address, true)

      :ok =
        RecordTable.run(
          db,
          "INSERT INTO relay_records (key,payload,checksum,account) VALUES (?,?,?,0)",
          [{:blob, key}, {:blob, payload}, {:blob, checksum}]
        )

      Sqlite3.close(db)

      assert {:error, :unsupported_source_record_address} =
               Format32.preflight(path,
                 provenance: AttentionServer.MigrationFixtures.provenance(path),
                 scratch_root: root
               )

      {:ok, db} = Sqlite3.open(path)
      :ok = RecordTable.run(db, "DELETE FROM relay_records WHERE key = ?", [{:blob, key}])
      Sqlite3.close(db)
    end
  end
end

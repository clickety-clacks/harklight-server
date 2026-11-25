defmodule AttentionServer.Store.OpenCleanupTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias AttentionServer.{JSON, LargeData, Relay, Store}
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @uuid "00000000-0000-4000-8000-000000000010"

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => @uuid,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item",
        "created_at" => 1,
        "title" => "Fixture",
        "body" => :binary.copy("x", LargeData.inline_limit() + 1)
      }
    }

    assert {:ok, response} = Relay.request_line(relay, "origin", JSON.encode_line(frame))
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)
    assert :ok = GenServer.stop(relay)
    %{path: path}
  end

  for mode <- [:snapshot, :keyed, :keyed_cutover] do
    test "a refused #{mode} open cannot prune a file while a writer holds the lock", %{path: path} do
      mode = unquote(mode)

      if mode == :snapshot, do: AttentionServer.PreChangeLogStore.snapshot_fixture(path)

      if mode == :keyed do
        assert {:ok, keyed} = Store.open(path, :keyed_cutover)
        assert :ok = Store.close(keyed)
      end

      assert {:ok, db} = Sqlite3.open(path)
      assert :ok = Sqlite3.execute(db, "BEGIN IMMEDIATE")
      paths = orphan_files(path)

      try do
        log = capture_log(fn -> assert {:error, _} = Store.open(path, mode) end)
        assert log =~ "cause=busy"
        assert Enum.map(paths, &File.read/1) == [{:ok, "in flight"}, {:ok, "in flight"}]
      after
        Sqlite3.execute(db, "ROLLBACK")
        Sqlite3.close(db)
      end

      assert {:ok, store} = Store.open(path, mode)
      assert :ok = Store.close(store)
      refute Enum.any?(paths, &File.exists?/1)
    end
  end

  test "a refused reference registry refresh leaves ordinary and large orphan files intact", %{
    path: path
  } do
    assert {:ok, db} = Sqlite3.open(path)

    assert :ok =
             Sqlite3.execute(db, """
             CREATE TRIGGER fail_refresh BEFORE DELETE ON relay_large_data_refs
             BEGIN SELECT RAISE(ABORT, 'refresh refused'); END;
             """)

    paths = orphan_files(path)

    try do
      assert {:error, _} = Store.open(path)
      assert Enum.map(paths, &File.read/1) == [{:ok, "in flight"}, {:ok, "in flight"}]
    after
      Sqlite3.close(db)
    end
  end

  for mode <- [:snapshot, :keyed] do
    test "a #{mode} open whose generation moves before cleanup is refused and prunes nothing", %{
      path: path
    } do
      mode = unquote(mode)

      bump =
        if mode == :keyed do
          assert {:ok, keyed} = Store.open(path, :keyed_cutover)
          assert :ok = Store.close(keyed)
          "UPDATE relay_record_generation SET generation = generation + 1 WHERE id = 1"
        else
          :ok = AttentionServer.PreChangeLogStore.snapshot_fixture(path)
          "UPDATE relay_state SET version = version + 1 WHERE id = 1"
        end

      assert {:ok, db} = Sqlite3.open(path)

      assert :ok =
               Sqlite3.execute(db, """
               CREATE TRIGGER move_generation AFTER INSERT ON relay_large_data_refs
               BEGIN #{bump}; END;
               """)

      paths = orphan_files(path)

      try do
        log =
          capture_log(fn -> assert {:error, :storage_unavailable} = Store.open(path, mode) end)

        assert log =~ "cause=stale_generation"
        assert Enum.map(paths, &File.read/1) == [{:ok, "in flight"}, {:ok, "in flight"}]
        assert :ok = Sqlite3.execute(db, "DROP TRIGGER move_generation")
      after
        Sqlite3.close(db)
      end

      assert {:ok, store} = Store.open(path, mode)
      assert :ok = Store.close(store)
      refute Enum.any?(paths, &File.exists?/1)
    end
  end

  defp orphan_files(path) do
    Enum.map([@uuid, String.duplicate("f", 64)], fn name ->
      file = Path.join(path <> ".uploads", name)
      File.write!(file, "in flight")
      file
    end)
  end
end

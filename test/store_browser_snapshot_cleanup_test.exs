defmodule AttentionServer.StoreBrowserSnapshotCleanupTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{BrowserRecipient, Engine, Store}
  alias Exqlite.Sqlite3

  test "browser recipient progress is valid without creating a device record" do
    person = "00000000-0000-4000-8000-000000000010"
    {:ok, engine} = Engine.operator_bootstrap(Engine.new(), person, String.duplicate("a", 64))

    browser =
      BrowserRecipient.key(
        person,
        String.duplicate("a", 43),
        "00000000-0000-4000-8000-000000000011"
      )

    engine = %{
      engine
      | cursors: %{{person, browser} => 0},
        conversation_cursors: %{{person, browser} => 0},
        conversation_opened: MapSet.new([{person, browser}])
    }

    path =
      Path.join(
        System.tmp_dir!(),
        "harklight-browser-progress-#{System.unique_integer([:positive])}"
      )

    {:ok, store} = Store.open(path)

    on_exit(fn ->
      File.rm_rf!(path <> ".uploads")
      File.rm_rf!(path)
    end)

    assert {:ok, committed} =
             Store.commit(store, engine, %{
               person: person,
               actor: browser,
               action: "browser_progress"
             })

    assert Map.get(committed.engine.cursors, {person, browser}) == 0
    assert Map.get(committed.engine.devices, browser) == nil
    assert :ok = Store.close(committed)
  end

  test "recipient cleanup removes only its snapshot and all associated paging rows" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-browser-snapshot-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    path = Path.join(dir, "relay.sqlite3")
    {:ok, store} = Store.open(path)

    on_exit(fn ->
      Store.close(store)
      File.rm_rf!(dir)
    end)

    person = "account-person"

    browser =
      BrowserRecipient.key(
        person,
        String.duplicate("a", 43),
        "00000000-0000-4000-8000-000000000001"
      )

    device = String.duplicate("a", 64)
    insert_snapshot(store.db, person, browser, "browser-snapshot")
    insert_snapshot(store.db, person, device, "device-snapshot")
    insert_children(store.db, person, browser, "browser-snapshot")
    insert_children(store.db, person, device, "device-snapshot")

    assert :ok = Store.discard_inbox_snapshots_for_device(store, person, browser)
    assert count(store.db, "SELECT count(*) FROM relay_inbox_snapshots") == 1
    assert count(store.db, "SELECT count(*) FROM relay_inbox_item_pages") == 1
    assert count(store.db, "SELECT count(*) FROM relay_inbox_receipt_sequences") == 1
    assert count(store.db, "SELECT count(*) FROM relay_inbox_operation_receipts") == 1
    assert count(store.db, "SELECT count(*) FROM relay_inbox_batches") == 1
    assert count(store.db, "SELECT count(*) FROM relay_inbox_receipt_pages") == 1
    assert :ok = Store.discard_browser_inbox_snapshots(store)
    assert count(store.db, "SELECT count(*) FROM relay_inbox_snapshots") == 1
  end

  defp insert_snapshot(db, person, device, snapshot_id) do
    insert(
      db,
      "INSERT INTO relay_inbox_snapshots (person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete) VALUES (?, ?, ?, ?, 0, 0, 'pending', 1, 1, 1, ?, 0, 0)",
      [
        person,
        device,
        "startup-#{snapshot_id}",
        snapshot_id,
        {:blob, <<0>>}
      ]
    )
  end

  defp insert_children(db, person, device, snapshot_id) do
    prefix = [person, device, snapshot_id]
    blob = {:blob, <<0>>}

    insert(
      db,
      "INSERT INTO relay_inbox_item_pages (person_id, device_id, snapshot_id, start_offset, payload, checksum) VALUES (?, ?, ?, 0, ?, ?)",
      prefix ++ [blob, blob]
    )

    insert(
      db,
      "INSERT INTO relay_inbox_receipt_sequences (person_id, device_id, snapshot_id, start_offset, payload, checksum) VALUES (?, ?, ?, 0, ?, ?)",
      prefix ++ [blob, blob]
    )

    insert(
      db,
      "INSERT INTO relay_inbox_operation_receipts (person_id, device_id, snapshot_id, op_id) VALUES (?, ?, ?, ?)",
      prefix ++ ["op"]
    )

    insert(
      db,
      "INSERT INTO relay_inbox_batches (person_id, device_id, snapshot_id, batch_id, input_digest, result, result_checksum) VALUES (?, ?, ?, ?, ?, ?, ?)",
      prefix ++ ["batch", blob, blob, blob]
    )

    insert(
      db,
      "INSERT INTO relay_inbox_receipt_pages (person_id, device_id, snapshot_id, start_offset, next_offset, result, result_checksum) VALUES (?, ?, ?, 0, 0, ?, ?)",
      prefix ++ [blob, blob]
    )
  end

  defp insert(db, sql, params) do
    {:ok, statement} = Sqlite3.prepare(db, sql)
    :ok = Sqlite3.bind(statement, params)
    :done = Sqlite3.step(db, statement)
    :ok = Sqlite3.release(db, statement)
  end

  defp count(db, sql) do
    {:ok, statement} = Sqlite3.prepare(db, sql)
    {:ok, [[result]]} = Sqlite3.fetch_all(db, statement)
    :ok = Sqlite3.release(db, statement)
    result
  end
end

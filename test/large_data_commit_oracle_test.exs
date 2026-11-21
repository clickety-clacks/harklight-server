defmodule AttentionServer.LargeDataCommitOracleTest do
  # Differential check of the incremental large-data commit path on real Relay
  # operations: every state the Relay proposes to the store is captured before
  # any large-data transformation, and the incremental result for that exact
  # proposed state must equal the full path's result for it. Requested content,
  # revisions and withdrawal are verified independently of both paths, and a
  # failed snapshot write that would have changed the reference index must not
  # advance the cache.
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, LargeData, Relay, Store, TLSFixtures}

  @person "A"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "large-oracle-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "relay.sqlite3")
    {relay, key} = start_relay(path, certs)

    :erlang.trace(relay, true, [:call, {:tracer, self()}])
    # Include callers using the default-argument wrapper as well as callers
    # that pass projected entries explicitly.
    commit = {Store, :commit, 4}
    :erlang.trace_pattern(commit, true, [:local])

    on_exit(fn -> :erlang.trace_pattern(commit, false, [:local]) end)
    %{path: path, certs: certs, relay: relay, key: key}
  end

  defp start_existing(path) do
    start_link_supervised!({Relay, store_path: path, enrollment_fixture_mode: true},
      id: make_ref(),
      restart: :temporary
    )
  end

  defp start_relay(path, certs) do
    relay = start_existing(path)

    :ok = Relay.setup_person(relay, @person)

    {:ok, key} =
      Relay.setup_device_certificate(relay, @person, TLSFixtures.certificate_der!(certs, "a1"))

    :ok = Relay.setup_sender(relay, key, "app:test")
    {relay, key}
  end

  defp op(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp request(relay, key, payload) do
    frame = JSON.encode!(Map.put_new(payload, "v", 1)) <> "\n"
    {:ok, line} = Relay.request_line(relay, key, frame)
    {:ok, response} = JSON.decode_line(line)
    response
  end

  defp fyi(id, title, body, extra \\ %{}) do
    Map.merge(
      %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => 1,
        "title" => title,
        "body" => body,
        "message_type" => "fyi",
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [
            %{
              "id" => "seen",
              "label" => "Seen",
              "kind" => "value",
              "value" => %{"acknowledgment" => "seen"}
            }
          ]
        }
      },
      extra
    )
  end

  defp large(seed), do: Base.encode64(:binary.copy(<<seed>>, 1_700_000))

  defp progress(percent),
    do: %{
      "updated_at" => "2026-11-11T10:00:00Z",
      "percent" => percent,
      "stage" => "coding",
      "note" => "step #{percent}",
      "segments" => [
        %{
          "stage" => "coding",
          "width" => percent,
          "note" => "step #{percent}",
          "visit_at" => "2026-11-11T10:00:00Z",
          "reported_percent" => percent,
          "delta" => percent
        }
      ]
    }

  defp full(proposed, dir) do
    {:ok, durable, _created} = LargeData.externalize(proposed, dir)
    true = LargeData.valid_refs?(durable)
    {durable, LargeData.persist_refs(durable), LargeData.persisted_references(durable)}
  end

  # Every commit proposed since the last call: the incremental result for the
  # exact proposed state must equal the full path for it.
  defp assert_commits_equivalent(relay) do
    committed = :sys.get_state(relay).store
    calls = drain_commits([])
    assert calls != []

    for {before, proposed} <- calls, before.db != nil do
      {full_durable, full_persisted, full_rows} = full(proposed, before.upload_dir)

      if before.large_base do
        assert {:ok, durable, persisted, index, _created} =
                 LargeData.commit_incremental(
                   proposed,
                   before.large_base,
                   before.large_persisted,
                   before.large_index,
                   before.upload_dir
                 )

        assert durable == full_durable
        assert persisted == full_persisted
        assert LargeData.reference_index_rows(index) == full_rows
        assert LargeData.reference_index_ids(index) == LargeData.refs(full_durable)
      end
    end

    if committed.large_base do
      {_before, last} = List.last(calls)
      {full_durable, full_persisted, full_rows} = full(last, committed.upload_dir)
      assert committed.large_base == full_durable
      assert committed.large_persisted == full_persisted
      assert LargeData.reference_index_rows(committed.large_index) == full_rows
    end

    committed
  end

  defp drain_commits(acc) do
    receive do
      {:trace, _pid, :call, {Store, :commit, [before, proposed, _audit, _projected]}} ->
        drain_commits([{before, proposed} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp record(relay, id),
    do: Map.fetch!(:sys.get_state(relay).store.engine.messages, {@person, "app:test", id})

  defp body_bytes(%LargeData.Ref{} = ref), do: LargeData.read!(ref)
  defp body_bytes(body) when is_binary(body), do: body

  defp assert_body(relay, id, expected) do
    assert body_bytes(record(relay, id).message["body"]) == expected
  end

  defp db_rows(path) do
    {:ok, db} = Exqlite.Sqlite3.open(path)

    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(
        db,
        "SELECT path, marker, id, size, sha256 FROM relay_large_data_refs ORDER BY path"
      )

    {:ok, rows} = Exqlite.Sqlite3.fetch_all(db, stmt)
    :ok = Exqlite.Sqlite3.release(db, stmt)
    :ok = Exqlite.Sqlite3.close(db)
    rows
  end

  test "real operations: incremental equals the full path on each proposed state",
       %{relay: relay, key: key} do
    handle = op(9_000)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => handle,
               "message" => fyi("plain", "Plain", "small body")
             })

    assert_commits_equivalent(relay)
    assert_body(relay, "plain", "small body")

    big1 = large(1)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => handle,
               "message" => fyi("big", "Big", big1)
             })

    store = assert_commits_equivalent(relay)
    assert map_size(store.large_index) > 0
    assert_body(relay, "big", big1)
    assert %LargeData.Ref{} = record(relay, "big").message["body"]

    big2 = large(2)

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, key, %{
               "type" => "conversation_revise",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "big",
               "base_revision" => 1,
               "message" => fyi("big", "Big, revised", big2)
             })

    assert_commits_equivalent(relay)
    assert_body(relay, "big", big2)
    assert record(relay, "big").message["title"] == "Big, revised"

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(4),
               "session_handle" => handle,
               "message" => fyi("progress", "Progress", large(3), %{"progress" => progress(20)})
             })

    assert_commits_equivalent(relay)

    for {percent, n} <- [{40, 5}, {60, 6}, {80, 7}] do
      body = large(n)

      assert %{"type" => "ok", "revision" => revision} =
               request(relay, key, %{
                 "type" => "conversation_revise",
                 "op_id" => op(n),
                 "sender_id" => "app:test",
                 "message_id" => "progress",
                 "base_revision" => n - 4,
                 "message" =>
                   fyi("progress", "Progress", body, %{"progress" => progress(percent)})
               })

      assert revision == n - 3
      assert_commits_equivalent(relay)
      assert_body(relay, "progress", body)
      assert record(relay, "progress").message["progress"]["percent"] == percent
    end

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "conversation_withdraw",
               "op_id" => op(8),
               "sender_id" => "app:test",
               "message_id" => "big"
             })

    store = assert_commits_equivalent(relay)
    assert %{state: "retracted"} = record(relay, "big").terminal

    for {_id, ref} <- LargeData.reference_index_ids(store.large_index) do
      assert File.exists?(Path.join(store.upload_dir, ref.id))
    end
  end

  test "a failed write that would change the index leaves the cache; a retry commits",
       %{relay: relay, key: key, path: path} do
    original = large(9)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => op(9_001),
               "message" => fyi("big", "Big", original)
             })

    before = assert_commits_equivalent(relay)
    assert map_size(before.large_index) > 0
    {_path, {_row, old_ref}} = Enum.at(Enum.to_list(before.large_index), 0)
    old_file = Path.join(before.upload_dir, old_ref.id)
    assert File.exists?(old_file)

    replacement = large(10)

    revise = %{
      "type" => "conversation_revise",
      "op_id" => op(2),
      "sender_id" => "app:test",
      "message_id" => "big",
      "base_revision" => 1,
      "message" => fyi("big", "Big, replaced", replacement)
    }

    {:ok, blocker} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.execute(blocker, "BEGIN IMMEDIATE")

    failed = request(relay, key, revise)
    refute failed["type"] == "ok"

    # The rejected proposal would have replaced the reference.
    [{store_before, proposed} | _] = drain_commits([])

    assert {:ok, _durable, _persisted, candidate_index, _created} =
             LargeData.commit_incremental(
               proposed,
               store_before.large_base,
               store_before.large_persisted,
               store_before.large_index,
               store_before.upload_dir
             )

    assert candidate_index != before.large_index

    after_failure = :sys.get_state(relay).store
    assert after_failure.large_base === before.large_base
    assert after_failure.large_persisted === before.large_persisted
    assert after_failure.large_index === before.large_index
    assert after_failure.engine === before.engine
    assert after_failure.version == before.version
    assert File.exists?(old_file)
    assert_body(relay, "big", original)

    :ok = Exqlite.Sqlite3.execute(blocker, "ROLLBACK")
    :ok = Exqlite.Sqlite3.close(blocker)

    assert %{"type" => "ok", "revision" => 2} = request(relay, key, revise)
    retried = assert_commits_equivalent(relay)
    assert retried.version == before.version + 1
    assert retried.large_index != before.large_index
    assert_body(relay, "big", replacement)

    assert db_rows(path) == LargeData.reference_index_rows(retried.large_index)
    retained_ids = LargeData.reference_index_ids(retried.large_index)
    still_referenced = Map.has_key?(retained_ids, old_ref.id)
    assert File.exists?(old_file) == still_referenced

    GenServer.stop(relay)
    relay = start_existing(path)
    assert_body(relay, "big", replacement)
    assert record(relay, "big").message["title"] == "Big, replaced"
  end

  test "Store.commit's own failure return keeps the cache when the proposal changes references",
       %{relay: relay, key: key, path: path} do
    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => op(9_002),
               "message" => fyi("big", "Big", large(11))
             })

    _ = drain_commits([])
    replacement = large(12)

    # Capture the Relay's own proposal for a revision to new large content; the
    # held write lock makes that request fail, so the Relay keeps its old state.
    {:ok, blocker} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.execute(blocker, "BEGIN IMMEDIATE")

    rejected =
      request(relay, key, %{
        "type" => "conversation_revise",
        "op_id" => op(2),
        "sender_id" => "app:test",
        "message_id" => "big",
        "base_revision" => 1,
        "message" => fyi("big", "Big, replaced", replacement)
      })

    refute rejected["type"] == "ok"
    :ok = Exqlite.Sqlite3.execute(blocker, "ROLLBACK")
    [{_before, captured} | _] = drain_commits([])
    GenServer.stop(relay)

    audit = %{person: @person, actor: "test", action: "oracle"}

    # The first snapshot commit after open takes the full path and seeds the cache.
    {:ok, opened} = Store.open(path)
    clock = opened.engine.pending_clock_high_water + 1
    first = %{opened.engine | pending_clock_high_water: clock}
    {:ok, seeded} = Store.commit(opened, first, audit)
    assert seeded.large_base != nil
    proposed = %{captured | pending_clock_high_water: clock}

    assert {:ok, _durable, _persisted, candidate_index, _created} =
             LargeData.commit_incremental(
               proposed,
               seeded.large_base,
               seeded.large_persisted,
               seeded.large_index,
               seeded.upload_dir
             )

    assert candidate_index != seeded.large_index

    :ok = Exqlite.Sqlite3.execute(blocker, "BEGIN IMMEDIATE")
    assert {:error, :storage_unavailable, failed} = Store.commit(seeded, proposed, audit)
    :ok = Exqlite.Sqlite3.execute(blocker, "ROLLBACK")
    :ok = Exqlite.Sqlite3.close(blocker)

    assert failed.large_index === seeded.large_index
    assert failed.large_base === seeded.large_base
    assert failed.large_persisted === seeded.large_persisted
    assert failed.engine === seeded.engine
    assert failed.version == seeded.version
    assert failed.encoding_cache === seeded.encoding_cache

    # Retry through the store that the failure returned; success also shows the
    # proposal is valid, so the earlier failure came from the held lock.
    assert {:ok, retried} = Store.commit(failed, proposed, audit)
    {full_durable, full_persisted, full_rows} = full(proposed, retried.upload_dir)
    assert retried.version == seeded.version + 1
    assert retried.large_base == full_durable
    assert retried.large_persisted == full_persisted
    assert LargeData.reference_index_rows(retried.large_index) == full_rows
    assert retried.large_index != seeded.large_index
    assert db_rows(path) == full_rows
    :ok = Store.close(retried)

    {:ok, reopened} = Store.open(path)
    message_key = {@person, "app:test", "big"}
    assert body_bytes(reopened.engine.messages[message_key].message["body"]) == replacement
    :ok = Store.close(reopened)
  end
end

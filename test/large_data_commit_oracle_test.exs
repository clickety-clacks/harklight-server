defmodule AttentionServer.LargeDataCommitOracleTest do
  # Real Relay operations (forward, revision, Progress same-ID revision,
  # withdrawal, large bodies) must leave the store's cached commit base equal
  # to what the full large-data path computes from the committed engine, and a
  # failed snapshot write must not advance that cache.
  use ExUnit.Case, async: false

  alias AttentionServer.{JSON, LargeData, Relay, Store, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  setup %{certs: certs} do
    dir = Path.join(System.tmp_dir!(), "large-oracle-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "relay.sqlite3"), certs: certs}
  end

  defp start_relay(path, certs) do
    relay =
      start_link_supervised!({Relay, store_path: path, enrollment_fixture_mode: true},
        id: make_ref(),
        restart: :temporary
      )

    :ok = Relay.setup_person(relay, "A")

    {:ok, key} =
      Relay.setup_device_certificate(relay, "A", TLSFixtures.certificate_der!(certs, "a1"))

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

  defp seen,
    do: %{
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

  defp fyi(id, title, body, extra \\ %{}) do
    Map.merge(
      %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => 1,
        "title" => title,
        "body" => body,
        "message_type" => "fyi",
        "answer_ui" => seen()
      },
      extra
    )
  end

  defp large(seed), do: Base.encode64(:binary.copy(<<seed>>, 1_700_000))

  defp progress(percent),
    do: %{
      "updated_at" => "2026-11-11T10:0#{div(percent, 20)}:00Z",
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

  # The cache must equal the full path applied to the committed engine.
  defp assert_oracle(relay) do
    store = :sys.get_state(relay).store
    assert %Store{large_base: base} = store
    assert base != nil
    assert store.engine == base or store.progress_version > 0
    assert {:ok, durable, []} = LargeData.externalize(base, store.upload_dir)
    assert durable == base
    assert store.large_persisted == LargeData.persist_refs(base)
    assert LargeData.reference_index_rows(store.large_index) ==
             LargeData.persisted_references(base)
    assert LargeData.reference_index_ids(store.large_index) == LargeData.refs(base)
    store
  end

  test "forward, revision, Progress revision, large bodies and withdrawal keep the cache exact",
       %{path: path, certs: certs} do
    {relay, key} = start_relay(path, certs)
    handle = op(9_000)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => handle,
               "message" => fyi("plain", "Plain", "small body")
             })

    assert_oracle(relay)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => handle,
               "message" => fyi("big", "Big", large(1))
             })

    store = assert_oracle(relay)
    assert map_size(store.large_index) > 0

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, key, %{
               "type" => "conversation_revise",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "big",
               "base_revision" => 1,
               "message" => fyi("big", "Big, revised", large(2))
             })

    assert_oracle(relay)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(4),
               "session_handle" => handle,
               "message" => fyi("progress", "Progress", large(3), %{"progress" => progress(20)})
             })

    assert_oracle(relay)

    for {percent, n} <- [{40, 5}, {60, 6}, {80, 7}] do
      assert %{"type" => "ok"} =
               request(relay, key, %{
                 "type" => "conversation_revise",
                 "op_id" => op(n),
                 "sender_id" => "app:test",
                 "message_id" => "progress",
                 "base_revision" => n - 4,
                 "message" =>
                   fyi("progress", "Progress", large(n), %{"progress" => progress(percent)})
               })

      assert_oracle(relay)
    end

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "conversation_withdraw",
               "op_id" => op(8),
               "sender_id" => "app:test",
               "message_id" => "big"
             })

    store = assert_oracle(relay)
    ids = store.large_index |> LargeData.reference_index_ids() |> Map.keys() |> MapSet.new()

    on_disk =
      store.upload_dir
      |> File.ls!()
      |> Enum.filter(&Regex.match?(~r/\A[0-9a-f]{64}\z/, &1))
      |> MapSet.new()

    assert MapSet.subset?(ids, on_disk)
  end

  test "a failed snapshot write leaves the cached base unchanged and a retry commits",
       %{path: path, certs: certs} do
    {relay, key} = start_relay(path, certs)

    assert %{"type" => "ok"} =
             request(relay, key, %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => op(9_001),
               "message" => fyi("big", "Big", large(9))
             })

    assert_oracle(relay)
    GenServer.stop(relay)

    assert {:ok, store} = Store.open(path)
    # The first commit after open takes the full path and seeds the base.
    assert store.large_base == nil
    audit = %{person: "A", actor: "test", action: "oracle"}
    first = %{store.engine | pending_clock_high_water: store.engine.pending_clock_high_water + 1}
    assert {:ok, store} = Store.commit(store, first, audit)
    assert store.large_base == store.engine
    assert map_size(store.large_index) > 0

    # Hold the write lock from another connection so the snapshot write fails.
    {:ok, blocker} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.execute(blocker, "BEGIN IMMEDIATE")
    next = %{store.engine | pending_clock_high_water: store.engine.pending_clock_high_water + 1}

    assert {:error, :storage_unavailable, failed} = Store.commit(store, next, audit)
    assert failed.large_base === store.large_base
    assert failed.large_persisted === store.large_persisted
    assert failed.large_index === store.large_index
    assert failed.engine === store.engine
    assert failed.version == store.version

    :ok = Exqlite.Sqlite3.execute(blocker, "ROLLBACK")
    :ok = Exqlite.Sqlite3.close(blocker)

    assert {:ok, retried} = Store.commit(failed, next, audit)
    assert retried.version == store.version + 1
    assert retried.large_base == next
    assert retried.large_persisted == LargeData.persist_refs(next)
    assert LargeData.reference_index_rows(retried.large_index) ==
             LargeData.persisted_references(next)
    :ok = Store.close(retried)

    assert {:ok, reopened} = Store.open(path)
    assert reopened.engine.pending_clock_high_water == next.pending_clock_high_water
    :ok = Store.close(reopened)
  end
end

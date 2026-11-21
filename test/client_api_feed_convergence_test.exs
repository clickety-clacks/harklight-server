defmodule AttentionServer.ClientAPIFeedConvergenceTest do
  # Spec §10: a snapshot at rev S plus the feed from S converges. Clients catch
  # up after every step, each row rendered from the state of that moment, so a
  # change that writes no row leaves a client behind and fails here.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{FeedRenderer, Principal, Reads}

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @principal %Principal{person: "A", kind: :device, key: "reader"}

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-convergence-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))

    assert {:ok, relay} =
             Relay.start_link(
               store_path: Path.join(dir, "state.sqlite3"),
               feature_flags_path: flags,
               agent_fixture_mode: true
             )

    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "phone")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    %{relay: relay}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, %{"type" => "ok"} = response} = JSON.decode_line(line)
    response
  end

  defp forward(relay, id, n, extra \\ %{}) do
    request(relay, "origin", %{
      "type" => "forward",
      "op_id" => op(n),
      "session_handle" => @handle,
      "message" =>
        Map.merge(
          %{"sender_id" => "app:test", "message_id" => id, "created_at" => n, "title" => id},
          extra
        )
    })
  end

  # The script, one step at a time: arrivals, a chat, a receipt, answers, an
  # open, a read on another device, a revision, a retraction and a result.
  defp steps(relay) do
    [
      fn -> forward(relay, "one", 2) end,
      fn -> forward(relay, "two", 3, %{"urgency" => "high"}) end,
      fn -> forward(relay, "three", 4, %{"thread" => "plan"}) end,
      fn ->
        request(relay, "reader", %{
          "type" => "discussion",
          "op_id" => op(10),
          "sender_id" => "app:test",
          "message_id" => "one",
          "request_revision" => 1,
          "text" => "Question"
        })
      end,
      fn ->
        [%{"feed_seq" => seq, "event_id" => event}] = feed_entries(relay, "discussion")

        request(relay, "origin", %{
          "type" => "conversation_reply",
          "op_id" => op(11),
          "sender_id" => "app:test",
          "message_id" => "one",
          "request_revision" => 1,
          "in_reply_to" => String.to_integer(event),
          "text" => "Answer"
        })

        {:ok, _} = Relay.origin_ack(relay, "origin", seq)
      end,
      fn -> forward(relay, "four", 5, %{"thread" => "plan"}) end,
      fn ->
        [reply] =
          for {{"A", seq}, %{payload: %{"kind" => "reply", "author" => "origin"}}} <-
                :sys.get_state(relay).store.engine.conversation_events,
              do: seq

        request(relay, "phone", %{
          "type" => "conversation_read",
          "op_id" => op(13),
          "sender_id" => "app:test",
          "message_id" => "one",
          "read_through_conversation_seq" => reply
        })
      end,
      fn ->
        request(relay, "reader", %{
          "type" => "answer",
          "op_id" => op(14),
          "sender_id" => "app:test",
          "message_id" => "two",
          "answer" => nil
        })
      end,
      fn ->
        request(relay, "origin", %{
          "type" => "conversation_revise",
          "op_id" => op(15),
          "sender_id" => "app:test",
          "message_id" => "three",
          "base_revision" => 1,
          "message" => %{
            "sender_id" => "app:test",
            "message_id" => "three",
            "created_at" => 4,
            "title" => "three, revised",
            "thread" => "plan"
          }
        })
      end,
      fn ->
        request(relay, "origin", %{
          "type" => "conversation_withdraw",
          "op_id" => op(16),
          "sender_id" => "app:test",
          "message_id" => "four"
        })
      end,
      fn ->
        %{"feed_seq" => terminal} =
          relay |> feed_entries("terminal") |> Enum.find(&(&1["message_id"] == "two"))

        {:ok, _} = Relay.origin_ack(relay, "origin", terminal)

        request(relay, "origin", %{
          "type" => "conversation_result",
          "op_id" => op(17),
          "sender_id" => "app:test",
          "message_id" => "two",
          "in_reply_to" => terminal,
          "status" => "received"
        })
      end,
      # Last, on an item nothing touches afterwards: a missed row would show.
      fn ->
        request(relay, "reader", %{
          "type" => "message_open",
          "op_id" => op(12),
          "sender_id" => "app:test",
          "message_id" => "one",
          "request_revision" => 1
        })
      end
    ]
  end

  defp feed_entries(relay, kind) do
    :sys.get_state(relay).store.engine.origin_feeds[{"A", "origin"}].entries
    |> Map.values()
    |> Enum.filter(&(&1["kind"] == kind))
  end

  # A client's model: resources it holds, by id, version-gated by rev.
  defp empty_model, do: %{cursor: 0, items: %{}, entries: %{}, counts: nil, settings: nil}

  defp put_versioned(map, id, %{"rev" => rev} = data) do
    case Map.get(map, id) do
      %{"rev" => held} when held >= rev -> map
      _ -> Map.put(map, id, data)
    end
  end

  defp catch_up(relay, model, dropped \\ fn _row -> false end) do
    state = :sys.get_state(relay)
    context = %{engine: state.store.engine, store: state.store, view: state.priority_views["A"]}
    {:ok, rows, more, position} = ChangeLog.read(state.store, "A", model.cursor, 500)
    refute more

    model =
      rows
      |> Enum.reject(dropped)
      |> Enum.reduce(model, fn row, model ->
        case FeedRenderer.render(context, @principal, row) do
          {:ok, data} -> apply_change(model, row.kind, data)
          :skip -> model
        end
      end)

    %{model | cursor: position}
  end

  defp apply_change(model, :item, data),
    do: %{model | items: put_versioned(model.items, data["item_id"], data)}

  defp apply_change(model, :item_removed, data),
    do: %{model | items: Map.delete(model.items, data["item_id"])}

  defp apply_change(model, :entry, data),
    do: %{model | entries: put_versioned(model.entries, data["entry_id"], data)}

  defp apply_change(model, :counts, data), do: %{model | counts: data}
  defp apply_change(model, :settings, data), do: %{model | settings: data}

  # A snapshot read, as a client takes it: inbox, every item's entries,
  # settings, with the inbox's position as the cursor.
  defp snapshot(relay) do
    {:ok, inbox} = Reads.inbox(relay, @principal, %{"limit" => "200"})

    entries =
      for item <- inbox["items"],
          {:ok, %{"entries" => entries}} =
            Reads.entries(relay, @principal, item["item_id"], %{"limit" => "200"}),
          entry <- entries,
          into: %{},
          do: {entry["entry_id"], entry}

    {:ok, %{"settings" => settings}} = Reads.settings(relay, @principal)

    %{
      cursor: inbox["rev"],
      items: Map.new(inbox["items"], &{&1["item_id"], &1}),
      entries: entries,
      counts: inbox["counts"],
      settings: settings
    }
  end

  defp assert_converged(model, final, opts \\ []) do
    assert Map.keys(model.items) |> Enum.sort() == Map.keys(final.items) |> Enum.sort()
    shown = if Keyword.get(opts, :rank_keys, true), do: & &1, else: &without_rank_keys/1

    for {id, item} <- final.items,
        do: assert({id, shown.(model.items[id])} == {id, shown.(item)})

    for {id, entry} <- final.entries,
        do: assert({id, model.entries[id]} == {id, entry})

    assert model.counts == final.counts
  end

  # Rank keys move when other items do, and the change log writes rank rows
  # only once its rank hook lands (review finding F3, accepted F5 plan).
  # Until then every other member must converge.
  defp without_rank_keys(item) do
    item
    |> update_in(["rank"], &Map.delete(&1, "key"))
    |> update_in(["chat"], &Map.delete(&1, "reply_rank_key"))
  end

  test "clients converge on everything but rank keys, after every step", %{relay: relay} do
    run(relay, rank_keys: false)
  end

  @tag skip: "waits on the change log's rank rows (review finding F3)"
  test "clients converge including rank keys, after every step", %{relay: relay} do
    run(relay, rank_keys: true)
  end

  # The check's own proof: a client that misses one item row (the Seen open
  # of the last step) is caught as diverged.
  test "a missed item row is caught", %{relay: relay} do
    missed = ChangeLog.item_resource({"A", "app:test", "one"})
    last = length(steps(relay)) - 1

    dropped = fn index ->
      if index == last,
        do: &(&1.kind == :item and &1.resource_id == missed),
        else: fn _row -> false end
    end

    assert_raise ExUnit.AssertionError, ~r/seen/, fn ->
      run(relay, rank_keys: false, dropped: dropped)
    end
  end

  # A client catching up after every step, and one starting from a
  # mid-run snapshot, each compared with a snapshot after every step.
  defp run(relay, opts) do
    keys? = Keyword.fetch!(opts, :rank_keys)
    dropped = Keyword.get(opts, :dropped, fn _index -> fn _row -> false end end)
    steps = relay |> steps() |> Enum.with_index()
    half = div(length(steps), 2)

    Enum.reduce(steps, {empty_model(), nil}, fn {step, index}, {live, joined} ->
      step.()
      live = catch_up(relay, live, dropped.(index))
      joined = if joined, do: catch_up(relay, joined, dropped.(index)), else: nil
      now = snapshot(relay)
      assert live.cursor == now.cursor
      assert_converged(live, now, rank_keys: keys?)
      if joined, do: assert_converged(joined, now, rank_keys: keys?)
      joined = if index == half - 1, do: now, else: joined
      {live, joined}
    end)
  end
end

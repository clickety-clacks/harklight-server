defmodule AttentionServer.ClientAPIFeedRendererTest do
  # The reads component's renderer for change-log rows: each row renders as
  # the contract's change data, and replaying the rendered log reaches what
  # the snapshot reads show.
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, JSON, Relay}
  alias AttentionServer.ClientAPI.{FeedRenderer, Principal, Reads}
  alias AttentionServer.ClientAPISchema, as: Schema

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  @contract Path.expand("../docs/client-api-v2.md", __DIR__)

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-feed-render-#{System.unique_integer([:positive])}")

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
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    %{relay: relay}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp script(relay) do
    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}] do
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(n),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => id,
                   "created_at" => n,
                   "title" => id
                 }
               })
    end

    assert %{"type" => "ok", "event_id" => event_id} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(10),
               "sender_id" => "app:test",
               "message_id" => "one",
               "request_revision" => 1,
               "text" => "Question"
             })

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(11),
               "sender_id" => "app:test",
               "message_id" => "one",
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(event_id),
               "text" => "Answer"
             })

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "two",
               "answer" => nil
             })
  end

  defp rendered_log(relay, principal) do
    state = :sys.get_state(relay)

    context = %{
      engine: state.store.engine,
      store: state.store,
      view: Map.get(state.priority_views, "A")
    }

    assert {:ok, rows, false, head} = ChangeLog.read(state.store, "A", 0, 500)
    assert rows != []

    changes =
      Enum.flat_map(rows, fn row ->
        case FeedRenderer.render(context, principal, row) do
          {:ok, data} -> [{row.rev, row.kind, data}]
          :skip -> []
        end
      end)

    {changes, head}
  end

  test "every rendered change matches the contract", %{relay: relay} do
    script(relay)
    defs = Schema.contract_defs(@contract)
    principal = %Principal{person: "A", kind: :device, key: "reader"}
    {changes, _head} = rendered_log(relay, principal)

    kinds = changes |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> Enum.sort()
    assert :item in kinds and :entry in kinds and :counts in kinds

    for {rev, kind, data} <- changes do
      name = %{item: "Item", entry: "Entry", counts: "Counts", settings: "Settings"}[kind]
      assert {rev, Schema.errors(defs, name, data)} == {rev, []}
      if kind in [:item, :entry, :settings], do: assert(data["rev"] == rev)
    end
  end

  test "replaying the rendered log reaches the snapshot reads", %{relay: relay} do
    script(relay)
    principal = %Principal{person: "A", kind: :device, key: "reader"}
    {changes, head} = rendered_log(relay, principal)

    # Apply in rev order with the version rule, as a client does.
    applied =
      Enum.reduce(changes, %{}, fn
        {_rev, :item, data}, acc -> Map.put(acc, {:item, data["item_id"]}, data)
        {_rev, :entry, data}, acc -> Map.put(acc, {:entry, data["entry_id"]}, data)
        {_rev, :counts, data}, acc -> Map.put(acc, :counts, data)
        {_rev, :settings, data}, acc -> Map.put(acc, :settings, data)
        _, acc -> acc
      end)

    assert {:ok, inbox} = Reads.inbox(relay, principal)
    assert inbox["rev"] == head
    assert applied[:counts] == inbox["counts"]

    for item <- inbox["items"] do
      assert applied[{:item, item["item_id"]}] == item

      assert {:ok, %{"entries" => entries}} = Reads.entries(relay, principal, item["item_id"])
      for entry <- entries, do: assert(applied[{:entry, entry["entry_id"]}] == entry)
    end

    replayed_items = for {{:item, id}, _} <- applied, do: id
    assert Enum.sort(replayed_items) == inbox["items"] |> Enum.map(& &1["item_id"]) |> Enum.sort()
  end

  test "a principal that no longer authenticates gets nothing rendered", %{relay: relay} do
    script(relay)
    state = :sys.get_state(relay)
    context = %{engine: state.store.engine, store: state.store, view: nil}
    {:ok, [row | _], _, _} = ChangeLog.read(state.store, "A", 0, 500)
    gone = %Principal{person: "A", kind: :device, key: "gone"}
    assert FeedRenderer.render(context, gone, row) == :skip
  end

  test "render_many renders a step's rows exactly as one at a time", %{relay: relay} do
    script(relay)
    principal = %Principal{person: "A", kind: :device, key: "reader"}
    state = :sys.get_state(relay)
    context = %{engine: state.store.engine, store: state.store, view: state.priority_views["A"]}
    {:ok, rows, false, _} = ChangeLog.read(state.store, "A", 0, 500)

    assert FeedRenderer.render_many(context, principal, rows) ==
             Enum.map(rows, &{&1, FeedRenderer.render(context, principal, &1)})

    gone = %Principal{person: "A", kind: :device, key: "gone"}
    assert Enum.all?(FeedRenderer.render_many(context, gone, rows), &match?({_, :skip}, &1))
  end

  # Review finding H2: a long chat's backlog renders in one pass. Rendering
  # 4x the entries must cost about 4x, not 16x (each row rebuilding the chat).
  test "a long chat's backlog renders in linear time", %{relay: relay} do
    principal = %Principal{person: "A", kind: :device, key: "reader"}

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "long",
                 "created_at" => 2,
                 "title" => "long"
               }
             })

    timing = fn ->
      state = :sys.get_state(relay)
      context = %{engine: state.store.engine, store: state.store, view: state.priority_views["A"]}
      {:ok, rows, _, _} = ChangeLog.read(state.store, "A", 0, 500)
      entry_rows = Enum.filter(rows, &(&1.kind == :entry))

      us =
        1..3
        |> Enum.map(fn _ ->
          elem(:timer.tc(fn -> FeedRenderer.render_many(context, principal, entry_rows) end), 0)
        end)
        |> Enum.min()

      {length(entry_rows), us}
    end

    chat = fn from, to ->
      for n <- from..to do
        assert %{"type" => "ok"} =
                 request(relay, "reader", %{
                   "type" => "discussion",
                   "op_id" => op(1_000 + n),
                   "sender_id" => "app:test",
                   "message_id" => "long",
                   "request_revision" => 1,
                   "text" => "note #{n}"
                 })
      end
    end

    chat.(1, 100)
    {small_rows, small_us} = timing.()
    chat.(101, 400)
    {large_rows, large_us} = timing.()

    assert small_rows == 100 and large_rows == 400
    # Linear is about 4x; quadratic would be about 16x.
    assert large_us < max(small_us, 1_000) * 8, "#{small_us} us for 100, #{large_us} us for 400"
  end
end

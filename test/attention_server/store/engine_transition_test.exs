defmodule AttentionServer.Store.EngineTransitionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{
    Engine,
    JSON,
    Store
  }

  @person "a"
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
  @owner_person "00000000-0000-4000-8000-0000000000aa"
  @derived [
    :advisory_id_index,
    :bounce_deadline_index,
    :bounce_discussed_messages,
    :bounce_origin_entries,
    :bounce_waiting_routes,
    :open_default_index,
    :route_b_index
  ]

  test "forward yields the same engine and response and reports every touched row" do
    engine = normalized_legacy_engine()
    line = forward_line()
    {legacy, reply} = Engine.request_line(engine, @origin, line, 100)
    {next, tracked_reply, touched} = Engine.request_line_with_touches(engine, @origin, line, 100)

    assert {:ok, response} = reply
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)
    assert tracked_reply == reply
    assert next == legacy
    assert touched == full_diff(engine, next)

    assert MapSet.member?(touched, {:row, :messages, {@person, "app:test/worker", "msg"}})
    assert MapSet.member?(touched, {:row, :events, {@person, 1}})
    assert MapSet.member?(touched, {:row, :persons, @person})
  end

  test "a duplicate or refused retry leaves the transition empty" do
    engine = setup_engine(Engine.new())
    {engine, {:ok, _}} = Engine.request_line(engine, @origin, forward_line(), 100)

    for body <- ["body", "different"] do
      {legacy, reply} = Engine.request_line(engine, @origin, forward_line(body), 200)

      assert {^legacy, ^reply, touched} =
               Engine.request_line_with_touches(engine, @origin, forward_line(body), 200)

      assert MapSet.size(touched) == 0
    end
  end

  test "sessionful send, open, discussion and answer retain the old facts and projections" do
    engine = normalized_legacy_engine()
    {:ok, send} = JSON.decode_line(forward_line())
    send = Map.put(send, "session_handle", "00000000-0000-4000-8000-000000000010")
    common = %{"v" => 1, "sender_id" => "app:test/worker", "message_id" => "msg"}

    steps = [
      {@origin, send},
      {@reader, Map.merge(common, %{"type" => "message_open", "request_revision" => 1})},
      {@reader,
       Map.merge(common, %{"type" => "discussion", "request_revision" => 1, "text" => "question"})},
      {@reader, Map.merge(common, %{"type" => "answer", "answer" => nil})}
    ]

    Enum.reduce(Enum.with_index(steps, 1), engine, fn {{actor, frame}, n}, current ->
      frame = Map.put(frame, "op_id", "00000000-0000-4000-8000-00000000000#{n}")
      line = JSON.encode!(frame) <> "\n"
      {legacy, reply} = Engine.request_line(current, actor, line, n * 100)
      assert {:ok, response} = reply
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)

      assert {^legacy, ^reply, touched} =
               Engine.request_line_with_touches(current, actor, line, n * 100)

      assert touched == full_diff(current, legacy)

      legacy
    end)
  end

  test "shuffled transitions agree with the full validator and cold index rebuild" do
    legacy_path = legacy_store_path()
    keyed_path = legacy_path <> ".keyed"
    File.cp!(legacy_path, keyed_path)
    {:ok, legacy} = Store.open(legacy_path)
    {:ok, old} = Store.open(keyed_path, :keyed_cutover)
    :ok = Store.close(old)
    {:ok, keyed} = Store.open(keyed_path, :keyed)
    {:ok, keyed} = Store.enable_change_log(keyed)
    {:ok, legacy} = Store.enable_change_log(legacy)
    {:ok, send} = JSON.decode_line(forward_line())
    send = Map.put(send, "session_handle", "00000000-0000-4000-8000-000000000010")
    :rand.seed(:exsss, {408, 517, 626})
    ids = Enum.map(1..12, &"oracle-#{&1}")

    arrivals =
      for id <- Enum.shuffle(ids),
          do: {@origin, put_in(send["message"]["message_id"], id)}

    responses =
      Enum.map(Enum.shuffle(ids), fn id ->
        frame = %{"v" => 1, "sender_id" => "app:test/worker", "message_id" => id}

        case :rand.uniform(3) do
          1 ->
            {@reader, Map.merge(frame, %{"type" => "answer", "answer" => nil})}

          2 ->
            {@reader,
             Map.merge(frame, %{
               "type" => "discussion",
               "request_revision" => 1,
               "text" => "question"
             })}

          3 ->
            {@origin,
             Map.merge(frame, %{"type" => "lifecycle", "state" => "cleared", "at" => 100})}
        end
      end)

    {legacy, keyed} =
      Enum.reduce(Enum.with_index(arrivals ++ responses, 100), {legacy, keyed}, fn
        {{actor, frame}, operation}, {legacy, keyed} ->
          op =
            "00000000-0000-4000-8000-" <>
              String.pad_leading(Integer.to_string(operation), 12, "0")

          line = frame |> Map.put("op_id", op) |> JSON.encode_line()
          {original, {:ok, response}} = Engine.request_line(legacy.engine, actor, line, operation)

          {changed, {:ok, ^response}, keys} =
            Engine.request_line_with_touches(keyed.engine, actor, line, operation)

          assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)
          assert keys == full_diff(keyed.engine, changed)
          assert full_diff(original, changed) == MapSet.new()
          audit = %{person: @person, actor: actor, action: frame["type"]}

          if frame["type"] == "forward" do
            key = {@person, "app:test/worker", frame["message"]["message_id"]}
            bad_original = put_in(original.messages[key].origin, "unknown")
            bad_changed = put_in(changed.messages[key].origin, "unknown")

            assert {:error, :storage_unavailable, ^legacy} =
                     Store.commit(legacy, bad_original, audit)

            assert {:error, :storage_unavailable, ^keyed} =
                     Store.commit(keyed, {bad_changed, keys}, audit)
          end

          assert {:ok, legacy} = Store.commit(legacy, original, audit)
          assert {:ok, keyed} = Store.commit(keyed, {changed, keys}, audit)
          assert full_diff(legacy.engine, keyed.engine) == MapSet.new()
          {legacy, keyed}
      end)

    :ok = Store.close(legacy)
    :ok = Store.close(keyed)
    assert {:ok, reopened} = Store.open(keyed_path, :keyed)
    assert {:ok, reopened} = Store.enable_change_log(reopened)
    assert index_state(keyed) == index_state(reopened)
    :ok = Store.close(reopened)
  end

  test "sender grants report the grant row" do
    engine = setup_engine(Engine.new())

    {next, :ok, keys} =
      Engine.with_touches(engine, fn current ->
        {:ok, current} = Engine.setup_sender(current, @reader, "app:test/other")
        {current, :ok}
      end)

    assert keys == full_diff(engine, next)
    assert MapSet.member?(keys, {:row, :sender_grants, {@person, @reader}})
  end

  test "operator bootstrap and recovery report every touched record" do
    owner = String.duplicate("c", 64)
    recovered = String.duplicate("d", 64)
    engine = Engine.new()

    {next, :ok, keys} =
      Engine.with_touches(engine, fn current ->
        {:ok, current} = Engine.operator_bootstrap(current, @owner_person, owner)
        {current, :ok}
      end)

    assert keys == full_diff(engine, next)
    assert MapSet.member?(keys, {:row, :owners, @owner_person})

    {recovered_engine, :ok, keys} =
      Engine.with_touches(next, fn current ->
        {:ok, current} = Engine.operator_recover(current, @owner_person, recovered)
        {current, :ok}
      end)

    assert keys == full_diff(next, recovered_engine)
    assert MapSet.member?(keys, {:member, :retired_owner_keys, owner})
  end

  defp index_state(store) do
    engine = store.engine

    route =
      engine.route_b_index
      |> Map.update!(:resolved_messages_by_person, &ordered_rows/1)
      |> Map.update!(:conversations_by_item, &ordered_rows/1)
      |> Map.update!(:pending_messages_by_person, &ordered_rows/1)
      |> Map.update!(:origin_replies_by_person, &ordered_rows/1)
      |> Map.update!(:sent_messages_by_agent, &ordered_rows/1)
      |> Map.update!(:agent_entry_pages, &ordered_rows/1)
      |> Map.update!(:messages_by_origin_session, fn origins ->
        Map.new(origins, fn {origin, sessions} -> {origin, sorted_lists(sessions)} end)
      end)
      |> Map.update!(:request_keys_by_id, &sorted_lists/1)
      |> Map.update!(:push_bindings, fn index ->
        %{
          index
          | queued: :gb_trees.to_list(index.queued),
            claims: :gb_trees.to_list(index.claims)
        }
      end)
      |> Map.update!(
        :unresolved_count_by_person,
        &Map.reject(&1, fn {_, count} -> count == 0 end)
      )

    engine
    |> Map.take(@derived)
    |> Map.put(:route_b_index, route)
    |> Map.update!(:bounce_deadline_index, &:gb_trees.to_list/1)
    |> Map.put(
      :origin_entry_lookup,
      Map.new(engine.origin_feeds, fn {key, feed} -> {key, feed.entry_seqs_by_handle} end)
    )
    |> Map.merge(Map.take(store, [:source_bindings, :upload_links, :authority_index]))
  end

  defp ordered_rows(trees) do
    Map.new(trees, fn {key, tree} -> {key, :gb_trees.to_list(tree)} end)
  end

  defp sorted_lists(rows) do
    Map.new(rows, fn {key, values} -> {key, Enum.sort(values)} end)
  end

  defp normalized_legacy_engine do
    {:ok, reopened} = Store.open(legacy_store_path())
    :ok = Store.close(reopened)
    reopened.engine
  end

  defp legacy_store_path do
    directory =
      Path.join(System.tmp_dir!(), "transition-oracle-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    path = Path.join(directory, "relay.sqlite3")
    {:ok, store} = Store.open(path)
    engine = setup_engine(store.engine)
    audit = %{person: @person, actor: "fixture", action: "setup"}
    {:ok, committed} = Store.commit(store, engine, audit)
    :ok = Store.close(committed)
    path
  end

  defp setup_engine(engine) do
    {:ok, engine} = Engine.setup_person(engine, @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, "app:test/worker")
    engine
  end

  defp forward_line(body \\ "body") do
    JSON.encode!(%{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000001",
      "message" => %{
        "sender_id" => "app:test/worker",
        "message_id" => "msg",
        "created_at" => 1,
        "title" => "title",
        "body" => body
      }
    }) <> "\n"
  end

  defp full_diff(old, next), do: AttentionServer.TouchOracle.diff(old, next)
end

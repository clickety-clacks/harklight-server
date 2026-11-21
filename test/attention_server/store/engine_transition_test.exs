defmodule AttentionServer.Store.EngineTransitionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{AgentChangeLog, ChangeLog, Engine, JSON, Store}

  @person "a"
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
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
    changed = ChangeLog.changed_resources(engine, next, touched)

    expected = ChangeLog.changed_resources(engine, next)
    assert changed == expected
    assert ChangeLog.changes(engine, next, changed) == ChangeLog.changes(engine, next)
    assert ChangeLog.new_resources(engine, next, changed) == ChangeLog.new_resources(engine, next)

    assert AgentChangeLog.changes(engine, next, changed) ==
             AgentChangeLog.changes(engine, next, expected)

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
      changed = ChangeLog.changed_resources(current, legacy, touched)
      expected = ChangeLog.changed_resources(current, legacy)

      assert ChangeLog.changes(current, legacy, changed) ==
               ChangeLog.changes(current, legacy, expected)

      assert ChangeLog.new_resources(current, legacy, changed) ==
               ChangeLog.new_resources(current, legacy, expected)

      assert AgentChangeLog.changes(current, legacy, changed) ==
               AgentChangeLog.changes(current, legacy, expected)

      legacy
    end)
  end

  defp normalized_legacy_engine do
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
    {:ok, reopened} = Store.open(path)
    :ok = Store.close(reopened)
    reopened.engine
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

  defp full_diff(old, next) do
    origin = origin_diff(old.origin_feeds, next.origin_feeds)

    old
    |> Map.from_struct()
    |> Map.drop([:origin_feeds | @derived])
    |> Enum.flat_map(fn {field, value} ->
      current = Map.fetch!(next, field)

      cond do
        value === current ->
          []

        is_struct(value, MapSet) ->
          for key <- MapSet.symmetric_difference(value, current), do: {:member, field, key}

        is_map(value) ->
          keys = MapSet.union(MapSet.new(Map.keys(value)), MapSet.new(Map.keys(current)))

          for key <- keys,
              Map.fetch(value, key) !== Map.fetch(current, key),
              do: {:row, field, key}

        true ->
          [{:field, field}]
      end
    end)
    |> Enum.concat(origin)
    |> MapSet.new()
  end

  defp origin_diff(before, after_feeds) do
    scopes = MapSet.union(MapSet.new(Map.keys(before)), MapSet.new(Map.keys(after_feeds)))

    Enum.flat_map(scopes, fn scope ->
      old = Map.get(before, scope, %{})
      next = Map.get(after_feeds, scope, %{})

      highs =
        if Map.fetch(old, :high) == Map.fetch(next, :high),
          do: [],
          else: [{:origin_feed, scope, :high}]

      old_entries = Map.get(old, :entries, %{})
      next_entries = Map.get(next, :entries, %{})
      keys = MapSet.union(MapSet.new(Map.keys(old_entries)), MapSet.new(Map.keys(next_entries)))

      entries =
        for key <- keys,
            Map.fetch(old_entries, key) != Map.fetch(next_entries, key),
            do: {:origin_feed, scope, {:entry, key}}

      highs ++ entries
    end)
  end
end

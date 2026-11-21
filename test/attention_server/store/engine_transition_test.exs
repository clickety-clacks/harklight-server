defmodule AttentionServer.Store.EngineTransitionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{AgentChangeLog, ChangeLog, Engine, JSON, Relay, Store}
  alias AttentionServer.Store.KeyedMigration
  alias Exqlite.Sqlite3

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
    assert Map.take(changed, Map.keys(expected)) == expected
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

  test "keyed Store commits replay through both real feeds and roll back together" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    {:ok, store} = Store.open(path, :keyed)
    {:ok, store} = Store.enable_change_log(store)

    audit = %{person: @person, actor: @origin, action: "forward"}
    {:ok, frame} = JSON.decode_line(forward_line())
    frame = Map.put(frame, "session_handle", "00000000-0000-4000-8000-000000000010")
    line = JSON.encode!(frame) <> "\n"
    {expected, reply} = Engine.request_line(store.engine, @origin, line, 100)

    {^expected, ^reply, touched} =
      Engine.request_line_with_touches(store.engine, @origin, line, 100)

    assert :ok =
             Sqlite3.execute(store.db, """
             CREATE TRIGGER keyed_agent_fault BEFORE INSERT ON agent_changes
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable, ^store} =
             Store.commit(store, {expected, touched}, audit)

    assert {:ok, 0} = ChangeLog.head(store, @person)
    assert {:ok, 0} = AgentChangeLog.head(store, {@person, @origin})
    assert :ok = Sqlite3.execute(store.db, "DROP TRIGGER keyed_agent_fault")

    assert {:ok, committed} = Store.commit(store, {expected, touched}, audit)
    assert committed.engine == expected
    assert committed.version == store.version + 1
    assert {:ok, person_head} = ChangeLog.head(committed, @person)
    assert person_head > 0
    assert {:ok, 1} = AgentChangeLog.head(committed, {@person, @origin})

    common = %{"v" => 1, "sender_id" => "app:test/worker", "message_id" => "msg"}

    operations = [
      %{"type" => "message_open", "request_revision" => 1},
      %{"type" => "discussion", "request_revision" => 1, "text" => "question"},
      %{"type" => "answer", "answer" => nil}
    ]

    committed =
      Enum.reduce(Enum.with_index(operations, 2), committed, fn {frame, n}, current ->
        attrs = Map.put(frame, "op_id", "00000000-0000-4000-8000-00000000000#{n}")
        line = JSON.encode!(Map.merge(common, attrs)) <> "\n"
        {legacy, reply} = Engine.request_line(current.engine, @reader, line, n * 100)
        assert {:ok, response} = reply
        assert {:ok, %{"type" => "ok"}} = JSON.decode_line(response)

        {^legacy, ^reply, keys} =
          Engine.request_line_with_touches(current.engine, @reader, line, n * 100)

        audit = %{audit | actor: @reader, action: frame["type"]}
        assert {:ok, next} = Store.commit(current, {legacy, keys}, audit)
        assert next.engine == legacy
        assert next.version == current.version + 1
        next
      end)

    :ok = Store.close(committed)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert full_diff(committed.engine, reopened.engine) == MapSet.new()
    assert reopened.version == committed.version
    :ok = Store.close(reopened)
  end

  test "keyed and full Store validation agree on actor and binding perturbations" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    {first, {:ok, _}} = Engine.request_line(legacy.engine, @origin, forward_line(), 100)
    {:ok, frame} = JSON.decode_line(forward_line())

    frame =
      frame
      |> put_in(["message", "message_id"], "another")
      |> Map.put(
        "op_id",
        "00000000-0000-4000-8000-000000000002"
      )

    {next, {:ok, _}} = Engine.request_line(first, @origin, JSON.encode!(frame) <> "\n", 200)
    item = {@person, "app:test/worker", "msg"}
    other = {@person, "app:test/worker", "another"}

    binding = %{
      "source_instance_id" => "source",
      "request_id" => "req",
      "request_revision" => "rev"
    }

    next = put_in(next.messages[item].source_binding, Map.put(binding, "acceptance_seq", 1))
    audit = %{person: @person, actor: @origin, action: "forward"}
    {:ok, legacy} = Store.commit(legacy, next, audit)

    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    {:ok, keyed} = Store.open(path, :keyed)

    invalid = [
      put_in(next.messages[other].source_binding, Map.put(binding, "acceptance_seq", 2)),
      put_in(next.messages[other].origin, "invalid actor")
    ]

    for proposed <- invalid do
      assert {:error, :storage_unavailable, ^keyed} =
               Store.commit(keyed, {proposed, MapSet.new([{:row, :messages, other}])}, audit)

      full = %{legacy | db: keyed.db}
      assert {:error, :storage_unavailable, ^full} = Store.commit(full, proposed, audit)
    end

    :ok = Store.close(keyed)
  end

  test "keyed reference rollback and final owner cleanup respect durable rows" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    {:ok, store} = Store.open(path, :keyed)
    audit = %{person: @person, actor: @origin, action: "forward"}

    {next, {:ok, _}, keys} =
      Engine.request_line_with_touches(store.engine, @origin, forward_line(), 100)

    item = {@person, "app:test/worker", "msg"}
    body = :binary.copy("r", AttentionServer.LargeData.inline_limit() + 1)
    next = replace_body(next, item, body)

    assert :ok =
             Sqlite3.execute(store.db, """
             CREATE TRIGGER keyed_reference_fault BEFORE INSERT ON relay_large_data_refs
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable, ^store} = Store.commit(store, {next, keys}, audit)
    :ok = Store.close(store)
    {:ok, store} = Store.open(path, :keyed)
    assert File.ls!(store.upload_dir) == []
    assert :ok = Sqlite3.execute(store.db, "DROP TRIGGER keyed_reference_fault")

    {:ok, store} = Store.commit(store, {next, keys}, audit)
    assert [filename] = File.ls!(store.upload_dir)
    file = Path.join(store.upload_dir, filename)
    assert File.read!(file) == body
    {:ok, frame} = JSON.decode_line(forward_line())
    frame = frame |> put_in(["message", "message_id"], "second")
    frame = Map.put(frame, "op_id", "00000000-0000-4000-8000-000000000002")

    {next, {:ok, _}, keys} =
      Engine.request_line_with_touches(store.engine, @origin, JSON.encode!(frame) <> "\n", 200)

    second = {@person, "app:test/worker", "second"}
    {:ok, store} = Store.commit(store, {replace_body(next, second, body), keys}, audit)

    {:ok, store} =
      Store.commit(
        store,
        {replace_body(store.engine, item, "short"), MapSet.new([{:row, :messages, item}])},
        audit
      )

    assert File.exists?(file)

    {:ok, store} =
      Store.commit(
        store,
        {replace_body(store.engine, second, "short"), MapSet.new([{:row, :messages, second}])},
        audit
      )

    refute File.exists?(file)
    :ok = Store.close(store)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert map_size(reopened.engine.messages) == 2
    :ok = Store.close(reopened)
  end

  test "retained ownership prevents deleting a device from the keyed store" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    {next, {:ok, _}} = Engine.request_line(legacy.engine, @origin, forward_line(), 100)
    audit = %{person: @person, actor: @origin, action: "forward"}
    {:ok, legacy} = Store.commit(legacy, next, audit)
    assert {:ok, version} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    {:ok, keyed} = Store.open(path, :keyed)
    proposed = %{keyed.engine | devices: Map.delete(keyed.engine.devices, @origin)}
    keys = MapSet.new([{:row, :devices, @origin}])
    assert {:error, :storage_unavailable, ^keyed} = Store.commit(keyed, {proposed, keys}, audit)
    assert {:ok, ^version} = AttentionServer.Store.RecordTable.generation(keyed.db)
    :ok = Store.close(keyed)
  end

  test "direct cursor and conversation writers emit their touched records" do
    engine = normalized_legacy_engine()
    {engine, {:ok, _}} = Engine.request_line(engine, @origin, forward_line(), 100)

    {next, :ok, keys} =
      Engine.with_touches(engine, fn current ->
        {:ok, current, _reply} = Engine.acknowledge(current, @reader, 1, 1)
        {:ok, current} = Engine.open_conversation(current, @reader)
        {:ok, current, _reply} = Engine.acknowledge_conversation(current, @reader, 0, 0)
        {current, :ok}
      end)

    assert keys == full_diff(engine, next)
    assert MapSet.member?(keys, {:row, :cursors, {@person, @reader}})
    assert MapSet.member?(keys, {:member, :conversation_opened, {@person, @reader}})
    assert MapSet.member?(keys, {:row, :conversation_cursors, {@person, @reader}})
  end

  test "draft keyed Relay matches legacy request adapters and reopens with the same facts" do
    legacy_path = legacy_store_path()
    keyed_path = legacy_path <> ".keyed"
    File.cp!(legacy_path, keyed_path)
    {:ok, old} = Store.open(keyed_path)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    flags = legacy_path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))

    {:ok, legacy} =
      Relay.start_link(store_path: legacy_path, feature_flags_path: flags, clock: fn -> 100 end)

    {:ok, keyed} =
      Relay.start_link(
        store_path: keyed_path,
        store_mode: :keyed,
        feature_flags_path: flags,
        clock: fn -> 100 end
      )

    assert Map.has_key?(:sys.get_state(keyed).priority_views, @person)

    {:ok, send} = JSON.decode_line(forward_line())

    send =
      Map.merge(send, %{"session_handle" => "00000000-0000-4000-8000-000000000010", "files" => []})

    target = %{"v" => 1, "sender_id" => "app:test/worker", "message_id" => "msg"}

    operations = [
      {@origin, send},
      {@reader, Map.merge(target, %{"type" => "message_open", "request_revision" => 1})},
      {@reader,
       Map.merge(target, %{"type" => "discussion", "request_revision" => 1, "text" => "question"})},
      {@origin,
       Map.merge(target, %{
         "type" => "conversation_reply",
         "request_revision" => 1,
         "in_reply_to" => 2,
         "text" => "answer to chat"
       })},
      {@reader,
       Map.merge(target, %{"type" => "conversation_read", "read_through_conversation_seq" => 2})},
      {@origin,
       Map.merge(target, %{
         "type" => "conversation_revise",
         "base_revision" => 1,
         "message" => Map.put(send["message"], "title", "updated")
       })},
      {@reader,
       Map.merge(target, %{"type" => "answer", "answer" => nil, "request_revision" => 2})}
    ]

    for {{actor, frame}, n} <- Enum.with_index(operations, 1) do
      line =
        JSON.encode!(Map.put(frame, "op_id", "00000000-0000-4000-8000-00000000000#{n}")) <> "\n"

      assert {:ok, expected} = Relay.request_line(legacy, actor, line)
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(expected)
      assert {:ok, ^expected} = Relay.request_line(keyed, actor, line)
    end

    GenServer.stop(legacy)
    GenServer.stop(keyed)
    {:ok, legacy} = Store.open(legacy_path)
    {:ok, keyed} = Store.open(keyed_path, :keyed)

    assert full_diff(normalize_action_times(legacy.engine), normalize_action_times(keyed.engine)) ==
             MapSet.new()

    :ok = Store.close(legacy)
    :ok = Store.close(keyed)
  end

  test "keyed advisory and closing projections match legacy output" do
    scope = {@person, @origin, "session"}
    old = setup_engine(Engine.new())
    next = put_in(old.devices[@reader].revoked, true)

    next =
      put_in(next.advisory_feeds[scope], %{
        high: 1,
        entries: %{1 => %{"advisory_id" => "id", "sender_id" => "sender", "message_id" => "item"}},
        receipts: MapSet.new()
      })

    next = put_in(next.advisory_id_index[{scope, "id"}], 1)

    keys =
      MapSet.new([
        {:row, :devices, @reader},
        {:advisory, scope, :high},
        {:advisory, scope, {:entry, 1}}
      ])

    keyed = ChangeLog.changed_resources(old, next, keys)
    legacy = ChangeLog.changed_resources(old, next)

    assert ChangeLog.closings(old, next, keys) == ChangeLog.closings(old, next)
    assert AgentChangeLog.changes(old, next, keyed) == AgentChangeLog.changes(old, next, legacy)
    assert map_size(AgentChangeLog.changes(old, next, keyed)) == 1
  end

  defp normalize_action_times(engine) do
    actions =
      Map.new(engine.origin_actions, fn {key, action} ->
        assert is_integer(action.at) and action.at > 0
        {key, %{action | at: 0}}
      end)

    %{engine | origin_actions: actions}
  end

  defp replace_body(engine, item, body) do
    engine
    |> put_in([Access.key!(:messages), item, :message, "body"], body)
    |> put_in([Access.key!(:messages), item, :revisions, 1, "body"], body)
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

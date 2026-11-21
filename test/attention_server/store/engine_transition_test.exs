defmodule AttentionServer.Store.EngineTransitionTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{
    AgentChangeLog,
    BounceDecision,
    BouncePolicy,
    ChangeLog,
    Engine,
    JSON,
    Relay,
    Store
  }

  alias AttentionServer.Store.KeyedMigration
  alias Exqlite.Sqlite3

  @person "a"
  @sender "app:test/worker"
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
    assert {:ok, reopened} = Store.enable_change_log(reopened)
    assert full_diff(committed.engine, reopened.engine) == MapSet.new()
    assert index_state(committed) == index_state(reopened)
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

  test "malformed keyed rows refuse cleanly without advancing generation" do
    path = legacy_store_path()
    {:ok, store} = Store.open(path, :keyed_cutover)
    audit = %{person: @person, actor: @origin, action: "bad facts"}

    for {engine, address} <- [
          {%{store.engine | browser_sign_in_clock_high_water: -5},
           {:field, :browser_sign_in_clock_high_water}},
          {put_in(store.engine.hosted_accounts["broken"], "not a map"),
           {:row, :hosted_accounts, "broken"}},
          {put_in(store.engine.push_bindings["broken"], %{}), {:row, :push_bindings, "broken"}},
          {store.engine, {:field, :unknown}},
          {store.engine, {:origin_feed, {@person, @origin}, {:unknown, 1}}}
        ] do
      touched = MapSet.new([address])

      assert {:error, :storage_unavailable, ^store} =
               Store.commit(store, {engine, touched}, audit)
    end

    assert {:ok, decoded, version} = KeyedMigration.load(store.db)
    assert version == store.version
    assert full_diff(decoded, store.engine) == MapSet.new()
    :ok = Store.close(store)
  end

  test "keyed commits reject malformed priority, cursor and retained item changes" do
    path = legacy_store_path()
    {:ok, initial} = Store.open(path)
    {engine, {:ok, _}} = Engine.request_line(initial.engine, @origin, forward_line(), 100)
    audit = %{person: @person, actor: @origin, action: "forward"}
    {:ok, legacy} = Store.commit(initial, engine, audit)
    keyed_path = path <> ".keyed"
    :ok = Store.close(legacy)
    File.cp!(path, keyed_path)
    {:ok, legacy} = Store.open(path)
    {:ok, prepared} = Store.open(keyed_path)
    assert {:ok, _} = KeyedMigration.convert(prepared)
    :ok = Store.close(prepared)
    {:ok, keyed} = Store.open(keyed_path, :keyed)
    item = {@person, @sender, "msg"}

    changes = [
      invalid_algorithm: &put_in(&1.algorithm_choices[@person], "unknown"),
      invalid_jev_model: &%{&1 | jev_model: 100},
      invalid_cursor_key: &%{&1 | agent_cursor_key: <<0>>},
      negative_order_high: &put_in(&1.free_order_highs[@person], -1),
      order_beyond_high: &put_in(&1.messages[item].free_order, 2_000_000),
      dangling_read: &put_in(&1.chat_read_positions[item], 3),
      dangling_score: &put_in(&1.reply_group_history[item], %{score: 0.9, scores: %{3 => 0.9}}),
      missing_published_item: &%{&1 | messages: Map.delete(&1.messages, item)}
    ]

    for {name, mutate} <- changes do
      next = mutate.(keyed.engine)
      touched = full_diff(keyed.engine, next)
      assert MapSet.size(touched) == 1, inspect(name)

      assert Store.commit(legacy, mutate.(legacy.engine), audit) ==
               {:error, :storage_unavailable, legacy},
             inspect(name)

      assert Store.commit(keyed, {next, touched}, audit) == {:error, :storage_unavailable, keyed},
             inspect(name)
    end

    answer_line =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "answer",
        "sender_id" => @sender,
        "message_id" => "msg",
        "answer" => nil,
        "op_id" => "00000000-0000-4000-8000-000000000002"
      })

    {answered, {:ok, reply}} = Engine.request_line(legacy.engine, @reader, answer_line, 200)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)

    {next, {:ok, ^reply}, touched} =
      Engine.request_line_with_touches(keyed.engine, @reader, answer_line, 200)

    answer_audit = %{person: @person, actor: @reader, action: "answer"}
    assert {:ok, legacy} = Store.commit(legacy, answered, answer_audit)
    assert {:ok, keyed} = Store.commit(keyed, {next, touched}, answer_audit)

    rewrite_timestamp = fn engine -> update_in(engine.messages[item].terminal.at, &(&1 + 1)) end
    rewritten = rewrite_timestamp.(keyed.engine)

    assert {:error, :storage_unavailable, ^legacy} =
             Store.commit(legacy, rewrite_timestamp.(legacy.engine), answer_audit)

    assert {:error, :storage_unavailable, ^keyed} =
             Store.commit(keyed, {rewritten, full_diff(keyed.engine, rewritten)}, answer_audit)

    :ok = Store.close(legacy)
    :ok = Store.close(keyed)
    assert {:ok, reopened} = Store.open(keyed_path, :keyed)
    assert reopened.version == keyed.version
    assert reopened.engine.messages[item].terminal === keyed.engine.messages[item].terminal
    assert :ok = Store.close(reopened)
  end

  test "shuffled transitions agree with the full validator and cold index rebuild" do
    legacy_path = legacy_store_path()
    keyed_path = legacy_path <> ".keyed"
    File.cp!(legacy_path, keyed_path)
    {:ok, legacy} = Store.open(legacy_path)
    {:ok, old} = Store.open(keyed_path)
    assert {:ok, _} = KeyedMigration.convert(old)
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

  test "keyed cleanup retains unresolved markers until their last owner disappears" do
    path = legacy_store_path()
    {:ok, store} = Store.open(path, :keyed_cutover)
    audit = %{person: @person, actor: @origin, action: "forward"}
    item = {@person, @sender, "msg"}
    body = :binary.copy("r", AttentionServer.LargeData.inline_limit() + 1)

    {next, {:ok, _}, keys} =
      Engine.request_line_with_touches(store.engine, @origin, forward_line(), 100)

    {:ok, store} = Store.commit(store, {replace_body(next, item, body), keys}, audit)
    ref = store.engine.messages[item].message["body"]
    marker = AttentionServer.LargeData.persist_refs(ref)
    touched = MapSet.new([{:row, :messages, item}])

    assert {:ok, store} =
             Store.commit(store, {replace_body(store.engine, item, marker), touched}, audit)

    assert File.read(ref.path) == {:ok, body}
    :ok = Store.close(store)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert reopened.engine.messages[item].message["body"] == marker
    assert File.read(ref.path) == {:ok, body}

    assert {:ok, committed} =
             Store.commit(
               reopened,
               {replace_body(reopened.engine, item, "short"), touched},
               audit
             )

    refute File.exists?(ref.path)
    :ok = Store.close(committed)
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

    subscribe =
      JSON.encode!(%{
        "v" => 1,
        "type" => "subscribe_remote",
        "op_id" => "00000000-0000-4000-8000-000000000020",
        "revision" => 1,
        "sender_ids" => ["app:test/worker"]
      }) <> "\n"

    assert {:ok, legacy_session} = Relay.open_session(legacy, @reader)
    assert {:ok, keyed_session} = Relay.open_session(keyed, @reader)
    assert {:ok, [subscribed]} = Relay.session_request_line(legacy, legacy_session, subscribe)
    assert {:ok, %{"type" => "ok", "revision" => 1}} = JSON.decode_line(subscribed)
    assert {:ok, [^subscribed]} = Relay.session_request_line(keyed, keyed_session, subscribe)

    levels =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "levels_set",
        "ref" => "levels",
        "op_id" => "00000000-0000-4000-8000-000000000024",
        "levels" => [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}]
      })

    assert {:ok, configured} = Relay.request_line(legacy, @reader, levels)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(configured)
    assert {:ok, ^configured} = Relay.request_line(keyed, @reader, levels)

    for {id, operation_id} <- [
          {"no-comment", "00000000-0000-4000-8000-000000000021"},
          {"cleared", "00000000-0000-4000-8000-000000000022"}
        ] do
      line =
        send
        |> put_in(["message", "message_id"], id)
        |> Map.put("op_id", operation_id)
        |> JSON.encode_line()

      assert {:ok, accepted} = Relay.request_line(legacy, @origin, line)
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)
      assert {:ok, ^accepted} = Relay.request_line(keyed, @origin, line)
    end

    no_comment =
      JSON.encode_line(%{
        "type" => "answer_no_comment",
        "request_id" => "no-comment",
        "request_revision" => 1
      })

    for duplicate? <- [false, true] do
      assert {:ok, answer} = Relay.request_line(legacy, @reader, no_comment)

      assert {:ok, %{"type" => "ok", "terminal_kind" => "no_comment"} = result} =
               JSON.decode_line(answer)

      assert Map.get(result, "duplicate", false) == duplicate?
      assert {:ok, ^answer} = Relay.request_line(keyed, @reader, no_comment)
    end

    clear =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "lifecycle",
        "op_id" => "00000000-0000-4000-8000-000000000023",
        "sender_id" => "app:test/worker",
        "message_id" => "cleared",
        "state" => "cleared",
        "at" => 98
      })

    assert {:ok, cleared} = Relay.request_line(legacy, @origin, clear)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(cleared)
    assert {:ok, ^cleared} = Relay.request_line(keyed, @origin, clear)

    live_keyed = :sys.get_state(keyed).store
    GenServer.stop(legacy)
    GenServer.stop(keyed)
    {:ok, legacy} = Store.open(legacy_path)
    {:ok, keyed} = Store.open(keyed_path, :keyed)
    {:ok, keyed} = Store.enable_change_log(keyed)

    assert full_diff(normalize_action_times(legacy.engine), normalize_action_times(keyed.engine)) ==
             MapSet.new()

    assert index_state(live_keyed) == index_state(keyed)

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

  test "keyed origin session changes durably publish availability for its open requests" do
    path = legacy_store_path()
    handle = "00000000-0000-4000-8000-000000000010"
    {:ok, store} = Store.open(path)
    {:ok, send} = JSON.decode_line(forward_line())
    line = send |> Map.put("session_handle", handle) |> JSON.encode_line()
    {engine, {:ok, accepted}} = Engine.request_line(store.engine, @origin, line, 100)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)

    assert {:ok, store} =
             Store.commit(store, engine, %{person: @person, actor: @origin, action: "setup"})

    assert {:ok, _} = KeyedMigration.convert(store)
    :ok = Store.close(store)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))

    {:ok, relay} =
      Relay.start_link(store_path: path, store_mode: :keyed, feature_flags_path: flags)

    support = %{
      "type" => "support",
      "op_id" => "00000000-0000-4000-8000-000000000011",
      "session_handle" => handle,
      "route" => "other",
      "cli_version" => "test",
      "unsupported_reason" => nil,
      "origin_listener" => true
    }

    assert {:ok, %{"listening_support" => %{"origin_listener" => true}}} =
             Relay.origin_support(relay, @origin, support)

    supported = :sys.get_state(relay).store.version
    assert {:ok, %{"duplicate" => true}} = Relay.origin_support(relay, @origin, support)
    assert :sys.get_state(relay).store.version == supported

    assert {:ok, %{"vendor_session_id" => "attached-vendor"}} =
             Relay.origin_attachment_update(relay, @origin, %{
               "type" => "attachment_update",
               "op_id" => "00000000-0000-4000-8000-000000000012",
               "session_handle" => handle,
               "vendor_session_id" => "attached-vendor"
             })

    assert :ok = Relay.origin_stream_attach(relay, @origin, handle, self())
    assert {:ok, %{"availability" => "listening"}} = Relay.origin_status(relay, @origin, handle)
    assert :ok = Relay.origin_stream_detach(relay, @origin, handle, self())
    assert :ok = Relay.origin_stream_attach(relay, @origin, :origin, self())
    assert {:ok, %{"availability" => "listening"}} = Relay.origin_status(relay, @origin, handle)
    assert :ok = Relay.origin_stream_detach(relay, @origin, :origin, self())

    assert {:ok, %{"availability" => "not_listening", "last_detached_at" => at}} =
             Relay.origin_status(relay, @origin, handle)

    assert is_integer(at)
    assert {:ok, changes, 5} = Relay.conversation_events(relay, @reader, 0)

    assert Enum.map(changes, & &1["availability"]) ==
             ~w(not_listening listening not_listening listening not_listening)

    before = :sys.get_state(relay).store
    scope = {@person, @origin}
    assert {:ok, agent_head} = AgentChangeLog.head(before, scope)
    assert {:ok, person_head} = ChangeLog.head(before, @person)
    assert :ok = Relay.origin_session_gone(relay, @origin, handle)

    live = :sys.get_state(relay).store
    assert live.engine.origin_sessions[{@person, @origin, handle}].gone
    seq = live.engine.conversation_highs[@person]

    assert %{payload: %{"kind" => "availability", "availability" => "gone"}} =
             live.engine.conversation_events[{@person, seq}]

    assert {:ok, [%{resource: {:message, _}}, %{resource: {:entry, ^seq}}], false, _} =
             AgentChangeLog.read(live, scope, agent_head, 10, handle)

    assert {:ok, person_next} = ChangeLog.head(live, @person)
    assert person_next > person_head
    assert :ok = Relay.origin_session_gone(relay, @origin, handle)
    assert :sys.get_state(relay).store.version == live.version
    GenServer.stop(relay)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert {:ok, reopened} = Store.enable_change_log(reopened)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    assert index_state(live) == index_state(reopened)
    :ok = Store.close(reopened)
  end

  test "keyed timer defers until support, retries a failed advisory commit and retains acknowledgement" do
    path = legacy_store_path()
    handle = "00000000-0000-4000-8000-000000000010"
    {:ok, store} = Store.open(path)

    {:ok, levels} =
      BouncePolicy.normalize_levels([
        %{"name" => "Follow up", "lower_bound" => 0, "bounce_after_days" => 1}
      ])

    assert {:ok, engine, _} = BounceDecision.configure_levels(store.engine, @person, levels, 100)
    {:ok, send} = JSON.decode_line(forward_line())

    line =
      send
      |> Map.put("session_handle", handle)
      |> put_in(["message", "message_type"], "fyi")
      |> put_in(["message", "answer_ui"], %{
        "schema" => "attention-client.actions/1",
        "actions" => [
          %{
            "id" => "seen",
            "label" => "Seen",
            "kind" => "value",
            "value" => %{"acknowledgment" => "seen"}
          }
        ]
      })
      |> JSON.encode_line()

    {engine, {:ok, accepted}} = Engine.request_line(engine, @origin, line, 100)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)

    assert {:ok, engine, _} =
             Engine.origin_support(engine, @origin, %{
               "type" => "support",
               "op_id" => "00000000-0000-4000-8000-000000000011",
               "session_handle" => handle,
               "route" => "claude-code",
               "cli_version" => "test",
               "unsupported_reason" => "route unavailable",
               "advisory_v1" => true
             })

    selected =
      BounceDecision.record_scores(
        engine,
        @person,
        [%{"sender_id" => "app:test/worker", "message_id" => "msg", "score" => 0.9}],
        100
      )

    assert due_at = BounceDecision.next_deadline(selected)

    assert {:ok, store} =
             Store.commit(store, selected, %{person: @person, actor: @origin, action: "setup"})

    assert {:ok, _} = KeyedMigration.convert(store)
    :ok = Store.close(store)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 100)

    opts = [
      store_path: path,
      store_mode: :keyed,
      feature_flags_path: flags,
      clock: fn -> :atomics.get(clock, 1) end
    ]

    assert {:ok, relay} = Relay.start_link(opts)
    state = :sys.get_state(relay)
    scope = {@person, @origin}
    session = {@person, @origin, handle}
    :atomics.put(clock, 1, due_at)
    send(relay, {:bounce_due, state.bounce_deadline_generation})
    deferred = :sys.get_state(relay).store
    assert deferred.version == state.store.version + 1
    assert MapSet.size(deferred.engine.bounce_waiting_routes[session]) == 1
    assert BounceDecision.next_deadline(deferred.engine) == nil
    GenServer.stop(relay)

    :atomics.put(clock, 1, 100)
    assert {:ok, relay} = Relay.start_link(opts)

    assert :sys.get_state(relay).store.engine.bounce_waiting_routes ==
             deferred.engine.bounce_waiting_routes

    assert {:ok, %{"listening_support" => %{"unsupported_reason" => nil}}} =
             Relay.origin_support(relay, @origin, %{
               "type" => "support",
               "op_id" => "00000000-0000-4000-8000-000000000015",
               "session_handle" => handle,
               "route" => "claude-code",
               "cli_version" => "test",
               "unsupported_reason" => nil,
               "advisory_v1" => true
             })

    state = :sys.get_state(relay)
    assert state.store.engine.bounce_waiting_routes == %{}
    assert {:ok, durable, generation} = KeyedMigration.load(state.store.db)
    assert generation == state.store.version
    assert durable.bounce_waiting_routes == %{}
    assert BounceDecision.next_deadline(state.store.engine) == due_at
    assert {:ok, decision_rev} = AgentChangeLog.head(state.store, scope)
    :atomics.put(clock, 1, due_at)

    assert :ok =
             Sqlite3.execute(state.store.db, """
             CREATE TRIGGER keyed_advisory_fault BEFORE INSERT ON agent_changes
             WHEN NEW.resource_id LIKE 'advisory:%'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    send(relay, {:bounce_due, state.bounce_deadline_generation})
    retry = :sys.get_state(relay)

    assert {:ok, %{"cursor" => 0, "entries" => []}} =
             Relay.origin_advisory_feed(relay, @origin, handle, 0)

    assert retry.store.version == state.store.version
    assert {:ok, ^decision_rev} = AgentChangeLog.head(retry.store, scope)
    assert :ok = Sqlite3.execute(state.store.db, "DROP TRIGGER keyed_advisory_fault")
    send(relay, {:bounce_due, retry.bounce_deadline_generation})
    store = :sys.get_state(relay).store

    assert {:ok, %{"cursor" => 1, "entries" => [%{"advisory_id" => id, "delivered" => false}]}} =
             Relay.origin_advisory_feed(relay, @origin, handle, 0)

    assert get_in(store.engine.advisory_feeds, [
             {@person, @origin, handle},
             :entries,
             1,
             "decided_at_rev"
           ]) == decision_rev

    assert {:ok, before_ack} = AgentChangeLog.head(store, scope)

    assert {:ok, %{"already_delivered" => false}} =
             Relay.origin_advisory_ack(relay, @origin, handle, id)

    committed = :sys.get_state(relay).store

    assert {:ok, [%{resource: {:message, _}}, %{resource: {:advisory, ^id}}], false, _} =
             AgentChangeLog.read(committed, scope, before_ack, 10, handle)

    GenServer.stop(relay)
    assert {:ok, reopened} = Relay.start_link(opts)

    assert {:ok, %{"cursor" => 1, "entries" => [%{"advisory_id" => ^id, "delivered" => true}]}} =
             Relay.origin_advisory_feed(reopened, @origin, handle, 0)

    assert {:ok, %{"already_delivered" => true}} =
             Relay.origin_advisory_ack(reopened, @origin, handle, id)

    live = :sys.get_state(reopened).store
    assert live.version == committed.version
    assert index_state(live) == index_state(committed)
    GenServer.stop(reopened)
  end

  test "keyed no-comment refuses an id shared by two senders before and after reopen" do
    path = legacy_store_path()
    {:ok, old} = Store.open(path)
    {:ok, engine} = Engine.setup_sender(old.engine, @origin, "app:test/other")
    audit = %{person: @person, actor: @origin, action: "setup"}
    {:ok, old} = Store.commit(old, engine, audit)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    opts = [store_path: path, store_mode: :keyed, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)
    assert {:ok, accepted} = Relay.request_line(relay, @origin, forward_line())
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)
    {:ok, second} = JSON.decode_line(forward_line())

    line =
      second
      |> Map.put("op_id", "00000000-0000-4000-8000-000000000021")
      |> put_in(["message", "sender_id"], "app:test/other")
      |> JSON.encode_line()

    assert {:ok, accepted} = Relay.request_line(relay, @origin, line)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)

    request =
      JSON.encode_line(%{
        "type" => "answer_no_comment",
        "request_id" => "msg",
        "request_revision" => 1
      })

    assert {:ok, refused} = Relay.request_line(relay, @reader, request)
    assert {:ok, %{"type" => "error", "reason" => "not_found"}} = JSON.decode_line(refused)
    committed = :sys.get_state(relay).store
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)
    assert {:ok, ^refused} = Relay.request_line(reopened, @reader, request)
    assert :sys.get_state(reopened).store.version == committed.version
    GenServer.stop(reopened)
  end

  test "keyed origin delivery acknowledgements and file use survive reopen" do
    path = legacy_store_path()
    {:ok, legacy} = Relay.start_link(store_path: path, clock: fn -> 100 end)
    {:ok, send} = JSON.decode_line(forward_line())

    line =
      send
      |> Map.put("session_handle", "00000000-0000-4000-8000-000000000010")
      |> JSON.encode_line()

    assert {:ok, accepted} = Relay.request_line(legacy, @origin, line)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)

    line =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "discussion",
        "op_id" => "00000000-0000-4000-8000-000000000012",
        "sender_id" => "app:test/worker",
        "message_id" => "msg",
        "request_revision" => 1,
        "text" => "Inspect the attachment",
        "files" => [%{"name" => "image.bin", "data_base64" => Base.encode64(<<0, 1, 255>>)}]
      })

    assert {:ok, discussed} = Relay.request_line(legacy, @reader, line)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(discussed)
    GenServer.stop(legacy)
    {:ok, old} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)

    assert {:ok, %{"entries" => [_, %{"delivered" => false, "files" => [file]}]}} =
             Relay.origin_feed(relay, @origin, 0)

    assert Base.decode64!(file["data_base64"]) == <<0, 1, 255>>
    ack2 = %{"op_id" => "00000000-0000-4000-8000-000000000013", "outcome" => "handed"}
    assert {:ok, %{"status" => "recorded"}} = Relay.origin_ack2(relay, @origin, 2, ack2)
    transfer = [%{"name" => "image.bin", "status" => "transferred"}]
    assert {:ok, %{"status" => "delivered"}} = Relay.origin_ack(relay, @origin, 2, transfer)
    outcome = %{"status" => "used"}
    op = "00000000-0000-4000-8000-000000000014"

    assert {:ok, %{"status" => "used"}} =
             Relay.origin_file_use(relay, @origin, 2, "image.bin", outcome, op)

    committed = :sys.get_state(relay).store
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)
    live = :sys.get_state(reopened).store
    assert full_diff(committed.engine, live.engine) == MapSet.new()
    assert index_state(committed) == index_state(live)
    assert {:ok, %{"duplicate" => true}} = Relay.origin_ack2(reopened, @origin, 2, ack2)

    assert {:ok, %{"status" => "already_delivered"}} =
             Relay.origin_ack(reopened, @origin, 2, transfer)

    assert {:ok, %{"duplicate" => true}} =
             Relay.origin_file_use(reopened, @origin, 2, "image.bin", outcome, op)

    assert :sys.get_state(reopened).store.version == committed.version
    GenServer.stop(reopened)
  end

  test "keyed agent v2 send, session end and retract replay from durable state" do
    path = legacy_store_path()
    {:ok, old} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)
    handle = "00000000-0000-4000-8000-000000000010"

    body = %{
      "sender" => "app:test/worker",
      "session" => handle,
      "message" => %{"title" => "Sent over v2", "created_at" => 1}
    }

    assert {:ok, 200, %{"message" => %{"item_id" => id}}, false} =
             agent_write(relay, :send, %{}, body, "00000000-0000-4000-8000-000000000011")

    end_op = "00000000-0000-4000-8000-000000000012"

    assert {:ok, 200, ended, false} =
             agent_write(relay, :session_end, %{session: handle}, %{}, end_op)

    clear_op = "00000000-0000-4000-8000-000000000013"

    assert {:ok, 200, %{"message" => %{"terminal" => %{"state" => "retracted"}}} = retracted,
            false} =
             agent_write(relay, :retract, %{item_id: id}, %{}, clear_op)

    committed = :sys.get_state(relay).store
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)

    assert_agent_receipt(
      agent_write(reopened, :session_end, %{session: handle}, %{}, end_op),
      ended
    )

    assert_agent_receipt(
      agent_write(reopened, :retract, %{item_id: id}, %{}, clear_op),
      retracted
    )

    live = :sys.get_state(reopened).store
    assert live.version == committed.version
    assert full_diff(committed.engine, live.engine) == MapSet.new()
    assert index_state(committed) == index_state(live)
    GenServer.stop(reopened)
  end

  test "keyed default timer commits only due answers and retries failed writes" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 100)

    opts = [
      store_path: path,
      store_mode: :keyed,
      feature_flags_path: flags,
      clock: fn -> :atomics.get(clock, 1) end
    ]

    {:ok, relay} = Relay.start_link(opts)
    handle = "00000000-0000-4000-8000-000000000010"
    {:ok, frame} = JSON.decode_line(forward_line())

    for {id, at, op} <- [
          {"due", 20_001, "00000000-0000-4000-8000-000000000021"},
          {"later", 40_002, "00000000-0000-4000-8000-000000000022"}
        ] do
      line =
        frame
        |> Map.merge(%{"session_handle" => handle, "op_id" => op})
        |> Map.update!("message", fn message ->
          Map.merge(message, %{
            "message_id" => id,
            "message_type" => "request",
            "answer_ui" => %{
              "schema" => "attention-client.actions/1",
              "actions" => [
                %{"id" => "later", "label" => "Later", "kind" => "value", "value" => %{"n" => 2}}
              ]
            },
            "default" => %{"at" => at, "value" => %{"n" => 2}}
          })
        end)
        |> JSON.encode_line()

      assert {:ok, reply} = Relay.request_line(relay, @origin, line)
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
    end

    before = :sys.get_state(relay)
    :atomics.put(clock, 1, 20_001)

    assert :ok =
             Sqlite3.execute(before.store.db, """
             CREATE TRIGGER keyed_default_fault BEFORE INSERT ON relay_audit
             WHEN NEW.action = 'default_answer_sweep'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    send(relay, {:default_sweep, before.default_timer_generation})
    retry = :sys.get_state(relay)
    assert retry.store.version == before.store.version
    assert :ok = Sqlite3.execute(retry.store.db, "DROP TRIGGER keyed_default_fault")
    send(relay, {:default_sweep, retry.default_timer_generation})
    live = :sys.get_state(relay).store
    assert live.version == before.store.version + 1

    assert %{terminal: %{by: "default", answer: %{"n" => 2}}} =
             live.engine.messages[{@person, "app:test/worker", "due"}]

    assert %{terminal: nil} = live.engine.messages[{@person, "app:test/worker", "later"}]

    GenServer.stop(relay)
    assert {:ok, reopened} = Store.open(path, :keyed)
    assert {:ok, reopened} = Store.enable_change_log(reopened)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    assert index_state(live) == index_state(reopened)
    :ok = Store.close(reopened)
  end

  test "keyed client API answer commits for a registered agent and replays after reopen" do
    path = legacy_store_path()
    agent = String.duplicate("c", 64)
    origin = String.duplicate("d", 64)
    owner = String.duplicate("e", 64)
    person = "00000000-0000-4000-8000-000000000029"
    {:ok, legacy} = Store.open(path)
    {:ok, engine} = Engine.operator_bootstrap(legacy.engine, person, owner)
    {:ok, engine} = Engine.setup_device(engine, person, origin)
    {:ok, engine} = Engine.setup_sender(engine, origin, "app:test/worker")

    {:ok, engine} =
      Engine.owner_register_agent(
        engine,
        person,
        owner,
        "00000000-0000-4000-8000-000000000030",
        agent
      )

    {:ok, engine} =
      Engine.owner_replace_agent_grants(engine, person, owner, agent, 1, ["app:test/worker"], [
        "app:test/worker"
      ])

    {:ok, legacy} = Store.commit(legacy, engine, %{person: person, actor: owner, action: "setup"})
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)
    {:ok, send} = JSON.decode_line(forward_line())

    line =
      send
      |> Map.put("session_handle", "00000000-0000-4000-8000-000000000010")
      |> JSON.encode_line()

    assert {:ok, accepted} = Relay.request_line(relay, origin, line)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)

    answer = %{
      "v" => 1,
      "type" => "answer",
      "sender_id" => "app:test/worker",
      "message_id" => "msg",
      "request_revision" => 1,
      "answer" => nil,
      "op_id" => "00000000-0000-4000-8000-000000000031"
    }

    assert {:ok, %{"seq" => 2}} = Relay.client_agent_answer(relay, agent, answer)
    live = :sys.get_state(relay).store
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)

    assert {:ok, %{"seq" => 2, "duplicate" => true}} =
             Relay.client_agent_answer(reopened, agent, answer)

    assert :sys.get_state(reopened).store.version == live.version

    assert :sys.get_state(reopened).store.engine.messages[{person, "app:test/worker", "msg"}].terminal.actor ==
             agent

    GenServer.stop(reopened)
  end

  test "keyed agent v2 revision, result and file-use replay after reopen" do
    path = legacy_store_path()
    {:ok, old} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)

    body = %{
      "sender" => "app:test/worker",
      "session" => "00000000-0000-4000-8000-000000000010",
      "message" => %{"title" => "Sent over v2", "created_at" => 1}
    }

    assert {:ok, 200, %{"message" => %{"item_id" => id}}, false} =
             agent_write(relay, :send, %{}, body, "00000000-0000-4000-8000-000000000041")

    item = %{item_id: id}
    revised = %{"message" => %{"title" => "Revised over v2", "created_at" => 1}}
    revise_op = "00000000-0000-4000-8000-000000000042"

    assert {:ok, 200, %{"message" => %{"revision" => 2}} = revision, false} =
             agent_write(relay, :revise, item, revised, revise_op, ["1"])

    state = :sys.get_state(relay)
    scope = {@person, @origin}
    alias AttentionServer.ClientAPI.AgentResources
    assert {:ok, {_, sender, message_id} = key} = AgentResources.own_item_key(state, scope, id)

    frame = %{
      "v" => 1,
      "sender_id" => sender,
      "message_id" => message_id,
      "request_revision" => 2
    }

    discuss =
      Map.merge(frame, %{
        "type" => "discussion",
        "op_id" => "00000000-0000-4000-8000-000000000043",
        "text" => "Attachment for the revised request",
        "files" => [%{"name" => "sample.bin", "data_base64" => Base.encode64(<<0, 5, 8>>)}]
      })

    assert {:ok, reply} = Relay.request_line(relay, @reader, JSON.encode_line(discuss))
    assert {:ok, %{"type" => "ok", "seq" => seq}} = JSON.decode_line(reply)

    assert {:ok, %{"entry_id" => entry, "files" => [%{"file_id" => file}]}} =
             AgentResources.entry(:sys.get_state(relay), scope, key, seq)

    turn = Map.put(item, :entry_id, entry)

    assert {:ok, 200, %{"stage_outcome" => "handed"}, false} =
             agent_write(
               relay,
               :chat_receipt,
               turn,
               %{"outcome" => "handed"},
               "00000000-0000-4000-8000-000000000044"
             )

    file_body = %{"file_id" => file, "transfer" => "transferred", "use" => "used"}
    file_op = "00000000-0000-4000-8000-000000000045"

    assert {:ok, 200, %{"file_id" => ^file, "transfer" => "transferred", "use" => "used"} = use,
            false} =
             agent_write(relay, :file_use, turn, file_body, file_op)

    answer =
      Map.merge(frame, %{
        "type" => "answer",
        "answer" => nil,
        "op_id" => "00000000-0000-4000-8000-000000000046"
      })

    assert {:ok, reply} = Relay.request_line(relay, @reader, JSON.encode_line(answer))
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
    result_body = %{"status" => "applied"}
    result_op = "00000000-0000-4000-8000-000000000047"

    assert {:ok, 200, %{"message" => %{"result" => %{"status" => "applied"}}} = result, false} =
             agent_write(relay, :result, item, result_body, result_op)

    live = :sys.get_state(relay).store
    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)

    assert_agent_receipt(
      agent_write(reopened, :revise, item, revised, revise_op, ["1"]),
      revision
    )

    assert_agent_receipt(agent_write(reopened, :file_use, turn, file_body, file_op), use)

    assert_agent_receipt(agent_write(reopened, :result, item, result_body, result_op), result)

    cold = :sys.get_state(reopened).store
    assert cold.version == live.version
    assert full_diff(cold.engine, live.engine) == MapSet.new()
    assert index_state(cold) == index_state(live)
    GenServer.stop(reopened)
  end

  test "keyed first agent feed delivery rolls back and replays one receipt" do
    path = legacy_store_path()
    {:ok, old} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(old)
    :ok = Store.close(old)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)
    handle = "00000000-0000-4000-8000-000000000010"
    {:ok, send} = JSON.decode_line(forward_line())
    line = send |> Map.put("session_handle", handle) |> JSON.encode_line()
    assert {:ok, accepted} = Relay.request_line(relay, @origin, line)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(accepted)
    scope = {@person, @origin}
    {:ok, cursor} = AgentChangeLog.head(:sys.get_state(relay).store, scope)

    discuss = %{
      "v" => 1,
      "type" => "discussion",
      "sender_id" => "app:test/worker",
      "message_id" => "msg",
      "request_revision" => 1,
      "op_id" => "00000000-0000-4000-8000-000000000051",
      "text" => "question"
    }

    assert {:ok, line} = Relay.request_line(relay, @reader, JSON.encode_line(discuss))
    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(line)
    before = :sys.get_state(relay).store

    ctx = %{
      relay: relay,
      principal: %{kind: :device, key: @origin, person: @person},
      params: %{session: handle}
    }

    alias AttentionServer.AgentAPI.FeedStep

    assert :ok =
             Sqlite3.execute(before.db, """
             CREATE TRIGGER keyed_agent_delivery_fault BEFORE INSERT ON relay_audit
             WHEN NEW.action = 'agent_feed_delivered'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :delivery_unavailable} = FeedStep.read(ctx, cursor, 1)
    assert :sys.get_state(relay).store.version == before.version
    assert :ok = Sqlite3.execute(before.db, "DROP TRIGGER keyed_agent_delivery_fault")

    assert {:ok, [%{"data" => %{"delivery" => %{"delivered" => false}}}], false, _} =
             FeedStep.read(ctx, cursor, 1)

    live = :sys.get_state(relay).store
    assert live.version == before.version + 1

    assert %{
             payload: %{
               "kind" => "delivery_ack",
               "stage" => "ack1",
               "event_id" => "2",
               "request_revision" => 1
             }
           } = live.engine.conversation_events[{@person, 2}]

    GenServer.stop(relay)
    {:ok, reopened} = Relay.start_link(opts)

    assert {:ok, [%{"data" => %{"delivery" => %{"delivered" => true}}}], false, _} =
             FeedStep.read(%{ctx | relay: reopened}, cursor, 1)

    assert :sys.get_state(reopened).store.version == live.version
    GenServer.stop(reopened)
  end

  test "keyed push registration, reassignment and claim retry use scoped bindings" do
    path = legacy_store_path()
    {:ok, store} = Store.open(path)
    other = String.duplicate("d", 64)
    {:ok, engine} = Engine.setup_person(store.engine, "other")
    {:ok, engine} = Engine.setup_device(engine, "other", other)
    {:ok, store} = Store.commit(store, engine, %{person: "other", actor: other, action: "setup"})
    assert {:ok, _} = KeyedMigration.convert(store)
    :ok = Store.close(store)
    {:ok, relay} = Relay.start_link(store_path: path, store_mode: :keyed)
    :ok = Relay.enable_push(relay)
    address = String.duplicate("ab", 32)

    for {device, address, op} <- [
          {@origin, address, "00000000-0000-4000-8000-000000000061"},
          {@reader, address, "00000000-0000-4000-8000-000000000062"},
          {other, String.duplicate("cd", 32), "00000000-0000-4000-8000-000000000063"}
        ] do
      frame = %{
        "v" => 1,
        "type" => "push_register",
        "op_id" => op,
        "revision" => 1,
        "profile_id" => "harklight-sandbox",
        "address_kind" => "apns_token",
        "address" => address
      }

      assert {:ok, reply} = Relay.request_line(relay, device, JSON.encode_line(frame))
      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
    end

    assert {:ok, reply} = Relay.request_line(relay, @origin, forward_line())
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
    before = :sys.get_state(relay).store
    refute before.engine.push_bindings[{@person, @origin}].active
    assert before.engine.push_bindings[{@person, @reader}].pending_high == 1
    assert before.engine.push_bindings[{"other", other}].pending_high == 0
    now = System.system_time(:millisecond) + 1_000

    assert :ok =
             Sqlite3.execute(before.db, """
             CREATE TRIGGER keyed_push_claim_fault BEFORE INSERT ON relay_audit
             WHEN NEW.action = 'push_claim'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable} = Relay.claim_push(relay, now)
    assert :sys.get_state(relay).store.version == before.version
    assert :ok = Sqlite3.execute(before.db, "DROP TRIGGER keyed_push_claim_fault")
    assert {:ok, %{device: @reader, address: ^address} = first} = Relay.claim_push(relay, now)
    assert {:ok, nil} = Relay.claim_push(relay, now + 29_999)
    assert {:ok, %{device: @reader} = retried} = Relay.claim_push(relay, now + 30_000)
    assert retried.claim_id != first.claim_id
    assert :ok = Relay.finish_push(relay, first, %{state: "provider_accepted"}, now + 30_001)
    assert :ok = Relay.finish_push(relay, retried, %{state: "provider_accepted"}, now + 30_001)

    remove =
      JSON.encode_line(%{
        "v" => 1,
        "type" => "push_remove",
        "op_id" => "00000000-0000-4000-8000-000000000064",
        "revision" => 2
      })

    assert {:ok, reply} = Relay.request_line(relay, @reader, remove)
    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
    live = :sys.get_state(relay).store
    assert live.engine.push_bindings[{@person, @reader}].revision == 2
    GenServer.stop(relay)
    {:ok, reopened} = Store.open(path, :keyed)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    assert index_state(live) == index_state(reopened)
    :ok = Store.close(reopened)
  end

  test "keyed owner agent grants retain revision and authority after reopen" do
    path = legacy_store_path()
    {:ok, store} = Store.open(path)
    person = "00000000-0000-4000-8000-000000000071"
    owner = String.duplicate("e", 64)
    agent = String.duplicate("f", 64)
    id = "00000000-0000-4000-8000-000000000072"
    assert {:ok, engine} = Engine.operator_bootstrap(store.engine, person, owner)

    assert {:ok, store} =
             Store.commit(store, engine, %{person: person, actor: owner, action: "bootstrap"})

    assert {:ok, _} = KeyedMigration.convert(store)
    :ok = Store.close(store)
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, store_mode: :keyed, feature_flags_path: flags]
    {:ok, relay} = Relay.start_link(opts)

    assert :ok = Relay.owner_register_agent(relay, person, owner, id, agent)

    assert {:error, :forbidden} =
             Relay.owner_replace_agent_grants(relay, person, @reader, agent, 1, [], [])

    assert :ok =
             Relay.owner_replace_agent_grants(relay, person, owner, agent, 1, ["app:test/*"], [])

    assert {:error, :stale_or_invalid_grant} =
             Relay.owner_replace_agent_grants(relay, person, owner, agent, 1, [], [])

    principal = %{kind: :agent, key: agent, person: person}
    assert :ok = ChangeLog.subscribe_closing(principal)
    assert :ok = Relay.owner_revoke_agent(relay, person, owner, agent)
    assert_receive {:client_closing, {:agent, ^agent}, :revoked}
    assert :ok = ChangeLog.unsubscribe_closing(principal)
    live = :sys.get_state(relay).store
    assert :ok = Relay.owner_revoke_agent(relay, person, owner, agent)
    assert :sys.get_state(relay).store.version == live.version
    GenServer.stop(relay)
    {:ok, reopened} = Store.open(path, :keyed)

    assert %{revoked: true, grant_revision: 3, enrollment_revision: 2, grant_history: history} =
             reopened.engine.agents[agent]

    assert Enum.map(history, & &1.action) == ~w(register replace revoke)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    :ok = Store.close(reopened)
  end

  test "keyed manual enrollment retries and approval preserve both authority and history" do
    path = legacy_store_path()
    {:ok, store} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(store)
    :ok = Store.close(store)
    opts = [store_path: path, store_mode: :keyed]
    {:ok, relay} = Relay.start_link(opts)
    owner = enrollment_certificate("owner")
    device = enrollment_certificate("device")
    {:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = AttentionServer.DeviceIdentity.from_certificate(device)

    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)
    assert {:ok, first} = Relay.pending_enroll(relay, device, "first")
    assert {:ok, renewed} = Relay.pending_enroll(relay, device, "renewed")
    assert first.approval_code == renewed.approval_code
    code = renewed.approval_code
    assert {:error, :not_found} = Relay.pending_status(relay, first.status_secret)

    before = :sys.get_state(relay).store

    assert :ok =
             Sqlite3.execute(before.db, """
             CREATE TRIGGER keyed_approval_fault BEFORE INSERT ON relay_audit
             WHEN NEW.action = 'owner_device_approve'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable} =
             Relay.owner_pending_approve(relay, person, owner_key, code, device_key, nil, [
               @sender
             ])

    assert :sys.get_state(relay).store.version == before.version
    assert :ok = Sqlite3.execute(before.db, "DROP TRIGGER keyed_approval_fault")

    assert {:ok, ^device_key} =
             Relay.owner_pending_approve(relay, person, owner_key, code, device_key, nil, [
               @sender
             ])

    live = :sys.get_state(relay).store
    GenServer.stop(relay)

    {:ok, reopened} = Relay.start_link(opts)

    assert {:ok, ^device_key} =
             Relay.owner_pending_approve(reopened, person, owner_key, code, device_key, nil, [
               @sender
             ])

    store = :sys.get_state(reopened).store
    assert store.version == live.version

    assert %{state: :approved, person: ^person, key: ^device_key, label: "renewed"} =
             store.engine.pending_history[code]

    assert MapSet.member?(store.engine.sender_grants[{person, device_key}], @sender)
    assert full_diff(live.engine, store.engine) == MapSet.new()
    GenServer.stop(reopened)
  end

  test "keyed staged upload through Relay persists one authorization across retry and reopen" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    opts = [store_path: path, store_mode: :keyed]
    {:ok, relay} = Relay.start_link(opts)
    bytes = <<0, 255, 42>>
    sha = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    source = Path.join(Path.dirname(path), "source.bin")
    File.write!(source, bytes)

    attrs = %{
      "op_id" => "00000000-0000-4000-8000-000000000078",
      "sender_id" => @sender,
      "message_id" => "attachment",
      "revision" => 1,
      "name" => "attachment.bin"
    }

    assert {:ok, lease} = Relay.upload_begin(relay, @origin, attrs, byte_size(bytes))

    assert {:ok, %{"upload_id" => id, "duplicate" => false}} =
             Relay.upload_file(relay, @origin, attrs, source, byte_size(bytes), sha, lease)

    assert :ok = Relay.upload_end(relay, lease)
    live = :sys.get_state(relay).store
    GenServer.stop(relay)

    {:ok, reopened} = Relay.start_link(opts)
    assert {:ok, duplicate_lease} = Relay.upload_begin(reopened, @origin, attrs, byte_size(bytes))

    assert {:ok, %{"upload_id" => ^id, "duplicate" => true}} =
             Relay.upload_file(
               reopened,
               @origin,
               attrs,
               source,
               byte_size(bytes),
               sha,
               duplicate_lease
             )

    assert :ok = Relay.upload_end(reopened, duplicate_lease)
    stored = :sys.get_state(reopened).store
    assert stored.version == live.version
    assert full_diff(live.engine, stored.engine) == MapSet.new()
    assert {:ok, upload_path} = Store.get_upload(stored, id)
    assert File.read!(upload_path) == bytes
    GenServer.stop(reopened)
  end

  test "keyed device removal ends its sessions and forgets abandoned uploads in one generation" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    opts = [store_path: path, store_mode: :keyed]
    {:ok, relay} = Relay.start_link(opts)
    handle = "00000000-0000-4000-8000-000000000079"
    {:ok, frame} = JSON.decode_line(forward_line())

    assert {:ok, reply} =
             Relay.request_line(
               relay,
               @origin,
               JSON.encode_line(Map.put(frame, "session_handle", handle))
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)

    attrs = %{
      "op_id" => "00000000-0000-4000-8000-000000000080",
      "sender_id" => @sender,
      "message_id" => "abandoned",
      "revision" => 1,
      "name" => "abandoned.bin"
    }

    data = "unreferenced"
    source = Path.join(Path.dirname(path), "source.bin")
    File.write!(source, data)
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    assert {:ok, lease} = Relay.upload_begin(relay, @origin, attrs, byte_size(data))

    assert {:ok, %{"upload_id" => id}} =
             Relay.upload_file(relay, @origin, attrs, source, byte_size(data), hash, lease)

    assert :ok = Relay.upload_end(relay, lease)

    before = :sys.get_state(relay).store
    assert {:ok, upload_path} = Store.get_upload(before, id)

    assert :ok =
             Sqlite3.execute(before.db, """
             CREATE TRIGGER keyed_device_remove_fault BEFORE INSERT ON relay_audit
             WHEN NEW.action = 'device_self_remove'
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable} = Relay.device_remove(relay, @origin)
    assert :sys.get_state(relay).store.version == before.version
    assert File.read!(upload_path) == data
    assert :ok = Sqlite3.execute(before.db, "DROP TRIGGER keyed_device_remove_fault")

    assert {:ok, %{"state" => "removed", "enrollment_revision" => 2}} =
             Relay.device_remove(relay, @origin)

    live = :sys.get_state(relay).store
    assert live.version == before.version + 1
    assert live.engine.origin_sessions[{@person, @origin, handle}].gone
    assert live.engine.devices[@origin].revoked
    refute live.engine.devices[@reader].revoked
    assert :error = Map.fetch(live.engine.uploads, id)
    refute File.exists?(upload_path)
    assert {:ok, %{"enrollment_revision" => 2}} = Relay.device_remove(relay, @origin)
    assert :sys.get_state(relay).store.version == live.version

    GenServer.stop(relay)
    {:ok, reopened} = Store.open(path, :keyed)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    assert index_state(live) == index_state(reopened)
    audit = %{person: @person, actor: @origin, action: "file_cleanup_retry"}
    assert {:ok, ^reopened} = Store.delete_uploads(reopened, [id], audit)
    :ok = Store.close(reopened)
  end

  test "keyed owner recovery retires one owner's tokens and preserves unrelated authority" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    a = "00000000-0000-4000-8000-000000000081"
    b = "00000000-0000-4000-8000-000000000082"
    a_key = String.duplicate("e", 64)
    b_key = String.duplicate("f", 64)
    cert = enrollment_certificate("replacement")
    {:ok, replacement} = AttentionServer.DeviceIdentity.from_certificate(cert)
    pem = :public_key.pem_encode([{:Certificate, cert, :not_encrypted}])
    {:ok, config} = AttentionServer.GuidedJoin.configuration("relay.test", 18444, 18443, pem)

    engine =
      Enum.reduce([{a, a_key}, {b, b_key}], legacy.engine, fn {person, owner}, engine ->
        {:ok, engine} = Engine.operator_bootstrap(engine, person, owner)
        sign_in = :crypto.hash(:sha256, person <> "browser")
        invitation = :crypto.hash(:sha256, person <> "join")

        {:ok, engine, _} =
          AttentionServer.BrowserSignIn.issue(engine, person, owner, sign_in, 100)

        {:ok, engine, _} =
          AttentionServer.GuidedJoin.create(engine, person, owner, invitation, config)

        engine
      end)

    {:ok, legacy} = Store.commit(legacy, engine, %{person: a, actor: a_key, action: "setup"})
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    opts = [store_path: path, store_mode: :keyed]
    {:ok, relay} = Relay.start_link(opts)

    assert {:ok, 1} = Relay.owner_revision(relay, a, a_key)
    assert :ok = Relay.operator_recover(relay, a, cert)
    assert {:error, :forbidden} = Relay.owner_revision(relay, a, a_key)
    assert {:ok, 2} = Relay.owner_revision(relay, a, replacement)
    assert {:ok, 1} = Relay.owner_revision(relay, b, b_key)
    live = :sys.get_state(relay).store
    assert MapSet.member?(live.engine.retired_owner_keys, a_key)
    assert [%{person: ^b}] = Map.values(live.engine.browser_sign_ins)
    assert [%{person: ^b}] = Map.values(live.engine.join_invitations)
    GenServer.stop(relay)

    {:ok, reopened} = Store.open(path, :keyed)
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    assert index_state(live) == index_state(reopened)
    :ok = Store.close(reopened)
  end

  test "keyed browser sign-in and guided enrollment replay through Relay after reopen" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    owner = enrollment_certificate("guided owner")
    device = enrollment_certificate("guided device")
    {:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    {:ok, device_key} = AttentionServer.DeviceIdentity.from_certificate(device)
    pem = :public_key.pem_encode([{:Certificate, owner, :not_encrypted}])
    {:ok, config} = AttentionServer.GuidedJoin.configuration("relay.test", 18444, 18443, pem)
    opts = [store_path: path, store_mode: :keyed, join_configuration: config]
    {:ok, relay} = Relay.start_link(opts)
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner)

    assert {:ok, %{"sign_in_token" => browser}} =
             Relay.owner_browser_sign_in_create(relay, person, owner_key)

    request = "00000000-0000-4000-8000-000000000083"

    assert {:ok, %{"invite_token" => token} = invitation} =
             Relay.owner_invite_create(relay, person, owner_key, request)

    assert {:ok, ^invitation} = Relay.owner_invite_create(relay, person, owner_key, request)
    secret = String.duplicate("12", 32)

    assert {:ok, %{"approval_code" => code, "person_id" => ^person} = pending} =
             Relay.invited_enroll(relay, token, device, "guided", secret)

    generation = :sys.get_state(relay).store.version
    GenServer.stop(relay)

    {:ok, reopened} = Relay.start_link(opts)
    assert {:ok, ^pending} = Relay.invited_enroll(reopened, token, device, "guided", secret)
    assert :sys.get_state(reopened).store.version == generation

    assert {:ok, %{person_id: ^person, owner_revision: 1}} =
             Relay.consume_browser_sign_in(reopened, browser)

    assert {:error, :invalid_sign_in} = Relay.consume_browser_sign_in(reopened, browser)

    assert {:ok, ^device_key} =
             Relay.owner_pending_approve(reopened, person, owner_key, code, device_key, nil, [])

    assert %{state: :approved, key: ^device_key, joined: true} =
             :sys.get_state(reopened).store.engine.pending_history[code]

    GenServer.stop(reopened)
  end

  test "keyed hosted delivery settings and dashboard device permissions survive reopen" do
    path = legacy_store_path()
    {:ok, legacy} = Store.open(path)
    assert {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    opts = [store_path: path, store_mode: :keyed, dashboard_origin: "https://relay.test"]
    {:ok, relay} = Relay.start_link(opts)
    assert {:ok, person} = Relay.hosted_account_create(relay, "hosted-fixture")
    assert {:ok, ^person} = Relay.hosted_account_create(relay, "hosted-fixture")
    assert {:ok, _} = Relay.hosted_sign_in_create(relay, person, "Hosted fixture")
    %{key: owner} = :sys.get_state(relay).store.engine.owners[person]
    device = enrollment_certificate("hosted device")
    {:ok, device_key} = AttentionServer.DeviceIdentity.from_certificate(device)
    assert {:ok, %{approval_code: code}} = Relay.pending_enroll(relay, device, "hosted device")

    assert {:ok, ^device_key} =
             Relay.owner_pending_approve(relay, person, owner, code, device_key, nil, [])

    assert :ok =
             Relay.save_dashboard_focus_permission(relay, person, owner, device_key, :see, true)

    assert :ok =
             Relay.save_dashboard_focus_permission(relay, person, owner, device_key, :push, true)

    assert {:ok, %{state: "STOPPED"}} = Relay.hosted_delivery_state(relay, person, "STOPPED")
    live = :sys.get_state(relay).store
    GenServer.stop(relay)

    {:ok, reopened} = Store.open(path, :keyed)

    assert %{person: ^person, label: "Hosted fixture"} =
             reopened.engine.hosted_accounts["hosted-fixture"]

    assert %{see_focus: true, push_view: true} = reopened.engine.devices[device_key]
    assert reopened.engine.delivery_states[person] == "STOPPED"
    assert full_diff(live.engine, reopened.engine) == MapSet.new()
    :ok = Store.close(reopened)
  end

  test "keyed cold cutover replays a retained live reply score once in both feeds" do
    alias AttentionServer.AgentAPI.FeedStep, as: AgentFeed
    alias AttentionServer.ClientAPI.{AgentResources, FeedStep, Reads, Resources}

    path = legacy_store_path()
    flags = path <> ".flags"
    File.write!(flags, JSON.encode!(%{"client_api_v2" => true}))
    opts = [store_path: path, feature_flags_path: flags, clock: fn -> 100 end]
    {:ok, relay} = Relay.start_link(opts)
    handle = "00000000-0000-4000-8000-000000000084"
    {item, live} = priority_reply(relay, "live-score", handle, 91)
    {aggregate_item, historical} = priority_reply(relay, "aggregate", handle, 94)
    audit = %{person: @person, actor: "server", action: "reply_group_scores"}
    current = :sys.get_state(relay).store

    {engine, projected} =
      Engine.retain_reply_scores(
        current.engine,
        @person,
        [score_entry(item, live, 0.9), score_entry(aggregate_item, historical, 0.7)],
        true
      )

    {:ok, current} = Store.commit(current, engine, audit, projected)

    {engine, projected} =
      Engine.retain_reply_scores(current.engine, @person, [score_entry(item, live, 0.9)], true)

    {:ok, current} = Store.commit(current, engine, audit, projected)
    :sys.replace_state(relay, &%{&1 | store: current})
    scope = {@person, @origin}
    {:ok, person_cursor} = ChangeLog.head(current, @person)
    {:ok, agent_cursor} = AgentChangeLog.head(current, scope)
    assert %{score: 0.9, scores: %{^live => 0.9}} = current.engine.reply_group_history[item]
    assert %{score: 0.7, scores: scores} = current.engine.reply_group_history[aggregate_item]
    assert scores == %{}
    GenServer.stop(relay)
    {:ok, legacy} = Store.open(path)
    {:ok, _} = KeyedMigration.convert(legacy)
    :ok = Store.close(legacy)
    opts = Keyword.put(opts, :store_mode, :keyed)
    {start_us, {:ok, relay}} = :timer.tc(fn -> Relay.start_link(opts) end)
    state = :sys.get_state(relay)
    assert state.store.engine.reply_group_history[item] == %{score: 0.9, scores: %{}}
    assert state.store.engine.reply_group_history[aggregate_item] == %{score: 0.7, scores: %{}}
    assert {:ok, next_person} = ChangeLog.head(state.store, @person)
    assert {:ok, next_agent} = AgentChangeLog.head(state.store, scope)
    assert next_person > person_cursor
    assert next_agent > agent_cursor

    person = %{person: @person, kind: :device, key: @reader}

    agent_ctx = %{
      relay: relay,
      principal: %{person: @person, kind: :device, key: @origin},
      params: %{session: handle}
    }

    assert {:ok, person_changes, false, ^next_person} =
             FeedStep.read(relay, person, person_cursor, 20)

    assert {:ok, agent_changes, false, ^next_agent} = AgentFeed.read(agent_ctx, agent_cursor, 20)
    item_id = Resources.item_id(state.store.engine.messages[item])
    assert {:ok, %{"entries" => person_entries}} = Reads.entries(relay, @reader, item_id)
    assert {:ok, agent_entry} = AgentResources.entry(state, scope, item, live)
    assert matching_entries(agent_changes, live) == [agent_entry]

    assert matching_entries(person_changes, live) ==
             Enum.filter(person_entries, &(&1["position"] == live))

    assert [%{"priority" => %{"level" => _} = priority}] = matching_entries(person_changes, live)
    refute Map.has_key?(priority, "score")
    assert matching_entries(agent_changes, historical) == []
    assert matching_entries(person_changes, historical) == []

    IO.puts(
      "keyed reply-score cold start #{start_us} us, person #{person_cursor}->#{next_person}, agent #{agent_cursor}->#{next_agent}"
    )

    GenServer.stop(relay)
    {:ok, restarted} = Relay.start_link(opts)
    assert {:ok, [], false, ^next_person} = FeedStep.read(restarted, person, next_person, 20)

    assert {:ok, [], false, ^next_agent} =
             AgentFeed.read(%{agent_ctx | relay: restarted}, next_agent, 20)

    GenServer.stop(restarted)

    {:ok, store} = Store.open(path, :keyed)
    {:ok, store} = Store.enable_change_log(store)
    {:ok, rows} = AttentionServer.Store.RecordTable.rows(store.db)
    projection = {store.engine, MapSet.new()}

    assert :ok =
             Sqlite3.execute(store.db, """
             CREATE TRIGGER keyed_priority_agent_fault BEFORE INSERT ON agent_changes
             BEGIN SELECT RAISE(ABORT, 'failed'); END
             """)

    assert {:error, :storage_unavailable, ^store} =
             Store.commit(store, projection, audit, [{@person, live}])

    assert {:ok, ^next_person} = ChangeLog.head(store, @person)
    assert {:ok, ^next_agent} = AgentChangeLog.head(store, scope)
    assert :ok = Sqlite3.execute(store.db, "DROP TRIGGER keyed_priority_agent_fault")
    assert {:ok, committed} = Store.commit(store, projection, audit, [{@person, live}])
    assert committed.version == store.version + 1
    assert {:ok, ^rows} = AttentionServer.Store.RecordTable.rows(committed.db)
    assert {:ok, bumped_person} = ChangeLog.head(committed, @person)
    assert {:ok, bumped_agent} = AgentChangeLog.head(committed, scope)
    assert bumped_person > next_person and bumped_agent > next_agent
    :ok = Store.close(committed)
  end

  defp priority_reply(relay, id, handle, n) do
    target = %{"v" => 1, "sender_id" => @sender, "message_id" => id}
    message = Map.merge(Map.drop(target, ["v"]), %{"created_at" => 1, "title" => "Request"})

    assert %{"type" => "ok"} =
             priority_request(relay, @origin, n, %{
               "v" => 1,
               "type" => "forward",
               "session_handle" => handle,
               "message" => message
             })

    assert %{"type" => "ok", "event_id" => turn} =
             priority_request(
               relay,
               @reader,
               n + 1,
               Map.merge(target, %{
                 "type" => "discussion",
                 "request_revision" => 1,
                 "text" => "Question"
               })
             )

    assert %{"type" => "ok", "seq" => position} =
             priority_request(
               relay,
               @origin,
               n + 2,
               Map.merge(target, %{
                 "type" => "conversation_reply",
                 "request_revision" => 1,
                 "in_reply_to" => String.to_integer(turn),
                 "text" => "Reply"
               })
             )

    {{@person, @sender, id}, position}
  end

  defp priority_request(relay, actor, n, frame) do
    op = "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")
    {:ok, line} = Relay.request_line(relay, actor, JSON.encode_line(Map.put(frame, "op_id", op)))
    {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp score_entry({_, sender, id}, position, score),
    do: %{
      "sender_id" => sender,
      "message_id" => id,
      "conversation_seq" => position,
      "score" => score
    }

  defp matching_entries(changes, position) do
    for %{"kind" => "entry", "data" => %{"position" => ^position} = entry} <- changes, do: entry
  end

  test "keyed local setup, dashboard levels, and owner grants persist across reopen" do
    path = legacy_store_path() <> ".new"
    opts = [store_path: path, store_mode: :keyed_cutover]
    {:ok, relay} = Relay.start_link(opts)
    owner = enrollment_certificate("settings owner")
    {:ok, key} = AttentionServer.DeviceIdentity.from_certificate(owner)
    {:ok, person} = Relay.operator_bootstrap(relay, owner)
    grantee = "grantee"
    device = String.duplicate("c", 64)
    assert :ok = Relay.setup_person(relay, grantee)
    assert :ok = Relay.setup_device(relay, person, device)
    assert :ok = Relay.setup_sender(relay, device, @sender)

    levels = [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}]
    assert {:ok, %{"levels" => ^levels}} = Relay.save_dashboard_levels(relay, person, key, levels)
    maintain = "00000000-0000-4000-8000-000000000099"
    enrollment = "00000000-0000-4000-8000-000000000100"
    target = %{"kind" => "device", "id" => device, "key_fingerprint" => device}
    assert :ok = Relay.owner_grant_maintainer(relay, person, key, maintain, grantee, ["devices"])

    assert :ok =
             Relay.owner_grant_enrollment_read(
               relay,
               person,
               key,
               device,
               enrollment,
               "source",
               target
             )

    assert :ok = Relay.owner_revoke_maintainer(relay, person, key, maintain)
    assert :ok = Relay.owner_revoke_enrollment_read(relay, person, key, enrollment)
    before = :sys.get_state(relay).store
    GenServer.stop(relay)

    {:ok, relay} = Relay.start_link(opts)
    assert {:ok, %{"levels" => ^levels}} = Relay.dashboard_levels(relay, person, key)
    actual = :sys.get_state(relay).store
    assert actual.version == before.version
    assert is_integer(actual.engine.maintainer_grants[maintain].revoked_at)
    assert actual.engine.enrollment_read_grants[enrollment].revoked
    assert full_diff(before.engine, actual.engine) == MapSet.new()
    GenServer.stop(relay)
  end

  test "keyed hosted test reset stages and removes only the designated account's device" do
    path = legacy_store_path() <> ".new"
    cert = enrollment_certificate("test reset")
    pem = :public_key.pem_encode([{:Certificate, cert, :not_encrypted}])
    {:ok, config} = AttentionServer.GuidedJoin.configuration("relay.test", 18444, 18443, pem)

    opts = [
      store_path: path,
      store_mode: :keyed_cutover,
      hosted_test_account: "designated",
      dashboard_origin: "https://relay.test",
      join_configuration: config
    ]

    {:ok, relay} = Relay.start_link(opts)
    assert {:ok, person} = Relay.hosted_account_create(relay, "designated")
    assert {:ok, separate} = Relay.hosted_account_create(relay, "separate")
    assert :ok = Relay.setup_device(relay, separate, @reader)
    assert {:ok, url} = Relay.hosted_sign_in_create(relay, person, "Tester")
    token = url |> String.split("#", parts: 2) |> List.last()

    assert {:ok, %{person_id: ^person, code: code}} =
             Relay.hosted_test_reset(relay, "device-waiting")

    assert :sys.get_state(relay).store.engine.pending_enrollments[code].person == person
    assert {:ok, %{person_id: ^person}} = Relay.hosted_test_reset(relay, "device-enrolled")
    %{engine: engine} = :sys.get_state(relay).store

    assert [{device, %{label: "Test device"}}] =
             Enum.filter(engine.devices, fn {_, row} -> row.person == person end)

    assert engine.cursors[{person, device}] == 0
    assert engine.subscriptions[{person, device}].revision == 0
    assert {:ok, %{person_id: ^person}} = Relay.hosted_test_reset(relay, "no-devices")
    live = :sys.get_state(relay).store
    GenServer.stop(relay)

    {:ok, reopened} = Relay.start_link(opts)
    assert full_diff(live.engine, :sys.get_state(reopened).store.engine) == MapSet.new()
    %{engine: engine} = :sys.get_state(reopened).store
    assert %{person: ^separate} = engine.devices[@reader]
    refute Map.has_key?(engine.devices, device)
    refute Map.has_key?(engine.cursors, {person, device})
    assert {:ok, %{person_id: ^person}} = Relay.consume_browser_sign_in(reopened, token)
    GenServer.stop(reopened)
  end

  test "keyed pause removes one account from ready claims and a badge excludes progress" do
    path = legacy_store_path()
    opts = [store_path: path, store_mode: :keyed_cutover]
    {:ok, relay} = Relay.start_link(opts)
    :ok = Relay.enable_push(relay)
    {:ok, paused_person} = Relay.hosted_account_create(relay, "paused-push")
    paused_device = String.duplicate("e", 64)
    :ok = Relay.setup_device(relay, paused_person, paused_device)
    :ok = Relay.setup_sender(relay, paused_device, @sender)

    for {key, n, address} <- [{paused_device, 171, "ab"}, {@reader, 172, "cd"}] do
      assert %{"type" => "ok"} =
               priority_request(relay, key, n, %{
                 "v" => 1,
                 "type" => "push_register",
                 "revision" => 1,
                 "profile_id" => "harklight-sandbox",
                 "address_kind" => "apns_token",
                 "address" => String.duplicate(address, 32)
               })
    end

    {:ok, forward} = JSON.decode_line(forward_line())
    assert %{"type" => "ok"} = priority_request(relay, paused_device, 173, forward)
    assert %{"type" => "ok"} = priority_request(relay, @origin, 174, forward)

    seen = %{
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

    progress =
      Map.merge(forward["message"], %{
        "message_id" => "progress",
        "message_type" => "fyi",
        "answer_ui" => seen,
        "progress" => %{"percent" => 100, "stage" => "done", "segments" => []}
      })

    assert %{"type" => "ok"} =
             priority_request(relay, @origin, 175, Map.put(forward, "message", progress))

    assert {:ok, %{state: "STOPPED"}} =
             Relay.hosted_delivery_state(relay, paused_person, "STOPPED")

    %{engine: engine} = :sys.get_state(relay).store
    index = engine.route_b_index
    assert [{{_, {@person, @reader}}, true}] = :gb_trees.to_list(index.push_bindings.queued)
    assert AttentionServer.RouteBIndex.push_badge(index, @person) == 1
    now = System.system_time(:millisecond) + 1000
    assert {:ok, %{device: @reader, badge: 1} = claim} = Relay.claim_push(relay, now)
    assert :ok = Relay.finish_push(relay, claim, %{state: "provider_accepted"}, now)
    assert {:ok, nil} = Relay.claim_push(relay, now)
    GenServer.stop(relay)

    {:ok, relay} = Relay.start_link(opts)
    :ok = Relay.enable_push(relay)
    assert {:ok, nil} = Relay.claim_push(relay, now)

    assert {:ok, %{state: "RESTORED"}} =
             Relay.hosted_delivery_state(relay, paused_person, "RESTORED")

    assert {:ok, %{device: ^paused_device, person: ^paused_person, badge: 1}} =
             Relay.claim_push(relay, now)

    GenServer.stop(relay)
  end

  defp enrollment_certificate(name) do
    %{cert: der} =
      :public_key.pkix_test_root_cert(String.to_charlist(name), key: {:namedCurve, :secp256r1})

    der
  end

  defp agent_write(relay, operation, params, body, op_id, match \\ []) do
    principal = %{person: @person, key: @origin}

    digest =
      AttentionServer.ClientAPI.Idempotency.digest("POST", [Atom.to_string(operation)], body)

    call = {:execute, {principal, operation, params, op_id, digest, match, body}, op_id, []}
    GenServer.call(relay, {:client_api, AttentionServer.ClientAPI.AgentWrites, call})
  end

  defp assert_agent_receipt({:stored, 200, encoded}, original) do
    assert {:ok, %{"agent_write" => 1, "outcome" => outcome} = stored} =
             JSON.decode_line(encoded <> "\n")

    assert outcome == Map.drop(original, ["message", "entry"])
    resource = original["message"] || original["entry"] || %{}
    assert stored["item_id"] == resource["item_id"]
    assert stored["entry_position"] == get_in(original, ["entry", "position"])
  end

  defp normalize_action_times(engine) do
    actions =
      Map.new(engine.origin_actions, fn {key, action} ->
        assert is_integer(action.at) and action.at > 0
        {key, %{action | at: 0}}
      end)

    %{engine | origin_actions: actions}
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

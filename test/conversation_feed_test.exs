defmodule AttentionServer.ConversationFeedTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    AgentAPIInProcess,
    BounceDecision,
    BouncePolicy,
    Engine,
    JSON,
    LargeData,
    PreChangeLogStore,
    Relay,
    RelayReaders,
    Store
  }

  alias AttentionServer.AgentChangeLog
  alias AttentionServer.ClientAPI.Reads
  alias AttentionServer.AgentAPI.Advisories
  alias Exqlite.Sqlite3
  alias AttentionServer.Store.SnapshotEncoding

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  defp state_snapshot(path) do
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, state_stmt} =
             Sqlite3.prepare(
               db,
               "SELECT version, payload, checksum FROM relay_state WHERE id = 1"
             )

    assert {:row, [version, payload, checksum]} = Sqlite3.step(db, state_stmt)
    assert :ok = Sqlite3.release(db, state_stmt)

    assert :ok = Sqlite3.close(db)
    {version, payload, checksum}
  end

  defp large_data_reference_rows(path) do
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(
               db,
               "SELECT path, marker, id, size, sha256 FROM relay_large_data_refs ORDER BY path"
             )

    rows = collect_large_data_reference_rows(db, stmt, [])
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    rows
  end

  defp collect_large_data_reference_rows(db, stmt, rows) do
    case Sqlite3.step(db, stmt) do
      {:row, row} -> collect_large_data_reference_rows(db, stmt, [row | rows])
      :done -> Enum.reverse(rows)
      error -> flunk("failed to read large-data reference rows: #{inspect(error)}")
    end
  end

  defp write_legacy_snapshot(path, engine) do
    {payload, _cache} = SnapshotEncoding.encode(engine, 30)
    {version, _old_payload, _checksum} = state_snapshot(path)
    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(
               db,
               "UPDATE relay_state SET version = ?, payload = ?, checksum = ? WHERE id = 1"
             )

    assert :ok =
             Sqlite3.bind(stmt, [
               version + 1,
               {:blob, payload},
               {:blob, :crypto.hash(:sha256, payload)}
             ])

    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
  end

  setup do
    dir =
      Path.join(System.tmp_dir!(), "harklight-conversation-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "state.sqlite3")}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp regular_files(dir) do
    dir
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Enum.sort()
  end

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  defp no_comment_request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, JSON.encode!(frame) <> "\n")
    assert {:ok, response} = JSON.decode_line(line)
    response
  end

  # The origin's view of its own message through client API v2: the agent
  # message, its entries, and one agent feed batch (which records ACK1).
  defp agent_item(relay, message_id, sender \\ "app:test"),
    do: AgentAPIInProcess.item_id(relay, "A", sender, message_id)

  defp agent_message(relay, message_id, key \\ "origin") do
    assert {:ok, %{"message" => message}} =
             AgentAPIInProcess.message(relay, "A", key, agent_item(relay, message_id))

    message
  end

  defp agent_entries(relay, message_id, key \\ "origin") do
    assert {:ok, %{"entries" => entries}} =
             AgentAPIInProcess.entries(relay, "A", key, agent_item(relay, message_id))

    entries
  end

  defp agent_feed(relay, key \\ "origin") do
    assert {:ok, changes, false, _position} = AgentAPIInProcess.changes(relay, "A", key, 0)
    changes
  end

  defp agent_write(relay, operation, message_id, params, body, n) do
    target = Map.put(params, :item_id, agent_item(relay, message_id))
    AgentAPIInProcess.write(relay, "A", "origin", operation, target, body, op(n))
  end

  # In-process reads keep large values as content-store references; the HTTP
  # encoder streams them.
  defp large_text(%LargeData.Ref{} = ref), do: LargeData.read!(ref)
  defp large_text(value) when is_binary(value), do: value

  defp origin_feed_entries(relay) do
    feed =
      Map.get(:sys.get_state(relay).store.engine.origin_feeds, {"A", "origin"}, %{entries: %{}})

    feed.entries |> Enum.sort() |> Enum.map(fn {_seq, entry} -> entry end)
  end

  # A device's ordinary-feed ACK, as a 0.1 session stored it: its cursor.
  defp acknowledge(store, key, seq) do
    engine = %{store.engine | cursors: Map.put(store.engine.cursors, {"A", key}, seq)}
    {:ok, store} = Store.commit(store, engine, %{person: "A", actor: key, action: "cursor_ack"})
    store
  end

  defp cold_store_reopen(path) do
    {output, status} =
      System.cmd(
        "mix",
        ["run", "--no-start", "--no-compile", "test/support/cold_store_reopen.exs"],
        cd: Path.expand("..", __DIR__),
        env: [
          {"MIX_ENV", "test"},
          {"COLD_STORE_PATH", path},
          {"COLD_PARENT_OS_PID", System.pid()},
          {"COLD_PERSON", "A"},
          {"COLD_ORIGIN", "origin"},
          {"COLD_HANDLE", @handle}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, "cold-reopen process failed (exit #{status}):\n#{output}"
    assert output =~ "COLD_STORE_REOPEN_AND_USE_OK"
  end

  defp wait_for_large_data_file(directory, port, deadline) do
    case File.ls!(directory)
         |> Enum.find(&Regex.match?(~r/\A[0-9a-f]{64}\z/, &1)) do
      nil ->
        cond do
          is_nil(Port.info(port)) ->
            child_exited_result(port)

          System.monotonic_time(:millisecond) >= deadline ->
            {:error, :timeout}

          true ->
            Process.sleep(10)
            wait_for_large_data_file(directory, port, deadline)
        end

      name ->
        {:ok, name}
    end
  end

  defp wait_for_child_file(path, port, deadline) do
    cond do
      File.regular?(path) ->
        :ok

      is_nil(Port.info(port)) ->
        child_exited_result(port)

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(10)
        wait_for_child_file(path, port, deadline)
    end
  end

  defp child_exited_result(port) do
    exit_status =
      receive do
        {^port, {:exit_status, status}} -> status
      after
        1_000 -> :unknown
      end

    {:error, {:child_exited, exit_status}, collect_child_output(port, [])}
  end

  defp collect_child_output(port, output) do
    receive do
      {^port, {:data, bytes}} ->
        collect_child_output(port, [bytes | output])
    after
      0 ->
        binary = output |> Enum.reverse() |> IO.iodata_to_binary()

        if byte_size(binary) > 8_192,
          do: binary_part(binary, byte_size(binary) - 8_192, 8_192),
          else: binary
    end
  end

  test "message_open stores one account-wide revision fact without changing feed or lifecycle cursors",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward = %{
      "type" => "forward",
      "op_id" => op(980),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "opened-on-any-device",
        "created_at" => 1,
        "title" => "Open this message"
      }
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)
    lifecycle_before = RelayReaders.events(relay, "reader", 0)
    origin_before = agent_entries(relay, "opened-on-any-device")
    conversation_before = RelayReaders.conversation_events(relay, "reader", 0)
    state_before = :sys.get_state(relay).store.engine

    open_op_id = op(981)

    opened = %{
      "type" => "message_open",
      "op_id" => open_op_id,
      "sender_id" => "app:test",
      "message_id" => "opened-on-any-device",
      "request_revision" => 1
    }

    assert %{
             "type" => "ok",
             "op_id" => ^open_op_id,
             "sender_id" => "app:test",
             "message_id" => "opened-on-any-device",
             "request_revision" => 1,
             "opened" => true
           } = request(relay, "reader", opened)

    state_after = :sys.get_state(relay).store.engine

    assert state_after.opened_revisions ==
             MapSet.new([{"A", "app:test", "opened-on-any-device", 1}])

    assert state_after.events == state_before.events
    assert state_after.conversation_events == state_before.conversation_events
    assert state_after.origin_feeds == state_before.origin_feeds
    assert state_after.cursors == state_before.cursors
    assert RelayReaders.events(relay, "reader", 0) == lifecycle_before
    assert agent_entries(relay, "opened-on-any-device") == origin_before
    assert RelayReaders.conversation_events(relay, "reader", 0) == conversation_before

    assert request(relay, "reader", opened)["type"] == "ok"

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "origin", %{opened | "op_id" => op(982)})

    assert MapSet.size(:sys.get_state(relay).store.engine.opened_revisions) == 1

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "request_revision"} =
             request(relay, "reader", %{opened | "op_id" => op(983), "request_revision" => 2})

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert MapSet.size(:sys.get_state(restarted).store.engine.opened_revisions) == 1
    assert request(restarted, "reader", opened)["type"] == "ok"
    assert RelayReaders.events(restarted, "reader", 0) == lifecycle_before
    assert agent_entries(restarted, "opened-on-any-device") == origin_before

    assert :sys.get_state(restarted).store.engine.origin_feeds ==
             state_before.origin_feeds

    GenServer.stop(restarted)
  end

  test "due advisory is durable on its original cursor and never advances the ordinary feed", %{
    path: path
  } do
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, System.system_time(:millisecond))
    clock_fn = fn -> :atomics.get(clock, 1) end
    opts = [store_path: path, clock: clock_fn]
    assert {:ok, relay} = Relay.start_link(opts)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    {:ok, levels} =
      BouncePolicy.normalize_levels([
        %{"name" => "Follow up", "lower_bound" => 0, "bounce_after_days" => 1}
      ])

    state = :sys.get_state(relay)

    {:ok, engine, _config} =
      BounceDecision.configure_levels(state.store.engine, "A", levels, clock_fn.())

    {:ok, configured_store} =
      Store.commit(state.store, engine, %{person: "A", actor: "owner", action: "test_levels"})

    :sys.replace_state(relay, &%{&1 | store: configured_store})

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(970),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "durable-advisory",
                 "created_at" => 1,
                 "title" => "Original request",
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
                 },
                 "message_type" => "fyi"
               }
             })

    # A 0.1 origin declared advisory support for its session; client API v2
    # never writes that record. The stored declaration still makes the 0.1
    # session's messages eligible, and v2 reads and acknowledges the advisory.
    PreChangeLogStore.in_relay(relay, fn store ->
      PreChangeLogStore.origin_support(store, "origin", %{
        "type" => "support",
        "op_id" => op(971),
        "session_handle" => @handle,
        "route" => "claude-code",
        "cli_version" => "test-only",
        "unsupported_reason" => nil,
        "advisory_v1" => true
      })
    end)

    ordinary_before = origin_feed_entries(relay)
    entries_before = agent_entries(relay, "durable-advisory")
    origin_high = :sys.get_state(relay).store.engine.origin_feeds[{"A", "origin"}].high
    state = :sys.get_state(relay)
    record = state.store.engine.messages[{"A", "app:test", "durable-advisory"}]
    due_at = record.accepted_at + 86_400_000

    selected =
      BounceDecision.record_scores(
        state.store.engine,
        "A",
        [%{"sender_id" => "app:test", "message_id" => "durable-advisory", "score" => 0.9}],
        record.accepted_at
      )

    {:ok, selected_store} =
      Store.commit(state.store, selected, %{
        person: "system",
        actor: "bounce_decision",
        action: "test_score_selection"
      })

    :sys.replace_state(relay, &%{&1 | store: selected_store})
    scope = {"A", "origin"}
    assert {:ok, decision_cursor} = AgentChangeLog.head(selected_store, scope)
    :atomics.put(clock, 1, due_at + 25)
    generation = :sys.get_state(relay).bounce_deadline_generation
    send(relay, {:bounce_due, generation})

    # The due decision is appended on the session's own advisory cursor.
    stored = :sys.get_state(relay).store.engine.advisory_feeds[{"A", "origin", @handle}]
    assert stored.high == 1
    assert stored.entries[1]["origin_cursor_at_decision"] == origin_high
    assert origin_feed_entries(relay) == ordinary_before

    assert %{"advisories" => [advisory]} = agent_message(relay, "durable-advisory")
    assert advisory["delivered"] == false
    assert advisory["decided_at_rev"] == decision_cursor
    refute Map.has_key?(advisory, "origin_cursor_at_decision")
    assert agent_entries(relay, "durable-advisory") == entries_before

    assert :sys.get_state(relay).store.engine.messages[{"A", "app:test", "durable-advisory"}].terminal ==
             nil

    message_key = {"A", "app:test", "durable-advisory"}
    advisory_id = advisory["advisory_id"]
    store = :sys.get_state(relay).store
    assert {:ok, before_ack} = AgentChangeLog.head(store, scope)

    assert [
             %{
               "advisory_id" => ^advisory_id,
               "delivered" => false,
               "decided_at_rev" => ^decision_cursor
             }
           ] =
             Advisories.for_message(store.engine, scope, message_key)

    ack = %{"advisory_id" => advisory_id}

    assert {:ok, 200, first_ack, false} =
             agent_write(relay, :advisory_ack, "durable-advisory", %{}, ack, 972)

    assert first_ack["delivered"] and not first_ack["already_delivered"]
    store = :sys.get_state(relay).store

    assert {:ok, [%{resource: {:message, ^message_key}}, %{resource: {:advisory, ^advisory_id}}],
            false, _} = AgentChangeLog.read(store, scope, before_ack, 10, @handle)

    assert %{"advisories" => [%{"delivered" => true}]} =
             agent_message(relay, "durable-advisory")

    assert origin_feed_entries(relay) == ordinary_before

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(opts)

    assert [
             %{
               "advisory_id" => ^advisory_id,
               "delivered" => true,
               "decided_at_rev" => ^decision_cursor
             }
           ] =
             Advisories.for_message(:sys.get_state(restarted).store.engine, scope, message_key)

    assert %{"advisories" => [%{"advisory_id" => ^advisory_id, "delivered" => true}]} =
             agent_message(restarted, "durable-advisory")

    assert :sys.get_state(restarted).store.engine.advisory_feeds[{"A", "origin", @handle}].high ==
             1

    assert origin_feed_entries(restarted) == ordinary_before

    assert {:ok, 200, duplicate_ack, false} =
             agent_write(restarted, :advisory_ack, "durable-advisory", %{}, ack, 973)

    assert duplicate_ack["delivered"] and duplicate_ack["already_delivered"]
    GenServer.stop(restarted)
  end

  test "authorized agent answer keeps its author through terminal replay and restart", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path, agent_fixture_mode: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    agent_key = String.duplicate("c", 64)
    assert :ok = Relay.setup_agent_fixture(relay, "A", op(701), agent_key, ["app:test"])

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(702),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "agent-terminal",
                 "created_at" => 1,
                 "title" => "Approve?"
               }
             })

    assert {:ok, %{"seq" => 2}} =
             Relay.fixture_agent_answer(relay, agent_key, %{
               "type" => "answer",
               "op_id" => op(703),
               "sender_id" => "app:test",
               "message_id" => "agent-terminal",
               "acceptance_seq" => 1,
               "deliberate" => true,
               "intent_ref" => "agent intent",
               "answer" => nil
             })

    # The origin's v2 view has no author field: an agent's answer is told
    # apart from a person's by carrying no delivery state.
    assert %{"terminal" => terminal} = agent_message(relay, "agent-terminal")
    assert terminal["state"] == "answered"
    assert Map.has_key?(terminal, "answer") and is_nil(terminal["answer"])
    refute Map.has_key?(terminal, "delivery")
    assert {:ok, [lifecycle], 2} = RelayReaders.events(relay, "reader", 1)
    assert lifecycle["author"] == "agent"
    assert lifecycle["state"] == "answered"
    GenServer.stop(relay)

    assert {:ok, restarted} = Relay.start_link(store_path: path, agent_fixture_mode: true)

    assert %{"terminal" => ^terminal} = agent_message(restarted, "agent-terminal")

    assert {:ok, [%{"author" => "agent", "state" => "answered"}], 2} =
             RelayReaders.events(restarted, "reader", 1)

    GenServer.stop(restarted)
  end

  test "the ordinary and conversation streams stay separate per device and replay after restart",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "old")
    assert :ok = Relay.setup_device(relay, "A", "new")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(601),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "split",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert %{"type" => "ok", "seq" => 1, "event_id" => "2"} =
             request(relay, "new", %{
               "type" => "discussion",
               "op_id" => op(602),
               "sender_id" => "app:test",
               "message_id" => "split",
               "request_revision" => 1,
               "text" => "More detail?"
             })

    assert {:ok, [ordinary], 1} = RelayReaders.events(relay, "old", 0)
    assert ordinary["type"] == "delivery"
    assert {:ok, [conversation], 1} = RelayReaders.conversation_events(relay, "new", 0)
    assert conversation["conversation_seq"] == 1
    assert conversation["event_id"] == "2"
    assert {:ok, [], 0} = RelayReaders.conversation_events(relay, "other", 0)

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "old", %{
               "type" => "answer",
               "op_id" => op(603),
               "sender_id" => "app:test",
               "message_id" => "split",
               "answer" => nil
             })

    assert {:ok, [_, %{"type" => "lifecycle", "seq" => 2}], 2} =
             RelayReaders.events(relay, "old", 0)

    assert {:ok, [new_terminal], 2} = RelayReaders.events(relay, "new", 1)
    assert new_terminal["type"] == "lifecycle"
    assert new_terminal["event_id"] == "3"
    assert new_terminal["author"] == "person"
    assert new_terminal["by"] == "answer"
    assert {:ok, [], 1} = RelayReaders.conversation_events(relay, "new", 1)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], 1} = RelayReaders.conversation_events(restarted, "new", 0)
    assert replayed["event_id"] == "2"
    assert {:ok, [_, _], 2} = RelayReaders.events(restarted, "old", 0)
    GenServer.stop(restarted)
  end

  test "a stored conversation wake reaches only opened devices and does not suppress a later new alert",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path, push_enabled: true)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "old")
    assert :ok = Relay.setup_device(relay, "A", "new")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(611),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "wake-one",
                 "created_at" => 1,
                 "title" => "First question"
               }
             })

    # Stored 0.1 progress: both devices acknowledged the first request, and
    # only "new" opened its conversation feed. Client API v2 writes neither.
    PreChangeLogStore.in_relay(relay, fn store ->
      store = acknowledge(store, "old", 1)
      store = acknowledge(store, "new", 1)
      PreChangeLogStore.open_conversation(store, "new")
    end)

    for device <- ["old", "new"] do
      assert %{"type" => "ok"} =
               request(relay, device, %{
                 "type" => "push_register",
                 "op_id" => if(device == "old", do: op(612), else: op(613)),
                 "revision" => 1,
                 "profile_id" => "harklight-sandbox",
                 "address_kind" => "apns_token",
                 "address" =>
                   if(device == "old",
                     do: String.duplicate("ab", 32),
                     else: String.duplicate("cd", 32)
                   )
               })
    end

    assert %{"type" => "ok"} =
             request(relay, "new", %{
               "type" => "discussion",
               "op_id" => op(614),
               "sender_id" => "app:test",
               "message_id" => "wake-one",
               "request_revision" => 1,
               "text" => "Can you clarify?"
             })

    bindings = :sys.get_state(relay).store.engine.push_bindings
    assert bindings[{"A", "old"}].pending_conversation_high == 0
    assert bindings[{"A", "new"}].pending_conversation_high == 1

    now_ms = System.system_time(:millisecond) + 300_001
    assert {:ok, claim} = Relay.claim_push(relay, now_ms)
    assert claim.device == "new"
    assert claim.conversation_high == 1
    assert claim.presentation == :background
    assert :ok = Relay.finish_push(relay, claim, %{state: "provider_accepted"}, now_ms + 1)

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(615),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "wake-two",
                 "created_at" => 2,
                 "title" => "New unresolved question"
               }
             })

    # Both devices become eligible on the new request; each gets the alert,
    # including the one whose silent conversation wake just went out.
    assert {:ok, first_alert} = Relay.claim_push(relay, now_ms + 300_001)
    assert first_alert.high == 2
    assert first_alert.presentation == :alert

    assert :ok =
             Relay.finish_push(
               relay,
               first_alert,
               %{state: "provider_accepted"},
               now_ms + 300_002
             )

    assert {:ok, second_alert} = Relay.claim_push(relay, now_ms + 300_003)
    assert second_alert.high == 2
    assert second_alert.presentation == :alert
    assert Enum.sort([first_alert.device, second_alert.device]) == ["new", "old"]
    assert {:ok, nil} = Relay.claim_push(relay, now_ms + 300_004)
    GenServer.stop(relay)
  end

  test "legacy reopen rejects a stored revision with a mismatched inline file digest", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "inline-file",
      "created_at" => 1,
      "title" => "Original request"
    }

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "message" => message
             })

    bytes = <<0, 17, 250, 99>>
    data = Base.encode64(bytes)
    digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(2),
               "sender_id" => "app:test",
               "message_id" => "inline-file",
               "base_revision" => 1,
               "message" => %{message | "title" => "Revised request"},
               "files" => [%{"name" => "inline.bin", "data_base64" => data}]
             })

    assert :ok = GenServer.stop(relay)

    key = {"A", "app:test", "inline-file"}
    assert {:ok, store} = Store.open(path)
    engine = store.engine

    assert [%{"tier" => "inline", "sha256" => ^digest, "data_base64" => ^data} = inline] =
             engine.messages[key].files_by_revision[2]

    assert :ok = Store.close(store)

    # The inline bytes and SQLite checksum stay valid. Only the stored file
    # digest in the message revision differs from the normalized attachment.
    invalid =
      put_in(engine.messages[key].files_by_revision[2], [
        %{inline | "sha256" => String.duplicate("0", 64)}
      ])

    write_legacy_snapshot(path, invalid)

    case Store.open(path) do
      {:ok, unexpected} ->
        assert :ok = Store.close(unexpected)
        flunk("Store.open accepted a revision with a mismatched inline file digest")

      error ->
        assert error == {:error, {:storage_unavailable, {:error, :invalid_state}}}
    end
  end

  test "inline attachments up to the threshold stay inline; above it the frame is refused",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    bytes = :binary.copy(<<0, 255, 42, 10>>, 65_536)
    file = %{"name" => "exact.bin", "data_base64" => Base.encode64(bytes)}

    message = %{
      "sender_id" => "app:test",
      "message_id" => "file-request",
      "created_at" => 1,
      "title" => "Please inspect this file"
    }

    send = %{
      "type" => "forward",
      "op_id" => op(501),
      "session_handle" => @handle,
      "message" => message,
      "files" => [file]
    }

    assert %{"type" => "error", "reason" => "invalid_field", "field" => "files"} =
             request(
               relay,
               "origin",
               Map.drop(send, ["session_handle"]) |> Map.put("op_id", op(500))
             )

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", send)
    assert {:ok, [origin_delivery], 1} = RelayReaders.events(relay, "origin", 0)
    refute Map.has_key?(origin_delivery, "files")

    assert {:ok, [recipient_delivery], 1} = RelayReaders.events(relay, "reader", 0)
    assert [stored] = recipient_delivery["files"]
    assert Base.decode64!(stored["data_base64"]) == bytes
    assert stored["size"] == byte_size(bytes)
    assert stored["sha256"] == Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    inline_bytes = :binary.copy(<<11, 0, 255, 29>>, 38_400)
    overflow_bytes = :binary.copy(<<4, 3, 2, 1>>, 30_000)
    assert byte_size(inline_bytes) < 262_144
    assert byte_size(overflow_bytes) < 262_144
    assert byte_size(inline_bytes) + byte_size(overflow_bytes) > 262_144
    before = :sys.get_state(relay).store
    stored_files = regular_files(before.upload_dir)

    # Protocol 0.1 staged the overflow as an upload. Nothing stages inline
    # data now: the engine refuses the entry, and no upload row or file is left.
    assert %{"type" => "error", "reason" => "invalid_field", "field" => "files"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(502),
               "sender_id" => "app:test",
               "message_id" => "file-request",
               "request_revision" => 1,
               "text" => "Please clarify",
               "files" => [
                 %{"name" => "inline.bin", "data_base64" => Base.encode64(inline_bytes)},
                 %{"name" => "overflow.bin", "data_base64" => Base.encode64(overflow_bytes)}
               ]
             })

    after_refusal = :sys.get_state(relay).store
    assert after_refusal.engine == before.engine
    assert after_refusal.engine.uploads == %{}

    assert regular_files(after_refusal.upload_dir) == stored_files

    assert agent_entries(relay, "file-request") == []

    assert {:ok, %{"limits" => %{"inline_decoded_bytes_per_entry" => 262_144}}} =
             Reads.me(relay, "origin")

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], 1} = RelayReaders.events(restarted, "reader", 0)
    assert replayed["files"] == recipient_delivery["files"]

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", send)

    GenServer.stop(restarted)
  end

  test "large message titles stay in the content store and survive a restart", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    title = Base.encode64(:crypto.strong_rand_bytes(1_572_870))
    assert byte_size(title) > 2 * 1024 * 1024
    title_hash = Base.encode16(:crypto.hash(:sha256, title), case: :lower)
    {_, before_payload, _} = state_snapshot(path)

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(503),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "large-title",
                 "created_at" => 1,
                 "title" => title
               }
             })

    {_, committed_payload, _} = state_snapshot(path)
    assert byte_size(committed_payload) - byte_size(before_payload) < 65_536

    assert {:attention_relay_state, 30, persisted} =
             :erlang.binary_to_term(committed_payload, [:safe])

    persisted_record = persisted.messages[{"A", "app:test", "large-title"}]
    persisted_title = persisted_record.message["title"]
    assert is_binary(persisted_title)
    assert persisted_title =~ "Full content requires a newer Harklight Server"

    assert Enum.any?(large_data_reference_rows(path), fn
             [path_key, marker, id, size, sha256] ->
               is_binary(path_key) and marker == persisted_title and id == title_hash and
                 size == byte_size(title) and sha256 == title_hash
           end)

    state = :sys.get_state(relay)
    record = state.store.engine.messages[{"A", "app:test", "large-title"}]
    assert %AttentionServer.LargeData.Ref{} = ref = record.message["title"]
    assert ref.size == byte_size(title)
    assert ref.sha256 == title_hash
    assert Base.encode16(:crypto.hash(:sha256, File.read!(ref.path)), case: :lower) == title_hash

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert %{"message" => %{"title" => restored}} = agent_message(restarted, "large-title")
    assert large_text(restored) == title
    GenServer.stop(restarted)
  end

  test "a later state commit does not re-read an existing large-data reference", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    title = String.duplicate("large-title-", div(LargeData.inline_limit(), 12) + 1)
    assert byte_size(title) > LargeData.inline_limit()

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(504),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "large-title-no-reread",
                 "created_at" => 1,
                 "title" => title
               }
             })

    state = :sys.get_state(relay)
    message_key = {"A", "app:test", "large-title-no-reread"}
    assert %LargeData.Ref{} = ref = state.store.engine.messages[message_key].message["title"]
    assert File.regular?(ref.path)

    {:ok, levels} =
      BouncePolicy.normalize_levels([
        %{"name" => "Follow up", "lower_bound" => 0, "bounce_after_days" => 1}
      ])

    {:ok, engine, _config} =
      BounceDecision.configure_levels(
        state.store.engine,
        "A",
        levels,
        System.system_time(:millisecond)
      )

    verify_pattern = {LargeData, :verify, 1}
    :erlang.trace_pattern(verify_pattern, true, [:local])
    :erlang.trace(self(), true, [:call, :set_on_spawn, {:tracer, self()}])

    try do
      assert {:ok, committed_store} =
               Store.commit(state.store, engine, %{
                 person: "A",
                 actor: "owner",
                 action: "test_large_data_reference_reuse"
               })

      assert committed_store.engine.messages[message_key].message["title"] == ref

      delivered = :erlang.trace_delivered(:all)
      assert_receive {:trace_delivered, :all, ^delivered}, 1_000

      assert_receive_large_data_verifications([]) == []
    after
      :erlang.trace(self(), false, [:call, :set_on_spawn])
      :erlang.trace_pattern(verify_pattern, false, [:local])
    end

    assert LargeData.read!(ref) == title
    GenServer.stop(relay)
  end

  defp assert_receive_large_data_verifications(calls) do
    receive do
      {:trace, _pid, :call, {LargeData, :verify, [ref]}} ->
        assert_receive_large_data_verifications([ref | calls])
    after
      0 -> Enum.reverse(calls)
    end
  end

  test "a reader without the external data sees an honest marker and serves unrelated records",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    large_text = Base.encode64(:crypto.strong_rand_bytes(1_572_870))
    assert byte_size(large_text) > 2 * 1024 * 1024

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(506),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "rollback-large",
                 "created_at" => 1,
                 "title" => large_text,
                 "body" => large_text
               }
             })

    reference_rows = large_data_reference_rows(path)
    assert length(reference_rows) >= 2

    assert Enum.all?(reference_rows, fn [_path, marker, id, size, sha256] ->
             marker =~ "Full content requires a newer Harklight Server" and
               id == Base.encode16(:crypto.hash(:sha256, large_text), case: :lower) and
               size == byte_size(large_text) and sha256 == id
           end)

    [rollback_marker | _] =
      Enum.map(reference_rows, fn [_path, marker, _id, _size, _sha] -> marker end)

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(507),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "rollback-small",
                 "created_at" => 2,
                 "title" => "Unrelated record",
                 "body" => rollback_marker
               }
             })

    GenServer.stop(relay)

    {_, payload, _} = state_snapshot(path)
    assert {:attention_relay_state, 30, persisted} = :erlang.binary_to_term(payload, [:safe])
    large_record = persisted.messages[{"A", "app:test", "rollback-large"}]
    title_marker = large_record.message["title"]
    body_marker = large_record.message["body"]
    assert is_binary(title_marker)
    assert is_binary(body_marker)
    assert title_marker =~ "Full content requires a newer Harklight Server"
    assert body_marker =~ "Full content requires a newer Harklight Server"

    data_dir = Path.expand(path) <> ".uploads"

    saved_files =
      Map.new(reference_rows, fn [_path_key, _marker, id, _size, _sha256] ->
        {id, File.read!(Path.join(data_dir, id))}
      end)

    assert Enum.all?(saved_files, fn {_id, bytes} -> bytes == large_text end)

    # Without the external data, Relay can still read these ordinary message strings.
    assert {:ok, legacy_relay} = Relay.start_link(store_path: path <> ".reader")
    legacy_engine = persisted

    :sys.replace_state(legacy_relay, fn state ->
      %{state | store: %{state.store | engine: legacy_engine}}
    end)

    legacy_messages = :sys.get_state(legacy_relay).store.engine.messages

    assert %{"title" => ^title_marker, "body" => ^body_marker} =
             legacy_messages[{"A", "app:test", "rollback-large"}].message

    assert %{"title" => "Unrelated record", "body" => small_body} =
             legacy_messages[{"A", "app:test", "rollback-small"}].message

    assert small_body == rollback_marker

    assert :ok =
             Store.prune_untracked_uploads(%Store{
               db: nil,
               engine: legacy_engine,
               version: 0,
               upload_dir: data_dir
             })

    write_legacy_snapshot(path, legacy_engine)
    GenServer.stop(legacy_relay)

    assert large_data_reference_rows(path) == reference_rows

    assert Map.new(reference_rows, fn [_path_key, _marker, id, _size, _sha256] ->
             {id, File.read!(Path.join(data_dir, id))}
           end) == saved_files

    assert {:ok, upgraded} = Relay.start_link(store_path: path)

    assert %{"message" => %{"title" => title_value, "body" => body_value}} =
             agent_message(upgraded, "rollback-large")

    assert large_text(title_value) == large_text
    assert large_text(body_value) == large_text

    assert %{"message" => %{"title" => "Unrelated record", "body" => ^rollback_marker}} =
             agent_message(upgraded, "rollback-small")

    GenServer.stop(upgraded)
  end

  test "reopen retains intact large data when its reference rows are missing", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    body = :binary.copy("x", LargeData.inline_limit() + 1)
    id = "retained-large-body"

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(508),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => 1,
                 "title" => "Retained content",
                 "body" => body
               }
             })

    store = :sys.get_state(relay).store
    key = {"A", "app:test", id}
    assert %LargeData.Ref{} = ref = store.engine.messages[key].message["body"]
    marker = LargeData.persist_refs(ref)
    literal_id = "authored-marker-text"

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(509),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => literal_id,
                 "created_at" => 1,
                 "title" => "Literal marker",
                 "body" => marker
               }
             })

    refs = large_data_reference_rows(path)
    assert refs != []
    GenServer.stop(relay)

    {:ok, db} = Sqlite3.open(path)
    assert :ok = Sqlite3.execute(db, "DELETE FROM relay_large_data_refs")
    assert :ok = Sqlite3.close(db)

    orphan = Path.join(store.upload_dir, String.duplicate("0", 64))
    File.write!(orphan, "orphan")

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:ok, reopened} = Store.open(path)
        assert reopened.engine.messages[key].message["body"] == marker
        assert reopened.engine.messages[{"A", "app:test", literal_id}].message["body"] == marker
        assert :ok = Store.close(reopened)
      end)

    assert File.read(ref.path) == {:ok, body}
    assert log =~ "event=harklight_unresolved_large_data_reference"
    assert log =~ "retained_blobs=1 sha256=#{ref.id}"
    refute log =~ body
    assert large_data_reference_rows(path) == []
    refute File.exists?(orphan)

    {:ok, db} = Sqlite3.open(path)

    {:ok, stmt} =
      Sqlite3.prepare(
        db,
        "INSERT INTO relay_large_data_refs (path, marker, id, size, sha256) VALUES (?, ?, ?, ?, ?)"
      )

    for [path_key, marker, id, size, sha256] <- refs do
      assert :ok = Sqlite3.bind(stmt, [{:blob, path_key}, marker, id, size, sha256])
      assert :done = Sqlite3.step(db, stmt)
      assert :ok = Sqlite3.reset(stmt)
    end

    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    assert {:ok, repaired} = Store.open(path)
    assert repaired.engine.messages[key].message["body"] == ref
    assert LargeData.read!(ref) == body

    # Also exercise retirement through the snapshot commit path. A marker
    # remains in the durable state after the last registered reference goes.
    next = LargeData.persist_refs(repaired.engine)
    audit = %{person: "A", actor: "origin", action: "retain_marker"}

    assert {:ok, committed} = Store.commit(repaired, next, audit)
    assert large_data_reference_rows(path) == []
    assert File.read(ref.path) == {:ok, body}
    assert :ok = Store.close(committed)
  end

  test "FORMAT29 inline large title and body survive the FORMAT30 alert settings upgrade", %{
    path: path
  } do
    # The engine holds the large strings inline, as a FORMAT29 writer did.
    {:ok, engine} = Engine.setup_person(Engine.new(), "A")
    {:ok, engine} = Engine.setup_device(engine, "A", "origin")
    {:ok, engine} = Engine.setup_sender(engine, "origin", "app:test")

    title = Base.encode64(:crypto.strong_rand_bytes(1_572_870))
    body = Base.encode64(:crypto.strong_rand_bytes(1_572_870))

    forward =
      JSON.encode!(%{
        "v" => 1,
        "type" => "forward",
        "op_id" => op(505),
        "session_handle" => @handle,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "format29-large-data",
          "created_at" => 1,
          "title" => title,
          "body" => body
        }
      }) <> "\n"

    {legacy_engine, {:ok, reply}} = Engine.request_line(engine, "origin", forward)
    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(reply)

    legacy_engine = Map.delete(legacy_engine, :alert_choices)
    {legacy_payload, _cache} = SnapshotEncoding.encode(legacy_engine, 29)
    assert byte_size(legacy_payload) > 2 * 1024 * 1024

    assert {:ok, empty} = Store.open(path)
    assert :ok = Store.close(empty)

    assert {:ok, db} = Sqlite3.open(path)

    assert {:ok, stmt} =
             Sqlite3.prepare(
               db,
               "UPDATE relay_state SET version = 0, payload = ?, checksum = ? WHERE id = 1"
             )

    assert :ok =
             Sqlite3.bind(stmt, [
               {:blob, legacy_payload},
               {:blob, :crypto.hash(:sha256, legacy_payload)}
             ])

    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, reopened} = Store.open(path)
    assert reopened.version == 1
    assert reopened.engine.alert_choices == %{}
    assert {1, payload, _checksum} = state_snapshot(path)
    assert {:attention_relay_state, 30, _} = :erlang.binary_to_term(payload, [:safe])

    assert :ok = Store.close(reopened)

    assert {:ok, reopened} = Store.open(path)
    assert reopened.version == 1
    assert reopened.engine.alert_choices == %{}
    record = reopened.engine.messages[{"A", "app:test", "format29-large-data"}]
    assert LargeData.read!(record.message["title"]) == title
    assert LargeData.read!(record.message["body"]) == body
    assert :ok = Store.close(reopened)
  end

  test "a failed large-data commit leaves no dangling reference and prunes its complete orphan",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    {:ok, db} = Sqlite3.open(path)

    :ok =
      Sqlite3.execute(
        db,
        "CREATE TRIGGER reject_large_commit BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'simulated commit boundary'); END"
      )

    :ok = Sqlite3.close(db)
    body = Base.encode64(:crypto.strong_rand_bytes(2_097_152))

    assert %{"type" => "error", "reason" => "storage_unavailable"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(504),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "commit-boundary",
                 "created_at" => 1,
                 "title" => "Commit boundary",
                 "body" => body
               }
             })

    assert :sys.get_state(relay).store.engine.messages == %{}
    assert large_data_reference_rows(path) == []
    data_dir = Path.expand(path) <> ".uploads"
    assert [orphan] = File.ls!(data_dir)
    assert Regex.match?(~r/\A[0-9a-f]{64}\z/, orphan)
    GenServer.stop(relay)

    {:ok, db} = Sqlite3.open(path)
    :ok = Sqlite3.execute(db, "DROP TRIGGER reject_large_commit")
    :ok = Sqlite3.close(db)

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert :sys.get_state(restarted).store.engine.messages == %{}
    assert large_data_reference_rows(path) == []
    assert File.ls!(data_dir) == []
    GenServer.stop(restarted)
  end

  test "a process crash after large data is synced but before its reference commits leaves no dangling state",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    GenServer.stop(relay)

    data_dir = Path.expand(path) <> ".uploads"
    ready_path = path <> ".large-data-ready"
    go_path = path <> ".large-data-go"

    mix = System.find_executable("mix")
    assert is_binary(mix)

    child =
      Port.open({:spawn_executable, String.to_charlist(mix)}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        :use_stdio,
        {:args,
         Enum.map(
           ["run", "--no-start", "--no-compile", "test/support/large_data_commit_child.exs"],
           &String.to_charlist/1
         )},
        {:cd, String.to_charlist(Path.expand("..", __DIR__))},
        {:env,
         [
           {~c"MIX_ENV", ~c"test"},
           {~c"WI2F2_STORE_PATH", String.to_charlist(path)},
           {~c"WI2F2_READY_PATH", String.to_charlist(ready_path)},
           {~c"WI2F2_GO_PATH", String.to_charlist(go_path)}
         ]}
      ])

    try do
      {:os_pid, child_pid} = Port.info(child, :os_pid)
      deadline = System.monotonic_time(:millisecond) + 30_000

      assert :ok = wait_for_child_file(ready_path, child, deadline)
      assert {:ok, lock_db} = Sqlite3.open(path)

      try do
        assert :ok = Sqlite3.execute(lock_db, "BEGIN IMMEDIATE")
        File.write!(go_path, "go")

        result = wait_for_large_data_file(data_dir, child, deadline)

        assert {:ok, blob_name} = result,
               "large-data child did not produce a content file: #{inspect(result)}"

        assert Base.encode16(
                 :crypto.hash(:sha256, File.read!(Path.join(data_dir, blob_name))),
                 case: :lower
               ) == blob_name

        {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(child_pid)])
        assert_receive {^child, {:exit_status, status}} when status != 0, 10_000
      after
        _ = Sqlite3.execute(lock_db, "ROLLBACK")
        _ = Sqlite3.close(lock_db)
      end
    after
      case Port.info(child, :os_pid) do
        {:os_pid, running_pid} ->
          _ = System.cmd("kill", ["-KILL", Integer.to_string(running_pid)])

        _ ->
          :ok
      end
    end

    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert :sys.get_state(restarted).store.engine.messages == %{}
    assert File.ls!(data_dir) == []
    GenServer.stop(restarted)
  end

  test "file transfer and agent use stay separate across lost reply and restart", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "file-turn",
      "created_at" => 1,
      "title" => "Need a file"
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(511),
               "session_handle" => @handle,
               "message" => message
             })

    file = %{"name" => "image.bin", "data_base64" => Base.encode64(<<0, 1, 255>>)}

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(512),
               "sender_id" => "app:test",
               "message_id" => "file-turn",
               "request_revision" => 1,
               "text" => "Inspect the attachment",
               "files" => [file]
             })

    assert [pending] = agent_entries(relay, "file-turn")
    assert pending["delivery"] == %{"delivered" => false, "acted" => false}
    assert [%{"file_id" => file_id, "name" => "image.bin"}] = pending["files"]
    assert {:ok, %{data: <<0, 1, 255>>}} = Reads.file(relay, "origin", file_id)
    turn = %{entry_id: pending["entry_id"]}
    transfer = %{"file_id" => file_id, "transfer" => "not_transferred", "reason" => "disk full"}

    # A report needs a delivered turn.
    assert {:error, "not_found", _, _} =
             agent_write(relay, :file_use, "file-turn", turn, transfer, 513)

    assert [_, _] = agent_feed(relay)

    assert {:ok, 200, %{"transfer" => "not_transferred"}, false} =
             agent_write(relay, :file_use, "file-turn", turn, transfer, 513)

    assert {:ok, 200, %{"duplicate" => true}, false} =
             agent_write(relay, :file_use, "file-turn", turn, transfer, 514)

    assert {:error, "already_closed", _, _} =
             agent_write(
               relay,
               :file_use,
               "file-turn",
               turn,
               %{"file_id" => file_id, "transfer" => "transferred"},
               515
             )

    assert [%{"delivery" => %{"delivered" => true}}] = agent_entries(relay, "file-turn")
    use = %{"file_id" => file_id, "use" => "unusable", "reason" => "not transferred"}

    assert {:ok, 200, %{"use" => "unusable"}, false} =
             agent_write(relay, :file_use, "file-turn", turn, use, 516)

    assert {:error, "already_closed", _, _} =
             agent_write(
               relay,
               :file_use,
               "file-turn",
               turn,
               %{"file_id" => file_id, "use" => "used"},
               517
             )

    assert {:ok, [discussion, delivery_ack, transfer_fact, use_fact], 4} =
             RelayReaders.conversation_events(relay, "reader", 0)

    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["stage"] == "ack1"
    assert delivery_ack["event_id"] == discussion["event_id"]

    assert Enum.map([discussion, transfer_fact, use_fact], & &1["kind"]) ==
             ~w(discussion file_use file_use)

    assert Enum.map([transfer_fact, use_fact], & &1["event_id"]) == ["2", "2"]
    assert transfer_fact["file"] == "image.bin"
    assert transfer_fact["status"] == "not_transferred"
    assert use_fact["status"] == "unusable"

    reports = [
      %{
        "file_id" => file_id,
        "transfer" => "not_transferred",
        "transfer_reason" => "disk full",
        "use" => "unusable",
        "use_reason" => "not transferred"
      }
    ]

    assert [%{"delivery" => %{"file_uses" => ^reports}}] = agent_entries(relay, "file-turn")

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert [%{"delivery" => %{"delivered" => true, "file_uses" => ^reports}}] =
             agent_entries(restarted, "file-turn")

    GenServer.stop(restarted)
  end

  test "format-17 requests, grants and receipts survive conversation-feed initialization once", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "legacy-request",
      "created_at" => 1,
      "title" => "Still available after upgrade?"
    }

    forward = %{"type" => "forward", "op_id" => op(90), "message" => message}
    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)
    GenServer.stop(relay)

    assert {:ok, before} = Store.open(path)
    prior_version = before.version
    prior = before.engine
    assert :ok = Store.close(before)

    legacy =
      Map.drop(prior, [:origin_feeds, :origin_sessions, :origin_receipts, :origin_actions])

    payload = :erlang.term_to_binary({:attention_relay_state, 17, legacy}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.version == prior_version + 1
    assert migrated.engine.persons == prior.persons
    assert migrated.engine.devices == prior.devices
    assert migrated.engine.sender_grants == prior.sender_grants
    assert migrated.engine.messages == prior.messages
    assert migrated.engine.events == prior.events
    assert migrated.engine.receipts == prior.receipts
    assert migrated.engine.origin_feeds == %{}
    assert migrated.engine.origin_sessions == %{}
    assert migrated.engine.origin_receipts == %{}
    assert migrated.engine.origin_actions == %{}
    assert :ok = Store.close(migrated)

    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", forward)

    assert agent_entries(restarted, "legacy-request") == []
    assert origin_feed_entries(restarted) == []
    GenServer.stop(restarted)

    assert {:ok, again} = Store.open(path)
    assert again.version == prior_version + 1
    assert :ok = Store.close(again)
  end

  test "a sent request and one exact null answer replay to only its origin after restart", %{
    path: path
  } do
    vendor_session_id = String.duplicate("session-", 40)
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "request-1",
      "created_at" => 1,
      "title" => "private title"
    }

    assert %{"type" => "ok", "seq" => 1, "session_handle" => @handle, "revision" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "vendor_session_id" => vendor_session_id,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => message
             })

    assert %{
             "type" => "ok",
             "seq" => 1,
             "session_handle" => @handle,
             "revision" => 1,
             "duplicate" => true
           } =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(1),
               "session_handle" => @handle,
               "vendor_session_id" => vendor_session_id,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => message
             })

    assert %{"session" => @handle, "revision" => 1} = sent = agent_message(relay, "request-1")
    refute Map.has_key?(sent, "terminal")
    assert agent_entries(relay, "request-1") == []
    item = agent_item(relay, "request-1")

    assert %{"type" => "error", "reason" => "request_conflict"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(2),
               "session_handle" => "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
               "message" => message
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "request-1",
               "answer" => nil
             })

    assert {:ok, [_, lifecycle], 2} = RelayReaders.events(relay, "reader", 0)
    assert %{"terminal" => terminal} = agent_message(relay, "request-1")
    assert terminal["state"] == "answered"
    assert Map.has_key?(terminal, "answer") and is_nil(terminal["answer"])
    assert terminal["delivery"] == %{"delivered" => false, "acted" => false}

    # Only its origin reads it.
    assert {:error, :not_found} = AgentAPIInProcess.message(relay, "B", "other", item)
    assert {:ok, [], false, _} = AgentAPIInProcess.changes(relay, "B", "other", 0)

    assert {:error, "not_found", _, _} =
             AgentAPIInProcess.write(
               relay,
               "B",
               "other",
               :answer_receipt,
               %{item_id: item},
               %{"outcome" => "handed"},
               op(7)
             )

    # The origin's v2 feed hands it the answer, which records ACK1 once.
    assert [%{"kind" => "message", "data" => %{"terminal" => %{"state" => "answered"}}}] =
             agent_feed(relay)

    assert {:ok, [], 2} = RelayReaders.events(relay, "reader", 2)
    assert {:ok, [delivery_ack], 1} = RelayReaders.conversation_events(relay, "reader", 0)
    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["stage"] == "ack1"
    assert delivery_ack["event_id"] == lifecycle["event_id"]
    assert delivery_ack["request_revision"] == 1
    refute Map.has_key?(delivery_ack, "answer")
    assert {:ok, [^delivery_ack], 1} = RelayReaders.conversation_events(relay, "origin", 0)

    assert [_] = agent_feed(relay)
    assert {:ok, [], 1} = RelayReaders.conversation_events(relay, "reader", 1)

    assert %{"terminal" => %{"delivery" => %{"delivered" => true, "acted" => false}}} =
             agent_message(relay, "request-1")

    GenServer.stop(relay)
    assert {:ok, stored} = Store.open(path)
    session = stored.engine.origin_sessions[{"A", "origin", @handle}]
    assert session.vendor_session_id == vendor_session_id
    assert session.support["route"] == "claude-code"
    assert :ok = Store.close(stored)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert %{"terminal" => replayed} = agent_message(restarted, "request-1")
    assert Map.drop(replayed, ["delivery"]) == Map.drop(terminal, ["delivery"])
    assert replayed["delivery"] == %{"delivered" => true, "acted" => false}

    result = %{
      "type" => "conversation_result",
      "op_id" => op(4),
      "sender_id" => "app:test",
      "message_id" => "request-1",
      "in_reply_to" => 2,
      "status" => "received"
    }

    assert %{"type" => "ok", "status" => "received", "result_revision" => 1} =
             request(restarted, "origin", result)

    assert %{"type" => "ok", "status" => "received", "duplicate" => true} =
             request(restarted, "origin", result)

    assert %{"terminal" => %{"delivery" => %{"acted" => true}}, "result" => result_state} =
             agent_message(restarted, "request-1")

    assert %{"status" => "received", "revision" => 1} = result_state
    assert [%{"kind" => "result", "author" => "origin"}] = agent_entries(restarted, "request-1")

    assert {:ok, %{"source_status" => "source_reported", "status" => "received"}} =
             Engine.source_status(
               :sys.get_state(restarted).store.engine,
               "A",
               "app:test",
               "request-1"
             )

    assert %{"type" => "ok", "status" => "applied", "result_revision" => 2} =
             request(restarted, "origin", %{result | "op_id" => op(5), "status" => "applied"})

    assert %{"type" => "error", "reason" => "receipt_conflict"} =
             request(restarted, "origin", %{result | "op_id" => op(6), "status" => "refused"})

    assert [%{"kind" => "result"}, %{"kind" => "result"}] =
             agent_entries(restarted, "request-1")

    assert {:ok, 200, _, false} =
             AgentAPIInProcess.write(
               restarted,
               "A",
               "origin",
               :session_end,
               %{session: @handle},
               %{},
               op(8)
             )

    assert %{"availability" => %{"state" => "gone"}} = agent_message(restarted, "request-1")
    GenServer.stop(restarted)
    assert {:ok, again} = Relay.start_link(store_path: path)
    assert %{"availability" => %{"state" => "gone"}} = agent_message(again, "request-1")
    GenServer.stop(again)
  end

  test "person discussion reaches the original feed, replies act on it, and relay hides person text from origin",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    message = %{
      "sender_id" => "app:test",
      "message_id" => "r1",
      "created_at" => 1,
      "title" => "May I proceed?"
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(11),
               "session_handle" => @handle,
               "message" => message
             })

    assert %{"type" => "ok", "seq" => 1, "event_id" => "2"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(12),
               "sender_id" => "app:test",
               "message_id" => "r1",
               "request_revision" => 1,
               "text" => "Please explain the rollout plan"
             })

    assert {:ok, [origin_change], 1} = RelayReaders.conversation_events(relay, "origin", 0)
    refute String.contains?(JSON.encode!(origin_change), "rollout plan")
    assert {:ok, [recipient_change], 1} = RelayReaders.conversation_events(relay, "reader", 0)
    assert recipient_change["text"] == "Please explain the rollout plan"
    assert recipient_change["event_id"] == "2"

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "reader", %{
               "type" => "conversation_reply",
               "op_id" => op(13),
               "sender_id" => "app:test",
               "message_id" => "r1",
               "request_revision" => 1,
               "in_reply_to" => 2,
               "text" => "A forged response"
             })

    reply = %{
      "type" => "conversation_reply",
      "op_id" => op(14),
      "sender_id" => "app:test",
      "message_id" => "r1",
      "request_revision" => 1,
      "in_reply_to" => 2,
      "text" => "First the canary, then the full rollout"
    }

    assert %{"type" => "ok", "seq" => 2, "event_id" => "3"} =
             request(relay, "origin", reply)

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(relay, "origin", reply)

    assert {:ok, [discussion_fact, reply_fact, acted_fact], 3} =
             RelayReaders.conversation_events(relay, "reader", 0)

    assert discussion_fact["event_id"] == "2"
    assert reply_fact["event_id"] == "3"
    assert acted_fact["kind"] == "acted"
    assert acted_fact["event_id"] == "2"
    assert acted_fact["acting_event_id"] == "3"

    assert [discussion, own_reply] = agent_entries(relay, "r1")
    assert discussion["kind"] == "reply"
    assert discussion["author"] == "person"
    assert discussion["delivery"] == %{"acted" => true, "delivered" => false}
    assert own_reply["kind"] == "reply"
    assert own_reply["author"] == "origin"
    assert own_reply["in_reply_to"] == discussion["entry_id"]
    assert own_reply["text"] == "First the canary, then the full rollout"

    agent_feed(relay)

    assert [%{"delivery" => %{"acted" => true, "delivered" => true}}, _] =
             agent_entries(relay, "r1")

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert [%{"delivery" => %{"acted" => true, "delivered" => true}}, _] =
             agent_entries(restarted, "r1")

    # Replies to one person event are not limited to one.
    assert %{"type" => "ok"} =
             request(restarted, "origin", %{
               reply
               | "op_id" => op(15),
                 "text" => "And the rollback plan is ready"
             })

    assert [%{"delivery" => %{"acted" => true}} = turn, _, second_reply] =
             agent_entries(restarted, "r1")

    assert second_reply["in_reply_to"] == turn["entry_id"]
    assert second_reply["text"] == "And the rollback plan is ready"

    GenServer.stop(restarted)
  end

  test "revision preserves earlier discussion, refuses stale final answer, and survives restart",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    first = %{
      "sender_id" => "app:test",
      "message_id" => "revised-request",
      "created_at" => 1,
      "title" => "Deploy now?",
      "choices" => ["Yes", "No"]
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(21),
               "session_handle" => @handle,
               "message" => first
             })

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(22),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "text" => "Which environment?"
             })

    updated = %{first | "title" => "Deploy to production now?"}

    revise = %{
      "type" => "conversation_revise",
      "op_id" => op(23),
      "sender_id" => "app:test",
      "message_id" => "revised-request",
      "base_revision" => 1,
      "in_reply_to" => 2,
      "message" => updated
    }

    assert %{"type" => "ok", "seq" => 2, "revision" => 2} =
             request(relay, "origin", revise)

    assert %{"type" => "ok", "seq" => 2, "revision" => 2, "duplicate" => true} =
             request(relay, "origin", revise)

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "origin", %{revise | "op_id" => op(24)})

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(25),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "answer" => "Yes"
             })

    assert %{"type" => "error", "reason" => "stale_revision", "current_revision" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(26),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "answer" => "Yes"
             })

    assert %{"type" => "ok", "seq" => 4} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(27),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 1,
               "text" => "Still asking about the first wording"
             })

    # The origin's v2 feed hands it both person turns (ACK1).
    agent_feed(relay)

    assert [old_discussion, revision, newer_discussion] =
             agent_entries(relay, "revised-request")

    assert old_discussion["author"] == "person"
    assert old_discussion["request_revision"] == 1
    assert old_discussion["delivery"] == %{"acted" => true, "delivered" => true}
    assert revision["author"] == "origin"
    assert revision["request_revision"] == 2
    assert revision["kind"] == "revision"
    assert newer_discussion["author"] == "person"
    assert newer_discussion["request_revision"] == 1

    assert %{"revision" => 2, "message" => current} = agent_message(relay, "revised-request")
    assert current == Map.drop(updated, ["sender_id", "message_id"])

    {:ok, [old_fact, _, _, newer_fact | _], _} =
      RelayReaders.conversation_events(relay, "reader", 0)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, [initial], 1} = RelayReaders.events(restarted, "reader", 0)

    assert {:ok, [_, revised, acted, _, old_ack, new_ack], 6} =
             RelayReaders.conversation_events(restarted, "reader", 0)

    assert Enum.map([old_ack, new_ack], & &1["event_id"]) == [
             old_fact["event_id"],
             newer_fact["event_id"]
           ]

    assert Enum.all?(
             [old_ack, new_ack],
             &(&1["kind"] == "delivery_ack" and &1["stage"] == "ack1")
           )

    assert initial["type"] == "delivery"
    assert initial["request_revision"] == 1
    assert initial["message"] == first
    assert revised["type"] == "conversation"
    assert revised["request_revision"] == 2
    assert revised["message"] == updated
    assert acted["kind"] == "acted"
    assert acted["event_id"] == "2"
    assert acted["acting_event_id"] == "3"

    assert %{"type" => "ok", "seq" => 2} =
             request(restarted, "reader", %{
               "type" => "answer",
               "op_id" => op(28),
               "sender_id" => "app:test",
               "message_id" => "revised-request",
               "request_revision" => 2,
               "answer" => "Yes"
             })

    assert %{"type" => "error", "reason" => "closed"} =
             request(restarted, "origin", %{revise | "op_id" => op(29), "base_revision" => 2})

    assert %{"revision" => 2, "terminal" => terminal} =
             agent_message(restarted, "revised-request")

    assert terminal["state"] == "answered"
    assert terminal["answer"] == "Yes"
    assert {:ok, [terminal_change], 2} = RelayReaders.events(restarted, "reader", 1)
    assert terminal_change["type"] == "lifecycle"
    assert terminal_change["request_revision"] == 2
    GenServer.stop(restarted)
  end

  test "No comment refuses invalid and future revisions, then accepts an authored older revision",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    first = %{
      "sender_id" => "app:test",
      "message_id" => "no-comment-revision",
      "created_at" => 1,
      "title" => "Deploy to staging?",
      "choices" => ["Yes", "No"]
    }

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(31),
               "session_handle" => @handle,
               "message" => first
             })

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(32),
               "sender_id" => "app:test",
               "message_id" => "no-comment-revision",
               "request_revision" => 1,
               "text" => "Which environment?"
             })

    updated = %{first | "title" => "Deploy to production?"}

    assert %{"type" => "ok", "revision" => 2} =
             request(relay, "origin", %{
               "type" => "conversation_revise",
               "op_id" => op(33),
               "sender_id" => "app:test",
               "message_id" => "no-comment-revision",
               "base_revision" => 1,
               "in_reply_to" => 2,
               "message" => updated
             })

    assert %{"type" => "error", "reason" => "invalid_field"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision"
             })

    assert %{"type" => "error", "reason" => "invalid_field"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 0
             })

    assert %{
             "type" => "error",
             "reason" => "stale_revision",
             "current_revision" => 2
           } =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 3
             })

    refute Map.has_key?(agent_message(relay, "no-comment-revision"), "terminal")

    assert %{"type" => "ok", "terminal_kind" => "no_comment"} =
             no_comment_request(relay, "reader", %{
               "type" => "answer_no_comment",
               "request_id" => "no-comment-revision",
               "request_revision" => 1
             })

    assert %{"revision" => 2, "terminal" => terminal} =
             agent_message(relay, "no-comment-revision")

    assert terminal["state"] == "answered"
    assert terminal["outcome"] == %{"kind" => "no_comment"}
    refute Map.has_key?(terminal, "answer")

    GenServer.stop(relay)
  end

  test "answered and no-comment requests keep chat open for both sides",
       %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {message_id, title, operation} <- [
          {"answered-chat", "Answer this?", 801},
          {"no-comment-chat", "Skip this answer?", 802}
        ] do
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(operation),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => message_id,
                   "created_at" => operation,
                   "title" => title
                 }
               })
    end

    prior_discussions =
      for {message_id, operation, text} <- [
            {"answered-chat", 810, "Before the answer"},
            {"no-comment-chat", 811, "Before No comment"}
          ],
          into: %{} do
        frame = %{
          "type" => "discussion",
          "op_id" => op(operation),
          "sender_id" => "app:test",
          "message_id" => message_id,
          "request_revision" => 1,
          "text" => text
        }

        assert %{"type" => "ok"} = discussion = request(relay, "reader", frame)

        reply_body = %{
          "in_reply_to" => entry_id(relay, message_id, text),
          "text" => "Already sent before answer"
        }

        assert {:ok, 200, %{"entry" => reply}, false} =
                 agent_write(relay, :reply, message_id, %{}, reply_body, operation + 1)

        pending_frame = %{
          frame
          | "op_id" => op(operation + 2),
            "text" => "Pending before answer"
        }

        assert %{"type" => "ok"} = request(relay, "reader", pending_frame)

        {message_id,
         %{
           frame: frame,
           event: discussion,
           reply_body: reply_body,
           reply_op: operation + 1,
           reply: reply,
           pending: entry_id(relay, message_id, "Pending before answer")
         }}
      end

    answer =
      request(relay, "reader", %{
        "type" => "answer",
        "op_id" => op(803),
        "sender_id" => "app:test",
        "message_id" => "answered-chat",
        "request_revision" => 1,
        "answer" => "Yes"
      })

    assert answer["type"] == "ok"

    no_comment =
      no_comment_request(relay, "reader", %{
        "type" => "answer_no_comment",
        "request_id" => "no-comment-chat",
        "request_revision" => 1
      })

    assert no_comment["type"] == "ok"
    assert no_comment["terminal_kind"] == "no_comment"

    # The first answer settled each request, but chat stays open both ways.
    for {message_id, ref, person_op, agent_op, person_text, agent_text} <- [
          {"answered-chat", "answer", 804, 805, "Can we discuss that answer?",
           "Yes, here is more detail."},
          {"no-comment-chat", "no-comment", 806, 807, "Can I explain why I chose no comment?",
           "Of course; what would you like to explain?"}
        ] do
      assert %{"v" => 1, "type" => "ok", "ref" => ref_back} =
               request(relay, "reader", %{
                 "type" => "discussion",
                 "op_id" => op(person_op),
                 "ref" => "person-#{ref}-ref",
                 "sender_id" => "app:test",
                 "message_id" => message_id,
                 "request_revision" => 1,
                 "text" => person_text
               })

      assert ref_back == "person-#{ref}-ref"

      assert {:ok, 200, _, false} =
               agent_write(
                 relay,
                 :reply,
                 message_id,
                 %{},
                 %{"in_reply_to" => prior_discussions[message_id].pending, "text" => agent_text},
                 agent_op
               )

      # The agent may answer the same chat message again; the terminal result
      # of the request is untouched.
      assert {:ok, 200, _, false} =
               agent_write(
                 relay,
                 :reply,
                 message_id,
                 %{},
                 %{
                   "in_reply_to" => entry_id(relay, message_id, person_text),
                   "text" => "A second reply to the same chat message"
                 },
                 agent_op + 100
               )
    end

    chats_before_refusals = chat_views(relay)

    assert %{
             "v" => 1,
             "type" => "error",
             "reason" => "invalid_field",
             "field" => "request_revision",
             "ref" => "stale-revision-ref"
           } =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(816),
               "ref" => "stale-revision-ref",
               "sender_id" => "app:test",
               "message_id" => "answered-chat",
               "request_revision" => 2,
               "text" => "This revision is invalid"
             })

    assert {:error, "not_found", _, _} =
             agent_write(
               relay,
               :reply,
               "no-comment-chat",
               %{},
               %{"in_reply_to" => "e9999", "text" => "This discussion does not exist"},
               817
             )

    assert chat_views(relay) == chats_before_refusals

    for {message_id, %{reply_body: body, reply_op: n}} <- prior_discussions do
      assert {:stored, 200, _} = agent_write(relay, :reply, message_id, %{}, body, n)
    end

    assert chat_views(relay) == chats_before_refusals

    for {message_id, operation} <- [{"answered-chat", 830}, {"no-comment-chat", 831}] do
      assert {:ok, 200, %{"entry" => %{"kind" => "result"}}, false} =
               agent_write(relay, :result, message_id, %{}, %{"status" => "received"}, operation)
    end

    assert %{"terminal" => answered} = agent_message(relay, "answered-chat")
    assert answered["state"] == "answered"
    assert answered["answer"] == "Yes"
    assert %{"terminal" => skipped} = agent_message(relay, "no-comment-chat")
    assert skipped["state"] == "answered"
    assert skipped["outcome"] == %{"kind" => "no_comment"}
    refute Map.has_key?(skipped, "answer")

    texts = fn entries, author ->
      for %{"kind" => "reply", "author" => ^author, "text" => text} <- entries, do: text
    end

    for {message_id, first, after_answer} <- [
          {"answered-chat", "Before the answer", "Can we discuss that answer?"},
          {"no-comment-chat", "Before No comment", "Can I explain why I chose no comment?"}
        ] do
      entries = agent_entries(relay, message_id)
      # Chat entries are replies only; the terminal is the message's, and the
      # result follows the chat.
      assert Enum.map(entries, & &1["kind"]) == List.duplicate("reply", 6) ++ ["result"]
      assert texts.(entries, "person") == [first, "Pending before answer", after_answer]

      assert texts.(entries, "origin") == [
               "Already sent before answer",
               if(message_id == "answered-chat",
                 do: "Yes, here is more detail.",
                 else: "Of course; what would you like to explain?"
               ),
               "A second reply to the same chat message"
             ]
    end

    assert {:ok, conversation_events, _high} =
             RelayReaders.conversation_events(relay, "reader", 0)

    refute Enum.any?(conversation_events, &Map.has_key?(&1, "answer"))

    for {_message_id, %{frame: frame, event: event}} <- prior_discussions do
      assert event["type"] == "ok"

      assert %{"type" => "ok", "duplicate" => true, "event_id" => event_id} =
               request(relay, "reader", frame)

      assert event_id == event["event_id"]

      assert Enum.any?(conversation_events, fn entry ->
               entry["event_id"] == event_id and entry["text"] == frame["text"]
             end)
    end

    for {_message_id, %{reply: reply}} <- prior_discussions do
      assert Enum.any?(conversation_events, fn entry ->
               entry["kind"] == "reply" and entry["text"] == reply["text"]
             end)
    end

    GenServer.stop(relay)
  end

  defp item_availability(relay) do
    assert {:ok, %{"item" => %{"availability" => availability}}} =
             AttentionServer.ClientAPI.Reads.item(relay, "reader", "i1")

    availability
  end

  # An open (or closed) `GET /v2/agent/changes?session=` feed of the origin.
  defp agent_feed_activity(relay, action) do
    feed = %{
      relay: relay,
      principal: %{kind: :device, person: "A", key: "origin"},
      params: %{session: @handle}
    }

    :ok = AttentionServer.AgentAPI.FeedActivity.observe(feed, action)
    _ = :sys.get_state(relay)
    :ok
  end

  defp entry_id(relay, message_id, text) do
    assert %{"entry_id" => id} =
             Enum.find(agent_entries(relay, message_id), &(&1["text"] == text))

    id
  end

  defp chat_views(relay) do
    for id <- ["answered-chat", "no-comment-chat"],
        do: {agent_message(relay, id), agent_entries(relay, id)}
  end

  test "answering one request leaves the chat of it and of its sibling open", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    for {message_id, operation} <- [{"answered", 840}, {"sibling", 841}] do
      assert %{"type" => "ok"} =
               request(relay, "origin", %{
                 "type" => "forward",
                 "op_id" => op(operation),
                 "session_handle" => @handle,
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => message_id,
                   "created_at" => operation,
                   "title" => "Request #{message_id}"
                 }
               })
    end

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(842),
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "answer" => "Done"
             })

    sibling_discussion_frame = %{
      "type" => "discussion",
      "op_id" => op(843),
      "sender_id" => "app:test",
      "message_id" => "sibling",
      "request_revision" => 1,
      "text" => "This sibling is still open"
    }

    assert %{"type" => "ok"} = request(relay, "reader", sibling_discussion_frame)

    assert {:ok, 200, _, false} =
             agent_write(
               relay,
               :reply,
               "sibling",
               %{},
               %{
                 "in_reply_to" => entry_id(relay, "sibling", "This sibling is still open"),
                 "text" => "The sibling reply is still open"
               },
               844
             )

    assert %{"v" => 1, "type" => "ok", "ref" => "answered-ref"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(845),
               "ref" => "answered-ref",
               "sender_id" => "app:test",
               "message_id" => "answered",
               "request_revision" => 1,
               "text" => "This request is answered"
             })

    assert %{"terminal" => %{"state" => "answered", "answer" => "Done"}} =
             agent_message(relay, "answered")

    refute Map.has_key?(agent_message(relay, "sibling"), "terminal")

    assert Enum.map(agent_entries(relay, "answered"), & &1["text"]) == [
             "This request is answered"
           ]

    assert {:ok, history, _high} = RelayReaders.conversation_events(relay, "reader", 0)
    assert Enum.any?(history, &(&1["text"] == "This sibling is still open"))
    assert Enum.any?(history, &(&1["text"] == "The sibling reply is still open"))

    GenServer.stop(relay)
  end

  test "origin withdrawal acts on a named turn and closes once without a second answer", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "A", "other_origin")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    original = %{
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "created_at" => 1,
      "title" => "Proceed?"
    }

    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(31),
               "session_handle" => @handle,
               "message" => original
             })

    assert {:ok, [acceptance], 1} = RelayReaders.events(relay, "reader", 0)
    assert acceptance["type"] == "delivery"
    assert acceptance["message"] == original
    refute Map.has_key?(acceptance, "state")
    refute Map.has_key?(acceptance, "retracted_at")
    refute Map.has_key?(acceptance, "terminal")

    discussion = %{
      "type" => "discussion",
      "op_id" => op(32),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "request_revision" => 1,
      "text" => "Is this still needed?"
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "reader", discussion)

    withdraw = %{
      "type" => "conversation_withdraw",
      "op_id" => op(33),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "in_reply_to" => 2
    }

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "other_origin", %{withdraw | "op_id" => op(34)})

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => retracted_at
           } = request(relay, "origin", withdraw)

    assert is_integer(retracted_at) and retracted_at > 0

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => ^retracted_at,
             "duplicate" => true
           } = request(relay, "origin", withdraw)

    assert %{
             "type" => "error",
             "reason" => "already_retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "origin", %{withdraw | "op_id" => op(39)})

    assert %{"type" => "error", "reason" => "not_origin"} =
             request(relay, "other_origin", %{withdraw | "op_id" => op(40)})

    assert %{"type" => "error", "reason" => "not_found"} =
             request(relay, "origin", %{withdraw | "op_id" => op(41), "message_id" => "missing"})

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(relay, "reader", discussion)

    assert {:ok, [page_delivery, lifecycle], 2} = RelayReaders.events(relay, "reader", 0)
    assert page_delivery["seq"] == acceptance["seq"]
    assert page_delivery["request_revision"] == acceptance["request_revision"]
    assert page_delivery["session_handle"] == acceptance["session_handle"]
    assert page_delivery["message"] == original
    assert page_delivery["state"] == "retracted"
    assert page_delivery["retracted_at"] == retracted_at
    assert lifecycle["seq"] == 2
    assert {:ok, [snapshot_lifecycle], 2} = RelayReaders.events(relay, "reader", 1)
    assert snapshot_lifecycle == lifecycle

    assert {:ok, [origin_reference | _], 2} = RelayReaders.events(relay, "origin", 0)
    assert origin_reference["type"] == "delivery"
    refute Map.has_key?(origin_reference, "message")
    refute Map.has_key?(origin_reference, "state")
    refute Map.has_key?(origin_reference, "retracted_at")

    agent_feed(relay)
    assert [discussion, terminal] = agent_entries(relay, "withdrawn")
    assert discussion["delivery"] == %{"acted" => true, "delivered" => true}
    assert terminal["kind"] == "lifecycle"
    assert terminal["author"] == "origin"
    assert terminal["state"] == "retracted"
    assert %{"terminal" => message_terminal} = agent_message(relay, "withdrawn")
    assert message_terminal == %{"state" => "retracted", "at_ms" => retracted_at}
    {:ok, [discussion_fact | _], _} = RelayReaders.conversation_events(relay, "reader", 0)
    assert {:ok, [lifecycle], 2} = RelayReaders.events(relay, "reader", 1)
    terminal_event_id = lifecycle["event_id"]
    assert lifecycle["author"] == "origin"
    assert lifecycle["state"] == "cleared"
    assert lifecycle["terminal_reason"] == "retracted"
    assert lifecycle["at"] == retracted_at
    assert lifecycle["retracted_at"] == retracted_at

    assert {:ok, [_, retraction, acted, delivery_ack], 4} =
             RelayReaders.conversation_events(relay, "reader", 0)

    assert delivery_ack["kind"] == "delivery_ack"
    assert delivery_ack["event_id"] == discussion_fact["event_id"]
    assert retraction["kind"] == "retraction"
    assert retraction["terminal_state"] == "retracted"
    assert retraction["retracted_at"] == retracted_at
    assert retraction["event_id"] == terminal_event_id
    assert retraction["request_revision"] == 1
    assert retraction["session_handle"] == @handle
    assert acted["kind"] == "acted"
    assert acted["acting_event_id"] == terminal_event_id

    assert %{"type" => "error", "reason" => "unknown_type"} =
             request(relay, "origin", %{
               "type" => "answer_receipt",
               "receipt_version" => 1,
               "sender_id" => "app:test",
               "message_id" => "withdrawn",
               "terminal_seq" => 2
             })

    queued_answer = %{
      "type" => "answer",
      "op_id" => op(36),
      "sender_id" => "app:test",
      "message_id" => "withdrawn",
      "request_revision" => 1,
      "answer" => "stale"
    }

    assert %{
             "type" => "error",
             "reason" => "retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "reader", queued_answer)

    assert %{
             "type" => "error",
             "reason" => "retracted",
             "terminal" => %{
               "state" => "retracted",
               "retracted_at" => ^retracted_at,
               "terminal_seq" => 2
             }
           } = request(relay, "reader", queued_answer)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [replayed], 2} = RelayReaders.events(restarted, "reader", 1)
    assert replayed["state"] == "cleared"
    assert replayed["terminal_reason"] == "retracted"
    assert replayed["at"] == retracted_at
    assert replayed["retracted_at"] == retracted_at

    assert {:ok, [_, %{"kind" => "retraction", "event_id" => retraction_id}, _, ^delivery_ack], 4} =
             RelayReaders.conversation_events(restarted, "reader", 0)

    assert retraction_id == terminal_event_id
    assert [_, ^terminal] = agent_entries(restarted, "withdrawn")
    assert %{"terminal" => ^message_terminal} = agent_message(restarted, "withdrawn")

    assert %{
             "type" => "ok",
             "seq" => 2,
             "state" => "retracted",
             "retracted_at" => ^retracted_at,
             "duplicate" => true
           } = request(restarted, "origin", withdraw)

    assert %{"type" => "ok", "seq" => 3} =
             request(restarted, "origin", %{
               "type" => "forward",
               "op_id" => op(37),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "ordinary-clear",
                 "created_at" => 2,
                 "title" => "No longer needed"
               }
             })

    assert %{"type" => "ok", "seq" => 4} =
             request(restarted, "origin", %{
               "type" => "lifecycle",
               "op_id" => op(38),
               "sender_id" => "app:test",
               "message_id" => "ordinary-clear",
               "state" => "cleared",
               "at" => 3
             })

    assert {:ok, [ordinary_clear], 4} = RelayReaders.events(restarted, "reader", 3)
    assert ordinary_clear["state"] == "cleared"
    refute Map.has_key?(ordinary_clear, "terminal_reason")
    refute Map.has_key?(ordinary_clear, "retracted_at")

    # A withdrawn request still takes chat from the person.
    assert %{"type" => "ok"} =
             request(restarted, "reader", %{
               "type" => "discussion",
               "op_id" => op(35),
               "sender_id" => "app:test",
               "message_id" => "withdrawn",
               "request_revision" => 1,
               "text" => "A late response"
             })

    GenServer.stop(restarted)
  end

  test "a committed origin discussion reply remains nonterminal and retryable after retraction",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(50),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "discussion-reply",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    discussion = %{
      "type" => "discussion",
      "op_id" => op(51),
      "sender_id" => "app:test",
      "message_id" => "discussion-reply",
      "request_revision" => 1,
      "text" => "Please clarify."
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "reader", discussion)

    reply = %{
      "type" => "conversation_reply",
      "op_id" => op(52),
      "sender_id" => "app:test",
      "message_id" => "discussion-reply",
      "in_reply_to" => 2,
      "request_revision" => 1,
      "text" => "The usual route."
    }

    assert %{"type" => "ok", "seq" => 2} = request(relay, "origin", reply)

    assert %{"type" => "ok", "seq" => 2, "state" => "retracted"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(53),
               "sender_id" => "app:test",
               "message_id" => "discussion-reply"
             })

    assert %{"type" => "ok", "seq" => 2, "duplicate" => true} =
             request(relay, "origin", reply)

    entries = agent_entries(relay, "discussion-reply")

    assert Enum.map(entries, &{&1["kind"], &1["author"]}) ==
             [{"reply", "person"}, {"reply", "origin"}, {"lifecycle", "origin"}]

    assert List.last(entries)["state"] == "retracted"
    assert %{"terminal" => %{"state" => "retracted"}} = agent_message(relay, "discussion-reply")

    # After the withdrawal the agent may still reply, and the person may still
    # write; the retraction stays as it was.
    assert %{"type" => "ok"} =
             request(relay, "origin", %{reply | "op_id" => op(54), "text" => "A late reply"})

    assert %{"type" => "ok"} =
             request(relay, "reader", %{discussion | "op_id" => op(55), "text" => "Thanks"})

    later = agent_entries(relay, "discussion-reply")
    assert Enum.take(later, 3) == entries

    assert Enum.map(Enum.drop(later, 3), &{&1["author"], &1["text"]}) ==
             [{"origin", "A late reply"}, {"person", "Thanks"}]

    assert Enum.count(later, &(&1["kind"] == "lifecycle")) == 1
    assert %{"terminal" => %{"state" => "retracted"}} = agent_message(relay, "discussion-reply")

    GenServer.stop(relay)
  end

  test "a committed terminal answer wins over a later origin retraction", %{path: path} do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(60),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "answer-wins",
                 "created_at" => 1,
                 "title" => "Question"
               }
             })

    assert %{"type" => "ok", "seq" => 2} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(61),
               "sender_id" => "app:test",
               "message_id" => "answer-wins",
               "request_revision" => 1,
               "answer" => "Done"
             })

    assert %{"type" => "error", "reason" => "closed"} =
             request(relay, "origin", %{
               "type" => "conversation_withdraw",
               "op_id" => op(62),
               "sender_id" => "app:test",
               "message_id" => "answer-wins"
             })

    assert {:ok, [lifecycle], 2} = RelayReaders.events(relay, "reader", 1)
    assert lifecycle["state"] == "answered"

    assert %{"terminal" => terminal} = agent_message(relay, "answer-wins")
    assert terminal["state"] == "answered"
    assert terminal["answer"] == "Done"
    assert agent_entries(relay, "answer-wins") == []
    GenServer.stop(relay)
  end

  test "an open request reports live availability without adding conversation history",
       %{
         path: path
       } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_person(relay, "B")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_device(relay, "B", "other")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(51),
               "session_handle" => @handle,
               "route" => "claude-code",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "availability",
                 "created_at" => 1,
                 "title" => "Private question"
               }
             })

    assert {:ok, [initial], 1} = RelayReaders.events(relay, "reader", 0)
    assert initial["availability"] == "not_listening"
    assert initial["session_handle"] == @handle
    assert initial["request_revision"] == 1
    assert initial["event_id"] == "1"
    assert initial["listening_support"]["route"] == "claude-code"
    assert initial["listening_support"]["origin"] == "origin"
    assert is_integer(initial["listening_support"]["reported_at"])

    agent_feed_activity(relay, :open)
    assert item_availability(relay) == %{"state" => "listening"}
    assert {:ok, [], 0} = RelayReaders.conversation_events(relay, "reader", 0)

    agent_feed_activity(relay, :close)

    assert %{"state" => "not_listening", "last_detached_at_ms" => stamp} =
             item_availability(relay)

    assert is_integer(stamp)

    for n <- [52, 53] do
      assert {:ok, 200, _, false} =
               AgentAPIInProcess.write(
                 relay,
                 "A",
                 "origin",
                 :session_end,
                 %{session: @handle},
                 %{},
                 op(n)
               )
    end

    assert %{"state" => "gone"} = item_availability(relay)
    assert {:ok, [], 0} = RelayReaders.events(relay, "other", 0)

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)
    assert {:ok, [initial], 1} = RelayReaders.events(restarted, "reader", 0)
    assert {:ok, [], 0} = RelayReaders.conversation_events(restarted, "reader", 0)
    assert %{"state" => "gone"} = item_availability(restarted)

    assert %{"availability" => %{"state" => "gone"}} = agent_message(restarted, "availability")

    GenServer.stop(restarted)
  end

  test "availability refreshes after restart without replaying chat history", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(81),
               "session_handle" => @handle,
               "route" => "other",
               "cli_version" => "test-only",
               "unsupported_reason" => nil,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "availability-replay",
                 "created_at" => 1,
                 "title" => "Availability survives restart"
               }
             })

    {before_version, before_payload, before_checksum} = state_snapshot(path)
    assert :crypto.hash(:sha256, before_payload) == before_checksum

    agent_feed_activity(relay, :open)
    {after_version, after_payload, after_checksum} = state_snapshot(path)

    assert after_version == before_version + 1
    assert after_payload != before_payload
    assert :crypto.hash(:sha256, after_payload) == after_checksum
    assert item_availability(relay) == %{"state" => "listening"}
    assert {:ok, [], 0} = RelayReaders.conversation_events(relay, "reader", 0)

    assert :ok = Relay.setup_person(relay, "B")
    {next_version, next_payload, next_checksum} = state_snapshot(path)
    assert next_version == after_version + 1
    assert next_payload != after_payload
    assert :crypto.hash(:sha256, next_payload) == next_checksum

    GenServer.stop(relay)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert {:ok, [initial], 1} = RelayReaders.events(restarted, "reader", 0)
    assert initial["availability"] == "not_listening"

    assert {:ok, [], 0} = RelayReaders.conversation_events(restarted, "reader", 0)

    assert %{"state" => "not_listening", "last_detached_at_ms" => stamp} =
             item_availability(restarted)

    assert is_integer(stamp)

    assert {:error, :person_exists} = Relay.setup_person(restarted, "B")

    {_version, restored_payload, restored_checksum} = state_snapshot(path)
    assert :crypto.hash(:sha256, restored_payload) == restored_checksum

    GenServer.stop(restarted)
  end

  test "a stored 0.1 support record decodes on a cold reopen and still feeds availability", %{
    path: path
  } do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")

    forward = %{
      "type" => "forward",
      "op_id" => op(91),
      "session_handle" => @handle,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "support-receipt",
        "created_at" => 1,
        "title" => "Support survives restart"
      }
    }

    assert %{"type" => "ok", "seq" => 1} = request(relay, "origin", forward)

    GenServer.stop(relay)

    # A 0.1 origin declared listening support for its session; client API v2
    # never writes one. The stored record survives a cold reopen.
    support = %{
      "type" => "support",
      "op_id" => op(92),
      "session_handle" => @handle,
      "route" => "claude-code",
      "cli_version" => "test-only",
      "unsupported_reason" => nil
    }

    PreChangeLogStore.write(path, &PreChangeLogStore.origin_support(&1, "origin", support))
    {:ok, store} = Store.open(path)
    declared = store.engine.origin_sessions[{"A", "origin", @handle}].support
    :ok = Store.close(store)
    assert %{"route" => "claude-code", "origin" => "origin"} = declared
    assert is_integer(declared["reported_at"])

    cold_store_reopen(path)
    assert {:ok, restarted} = Relay.start_link(store_path: path)

    assert :sys.get_state(restarted).store.engine.origin_sessions[{"A", "origin", @handle}].support ==
             declared

    agent_feed_activity(restarted, :open)
    assert item_availability(restarted) == %{"state" => "listening"}
    assert {:ok, [], 0} = RelayReaders.conversation_events(restarted, "reader", 0)

    duplicate_state = state_snapshot(path)

    assert %{"type" => "ok", "seq" => 1, "duplicate" => true} =
             request(restarted, "origin", forward)

    assert state_snapshot(path) == duplicate_state

    {attach_version, attach_payload, _} = duplicate_state
    assert :ok = Relay.setup_person(restarted, "B")
    {folded_version, folded_payload, folded_checksum} = state_snapshot(path)
    assert folded_version > attach_version
    assert folded_payload != attach_payload
    assert :crypto.hash(:sha256, folded_payload) == folded_checksum

    GenServer.stop(restarted)
    assert {:ok, folded_restart} = Relay.start_link(store_path: path)

    assert :sys.get_state(folded_restart).store.engine.origin_sessions[{"A", "origin", @handle}].support ==
             declared

    GenServer.stop(folded_restart)
  end
end

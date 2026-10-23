defmodule AttentionServer.PrioritizationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DashboardView, Engine, JSON, JevClient, Placement, Relay, Store}
  alias Exqlite.Sqlite3

  @endpoint "https://api.typesafe.ai/v1/systemone"
  @model "jev-latest"

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "harklight-prioritization-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir, path: Path.join(dir, "relay.sqlite3")}
  end

  defp op(n),
    do:
      "00000000-0000-4000-8000-" <>
        (n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0"))

  defp item(id, accepted_seq, deadline \\ nil) do
    message = %{"title" => "Request #{id}", "body" => "Details for #{id}"}
    message = if is_integer(deadline), do: Map.put(message, "by_when", deadline), else: message

    %{
      identity: {"app:test", id},
      accepted_seq: accepted_seq,
      revision: 1,
      message: message
    }
  end

  defp raw(frame), do: JSON.encode!(Map.put_new(frame, "v", 1)) <> "\n"

  defp request(relay, key, frame) do
    assert {:ok, line} = Relay.request_line(relay, key, raw(frame))
    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp session_request(relay, session, frame) do
    assert {:ok, lines} = Relay.session_request_line(relay, session, raw(frame))

    Enum.map(lines, fn line ->
      assert {:ok, reply} = JSON.decode_line(line)
      reply
    end)
  end

  defp drain(relay, session) do
    assert {:ok, lines} = Relay.session_drain(relay, session)

    Enum.map(lines, fn line ->
      assert {:ok, reply} = JSON.decode_line(line)
      reply
    end)
  end

  defp start_relay(path) do
    assert {:ok, relay} = Relay.start_link(store_path: path)
    Process.unlink(relay)
    on_exit(fn -> if Process.alive?(relay), do: GenServer.stop(relay) end)
    relay
  end

  defp use_synthetic_jev_key(dir) do
    previous_key_path = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    key_path = Path.join(dir, "synthetic-jev-key")
    key = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
    File.write!(key_path, key <> "\n")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", key_path)

    on_exit(fn ->
      if is_binary(previous_key_path),
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous_key_path),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")
    end)
  end

  defp configure_jev_choice(relay) do
    state = :sys.get_state(relay)

    assert {:ok, engine} =
             Engine.set_jev_connection(
               state.store.engine,
               "https://127.0.0.1:1/v1/systemone",
               @model
             )

    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")

    assert {:ok, saved} =
             Store.commit(state.store, engine, %{
               person: "person-a",
               actor: "owner",
               action: "choice"
             })

    :sys.replace_state(relay, &%{&1 | store: saved})
  end

  defp start_synthetic_priority_pass(relay) do
    test_process = self()
    relay_pid = relay

    state =
      :sys.replace_state(relay, fn state ->
        person = "person-a"
        sequence = Map.get(state.priority_sequences, person, 0) + 1
        generation = Map.get(state.priority_generations, person, 0) + 1

        {pid, monitor} =
          spawn_monitor(fn ->
            receive do
              {:complete, result} ->
                send(relay_pid, {:prioritization_result, person, sequence, generation, result})
                barrier_result = Relay.events(relay_pid, "device-a", 0)
                send(test_process, {:priority_result_handled, self(), barrier_result})
            end
          end)

        job = %{pid: pid, monitor: monitor, sequence: sequence, generation: generation}

        %{
          state
          | priority_generations: Map.put(state.priority_generations, person, generation),
            priority_sequences: Map.put(state.priority_sequences, person, sequence),
            priority_jobs: Map.put(state.priority_jobs, person, job),
            priority_queued: Map.delete(state.priority_queued, person)
        }
      end)

    job = state.priority_jobs["person-a"]
    on_exit(fn -> if Process.alive?(job.pid), do: Process.exit(job.pid, :kill) end)
    job
  end

  defp complete_synthetic_priority_pass(job, result) do
    expected_pid = job.pid
    send(job.pid, {:complete, result})
    assert_receive {:priority_result_handled, ^expected_pid, {:ok, _events, _sequence}}
  end

  defp setup_person(relay) do
    assert :ok = Relay.setup_person(relay, "person-a")
    assert :ok = Relay.setup_device(relay, "person-a", "device-a")
    assert :ok = Relay.setup_device(relay, "person-a", "device-b")
    assert :ok = Relay.setup_sender(relay, "device-a", "app:test")
  end

  defp forward(relay, n, id, deadline \\ nil) do
    message = %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => n,
      "title" => "Request #{id}"
    }

    message = if is_integer(deadline), do: Map.put(message, "by_when", deadline), else: message

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(n),
               "message" => message
             })
  end

  test "baseline orders deadlines first and then uses newest acceptance" do
    items = [
      item("newest", 4),
      Map.update!(item("invalid-deadline", 5), :message, &Map.put(&1, "by_when", "tomorrow")),
      item("later-deadline", 2, 20),
      item("earliest", 1, 10),
      item("older", 3)
    ]

    entries = Placement.place("baseline", items, [], nil)

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "earliest", "due_at" => 10},
             %{"sender_id" => "app:test", "message_id" => "later-deadline", "due_at" => 20},
             %{"sender_id" => "app:test", "message_id" => "invalid-deadline"},
             %{"sender_id" => "app:test", "message_id" => "newest"},
             %{"sender_id" => "app:test", "message_id" => "older"}
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))
    refute Map.has_key?(Enum.at(entries, 2), "due_at")

    assert {:ok, baseline} = Engine.setup_person(Engine.new(), "person-a")
    assert Placement.reassess(baseline, "person-a", []) == :none
  end

  test "selected placement retains the last whole-set order and baseline-places new arrivals" do
    items = [
      item("older", 1),
      item("new-late", 4, 30),
      item("new-early", 3, 10),
      item("newest", 2)
    ]

    previous = [
      %{
        "sender_id" => "app:test",
        "message_id" => "newest",
        "reason" => "Ranked by how soon it needs handling"
      },
      %{
        "sender_id" => "app:test",
        "message_id" => "older",
        "reason" => "Ranked by how soon it needs handling"
      }
    ]

    entries = Placement.place("jev-set", items, previous, nil)

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "newest"},
             %{"sender_id" => "app:test", "message_id" => "older"},
             %{"sender_id" => "app:test", "message_id" => "new-early", "due_at" => 10},
             %{"sender_id" => "app:test", "message_id" => "new-late", "due_at" => 30}
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))

    assert {:ok, engine} = Engine.setup_person(Engine.new(), "person-a")
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    assert Placement.reassess(engine, "person-a", []) == :whole_set
  end

  test "ranked placement carries deadlines and omits reasons" do
    items = [item("older", 1, 100), item("newest", 2)]

    entries =
      Placement.place(
        "jev-set",
        items,
        [],
        {:ranked, [{"app:test", "older"}, {"app:test", "newest"}]}
      )

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "older", "due_at" => 100},
             %{"sender_id" => "app:test", "message_id" => "newest"}
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))
  end

  test "Jev composes one typed score question for every item in the same full set" do
    items = [item("later", 1, 200), item("earliest", 2, 100)]

    assert {:ok, request} =
             Placement.compose("jev-set", items, %{endpoint: @endpoint, model: @model})

    assert request.uri.host == "api.typesafe.ai"
    assert request.uri.path == "/v1/systemone"

    assert {:ok,
            %{
              "model" => @model,
              "questions" => questions,
              "state" => %{"pending_items" => pending}
            }} = JSON.decode_line(request.body <> "\n")

    assert Map.keys(questions) |> Enum.sort() == ["candidate_1", "candidate_2"]
    assert Enum.all?(questions, fn {_id, question} -> question["type"] == "score" end)
    assert Enum.map(pending, & &1["message_id"]) == ["earliest", "later"]
    assert request.candidates == [{"app:test", "earliest"}, {"app:test", "later"}]
  end

  test "Jev scores must cover every candidate and are sorted with stable baseline ties" do
    candidates = [{"app:test", "first"}, {"app:test", "second"}, {"app:test", "third"}]

    questions = %{
      "candidate_1" => hd(candidates),
      "candidate_2" => Enum.at(candidates, 1),
      "candidate_3" => List.last(candidates)
    }

    response =
      ~s({"model":"jev-latest","answers":{"candidate_1":{"type":"score","score":1.5},"candidate_2":{"type":"score","score":2},"candidate_3":{"type":"score","score":2}}})

    assert {:ok, [{"app:test", "second"}, {"app:test", "third"}, {"app:test", "first"}]} =
             JevClient.rank_response(response, questions, candidates)

    assert {:error, :malformed_response} =
             JevClient.rank_response(
               ~s({"answers":{"candidate_1":{"type":"score","score":1}}}),
               questions,
               candidates
             )

    assert {:error, :malformed_response} =
             JevClient.rank_response(
               ~s({"answers":{"candidate_1":{"type":"score","score":1},"candidate_2":{"type":"score","score":1},"candidate_3":{"type":"score","score":3}}}),
               questions,
               candidates
             )
  end

  test "algorithm choices are per person and setting the current choice is a no-op" do
    assert {:ok, engine} = Engine.setup_person(Engine.new(), "person-a")
    assert {:ok, ^engine} = Engine.set_algorithm_choice(engine, "person-a", "baseline")
    assert {:ok, selected} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    assert Placement.choice(selected, "person-a") == "jev-set"
    assert Placement.choice(selected, "another-person") == "baseline"
    assert {:ok, ^selected} = Engine.set_algorithm_choice(selected, "person-a", "jev-set")
  end

  test "format 22 migrates with baseline defaults and persists as format 23", %{path: path} do
    assert {:ok, initial} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(initial.engine, "person-a")
    assert :ok = Store.close(initial)

    old_engine =
      engine
      |> Map.delete(:algorithm_choices)
      |> Map.delete(:jev_endpoint)
      |> Map.delete(:jev_model)

    payload = :erlang.term_to_binary({:attention_relay_state, 22, old_engine}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)

    assert {:ok, migrated} = Store.open(path)
    assert migrated.engine.algorithm_choices == %{}
    assert migrated.engine.jev_endpoint == nil
    assert migrated.engine.jev_model == nil
    assert Placement.choice(migrated.engine, "person-a") == "baseline"
    assert :ok = Store.close(migrated)

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, select} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
    assert {:row, [saved_payload]} = Sqlite3.step(db, select)
    assert :ok = Sqlite3.release(db, select)
    assert :ok = Sqlite3.close(db)

    assert {:attention_relay_state, 23, _saved_engine} =
             :erlang.binary_to_term(saved_payload, [:safe])
  end

  test "choice and Jev connection persist without storing a key path or bytes", %{path: path} do
    assert {:ok, store} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(store.engine, "person-a")
    assert {:ok, engine} = Engine.set_jev_connection(engine, @endpoint, @model)
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")

    assert {:ok, saved} =
             Store.commit(store, engine, %{person: "person-a", actor: "owner", action: "settings"})

    assert :ok = Store.close(saved)

    assert {:ok, reopened} = Store.open(path)
    assert Placement.choice(reopened.engine, "person-a") == "jev-set"
    assert reopened.engine.jev_endpoint == @endpoint
    assert reopened.engine.jev_model == @model
    refute Map.has_key?(Map.from_struct(reopened.engine), :jev_key_path)
    refute Map.has_key?(Map.from_struct(reopened.engine), :jev_key)
    assert :ok = Store.close(reopened)
  end

  test "Jev key bytes and key path stay out of stored state, logs and the config page", %{
    path: path,
    dir: dir
  } do
    previous_key_path = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    key_path = Path.join(dir, "private-jev-key")
    secret = "test-key-bytes-must-never-leak-6ab64a"
    File.write!(key_path, secret <> "\n")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", key_path)

    on_exit(fn ->
      if is_binary(previous_key_path),
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous_key_path),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")
    end)

    assert JevClient.key_status() == "readable"
    assert {:ok, store} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(store.engine, "person-a")
    assert {:ok, engine} = Engine.set_jev_connection(engine, @endpoint, @model)
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")

    assert {:ok, saved} =
             Store.commit(store, engine, %{person: "person-a", actor: "owner", action: "settings"})

    assert :ok = Store.close(saved)

    persisted =
      Path.wildcard(path <> "*")
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&File.read!/1)
      |> IO.iodata_to_binary()

    refute persisted =~ secret
    refute persisted =~ key_path

    page =
      DashboardView.prioritization(
        "Owner",
        %{
          algorithm_id: "jev-set",
          endpoint: @endpoint,
          model: @model,
          key_status: "readable",
          basis: "fallback"
        },
        nil,
        []
      )

    assert page =~ "Soonest deadline first, then newest"
    assert page =~ "Jev re-ranking"
    assert page =~ "Jev key:"
    assert length(Regex.scan(~r/Waiting items are in basic order until Jev answers\./, page)) == 1
    refute page =~ "<p>Choose how waiting items are ordered on this server.</p>"
    refute page =~ "The key is read from the server's configured key file."
    refute page =~ ">Baseline</option>"
    refute page =~ secret
    refute page =~ key_path
    refute page =~ "ATTENTION_SERVER_JEV_KEY_PATH"

    baseline_page =
      DashboardView.prioritization(
        "Owner",
        %{
          algorithm_id: "baseline",
          endpoint: @endpoint,
          model: @model,
          key_status: "readable",
          basis: "selected"
        },
        nil,
        []
      )

    refute baseline_page =~ "Waiting items are in basic order until Jev answers."

    assert {:ok, request} =
             JevClient.compose(
               [item("private-key-check", 1)],
               "https://127.0.0.1:1/v1/systemone",
               @model
             )

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, :unavailable} = JevClient.evaluate(request)
      end)

    refute logs =~ secret
    refute logs =~ key_path
  end

  test "view fetch is complete, supports unchanged replies and works during a pending checkpoint",
       %{path: path} do
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "older")
    forward(relay, 2, "newer", 200)
    assert {:ok, session} = Relay.open_websocket_session(relay, "device-b", nil, nil)

    [start] =
      session_request(relay, session, %{
        "type" => "inbox_start",
        "paging_version" => 1,
        "startup_id" => "view-during-checkpoint",
        "since" => 0,
        "ref" => "start"
      })

    assert start["type"] == "ok"

    assert %{
             "type" => "ok",
             "algorithm" => "baseline",
             "basis" => "selected",
             "entries" => entries
           } =
             hd(session_request(relay, session, %{"type" => "view_get", "ref" => "view-1"}))

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "newer", "due_at" => 200},
             %{"sender_id" => "app:test", "message_id" => "older"}
           ]

    view_rev = :sys.get_state(relay).priority_views["person-a"].view_rev

    assert %{"type" => "ok", "unchanged" => true, "view_rev" => ^view_rev} =
             hd(
               session_request(relay, session, %{
                 "type" => "view_get",
                 "ref" => "view-2",
                 "known_view_rev" => view_rev
               })
             )

    assert %{"type" => "error", "reason" => "invalid_field", "ref" => "view-bad"} =
             hd(
               session_request(relay, session, %{
                 "type" => "view_get",
                 "ref" => "view-bad",
                 "unknown" => true
               })
             )

    assert %{"type" => "error", "reason" => "unsupported_version", "ref" => "view-old"} =
             hd(
               session_request(relay, session, %{
                 "v" => 2,
                 "type" => "view_get",
                 "ref" => "view-old"
               })
             )

    GenServer.stop(relay)
  end

  test "ready sessions get one coalesced out-of-band view notice only after fetching a view", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)

    assert {:ok, supporting} = Relay.open_websocket_session(relay, "device-a", nil, nil)
    assert {:ok, legacy} = Relay.open_websocket_session(relay, "device-b", nil, nil)

    assert List.last(session_request(relay, supporting, %{"type" => "resync", "since" => 0}))[
             "type"
           ] ==
             "resynced"

    assert List.last(session_request(relay, legacy, %{"type" => "resync", "since" => 0}))["type"] ==
             "resynced"

    assert %{"type" => "ok", "algorithm" => "baseline"} =
             hd(session_request(relay, supporting, %{"type" => "view_get", "ref" => "view"}))

    forward(relay, 1, "first-new")
    forward(relay, 2, "second-new")

    supporting_frames = drain(relay, supporting)
    legacy_frames = drain(relay, legacy)
    notices = Enum.filter(supporting_frames, &(&1["type"] == "view_changed"))
    assert length(notices) == 1
    refute Map.has_key?(hd(notices), "seq")
    refute Enum.any?(legacy_frames, &(&1["type"] == "view_changed"))

    assert {:ok, events, 2} = Relay.events(relay, "device-a", 0)
    assert Enum.map(events, & &1["seq"]) == [1, 2]

    latest = hd(session_request(relay, supporting, %{"type" => "view_get", "ref" => "latest"}))
    assert hd(notices)["view_rev"] == latest["view_rev"]
    GenServer.stop(relay)
  end

  test "reordered snapshots page every waiting item before resolved items and retries are stable",
       %{path: path} do
    relay = start_relay(path)
    setup_person(relay)

    for n <- 1..28 do
      deadline = if n <= 25, do: (30 - n) * 1_000, else: nil
      forward(relay, n, "item-#{String.pad_leading(Integer.to_string(n), 2, "0")}", deadline)
    end

    resolved_ids = ["item-01", "item-03", "item-27"]

    for {id, n} <- Enum.with_index(resolved_ids, 100) do
      assert %{"type" => "ok"} =
               request(relay, "device-a", %{
                 "type" => "answer",
                 "op_id" => op(n),
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "answer" => nil
               })
    end

    assert {:ok, session} = Relay.open_websocket_session(relay, "device-b", nil, nil)

    [first] =
      session_request(relay, session, %{
        "type" => "inbox_start",
        "paging_version" => 1,
        "startup_id" => "reordered-snapshot",
        "since" => 0,
        "ref" => "start"
      })

    assert first["type"] == "ok"
    assert length(first["items"]) == 20

    # A fetch remains available while the newly built snapshot awaits attach.
    assert %{"type" => "ok", "entries" => _} =
             hd(
               session_request(relay, session, %{"type" => "view_get", "ref" => "checkpoint-view"})
             )

    retry_start =
      session_request(relay, session, %{
        "type" => "inbox_start",
        "paging_version" => 1,
        "startup_id" => "reordered-snapshot",
        "since" => 0,
        "ref" => "start-retry"
      })
      |> hd()

    assert retry_start["snapshot_id"] == first["snapshot_id"]
    assert retry_start["items"] == first["items"]

    page_frame = %{
      "type" => "inbox_page",
      "paging_version" => 1,
      "snapshot_id" => first["snapshot_id"],
      "cursor" => first["next_cursor"],
      "ref" => "page-2"
    }

    second = hd(session_request(relay, session, page_frame))
    second_retry = hd(session_request(relay, session, page_frame))
    assert second_retry["items"] == second["items"]

    all_items = first["items"] ++ second["items"]
    ids = Enum.map(all_items, & &1["acceptance"]["message_id"])
    waiting_ids = Enum.reject(ids, &(&1 in resolved_ids))
    resolved_page_ids = Enum.filter(ids, &(&1 in resolved_ids))

    expected_waiting =
      Enum.reject(
        1..28,
        &("item-#{String.pad_leading(Integer.to_string(&1), 2, "0")}" in resolved_ids)
      )
      |> Enum.sort_by(fn n -> if n <= 25, do: {0, (30 - n) * 1_000, -n}, else: {1, 0, -n} end)
      |> Enum.map(&"item-#{String.pad_leading(Integer.to_string(&1), 2, "0")}")

    assert waiting_ids == expected_waiting
    assert resolved_page_ids == ["item-27", "item-03", "item-01"]
    assert length(ids) == length(Enum.uniq(ids))

    assert Enum.all?(
             Enum.take(all_items, length(expected_waiting)),
             &(not Map.has_key?(&1, "terminal"))
           )

    assert Enum.all?(
             Enum.drop(all_items, length(expected_waiting)),
             &Map.has_key?(&1, "terminal")
           )

    assert hd(ids) == "item-25"

    engine = :sys.get_state(relay).store.engine

    assert {:ok, incomplete_order, _receipts, _operation_ids, _high} =
             Engine.inbox_snapshot(engine, "device-b", 0, [{"app:test", "item-25"}])

    assert hd(incomplete_order) == {"app:test", "item-25"}

    missing_waiting =
      expected_waiting
      |> Enum.reject(&(&1 == "item-25"))
      |> Enum.sort_by(fn id -> -String.to_integer(String.trim_leading(id, "item-")) end)

    assert Enum.slice(incomplete_order, 1, length(missing_waiting)) ==
             Enum.map(missing_waiting, &{"app:test", &1})

    GenServer.stop(relay)
  end

  test "a missing key keeps the saved Jev choice on baseline order and intake remains available",
       %{path: path} do
    previous_key_path = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    missing_key_path = Path.join(Path.dirname(path), "missing-key")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", missing_key_path)

    on_exit(fn ->
      if is_binary(previous_key_path),
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous_key_path),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")
    end)

    relay = start_relay(path)
    setup_person(relay)
    engine = :sys.get_state(relay).store.engine
    assert {:ok, engine} = Engine.set_jev_connection(engine, @endpoint, @model)
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    store = :sys.get_state(relay).store

    assert {:ok, saved} =
             Store.commit(store, engine, %{person: "person-a", actor: "owner", action: "choice"})

    :sys.replace_state(relay, &%{&1 | store: saved})

    assert {:ok, session} = Relay.open_websocket_session(relay, "device-a", nil, nil)

    assert %{"type" => "ok", "algorithm" => "jev-set", "basis" => "fallback", "entries" => []} =
             hd(session_request(relay, session, %{"type" => "view_get", "ref" => "fallback"}))

    forward(relay, 1, "while-unavailable")

    assert %{
             "type" => "ok",
             "algorithm" => "jev-set",
             "basis" => "fallback",
             "entries" => entries
           } =
             hd(
               session_request(relay, session, %{
                 "type" => "view_get",
                 "ref" => "fallback-after-intake"
               })
             )

    assert Enum.map(entries, & &1["message_id"]) == ["while-unavailable"]
    GenServer.stop(relay)
  end

  test "a completed synthetic Jev pass applies whole-set order and leaves the relay available",
       %{path: path, dir: dir} do
    use_synthetic_jev_key(dir)
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "older", 200)
    forward(relay, 2, "newer", 100)
    forward(relay, 3, "undated")
    assert {:ok, session} = Relay.open_websocket_session(relay, "device-a", nil, nil)

    configure_jev_choice(relay)

    assert %{
             "type" => "ok",
             "algorithm" => "jev-set",
             "basis" => "fallback",
             "entries" => baseline_entries
           } = hd(session_request(relay, session, %{"type" => "view_get", "ref" => "before"}))

    assert baseline_entries == [
             %{"sender_id" => "app:test", "message_id" => "newer", "due_at" => 100},
             %{"sender_id" => "app:test", "message_id" => "older", "due_at" => 200},
             %{"sender_id" => "app:test", "message_id" => "undated"}
           ]

    assert Enum.all?(baseline_entries, &(not Map.has_key?(&1, "reason")))

    engine = :sys.get_state(relay).store.engine
    ranked_identities = [{"app:test", "older"}, {"app:test", "undated"}, {"app:test", "newer"}]
    assert Placement.choice(engine, "person-a") == "jev-set"
    assert Placement.valid_permutation?(Placement.items(engine, "person-a"), ranked_identities)

    job = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(job, {:ok, ranked_identities})

    assert %{
             "type" => "ok",
             "algorithm" => "jev-set",
             "basis" => "selected",
             "entries" => selected_entries
           } = hd(session_request(relay, session, %{"type" => "view_get", "ref" => "after"}))

    assert selected_entries == [
             %{"sender_id" => "app:test", "message_id" => "older", "due_at" => 200},
             %{"sender_id" => "app:test", "message_id" => "undated"},
             %{"sender_id" => "app:test", "message_id" => "newer", "due_at" => 100}
           ]

    assert Enum.all?(selected_entries, &(not Map.has_key?(&1, "reason")))
    assert Process.alive?(relay)
    state = :sys.get_state(relay)
    assert state.priority_jobs == %{}
    assert state.priority_queued == %{}
    GenServer.stop(relay)
  end

  test "unavailable and malformed synthetic pass results keep baseline fallback and intake working",
       %{dir: dir} do
    use_synthetic_jev_key(dir)

    for {suffix, result} <- [
          {"unavailable", {:error, :unavailable}},
          {"malformed", {:ok, [{"app:test", "not-in-the-waiting-set"}]}}
        ] do
      relay = start_relay(Path.join(dir, "#{suffix}.sqlite3"))
      setup_person(relay)
      forward(relay, 1, "older")
      forward(relay, 2, "newer")
      assert {:ok, session} = Relay.open_websocket_session(relay, "device-a", nil, nil)

      configure_jev_choice(relay)
      job = start_synthetic_priority_pass(relay)
      complete_synthetic_priority_pass(job, result)

      assert %{
               "type" => "ok",
               "algorithm" => "jev-set",
               "basis" => "fallback",
               "entries" => fallback_entries
             } = hd(session_request(relay, session, %{"type" => "view_get", "ref" => "fallback"}))

      assert fallback_entries == [
               %{"sender_id" => "app:test", "message_id" => "newer"},
               %{"sender_id" => "app:test", "message_id" => "older"}
             ]

      assert Enum.all?(fallback_entries, &(not Map.has_key?(&1, "reason")))
      assert Process.alive?(relay)
      state = :sys.get_state(relay)
      assert state.priority_jobs == %{}
      assert state.priority_queued == %{}

      forward(relay, 3, "arrived-after-pass")

      assert %{
               "type" => "ok",
               "algorithm" => "jev-set",
               "basis" => "fallback",
               "entries" => updated_entries
             } =
               hd(
                 session_request(relay, session, %{
                   "type" => "view_get",
                   "ref" => "after-intake"
                 })
               )

      assert updated_entries == [
               %{"sender_id" => "app:test", "message_id" => "arrived-after-pass"},
               %{"sender_id" => "app:test", "message_id" => "newer"},
               %{"sender_id" => "app:test", "message_id" => "older"}
             ]

      assert Enum.all?(updated_entries, &(not Map.has_key?(&1, "reason")))

      assert {:ok, events, 3} = Relay.events(relay, "device-a", 0)
      assert length(events) == 3
      assert Process.alive?(relay)
      GenServer.stop(relay)
    end
  end

  test "restart with a saved Jev choice stays up and serves baseline fallback", %{path: path} do
    previous_key_path = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    missing_key_path = Path.join(Path.dirname(path), "missing-key-after-restart")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", missing_key_path)

    on_exit(fn ->
      if is_binary(previous_key_path),
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous_key_path),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")
    end)

    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "later-deadline", 20)
    forward(relay, 2, "earlier-deadline", 10)
    forward(relay, 3, "undated")

    engine = :sys.get_state(relay).store.engine
    assert {:ok, engine} = Engine.set_jev_connection(engine, @endpoint, @model)
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    store = :sys.get_state(relay).store

    assert {:ok, saved} =
             Store.commit(store, engine, %{person: "person-a", actor: "owner", action: "choice"})

    :sys.replace_state(relay, &%{&1 | store: saved})
    GenServer.stop(relay)

    restarted = start_relay(path)
    assert Process.alive?(restarted)
    assert {:ok, session} = Relay.open_websocket_session(restarted, "device-a", nil, nil)

    assert %{
             "type" => "ok",
             "algorithm" => "jev-set",
             "basis" => "fallback",
             "entries" => entries
           } =
             hd(session_request(restarted, session, %{"type" => "view_get", "ref" => "restart"}))

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "earlier-deadline", "due_at" => 10},
             %{"sender_id" => "app:test", "message_id" => "later-deadline", "due_at" => 20},
             %{"sender_id" => "app:test", "message_id" => "undated"}
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))

    forward(restarted, 4, "intake-after-restart")
    assert Process.alive?(restarted)
    assert {:ok, events, 4} = Relay.events(restarted, "device-a", 0)
    assert length(events) == 4
    GenServer.stop(restarted)
  end

  test "a process restart gives the same view a new revision", %{path: path} do
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "same-view")
    assert {:ok, first_session} = Relay.open_websocket_session(relay, "device-a", nil, nil)
    first = hd(session_request(relay, first_session, %{"type" => "view_get", "ref" => "before"}))
    GenServer.stop(relay)

    restarted = start_relay(path)
    assert {:ok, second_session} = Relay.open_websocket_session(restarted, "device-a", nil, nil)

    second =
      hd(session_request(restarted, second_session, %{"type" => "view_get", "ref" => "after"}))

    assert first["algorithm"] == second["algorithm"]
    assert first["basis"] == second["basis"]
    assert first["entries"] == second["entries"]
    refute first["view_rev"] == second["view_rev"]
    GenServer.stop(restarted)
  end
end

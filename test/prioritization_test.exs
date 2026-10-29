defmodule AttentionServer.PrioritizationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    DashboardView,
    DeviceIdentity,
    Engine,
    JSON,
    JevClient,
    JevRanking,
    Placement,
    Relay,
    Store,
    TLSFixtures
  }

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

  defp start_relay(path, opts \\ []) do
    assert {:ok, relay} = Relay.start_link(Keyword.put(opts, :store_path, path))
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

  defp start_synthetic_priority_pass(relay, person \\ "person-a") do
    test_process = self()
    relay_pid = relay

    state =
      :sys.replace_state(relay, fn state ->
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

    job = state.priority_jobs[person]
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

  defp forward(relay, n, id, deadline \\ nil, body \\ nil) do
    message = %{
      "sender_id" => "app:test",
      "message_id" => id,
      "created_at" => n,
      "title" => "Request #{id}"
    }

    message = if is_integer(deadline), do: Map.put(message, "by_when", deadline), else: message
    message = if is_binary(body), do: Map.put(message, "body", body), else: message

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(n),
               "message" => message
             })
  end

  test "baseline returns urgency priority and orders urgency, deadline, then newest" do
    items = [
      item("newest", 4),
      Map.update!(item("invalid-deadline", 5), :message, &Map.put(&1, "by_when", "tomorrow")),
      item("later-deadline", 2, 20),
      item("earliest", 1, 10),
      item("older", 3)
    ]

    entries = Placement.place("baseline", items, [], nil)

    assert entries == [
             %{
               "sender_id" => "app:test",
               "message_id" => "earliest",
               "priority" => "normal",
               "due_at" => 10
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "later-deadline",
               "priority" => "normal",
               "due_at" => 20
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "invalid-deadline",
               "priority" => "normal"
             },
             %{"sender_id" => "app:test", "message_id" => "newest", "priority" => "normal"},
             %{"sender_id" => "app:test", "message_id" => "older", "priority" => "normal"}
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
        "priority" => "now",
        "reason" => "Ranked by how soon it needs handling"
      },
      %{
        "sender_id" => "app:test",
        "message_id" => "older",
        "priority" => "low",
        "reason" => "Ranked by how soon it needs handling"
      },
      %{
        "sender_id" => "app:test",
        "message_id" => "new-late",
        "priority" => "high",
        "reason" => "Ranked by how soon it needs handling"
      }
    ]

    entries = Placement.place("jev-set", items, previous, nil)

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "newest", "priority" => "now"},
             %{"sender_id" => "app:test", "message_id" => "older", "priority" => "low"},
             %{
               "sender_id" => "app:test",
               "message_id" => "new-late",
               "priority" => "high",
               "due_at" => 30
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "new-early",
               "priority" => "normal",
               "due_at" => 10
             }
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))

    assert {:ok, engine} = Engine.setup_person(Engine.new(), "person-a")
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    assert Placement.reassess(engine, "person-a", []) == :whole_set
  end

  test "scored placement carries deadlines, post-priority, and omits reasons" do
    items = [item("older", 1, 100), item("newest", 2)]

    entries =
      Placement.place(
        "jev-set",
        items,
        [],
        {:scored, %{{"app:test", "older"} => 2.5, {"app:test", "newest"} => 1.0}}
      )

    assert entries == [
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "priority" => "now",
               "due_at" => 100
             },
             %{"sender_id" => "app:test", "message_id" => "newest", "priority" => "normal"}
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

  test "Jev receives effective urgency and omits an invalid deadline" do
    urgent =
      item("urgent", 1)
      |> Map.update!(
        :message,
        &(&1 |> Map.put("urgency", "now") |> Map.put("by_when", "tomorrow"))
      )

    due =
      item("due", 2, 100)
      |> Map.update!(:message, &Map.put(&1, "urgency", "low"))

    assert {:ok, request} =
             Placement.compose("jev-set", [urgent, due], %{
               endpoint: @endpoint,
               model: @model,
               now: 0
             })

    assert {:ok,
            %{
              "state" => %{"pending_items" => [urgent_item, due_item]},
              "questions" => questions
            }} = JSON.decode_line(request.body <> "\n")

    assert urgent_item["urgency"] == "now"
    assert urgent_item["stake"] == ""
    refute Map.has_key?(urgent_item, "by_when")
    assert due_item["urgency"] == "low"
    assert due_item["by_when"] == "1970-01-01T00:00:00Z"
    assert due_item["due_in_minutes"] == 0

    assert questions["candidate_1"]["instructions"] ==
             "Rate candidate 1 in the supplied pending set for how soon it should be handled. " <>
               "Consider its title, body, stated stake, deadline and the sender's stated urgency relative to the other pending items. " <>
               "The current time is now; due_in_minutes is the time left before the deadline, negative once it has passed."

    assert questions["candidate_1"]["criteria"] == [
             "Low priority: this can wait while more time-sensitive items are handled.",
             "Normal priority: this should be handled soon, but other items can reasonably come first.",
             "High priority: delaying this item has clear near-term consequences; handle it before less urgent items.",
             "Now: this needs the person's attention right away, ahead of everything else."
           ]
  end

  test "Jev returns valid candidate scores and ignores missing and unknown questions" do
    candidates = [{"app:test", "first"}, {"app:test", "second"}, {"app:test", "third"}]

    questions = %{
      "candidate_1" => hd(candidates),
      "candidate_2" => Enum.at(candidates, 1),
      "candidate_3" => List.last(candidates)
    }

    response =
      ~s({"model":"jev-latest","answers":{"candidate_1":{"type":"score","score":1.5},"candidate_2":{"type":"score","score":2},"candidate_3":{"type":"score","score":2}}})

    assert {:ok,
            %{
              {"app:test", "first"} => 1.5,
              {"app:test", "second"} => 2.0,
              {"app:test", "third"} => 2.0
            }} =
             JevClient.rank_response(response, questions, candidates)

    assert {:ok, %{{"app:test", "first"} => 1.0}} =
             JevClient.rank_response(
               ~s({"answers":{"candidate_1":{"type":"score","score":1},"candidate_2":{"type":"score","score":4},"unknown":{"type":"score","score":3}}}),
               questions,
               candidates
             )

    assert {:ok, %{}} =
             JevClient.rank_response(
               ~s({"answers":{"candidate_1":{"type":"score","score":4},"candidate_3":{"type":"score","score":3}}}),
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

  test "format 22 migrates with baseline defaults and persists as format 24", %{path: path} do
    assert {:ok, initial} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(initial.engine, "person-a")
    assert :ok = Store.close(initial)

    old_engine =
      engine
      |> Map.delete(:algorithm_choices)
      |> Map.delete(:jev_endpoint)
      |> Map.delete(:jev_model)
      |> Map.delete(:open_default_index)

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

    assert {:attention_relay_state, 24, saved_engine} =
             :erlang.binary_to_term(saved_payload, [:safe])

    refute Map.has_key?(saved_engine, :open_default_index)
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

  test "self-hosted choice saves before setup and keeps the effective view in basic order", %{
    path: path,
    dir: dir
  } do
    previous_key_path = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")

    on_exit(fn ->
      if is_binary(previous_key_path),
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous_key_path),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")
    end)

    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    relay = start_relay(path)
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    assert {:ok, person} = Relay.operator_bootstrap(relay, owner_cert)
    assert {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    assert :ok = Relay.setup_device(relay, person, "device-a")
    assert :ok = Relay.setup_sender(relay, "device-a", "app:test")
    forward(relay, 1, "later", 200)
    forward(relay, 2, "earlier", 100)

    initial_endpoint = "https://127.0.0.1:1/v1/systemone"

    assert {:ok, settings} =
             Relay.save_dashboard_prioritization(
               relay,
               person,
               owner_key,
               person,
               "jev-set",
               "",
               ""
             )

    assert %{algorithm_id: "jev-set", endpoint: "", model: "", basis: "fallback"} =
             settings

    assert DashboardView.prioritization("Owner", settings) =~
             "Jev setup is incomplete; waiting items are in basic order."

    assert {:ok, session} = Relay.open_websocket_session(relay, "device-a", nil, nil)

    assert [%{"algorithm" => "jev-set", "basis" => "fallback", "entries" => entries}] =
             session_request(relay, session, %{"type" => "view_get", "ref" => "unconfigured"})

    assert Enum.map(entries, & &1["message_id"]) == ["earlier", "later"]

    assert {:ok, settings} =
             Relay.save_dashboard_prioritization(
               relay,
               person,
               owner_key,
               person,
               "jev-set",
               initial_endpoint,
               @model
             )

    assert %{
             algorithm_id: "jev-set",
             endpoint: ^initial_endpoint,
             model: @model,
             basis: "fallback"
           } =
             settings

    assert DashboardView.prioritization("Owner", settings) =~
             "Jev setup is incomplete; waiting items are in basic order."

    key_path = Path.join(dir, "synthetic-key")
    File.write!(key_path, "synthetic-test-key")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", key_path)
    assert {:ok, settings} = Relay.dashboard_prioritization(relay, person, owner_key, person)

    assert settings.basis == "fallback"

    assert DashboardView.prioritization("Owner", settings) =~
             "Waiting items are in basic order until Jev answers."

    job = start_synthetic_priority_pass(relay, person)

    complete_synthetic_priority_pass(
      job,
      {:ok, %{{"app:test", "later"} => 3.0, {"app:test", "earlier"} => 0.0}}
    )

    assert [%{"basis" => "selected", "entries" => selected_entries}] =
             session_request(relay, session, %{"type" => "view_get", "ref" => "selected"})

    assert Enum.map(selected_entries, & &1["message_id"]) == ["later", "earlier"]

    changed_endpoint = "https://127.0.0.1:1/v1/another-systemone"

    assert {:ok, settings} =
             Relay.save_dashboard_prioritization(
               relay,
               person,
               owner_key,
               person,
               "jev-set",
               changed_endpoint,
               @model
             )

    assert settings.basis == "fallback"

    assert DashboardView.prioritization("Owner", settings) =~
             "Waiting items are in basic order until Jev answers."

    assert [%{"basis" => "fallback", "entries" => fallback_entries}] =
             session_request(relay, session, %{"type" => "view_get", "ref" => "new-connection"})

    assert Enum.map(fallback_entries, & &1["message_id"]) == ["earlier", "later"]

    version = :sys.get_state(relay).store.version
    view_rev = :sys.get_state(relay).priority_views[person].view_rev

    assert {:ok, _} =
             Relay.save_dashboard_prioritization(
               relay,
               person,
               owner_key,
               person,
               "jev-set",
               changed_endpoint,
               @model
             )

    assert :sys.get_state(relay).store.version == version
    assert :sys.get_state(relay).priority_views[person].view_rev == view_rev

    GenServer.stop(relay)
    assert {:ok, persisted} = Store.open(path)
    assert Placement.choice(persisted.engine, person) == "jev-set"
    assert persisted.engine.jev_endpoint == changed_endpoint
    assert persisted.engine.jev_model == @model
    assert :ok = Store.close(persisted)
  end

  test "shared connection changes invalidate every Jev person's cache and pending result", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    old_endpoint = "https://127.0.0.1:1/v1/old-systemone"
    new_endpoint = "https://127.0.0.1:1/v1/new-systemone"
    new_model = "jev-next"
    parent = self()
    {:ok, block_old_b} = Agent.start_link(fn -> false end)
    {:ok, calls} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(block_old_b), do: Agent.stop(block_old_b) end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)

    evaluator = fn request ->
      sender = request.candidates |> hd() |> elem(0)
      Agent.update(calls, &[{sender, request.uri.path, request.model, request.candidates} | &1])

      if sender == "app:b" and request.uri.path == "/v1/old-systemone" and
           Agent.get(block_old_b, & &1) do
        send(parent, {:old_b_pending, self()})

        receive do
          :release -> :ok
        end
      end

      {:ok, Map.new(request.candidates, &{&1, 2.0})}
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    owner_cert = TLSFixtures.certificate_der!(certs, "a1")
    assert {:ok, owner_person} = Relay.operator_bootstrap(relay, owner_cert)
    assert {:ok, owner_key} = DeviceIdentity.from_certificate(owner_cert)
    assert :ok = Relay.setup_person(relay, "person-b")
    assert :ok = Relay.setup_person(relay, "person-c")

    for {person, device, sender} <- [
          {owner_person, "device-a", "app:a"},
          {"person-b", "device-b", "app:b"},
          {"person-c", "device-c", "app:c"}
        ] do
      assert :ok = Relay.setup_device(relay, person, device)
      assert :ok = Relay.setup_sender(relay, device, sender)
    end

    assert {:ok, _} =
             Relay.save_dashboard_prioritization(
               relay,
               owner_person,
               owner_key,
               owner_person,
               "jev-set",
               old_endpoint,
               @model
             )

    state = :sys.get_state(relay)
    assert {:ok, engine} = Engine.set_algorithm_choice(state.store.engine, "person-b", "jev-set")

    assert {:ok, saved} =
             Store.commit(state.store, engine, %{
               person: "person-b",
               actor: "owner",
               action: "choice"
             })

    :sys.replace_state(relay, &%{&1 | store: saved})

    forward_as = fn device, sender, n, id ->
      assert %{"type" => "ok"} =
               request(relay, device, %{
                 "type" => "forward",
                 "op_id" => op(n),
                 "message" => %{
                   "sender_id" => sender,
                   "message_id" => id,
                   "created_at" => n,
                   "title" => id
                 }
               })
    end

    forward_as.("device-a", "app:a", 1001, "a1")
    forward_as.("device-b", "app:b", 1002, "b1")
    forward_as.("device-c", "app:c", 1003, "c1")

    state =
      await_state(relay, fn state ->
        is_map(state.priority_caches[owner_person]) and
          is_map(state.priority_caches["person-b"]) and
          state.priority_views[owner_person].basis == "selected" and
          state.priority_views["person-b"].basis == "selected"
      end)

    assert Placement.choice(state.store.engine, "person-c") == "baseline"
    assert state.priority_views["person-c"].algorithm == "baseline"
    old_epochs = state.priority_cache_epochs

    Agent.update(block_old_b, fn _ -> true end)
    forward_as.("device-b", "app:b", 1004, "b2")
    assert_receive {:old_b_pending, pending_worker}, 2_000

    assert {:ok, _} =
             Relay.save_dashboard_prioritization(
               relay,
               owner_person,
               owner_key,
               owner_person,
               "jev-set",
               new_endpoint,
               new_model
             )

    changed = :sys.get_state(relay)
    assert changed.priority_cache_epochs[owner_person] > Map.get(old_epochs, owner_person, 0)
    assert changed.priority_cache_epochs["person-b"] > Map.get(old_epochs, "person-b", 0)
    refute Map.has_key?(changed.priority_caches, owner_person)
    refute Map.has_key?(changed.priority_caches, "person-b")
    assert changed.priority_views["person-b"].basis == "fallback"
    assert Placement.choice(changed.store.engine, "person-b") == "jev-set"
    assert Placement.choice(changed.store.engine, "person-c") == "baseline"

    Agent.update(block_old_b, fn _ -> false end)
    send(pending_worker, :release)

    await_state(relay, fn state ->
      is_map(state.priority_caches["person-b"]) and
        state.priority_views["person-b"].basis == "selected" and
        not Map.has_key?(state.priority_jobs, "person-b")
    end)

    observed = Agent.get(calls, & &1)

    assert Enum.any?(observed, fn {sender, path, model, _} ->
             sender == "app:a" and path == "/v1/new-systemone" and model == new_model
           end)

    assert Enum.any?(observed, fn {sender, path, model, candidates} ->
             sender == "app:b" and path == "/v1/new-systemone" and model == new_model and
               Enum.sort(candidates) == Enum.sort([{"app:b", "b1"}, {"app:b", "b2"}])
           end)
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

    assert page =~ "Most urgent first, then soonest deadline, then newest"
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
             %{
               "sender_id" => "app:test",
               "message_id" => "newer",
               "priority" => "normal",
               "due_at" => 200
             },
             %{"sender_id" => "app:test", "message_id" => "older", "priority" => "normal"}
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
             %{
               "sender_id" => "app:test",
               "message_id" => "newer",
               "priority" => "normal",
               "due_at" => 100
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "priority" => "normal",
               "due_at" => 200
             },
             %{"sender_id" => "app:test", "message_id" => "undated", "priority" => "normal"}
           ]

    assert Enum.all?(baseline_entries, &(not Map.has_key?(&1, "reason")))

    engine = :sys.get_state(relay).store.engine

    scores = %{
      {"app:test", "older"} => 0.2,
      {"app:test", "undated"} => 1.2,
      {"app:test", "newer"} => 2.2
    }

    assert Placement.choice(engine, "person-a") == "jev-set"
    assert Placement.valid_permutation?(Placement.items(engine, "person-a"), Map.keys(scores))

    job = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(job, {:ok, scores})

    assert %{
             "type" => "ok",
             "algorithm" => "jev-set",
             "basis" => "selected",
             "entries" => selected_entries
           } = hd(session_request(relay, session, %{"type" => "view_get", "ref" => "after"}))

    assert selected_entries == [
             %{
               "sender_id" => "app:test",
               "message_id" => "newer",
               "priority" => "high",
               "due_at" => 100
             },
             %{"sender_id" => "app:test", "message_id" => "undated", "priority" => "normal"},
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "priority" => "low",
               "due_at" => 200
             }
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
               %{"sender_id" => "app:test", "message_id" => "newer", "priority" => "normal"},
               %{"sender_id" => "app:test", "message_id" => "older", "priority" => "normal"}
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
               %{
                 "sender_id" => "app:test",
                 "message_id" => "arrived-after-pass",
                 "priority" => "normal"
               },
               %{"sender_id" => "app:test", "message_id" => "newer", "priority" => "normal"},
               %{"sender_id" => "app:test", "message_id" => "older", "priority" => "normal"}
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
             %{
               "sender_id" => "app:test",
               "message_id" => "earlier-deadline",
               "priority" => "normal",
               "due_at" => 10
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "later-deadline",
               "priority" => "normal",
               "due_at" => 20
             },
             %{"sender_id" => "app:test", "message_id" => "undated", "priority" => "normal"}
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

  test "a failed stand-in call schedules retry, recovers, and logs only pass metrics", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

    evaluator = fn request ->
      attempt = Agent.get_and_update(attempts, fn count -> {count, count + 1} end)

      if attempt == 0,
        do: {:error, :unavailable},
        else: {:ok, Map.new(request.candidates, &{&1, 2.7})}
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    setup_person(relay)
    configure_jev_choice(relay)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        forward(relay, 1, "private-title")
        await_state(relay, &(&1.priority_failures["person-a"] == 1))

        state = :sys.get_state(relay)
        retry = state.priority_retry_timers["person-a"]
        assert retry
        assert Process.read_timer(retry.ref) in 8_000..10_000
        assert state.priority_views["person-a"].basis == "fallback"

        send(relay, {:priority_retry, "person-a", retry.generation})
        await_state(relay, &(&1.priority_views["person-a"].basis == "selected"))
      end)

    assert Agent.get(attempts, & &1) == 2
    refute Map.has_key?(:sys.get_state(relay).priority_failures, "person-a")

    assert logs =~
             "event=harklight_priority_pass outcome=failed requests=1 finals=0 considered=1 clipped=0"

    assert logs =~
             "event=harklight_priority_pass outcome=ok requests=1 finals=0 considered=1 clipped=0"

    refute logs =~ "private-title"
    refute logs =~ "app:test"

    assert :sys.get_state(relay).priority_views["person-a"].entries
           |> hd()
           |> Map.fetch!("priority") == "now"
  end

  test "a quiet inbox re-ranks when the deadline enters the next class", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    now = 1_793_290_000_000
    due = now + 3_600_000 + 2_000
    {:ok, clock} = Agent.start_link(fn -> now end)
    on_exit(fn -> if Process.alive?(clock), do: Agent.stop(clock) end)

    evaluator = fn request ->
      {:ok, %{"state" => %{"pending_items" => [pending]}}} =
        JSON.decode_line(request.body <> "\n")

      score = if pending["due_in_minutes"] < 60, do: 2.7, else: 1.0
      {:ok, Map.new(request.candidates, &{&1, score})}
    end

    relay =
      start_relay(path,
        clock: fn -> Agent.get(clock, & &1) end,
        priority_evaluator: evaluator
      )

    setup_person(relay)
    configure_jev_choice(relay)
    forward(relay, 1, "deadline-only", due)
    state = await_state(relay, &Map.has_key?(&1.priority_deadline_timers, "person-a"))
    assert state.priority_views["person-a"].entries |> hd() |> Map.fetch!("priority") == "normal"
    assert state.priority_deadline_timers["person-a"].due == now + 2_000

    Agent.update(clock, fn _ -> now + 3_000 end)
    send(relay, {:priority_deadline, "person-a", now + 2_000})

    state =
      await_state(relay, fn state ->
        state.priority_views["person-a"].entries |> hd() |> Map.fetch!("priority") == "now"
      end)

    assert state.priority_views["person-a"].basis == "selected"
  end

  test "intake proceeds while a stand-in ranking request is waiting", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    parent = self()

    evaluator = fn request ->
      send(parent, {:ranking_waiting, self()})

      receive do
        :release -> {:ok, Map.new(request.candidates, &{&1, 1.0})}
      end
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    setup_person(relay)
    configure_jev_choice(relay)
    forward(relay, 1, "first")
    assert_receive {:ranking_waiting, worker}, 2_000
    forward(relay, 2, "accepted-while-ranking")
    assert {:ok, events, 2} = Relay.events(relay, "device-a", 0)
    assert length(events) == 2
    send(worker, :release)
    assert_receive {:ranking_waiting, next_worker}, 2_000
    send(next_worker, :release)
    await_state(relay, &(not Map.has_key?(&1.priority_jobs, "person-a")))
  end

  test "a mid-pass arrival reuses completed unaffected batches and logs both passes", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    parent = self()
    {:ok, gate} = Agent.start_link(fn -> true end)
    {:ok, calls} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(gate), do: Agent.stop(gate) end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)

    evaluator = fn request ->
      Agent.update(calls, &(&1 + 1))
      send(parent, {:provider_request, self(), request.candidates})

      if Agent.get(gate, & &1) do
        receive do
          :release -> :ok
        end
      end

      {:ok, Map.new(request.candidates, &{&1, 1.0})}
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    setup_person(relay)
    body = String.duplicate("x", 650)

    for n <- 1..100, do: forward(relay, n, "seed-#{n}", nil, body)
    configure_jev_choice(relay)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        forward(relay, 101, "initial-trigger", nil, body)
        engine = :sys.get_state(relay).store.engine

        expected_batches =
          JevRanking.run(
            Placement.items(engine, "person-a"),
            %{},
            engine.jev_endpoint,
            engine.jev_model,
            System.system_time(:millisecond),
            fn request -> {:ok, Map.new(request.candidates, &{&1, 1.0})} end
          )
          |> then(&length(&1.cache.batches))

        assert expected_batches > 1

        workers =
          for _ <- 1..expected_batches do
            assert_receive {:provider_request, pid, _candidates}, 2_000
            pid
          end

        forward(relay, 102, "mid-pass", nil, body)
        Agent.update(gate, fn _ -> false end)
        Enum.each(workers, &send(&1, :release))

        assert_receive {:provider_request, _next_pid, next_candidates}, 2_000
        assert {"app:test", "mid-pass"} in next_candidates

        await_state(relay, fn state ->
          cache = state.priority_caches["person-a"]

          is_map(cache) and not Map.has_key?(state.priority_jobs, "person-a") and
            Enum.any?(cache.batches, fn batch ->
              {"app:test", "mid-pass"} in batch.members and is_map(batch.scores)
            end)
        end)

        assert Agent.get(calls, & &1) == expected_batches + 1
        refute_receive {:provider_request, _pid, _candidates}, 0
      end)

    assert length(Regex.scan(~r/event=harklight_priority_pass /, logs)) == 2
    assert logs =~ "requests=1 finals=0"
  end

  defp await_state(relay, predicate, attempts \\ 100)
  defp await_state(_relay, _predicate, 0), do: flunk("priority pass did not finish")

  defp await_state(relay, predicate, attempts) do
    state = :sys.get_state(relay)

    if predicate.(state) do
      state
    else
      Process.sleep(20)
      await_state(relay, predicate, attempts - 1)
    end
  end
end

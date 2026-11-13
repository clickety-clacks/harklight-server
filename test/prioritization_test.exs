defmodule AttentionServer.PrioritizationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    BounceDecision,
    BouncePolicy,
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

  defp persisted_snapshot(path) do
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

  test "baseline omits scores and bands and orders urgency, deadline, then newest" do
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
               "due_at" => 10
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "later-deadline",
               "due_at" => 20
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "invalid-deadline"
             },
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
        "score" => 0.9,
        "reason" => "Ranked by how soon it needs handling"
      },
      %{
        "sender_id" => "app:test",
        "message_id" => "older",
        "score" => 0.1,
        "reason" => "Ranked by how soon it needs handling"
      },
      %{
        "sender_id" => "app:test",
        "message_id" => "new-late",
        "score" => 0.7,
        "reason" => "Ranked by how soon it needs handling"
      }
    ]

    entries = Placement.place("jev-set", items, previous, nil)

    assert entries == [
             %{"sender_id" => "app:test", "message_id" => "newest", "score" => 0.9},
             %{"sender_id" => "app:test", "message_id" => "older", "score" => 0.1},
             %{
               "sender_id" => "app:test",
               "message_id" => "new-late",
               "score" => 0.7,
               "due_at" => 30
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "new-early",
               "due_at" => 10
             }
           ]

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))

    assert {:ok, engine} = Engine.setup_person(Engine.new(), "person-a")
    assert {:ok, engine} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    assert Placement.reassess(engine, "person-a", []) == :whole_set
  end

  test "scored placement carries deadlines, numeric scores, and omits reasons" do
    items = [item("older", 1, 100), item("newest", 2)]

    entries =
      Placement.place(
        "jev-set",
        items,
        [],
        {:scored, %{{"app:test", "older"} => 0.9, {"app:test", "newest"} => 0.3}}
      )

    assert entries == [
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "score" => 0.9,
               "due_at" => 100
             },
             %{"sender_id" => "app:test", "message_id" => "newest", "score" => 0.3}
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
      |> Map.update!(:message, fn message ->
        message
        |> Map.put("urgency", "now")
        |> Map.put("by_when", "tomorrow")
        |> Map.put("message_type", "request")
        |> Map.put("message_kind", "blocker")
      end)

    due =
      item("due", 2, 100)
      |> Map.update!(:message, fn message ->
        message
        |> Map.put("urgency", "low")
        |> Map.put("message_type", "fyi")
        |> Map.put("message_kind", "success")
      end)

    untyped = item("untyped", 3)

    assert {:ok, request} =
             Placement.compose("jev-set", [urgent, due, untyped], %{
               endpoint: @endpoint,
               model: @model,
               now: 0
             })

    assert {:ok,
            %{
              "state" => %{"pending_items" => [urgent_item, untyped_item, due_item]},
              "questions" => questions
            }} = JSON.decode_line(request.body <> "\n")

    assert urgent_item["urgency"] == "now"
    assert urgent_item["stake"] == ""
    assert urgent_item["message_type"] == "request"
    assert urgent_item["message_kind"] == "blocker"
    refute Map.has_key?(urgent_item, "by_when")
    assert untyped_item["message_type"] == nil
    assert untyped_item["message_kind"] == nil
    assert due_item["urgency"] == "low"
    assert due_item["message_type"] == "fyi"
    assert due_item["message_kind"] == "success"
    assert due_item["by_when"] == "1970-01-01T00:00:00Z"
    assert due_item["due_in_minutes"] == 0

    instruction = questions["candidate_1"]["instructions"]
    assert instruction =~ "absolute importance"
    assert instruction =~ "Do not compare"
    assert instruction =~ "message type and message kind"
    assert instruction =~ "Sender urgency is evidence, not the answer"
    refute instruction =~ "relative to the other"
    assert length(questions["candidate_1"]["criteria"]) == 5
  end

  test "Jev maps the absolute rubric once and ignores invalid or missing answers" do
    ids = Map.new(1..10, &{"candidate_#{&1}", {"app:test", "#{&1}"}})

    answers =
      Map.new(
        Enum.with_index([0, 0.4, 0.6, 3.8, 4, -1, 4.2, "3", true, nil], 1),
        fn {value, i} -> {"candidate_#{i}", %{"type" => "score", "score" => value}} end
      )

    assert {:ok, scores} = JevClient.parse_scores(JSON.encode!(%{"answers" => answers}), ids)

    assert scores == %{
             {"app:test", "1"} => 0.0,
             {"app:test", "2"} => 0.1,
             {"app:test", "3"} => 0.15,
             {"app:test", "4"} => 0.95,
             {"app:test", "5"} => 1.0
           }

    assert {:ok, %{}} = JevClient.parse_scores("{\"answers\":{}}", ids)
  end

  test "algorithm choices are per person and setting the current choice is a no-op" do
    assert {:ok, engine} = Engine.setup_person(Engine.new(), "person-a")
    assert {:ok, ^engine} = Engine.set_algorithm_choice(engine, "person-a", "baseline")
    assert {:ok, selected} = Engine.set_algorithm_choice(engine, "person-a", "jev-set")
    assert Placement.choice(selected, "person-a") == "jev-set"
    assert Placement.choice(selected, "another-person") == "baseline"
    assert {:ok, ^selected} = Engine.set_algorithm_choice(selected, "person-a", "jev-set")
  end

  test "format 22 migrates with baseline defaults and persists as format 29", %{path: path} do
    assert {:ok, initial} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(initial.engine, "person-a")
    assert :ok = Store.close(initial)

    old_engine =
      engine
      |> Map.delete(:algorithm_choices)
      |> Map.delete(:jev_endpoint)
      |> Map.delete(:jev_model)
      |> Map.delete(:open_default_index)
      |> Map.delete(:bounce_levels)
      |> Map.delete(:bounce_revisions)
      |> Map.delete(:opened_revisions)
      |> Map.delete(:advisory_feeds)
      |> Map.delete(:delivery_states)

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
    assert migrated.engine.bounce_levels == %{}
    assert migrated.engine.bounce_revisions == %{}
    assert migrated.engine.opened_revisions == MapSet.new()
    assert migrated.engine.advisory_feeds == %{}
    assert :ok = Store.close(migrated)

    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, select} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id = 1")
    assert {:row, [saved_payload]} = Sqlite3.step(db, select)
    assert :ok = Sqlite3.release(db, select)
    assert :ok = Sqlite3.close(db)

    assert {:attention_relay_state, 29, saved_engine} =
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
      {:ok, %{{"app:test", "later"} => 1.0, {"app:test", "earlier"} => 0.0}}
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

      {:ok, Map.new(request.candidates, &{&1, 0.7})}
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
               "due_at" => 200
             },
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
      |> Enum.sort(:desc)
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

    # Free ignores deadline rank and keeps the original newest acceptance first.
    assert hd(ids) == "item-28"

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
               "due_at" => 100
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "due_at" => 200
             },
             %{"sender_id" => "app:test", "message_id" => "undated"}
           ]

    assert Enum.all?(baseline_entries, &(not Map.has_key?(&1, "reason")))

    engine = :sys.get_state(relay).store.engine

    scores = %{
      {"app:test", "older"} => 0.1,
      {"app:test", "undated"} => 0.3,
      {"app:test", "newer"} => 0.7
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
               "score" => %JSON.DecimalToken{raw: "0.7"},
               "due_at" => 100
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "undated",
               "score" => %JSON.DecimalToken{raw: "0.3"}
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "older",
               "score" => %JSON.DecimalToken{raw: "0.1"},
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

  test "a score-only change advances the view revision and returns the new number", %{
    path: path,
    dir: dir
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "same-item")
    configure_jev_choice(relay)
    assert {:ok, session} = Relay.open_websocket_session(relay, "device-a", nil, nil)

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "same-item"} => 0.71}})
    first = hd(session_request(relay, session, %{"type" => "view_get", "ref" => "first"}))
    assert first["basis"] == "selected"

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "same-item"} => 0.72}})

    second =
      hd(
        session_request(relay, session, %{
          "type" => "view_get",
          "ref" => "second",
          "known_view_rev" => first["view_rev"]
        })
      )

    assert second["basis"] == "selected"
    refute second["unchanged"]
    refute second["view_rev"] == first["view_rev"]

    assert Enum.map(second["entries"], &Placement.identity/1) ==
             Enum.map(first["entries"], &Placement.identity/1)

    assert hd(first["entries"])["score"] == %JSON.DecimalToken{raw: "0.71"}
    assert hd(second["entries"])["score"] == %JSON.DecimalToken{raw: "0.72"}
    refute Map.has_key?(hd(second["entries"]), "priority")
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
               %{
                 "sender_id" => "app:test",
                 "message_id" => "arrived-after-pass"
               },
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
             %{
               "sender_id" => "app:test",
               "message_id" => "earlier-deadline",
               "due_at" => 10
             },
             %{
               "sender_id" => "app:test",
               "message_id" => "later-deadline",
               "due_at" => 20
             },
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
        else: {:ok, Map.new(request.candidates, &{&1, 0.9})}
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
           |> Map.fetch!("score") == 0.9
  end

  test "private record appends only applied item scores without extra scoring or view changes", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    parent = self()
    record_path = Path.join(dir, "scores.jsonl")
    now = 1_793_290_000_000

    evaluator = fn request ->
      send(parent, {:scored_candidates, request.candidates})

      {:ok,
       %{
         {"app:test", "scored"} => 0.7,
         {"app:test", "invalid"} => 2,
         {"app:test", "absent"} => 0.9
       }}
    end

    relay =
      start_relay(path,
        jev_score_record_path: record_path,
        priority_evaluator: evaluator,
        clock: fn -> now end
      )

    setup_person(relay)
    forward(relay, 1, "scored")
    configure_jev_choice(relay)
    forward(relay, 2, "invalid")
    assert_receive {:scored_candidates, candidates}, 2_000
    assert MapSet.new(candidates) == MapSet.new([{"app:test", "scored"}, {"app:test", "invalid"}])
    state = await_state(relay, &(&1.priority_views["person-a"].basis == "selected"))
    :sys.get_state(state.applied_score_record)
    {:ok, row} = JSON.decode_line(File.read!(record_path))

    assert row == %{
             "time" => now,
             "person" => "person-a",
             "sender_id" => "app:test",
             "message_id" => "scored",
             "title" => "Request scored",
             "score" => %JSON.DecimalToken{raw: "0.7"},
             "model" => "jev-latest"
           }

    assert Bitwise.band(File.stat!(record_path).mode, 0o777) == 0o600

    expected =
      Placement.place(
        "jev-set",
        Placement.items(state.store.engine, "person-a"),
        [],
        {:ranked, %{{"app:test", "scored"} => 0.7, {"app:test", "invalid"} => 2}, []}
      )

    assert state.priority_views["person-a"].engine_entries == expected

    assert {:ok, session} = Relay.open_session(relay, "device-a")
    session_request(relay, session, %{"type" => "view_get", "ref" => "read-only"})
    :sys.get_state(state.applied_score_record)
    assert File.read!(record_path) == JSON.encode!(%{row | "score" => 0.7}) <> "\n"
    refute_received {:scored_candidates, _}

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "scored"} => 0.8}})
    :sys.get_state(state.applied_score_record)
    assert length(String.split(File.read!(record_path), "\n", trim: true)) == 2
  end

  test "the applied score commit fixes its bounce interval before publishing the scored view", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
    setup_person(relay)

    levels = [
      %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
      %{"name" => "Follow up", "lower_bound" => 0.5, "bounce_after_days" => 1.5},
      %{"name" => "Urgent", "lower_bound" => 0.8, "bounce_after_days" => 4}
    ]

    {:ok, normalized} = BouncePolicy.normalize_levels(levels)
    state = :sys.get_state(relay)

    {:ok, engine, _config} =
      BounceDecision.configure_levels(
        state.store.engine,
        "person-a",
        normalized,
        state.clock.()
      )

    {:ok, store} =
      Store.commit(state.store, engine, %{
        person: "person-a",
        actor: "owner",
        action: "test_levels"
      })

    :sys.replace_state(relay, &%{&1 | store: store})

    assert %{"type" => "ok", "seq" => 1} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(915),
               "session_handle" => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "fixed-bounce-selection",
                 "created_at" => 1,
                 "title" => "Scored request",
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

    configure_jev_choice(relay)
    first = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(
      first,
      {:ok, %{{"app:test", "fixed-bounce-selection"} => 0.7}}
    )

    state = await_state(relay, &(&1.priority_views["person-a"].basis == "selected"))
    record = state.store.engine.messages[{"person-a", "app:test", "fixed-bounce-selection"}]

    bounce_state =
      Map.fetch!(state.store.engine.bounce_revisions, {
        "person-a",
        "app:test",
        "fixed-bounce-selection"
      })

    selection = bounce_state.revisions[1].selection

    assert hd(state.priority_views["person-a"].entries)["score"] == 0.7
    assert selection["level_name"] == "Follow up"
    assert selection["bounce_after_days"] == 1.5
    assert selection["due_at_ms"] == record.accepted_at + 129_600_000

    second = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(
      second,
      {:ok, %{{"app:test", "fixed-bounce-selection"} => 0.9}}
    )

    persisted = :sys.get_state(relay).store.engine

    persisted_bounce =
      Map.fetch!(persisted.bounce_revisions, {
        "person-a",
        "app:test",
        "fixed-bounce-selection"
      })

    assert persisted_bounce.revisions[1].selection == selection

    GenServer.stop(relay)
    restarted = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)

    restarted_bounce =
      Map.fetch!(
        :sys.get_state(restarted).store.engine.bounce_revisions,
        {"person-a", "app:test", "fixed-bounce-selection"}
      )

    assert restarted_bounce.revisions[1].selection == selection
  end

  test "later arrivals and cache-only passes do not re-record unchanged scores", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    parent = self()
    record_path = Path.join(dir, "scores.jsonl")

    evaluator = fn request ->
      send(parent, {:score_request, request.candidates})
      {:ok, Map.new(request.candidates, &{&1, 0.7})}
    end

    relay = start_relay(path, jev_score_record_path: record_path, priority_evaluator: evaluator)
    setup_person(relay)
    configure_jev_choice(relay)
    forward(relay, 1, "first")
    assert_receive {:score_request, _}, 2_000
    state = await_state(relay, &(&1.priority_views["person-a"].basis == "selected"))
    writer = state.applied_score_record
    :sys.get_state(writer)
    first_line = File.read!(record_path)

    forward(relay, 2, "second")
    assert_receive {:score_request, _}, 2_000

    state =
      await_state(relay, fn state ->
        entries = state.priority_views["person-a"].engine_entries
        length(entries) == 2 and Enum.all?(entries, &(&1["score"] == 0.7))
      end)

    :sys.get_state(writer)
    lines = String.split(File.read!(record_path), "\n", trim: true)
    assert length(lines) == 2
    assert hd(lines) <> "\n" == first_line
    assert {:ok, %{"message_id" => "second"}} = JSON.decode_line(List.last(lines) <> "\n")

    pass =
      JevRanking.run(
        Placement.items(state.store.engine, "person-a"),
        state.priority_caches["person-a"],
        state.store.engine.jev_endpoint,
        state.store.engine.jev_model,
        state.clock.(),
        fn _ -> flunk("cached pass called scorer") end
      )

    assert pass.sent == 0
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, pass)
    :sys.get_state(writer)
    assert String.split(File.read!(record_path), "\n", trim: true) == lines
    refute_received {:score_request, _}

    scores = %{{"app:test", "first"} => 0.8, {"app:test", "second"} => 0.7}
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, scores})
    :sys.get_state(writer)
    assert length(String.split(File.read!(record_path), "\n", trim: true)) == 3

    :sys.replace_state(relay, fn state ->
      %{state | store: %{state.store | engine: %{state.store.engine | jev_model: "jev-other"}}}
    end)

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, scores})
    :sys.get_state(writer)

    rows =
      for line <- String.split(File.read!(record_path), "\n", trim: true) do
        {:ok, row} = JSON.decode_line(line <> "\n")
        row
      end

    assert length(rows) == 5
    assert Enum.map(Enum.take(rows, -2), & &1["model"]) == ["jev-other", "jev-other"]
  end

  test "unset path and hosted deployment never start or write a score record", %{dir: dir} do
    use_synthetic_jev_key(dir)

    for {mode, record_path} <- [{:self_hosted, nil}, {:saas, Path.join(dir, "hosted.jsonl")}] do
      relay =
        start_relay(Path.join(dir, "#{mode}.sqlite3"),
          deployment_mode: mode,
          jev_score_record_path: record_path
        )

      setup_person(relay)
      forward(relay, 1, "entry")
      configure_jev_choice(relay)
      job = start_synthetic_priority_pass(relay)
      complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "entry"} => 0.7}})
      state = :sys.get_state(relay)
      assert state.applied_score_record == nil
      assert state.priority_views["person-a"].basis == "selected"
    end

    refute File.exists?(Path.join(dir, "hosted.jsonl"))
  end

  test "discarded scoring results do not produce records", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    record_path = Path.join(dir, "scores.jsonl")
    relay = start_relay(path, jev_score_record_path: record_path)
    setup_person(relay)
    forward(relay, 1, "entry")
    configure_jev_choice(relay)
    job = start_synthetic_priority_pass(relay)

    :sys.replace_state(relay, fn state ->
      %{
        state
        | priority_generations:
            Map.put(state.priority_generations, "person-a", job.generation + 1)
      }
    end)

    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "entry"} => 0.7}})
    :sys.get_state(:sys.get_state(relay).applied_score_record)
    refute File.exists?(record_path)
  end

  test "record failure logs once without content and leaves ranking and delivery working", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path, jev_score_record_path: Path.join(dir, "missing/scores.jsonl"))
    setup_person(relay)
    forward(relay, 1, "private-entry")
    configure_jev_choice(relay)
    writer = :sys.get_state(relay).applied_score_record

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        for score <- [0.7, 0.8] do
          job = start_synthetic_priority_pass(relay)
          complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "private-entry"} => score}})
          :sys.get_state(writer)
          assert hd(:sys.get_state(relay).priority_views["person-a"].entries)["score"] == score
        end

        forward(relay, 2, "still-delivered")
        assert {:ok, events, 2} = Relay.events(relay, "device-a", 0)
        assert length(events) == 2
      end)

    assert length(Regex.scan(~r/event=harklight_applied_score_record outcome=write_failed/, logs)) ==
             1

    refute logs =~ "private-entry"
    refute logs =~ "missing"
    refute logs =~ "app:test"
  end

  test "waiting record writer does not block applied ranking or delivery and exits with relay", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path, jev_score_record_path: Path.join(dir, "scores.jsonl"))
    setup_person(relay)
    forward(relay, 1, "entry")
    configure_jev_choice(relay)
    writer = :sys.get_state(relay).applied_score_record
    :sys.suspend(writer)

    try do
      job = start_synthetic_priority_pass(relay)
      complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "entry"} => 0.7}})
      assert :sys.get_state(relay).priority_views["person-a"].basis == "selected"
      forward(relay, 2, "not-blocked")
      assert {:ok, events, 2} = Relay.events(relay, "device-a", 0)
      assert length(events) == 2
    after
      :sys.resume(writer)
    end

    :sys.get_state(writer)
    monitor = Process.monitor(writer)
    GenServer.stop(relay)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
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

      score = if pending["due_in_minutes"] < 60, do: 0.9, else: 0.3
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
    assert state.priority_views["person-a"].entries |> hd() |> Map.fetch!("score") == 0.3
    assert state.priority_deadline_timers["person-a"].due == now + 2_000

    Agent.update(clock, fn _ -> now + 3_000 end)
    send(relay, {:priority_deadline, "person-a", now + 2_000})

    state =
      await_state(relay, fn state ->
        state.priority_views["person-a"].entries |> hd() |> Map.fetch!("score") == 0.9
      end)

    assert state.priority_views["person-a"].basis == "selected"
  end

  test "new input cancels a superseded ranking request while intake proceeds", %{
    dir: dir,
    path: path
  } do
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
    assert_receive {:ranking_waiting, first_worker}, 2_000

    forward(relay, 2, "accepted-while-ranking")
    assert {:ok, events, 2} = Relay.events(relay, "device-a", 0)
    assert length(events) == 2

    assert_receive {:ranking_waiting, next_worker}, 2_000
    refute Process.alive?(first_worker)
    assert Process.alive?(next_worker)
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

      {:ok, Map.new(request.candidates, &{&1, 0.3})}
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
            fn request -> {:ok, Map.new(request.candidates, &{&1, 0.3})} end
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

  test "reply arrival schedules mixed ranking without a second push", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    parent = self()

    evaluator = fn request ->
      send(parent, {:reply_batch, request.candidates})
      {:ok, Map.new(request.candidates, &{&1, 0.1})}
    end

    relay = start_relay(path, priority_evaluator: evaluator, push_enabled: true)
    setup_person(relay)

    assert %{"type" => "ok"} =
             request(relay, "device-b", %{
               "type" => "push_register",
               "op_id" => op(799),
               "revision" => 1,
               "profile_id" => "harklight-sandbox",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ab", 32)
             })

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(800),
               "session_handle" => op(801),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "chat",
                 "created_at" => 1,
                 "title" => "Chat parent",
                 "body" => "Context"
               }
             })

    {:ok, session} = Relay.open_session(relay, "device-b")
    session_request(relay, session, %{"type" => "resync", "since" => 0})
    session_request(relay, session, %{"type" => "view_get", "ref" => "before"})
    configure_jev_choice(relay)

    discussion =
      request(relay, "device-b", %{
        "type" => "discussion",
        "op_id" => op(802),
        "sender_id" => "app:test",
        "message_id" => "chat",
        "request_revision" => 1,
        "text" => "Question"
      })

    reply =
      request(relay, "device-a", %{
        "type" => "conversation_reply",
        "op_id" => op(803),
        "sender_id" => "app:test",
        "message_id" => "chat",
        "request_revision" => 1,
        "in_reply_to" => String.to_integer(discussion["event_id"]),
        "text" => "Low importance reply"
      })

    seq = reply["seq"]
    bindings = :sys.get_state(relay).store.engine.push_bindings
    assert_receive {:reply_batch, candidates}, 2000
    assert MapSet.new(candidates) == MapSet.new([{"app:test", "chat"}, {"app:test", "chat", seq}])

    state =
      await_state(relay, fn state ->
        match?([%{"score" => 0.1}], state.priority_views["person-a"].replies)
      end)

    assert state.store.engine.push_bindings == bindings
    assert Enum.any?(drain(relay, session), &(&1["type"] == "view_changed"))
    assert [%{"message_id" => "chat"}] = state.priority_views["person-a"].entries
    engine = state.store.engine
    now = engine.push_bindings[{"person-a", "device-b"}].next_at
    assert {:ok, engine, first} = Engine.claim_push(engine, now)
    assert first.stream == :request

    assert {:ok, engine} =
             Engine.finish_push(
               engine,
               first,
               %{state: "provider_accepted", http_status: 200},
               now + 1
             )

    assert {:ok, engine, chat} = Engine.claim_push(engine, now + 1)
    assert chat.stream == :conversation
    assert chat.alert["body"] == "Low importance reply"

    assert {:ok, engine} =
             Engine.finish_push(
               engine,
               chat,
               %{state: "provider_accepted", http_status: 200},
               now + 2
             )

    assert engine.push_bindings[{"person-a", "device-b"}].pending_conversation_high == 0
  end

  test "unseen origin replies are revisioned separately, scored on resolved parents, and removed on read",
       %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    record_path = Path.join(dir, "reply-scores.jsonl")

    relay =
      start_relay(path,
        jev_score_record_path: record_path,
        priority_evaluator: fn _ -> {:error, :unavailable} end
      )

    setup_person(relay)
    assert {:ok, session} = Relay.open_session(relay, "device-b")

    view = fn ->
      session_request(relay, session, %{"type" => "view_get", "ref" => "reply-view"})
      |> Enum.find(&(&1["ref"] == "reply-view"))
    end

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(700),
               "session_handle" => op(701),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "parent",
                 "created_at" => 1,
                 "title" => "Parent title",
                 "body" => "Parent context",
                 "urgency" => "high"
               }
             })

    initial = view.()
    assert initial["replies"] == []

    discussion =
      request(relay, "device-b", %{
        "type" => "discussion",
        "op_id" => op(702),
        "sender_id" => "app:test",
        "message_id" => "parent",
        "request_revision" => 1,
        "text" => "Explain"
      })

    assert discussion["type"] == "ok"
    assert view.()["view_rev"] == initial["view_rev"]

    reply_frame = %{
      "type" => "conversation_reply",
      "op_id" => op(703),
      "sender_id" => "app:test",
      "message_id" => "parent",
      "request_revision" => 1,
      "in_reply_to" => String.to_integer(discussion["event_id"]),
      "text" => "Agent explanation"
    }

    reply = request(relay, "device-a", reply_frame)
    assert reply["type"] == "ok"
    seq = reply["seq"]
    expected = %{"sender_id" => "app:test", "message_id" => "parent", "conversation_seq" => seq}
    baseline = view.()
    assert baseline["replies"] == [expected]
    assert baseline["entries"] == initial["entries"]
    refute baseline["view_rev"] == initial["view_rev"]
    assert Map.delete(request(relay, "device-a", reply_frame), "duplicate") == reply
    assert view.()["view_rev"] == baseline["view_rev"]

    assert {:ok, terminal_line} =
             Relay.request_line(
               relay,
               "device-b",
               JSON.encode!(%{
                 "type" => "answer_no_comment",
                 "request_id" => "parent",
                 "request_revision" => 1
               }) <> "\n"
             )

    assert {:ok, %{"type" => "ok"}} = JSON.decode_line(terminal_line)
    assert view.()["entries"] == []
    assert view.()["replies"] == [expected]
    engine = :sys.get_state(relay).store.engine
    [item] = Placement.ranking_items(engine, "person-a")
    assert item.identity == {"app:test", "parent", seq}
    assert {:ok, composed} = JevClient.compose([item], @endpoint, @model, 1000)
    assert {:ok, payload} = JSON.decode_line(composed.body <> "\n")
    [pending] = payload["state"]["pending_items"]
    assert pending["kind"] == "reply"
    assert pending["parent_body"] == "Parent context"
    assert pending["body"] == "Agent explanation"
    assert pending["urgency"] == "high"
    configure_jev_choice(relay)
    before_score = view.()
    assert before_score["replies"] == [expected]
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{item.identity => 0.1}})
    scored = view.()
    assert [scored_reply] = scored["replies"]
    assert scored_reply["score"] == %JSON.DecimalToken{raw: "0.1"}
    assert scored["entries"] == []
    refute scored["view_rev"] == before_score["view_rev"]
    writer = :sys.get_state(relay).applied_score_record
    :sys.get_state(writer)
    assert {:ok, row} = JSON.decode_line(File.read!(record_path))
    assert row["kind"] == "reply"
    assert row["conversation_seq"] == seq
    assert row["title"] == "Parent title"
    assert row["score"] == %JSON.DecimalToken{raw: "0.1"}
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:error, :unavailable})
    assert view.()["replies"] == [expected]
    before_read = :sys.get_state(relay).store.engine.messages[{"person-a", "app:test", "parent"}]

    assert %{"type" => "ok"} =
             request(relay, "device-b", %{
               "type" => "conversation_read",
               "op_id" => op(705),
               "sender_id" => "app:test",
               "message_id" => "parent",
               "read_through_conversation_seq" => seq
             })

    after_read = view.()
    assert after_read["replies"] == []
    refute after_read["view_rev"] == scored["view_rev"]
    engine = :sys.get_state(relay).store.engine
    assert engine.messages[{"person-a", "app:test", "parent"}] == before_read
    assert Placement.ranking_items(engine, "person-a") == []

    assert [
             %{"kind" => "message"},
             %{
               "kind" => "reply",
               "unseen_count" => 0,
               "score_kind" => "historical",
               "score" => %JSON.DecimalToken{raw: "0.1"}
             }
           ] = after_read["list_items"]

    :sys.get_state(writer)
    assert File.read!(record_path) |> String.split("\n", trim: true) |> length() == 1
    GenServer.stop(relay)
    test_process = self()

    restarted =
      start_relay(path,
        jev_score_record_path: record_path,
        priority_evaluator: fn _ ->
          send(test_process, :unexpected_history_scoring)
          {:error, :unavailable}
        end
      )

    {:ok, session} = Relay.open_session(restarted, "device-b")
    assert view_for(restarted, session)["list_items"] == after_read["list_items"]
    assert :sys.get_state(restarted).priority_jobs == %{}
    assert :sys.get_state(restarted).priority_timers == %{}
    refute_received :unexpected_history_scoring
    :sys.get_state(:sys.get_state(restarted).applied_score_record)
    assert File.read!(record_path) |> String.split("\n", trim: true) |> length() == 1
  end

  for partial_read <- [false, true] do
    @partial_read partial_read
    test "group history survives restart and reactivates (partial read: #{partial_read})", %{
      dir: dir,
      path: path
    } do
      use_synthetic_jev_key(dir)
      relay = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
      setup_person(relay)
      {:ok, session} = Relay.open_session(relay, "device-b")
      assert view_for(relay, session)["list_items"] == []
      forward_chat(relay, "history", 900)
      first = add_reply(relay, "history", 902)
      second = add_reply(relay, "history", 904)
      baseline = view_for(relay, session)
      assert [group] = Enum.filter(baseline["list_items"], &(&1["kind"] == "reply"))
      assert group["unseen_count"] == 2
      assert group["conversation_seq"] == second
      assert group["open_at_conversation_seq"] == first
      refute Map.has_key?(group, "score")
      refute Map.has_key?(group, "score_kind")
      configure_jev_choice(relay)
      assert Enum.all?(view_for(relay, session)["replies"], &(not Map.has_key?(&1, "score")))
      job = start_synthetic_priority_pass(relay)

      complete_synthetic_priority_pass(
        job,
        {:ok,
         %{
           {"app:test", "history"} => 0.5,
           {"app:test", "history", first} => 0.9,
           {"app:test", "history", second} => 0.4
         }}
      )

      scored = view_for(relay, session)
      assert [%{"kind" => "reply"} = group, %{"kind" => "message"}] = scored["list_items"]
      assert group["score"] == %JSON.DecimalToken{raw: "0.9"}
      assert group["score_kind"] == "current"
      assert group["conversation_seq"] == second
      assert group["open_at_conversation_seq"] == first
      assert Enum.map(scored["replies"], & &1["conversation_seq"]) == [first, second]
      assert [%{"message_id" => "history"}] = scored["entries"]

      assert [%{"unchanged" => true} = unchanged] =
               session_request(relay, session, %{
                 "type" => "view_get",
                 "ref" => "same",
                 "known_view_rev" => scored["view_rev"]
               })

      refute Map.has_key?(unchanged, "list_items")
      parent = :sys.get_state(relay).store.engine.messages[{"person-a", "app:test", "history"}]

      if @partial_read do
        read_reply(relay, "history", first, 906)
        partial = view_for(relay, session)
        [row] = Enum.filter(partial["list_items"], &(&1["kind"] == "reply"))
        assert row["unseen_count"] == 1
        assert row["open_at_conversation_seq"] == second
        assert row["score"] == %JSON.DecimalToken{raw: "0.4"}
        refute partial["view_rev"] == scored["view_rev"]
      end

      read_reply(relay, "history", second, 907)
      seen = view_for(relay, session)
      expected_score = if @partial_read, do: "0.4", else: "0.9"
      assert [%{"kind" => "message"}, %{"kind" => "reply"} = history] = seen["list_items"]
      assert history["unseen_count"] == 0
      assert history["score"] == %JSON.DecimalToken{raw: expected_score}
      assert history["score_kind"] == "historical"
      assert history["conversation_seq"] == second
      assert history["open_at_conversation_seq"] == second
      assert seen["replies"] == []
      engine = :sys.get_state(relay).store.engine
      assert engine.messages[{"person-a", "app:test", "history"}] == parent
      assert Placement.reply_items(engine, "person-a") == []
      # The score-record writer is disabled: it is not the persistence source.
      assert :sys.get_state(relay).applied_score_record == nil
      GenServer.stop(relay)
      restarted = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
      {:ok, other_device} = Relay.open_session(restarted, "device-a")
      restored = view_for(restarted, other_device)
      assert List.last(restored["list_items"]) == history
      assert restored["replies"] == []
      refute restored["view_rev"] == seen["view_rev"]

      assert :sys.get_state(restarted).store.engine.chat_read_positions[
               {"person-a", "app:test", "history"}
             ] == second

      third = add_reply(restarted, "history", 908)
      active = view_for(restarted, other_device)
      [row] = Enum.filter(active["list_items"], &(&1["kind"] == "reply"))
      assert row["unseen_count"] == 1
      assert row["conversation_seq"] == third
      refute Map.has_key?(row, "score")
      refute Map.has_key?(row, "score_kind")
      # A real parent transition cannot move the independently active chat to history.
      assert {:ok, terminal} =
               Relay.request_line(
                 restarted,
                 "device-b",
                 JSON.encode!(%{
                   "type" => "answer_no_comment",
                   "request_id" => "history",
                   "request_revision" => 1
                 }) <> "\n"
               )

      assert {:ok, %{"type" => "ok"}} = JSON.decode_line(terminal)

      assert [%{"kind" => "reply", "unseen_count" => 1}, %{"kind" => "message"}] =
               view_for(restarted, other_device)["list_items"]

      assert [{"app:test", "history", ^third}] =
               Enum.map(
                 Placement.ranking_items(:sys.get_state(restarted).store.engine, "person-a"),
                 & &1.identity
               )
    end
  end

  test "reply-history commits preserve support receipts across restart", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)

    relay =
      start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)

    setup_person(relay)
    {:ok, session} = Relay.open_session(relay, "device-b")
    assert view_for(relay, session)["list_items"] == []

    forward_chat(relay, "reply-history", 2700)
    reply_seq = add_reply(relay, "reply-history", 2702)
    configure_jev_choice(relay)

    support = %{
      "type" => "support",
      "op_id" => op(2710),
      "session_handle" => op(2701),
      "route" => "claude-code",
      "cli_version" => "test-only",
      "unsupported_reason" => nil
    }

    assert {:ok, %{"listening_support" => declared}} =
             Relay.origin_support(relay, "device-a", support)

    assert :ok = Relay.origin_stream_attach(relay, "device-a", op(2701), self())

    {support_version, support_payload, support_checksum} = persisted_snapshot(path)
    assert :crypto.hash(:sha256, support_payload) == support_checksum

    job = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(
      job,
      {:ok, %{{"app:test", "reply-history", reply_seq} => 0.9}}
    )

    [active] =
      view_for(relay, session)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert active["score"] == %JSON.DecimalToken{raw: "0.9"}
    assert active["score_kind"] == "current"

    {reply_version, reply_payload, reply_checksum} = persisted_snapshot(path)

    assert reply_version == support_version + 1
    assert reply_payload != support_payload
    assert :crypto.hash(:sha256, reply_payload) == reply_checksum

    read_reply(relay, "reply-history", reply_seq, 2712)

    [historical] =
      view_for(relay, session)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert historical["unseen_count"] == 0
    assert historical["score"] == %JSON.DecimalToken{raw: "0.9"}
    assert historical["score_kind"] == "historical"

    GenServer.stop(relay)
    restarted = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
    {:ok, resumed_session} = Relay.open_session(restarted, "device-b")
    restored = view_for(restarted, resumed_session)
    assert List.last(restored["list_items"]) == historical

    assert {:ok, %{"listening_support" => ^declared, "availability" => "not_listening"}} =
             Relay.origin_status(restarted, "device-a", op(2701))

    assert {:ok, feed, _high} = Relay.conversation_events(restarted, "device-b", 0)
    assert Enum.any?(feed, &(&1["listening_support"] == declared))
    assert Enum.any?(feed, &(&1["availability"] == "listening"))

    duplicate_state = persisted_snapshot(path)

    assert {:ok, %{"duplicate" => true, "listening_support" => ^declared}} =
             Relay.origin_support(restarted, "device-a", support)

    assert persisted_snapshot(path) == duplicate_state

    next_reply_seq = add_reply(restarted, "reply-history", 2714)

    [reactivated] =
      view_for(restarted, resumed_session)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert reactivated["conversation_seq"] == next_reply_seq
    assert reactivated["unseen_count"] == 1
    refute Map.has_key?(reactivated, "score")
    refute Map.has_key?(reactivated, "score_kind")

    assert :sys.get_state(restarted).store.engine.reply_group_history[
             {"person-a", "app:test", "reply-history"}
           ].score == 0.9
  end

  test "never-scored history stays unscored and a person's turn does not reactivate it", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)
    {:ok, session} = Relay.open_session(relay, "device-b")
    forward_chat(relay, "plain", 920)
    seq = add_reply(relay, "plain", 922)
    read_reply(relay, "plain", seq, 924)
    before = view_for(relay, session)

    assert [%{"kind" => "message"}, %{"kind" => "reply", "unseen_count" => 0} = group] =
             before["list_items"]

    refute Map.has_key?(group, "score")
    refute Map.has_key?(group, "score_kind")

    assert %{"type" => "ok"} =
             request(relay, "device-b", %{
               "type" => "discussion",
               "op_id" => op(925),
               "sender_id" => "app:test",
               "message_id" => "plain",
               "request_revision" => 1,
               "text" => "Another question"
             })

    assert view_for(relay, session)["view_rev"] == before["view_rev"]
    GenServer.stop(relay)
    restarted = start_relay(path)
    {:ok, session} = Relay.open_session(restarted, "device-b")
    assert view_for(restarted, session)["list_items"] == before["list_items"]
    assert :sys.get_state(restarted).priority_jobs == %{}
  end

  test "a failed history commit neither publishes nor records the new score", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    record_path = Path.join(dir, "history-failure.jsonl")

    relay =
      start_relay(path,
        jev_score_record_path: record_path,
        priority_evaluator: fn _ -> {:error, :unavailable} end
      )

    setup_person(relay)
    forward_chat(relay, "failure", 940)
    seq = add_reply(relay, "failure", 942)
    configure_jev_choice(relay)
    {:ok, session} = Relay.open_session(relay, "device-b")
    before = view_for(relay, session)
    state = :sys.get_state(relay)

    assert :ok =
             Sqlite3.execute(
               state.store.db,
               "CREATE TRIGGER reject_history BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END"
             )

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "failure", seq} => 0.9}})
    assert view_for(relay, session)["list_items"] == before["list_items"]
    assert :sys.get_state(relay).store.engine.reply_group_history == %{}
    :sys.get_state(state.applied_score_record)
    assert not File.exists?(record_path) or File.read!(record_path) == ""
    assert :ok = Sqlite3.execute(state.store.db, "DROP TRIGGER reject_history")
  end

  test "pre-history format 24 restores existing chats without inventing scores", %{path: path} do
    relay = start_relay(path)
    setup_person(relay)
    forward_chat(relay, "legacy", 930)
    seq = add_reply(relay, "legacy", 932)
    read_reply(relay, "legacy", seq, 934)

    old_engine =
      :sys.get_state(relay).store.engine
      |> Map.delete(:reply_group_history)
      |> Map.delete(:free_order_highs)
      |> Map.update!(
        :messages,
        &Map.new(&1, fn {key, record} ->
          {key, Map.delete(record, :free_order)}
        end)
      )
      |> Map.update!(
        :conversation_events,
        &Map.new(&1, fn {key, event} ->
          {key, Map.delete(event, :free_order)}
        end)
      )

    GenServer.stop(relay)
    payload = :erlang.term_to_binary({:attention_relay_state, 24, old_engine}, [{:compressed, 6}])
    assert {:ok, db} = Sqlite3.open(path)
    assert {:ok, stmt} = Sqlite3.prepare(db, "UPDATE relay_state SET payload = ?, checksum = ?")
    assert :ok = Sqlite3.bind(stmt, [{:blob, payload}, {:blob, :crypto.hash(:sha256, payload)}])
    assert :done = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    restarted = start_relay(path)
    {:ok, session} = Relay.open_session(restarted, "device-b")

    assert [%{"kind" => "message"}, %{"kind" => "reply", "unseen_count" => 0} = group] =
             view_for(restarted, session)["list_items"]

    refute Map.has_key?(group, "score")
    assert group["conversation_seq"] == seq
    engine = :sys.get_state(restarted).store.engine
    assert engine.reply_group_history == %{}
    assert engine.free_order_highs == %{}
    assert group["free_section"] == %{"state" => "history", "group" => "earlier_chat_replies"}
    refute Map.has_key?(engine.messages[{"person-a", "app:test", "legacy"}], :free_order)
    GenServer.stop(restarted)
    again = start_relay(path)
    assert :sys.get_state(again).store.engine.free_order_highs == %{}
    {:ok, session} = Relay.open_session(again, "device-b")
    add_reply(again, "legacy", 940)
    [reply, parent] = view_for(again, session)["list_items"]
    assert reply["free_section"] == %{"state" => "active", "group" => "common"}
    assert parent["free_section"] == %{"state" => "active", "group" => "earlier_messages"}
    assert :sys.get_state(again).store.engine.free_order_highs == %{"person-a" => 1}
  end

  test "mixed projection keeps message threads intact and historical reply chronology stable" do
    messages =
      for {id, seq, terminal} <- [
            {"a", 1, nil},
            {"b", 2, nil},
            {"p", 3, nil},
            {"old", 4, %{seq: 5}}
          ],
          into: %{} do
        {{"person", "sender", id},
         %{accepted_seq: seq, terminal: terminal, message: %{}, revision: 1}}
      end

    events =
      for {id, seq} <- [{"p", 10}, {"p", 20}, {"old", 30}], into: %{} do
        {{"person", seq},
         %{key: {"person", "sender", id}, payload: %{"kind" => "reply", "author" => "origin"}}}
      end

    engine = %Engine{
      algorithm_choices: %{"person" => "jev-set"},
      messages: messages,
      conversation_events: events,
      chat_read_positions: %{{"person", "sender", "old"} => 30}
    }

    entries = [
      %{"sender_id" => "sender", "message_id" => "a", "thread" => "thread", "score" => 0.95},
      %{"sender_id" => "sender", "message_id" => "p", "conversation_seq" => 10, "score" => 0.9},
      %{"sender_id" => "sender", "message_id" => "b", "thread" => "thread", "score" => 0.7},
      %{"sender_id" => "sender", "message_id" => "p", "score" => 0.5},
      %{"sender_id" => "sender", "message_id" => "p", "conversation_seq" => 20, "score" => 0.4}
    ]

    rows = project_list(engine, "person", entries)

    assert Enum.map(rows, &{&1["kind"], &1["message_id"]}) ==
             [
               {"message", "a"},
               {"message", "b"},
               {"reply", "p"},
               {"message", "p"},
               {"message", "old"},
               {"reply", "old"}
             ]

    assert Enum.at(rows, 2)["score"] == 0.9
    assert Enum.at(rows, 2)["conversation_seq"] == 20
    assert Enum.at(rows, 2)["open_at_conversation_seq"] == 10
    assert Enum.all?(rows, &(not Map.has_key?(&1, "body")))
    # A second account's retained chats cannot enter this projection.
    assert project_list(engine, "other", []) == []
  end

  test "many reply groups retain highest-score anchors and first-position ties" do
    ids = Enum.map(1..200, &Integer.to_string/1)
    key = fn id -> {"person", "sender", id} end

    events =
      for {id, index} <- Enum.with_index(ids), offset <- 1..3, into: %{} do
        seq = index * 3 + offset
        {{"person", seq}, %{key: key.(id), payload: %{"kind" => "reply", "author" => "origin"}}}
      end

    engine = %Engine{conversation_events: events, algorithm_choices: %{"person" => "jev-set"}}

    entry = fn id, index, offset, score ->
      %{
        "sender_id" => "sender",
        "message_id" => id,
        "conversation_seq" => index * 3 + offset,
        "score" => score
      }
    end

    low = for {id, index} <- Enum.with_index(ids), do: entry.(id, index, 1, 0.4)
    high = for {id, index} <- Enum.reverse(Enum.with_index(ids)), do: entry.(id, index, 2, 0.9)
    tied = for {id, index} <- Enum.with_index(ids), do: entry.(id, index, 3, 0.9)
    separator = %{"sender_id" => "sender", "message_id" => "separator"}

    rows = project_list(engine, "person", low ++ high ++ [separator] ++ tied)
    {groups, [message]} = Enum.split(rows, 200)
    assert message == Map.put(separator, "kind", "message")
    assert Enum.map(groups, & &1["message_id"]) == Enum.reverse(ids)
    assert Enum.all?(groups, &(&1["score"] == 0.9 and &1["unseen_count"] == 3))
    assert Enum.map(groups, & &1["conversation_seq"]) == Enum.to_list(600..3//-3)
    assert Enum.map(groups, & &1["open_at_conversation_seq"]) == Enum.to_list(598..1//-3)

    # Without scores, the first engine entry still anchors each chat; no score
    # or score-kind is manufactured to choose a position.
    unscored = Enum.map(low ++ [separator] ++ high ++ tied, &Map.delete(&1, "score"))
    rows = project_list(engine, "person", unscored)
    assert Enum.map(Enum.take(rows, 200), & &1["message_id"]) == ids
    assert List.last(rows) == Map.put(separator, "kind", "message")

    assert Enum.all?(
             rows,
             &(not Map.has_key?(&1, "score") and not Map.has_key?(&1, "score_kind"))
           )
  end

  test "one shared reply projection retains long seen history and reactivates only new replies" do
    key = {"person", "sender", "chat"}

    events =
      for seq <- 1..2000, into: %{} do
        payload =
          if seq <= 1000 do
            %{"kind" => "reply", "author" => "origin", "text" => "Retained reply"}
          else
            %{"kind" => "discussion", "author" => "person", "text" => "Retained discussion"}
          end

        {{"person", seq}, %{key: key, payload: payload}}
      end

    engine = %Engine{
      conversation_events: events,
      messages: %{
        key => %{
          message: %{"title" => "Parent", "body" => "Context"},
          accepted_seq: 1,
          revision: 1,
          terminal: %{seq: 2}
        }
      },
      chat_read_positions: %{key => 1000},
      reply_group_history: %{key => %{score: 0.8, scores: %{}}}
    }

    {items, summary} = Placement.ranking_projection(engine, "person")
    assert items == []
    assert map_size(summary) == 1
    rows = Placement.list_items(engine, "person", [], summary)

    assert [
             %{"kind" => "message"},
             %{
               "kind" => "reply",
               "conversation_seq" => 1000,
               "open_at_conversation_seq" => 1000,
               "unseen_count" => 0,
               "score" => 0.8,
               "score_kind" => "historical"
             }
           ] = rows

    assert map_size(engine.conversation_events) == 2000

    event = %{
      key: key,
      payload: %{"kind" => "reply", "author" => "origin", "text" => "New reply"}
    }

    next = %{engine | conversation_events: Map.put(events, {"person", 2001}, event)}
    {[item], summary} = Placement.ranking_projection(next, "person")
    assert item.identity == {"sender", "chat", 2001}
    assert item.message["body"] == "New reply"
    entries = Placement.place("baseline", [item], [], nil)

    assert [%{"kind" => "reply"} = active, %{"kind" => "message"}] =
             Placement.list_items(next, "person", entries, summary)

    assert active["conversation_seq"] == 2001
    assert active["open_at_conversation_seq"] == 2001
    assert active["unseen_count"] == 1
    refute Map.has_key?(active, "score")
    refute Map.has_key?(active, "score_kind")
    assert map_size(next.conversation_events) == 2001
    assert next.messages == engine.messages
    assert next.chat_read_positions == engine.chat_read_positions
  end

  test "failed durable acceptance publishes neither a parent nor a common key", %{path: path} do
    relay = start_relay(path)
    setup_person(relay)
    state = :sys.get_state(relay)

    assert :ok =
             Sqlite3.execute(
               state.store.db,
               "CREATE TRIGGER reject_arrival BEFORE UPDATE ON relay_state BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END"
             )

    frame = %{
      "type" => "forward",
      "op_id" => op(3100),
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "atomic",
        "created_at" => 1,
        "title" => "Atomic arrival"
      }
    }

    assert request(relay, "device-a", frame)["type"] == "error"
    engine = :sys.get_state(relay).store.engine
    assert engine.messages == %{}
    assert engine.free_order_highs == %{}
    assert :ok = Sqlite3.execute(state.store.db, "DROP TRIGGER reject_arrival")
    assert request(relay, "device-a", frame)["type"] == "ok"

    assert :sys.get_state(relay).store.engine.messages[{"person-a", "app:test", "atomic"}].free_order ==
             1
  end

  test "Free acceptance interleaves streams durably without rekeying replay or Seen", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)
    forward_chat(relay, "older", 3000)
    seq = add_reply(relay, "older", 3002)
    forward_chat(relay, "newer", 3004)
    {:ok, session} = Relay.open_session(relay, "device-b")
    view = view_for(relay, session)
    assert view["free_order_version"] == 1

    assert Enum.map(view["list_items"], &{&1["kind"], &1["message_id"]}) ==
             [{"message", "newer"}, {"reply", "older"}, {"message", "older"}]

    engine = :sys.get_state(relay).store.engine
    assert engine.free_order_highs == %{"person-a" => 3}
    assert engine.messages[{"person-a", "app:test", "older"}].free_order == 1
    assert engine.conversation_events[{"person-a", seq}].free_order == 2
    forward_chat(relay, "older", 3000)
    assert view_for(relay, session)["view_rev"] == view["view_rev"]
    read_reply(relay, "older", seq, 3006)
    seen = view_for(relay, session)

    assert List.last(seen["list_items"])["free_section"] ==
             %{"state" => "history", "group" => "common"}

    refute seen["view_rev"] == view["view_rev"]
    assert :sys.get_state(relay).store.engine.free_order_highs == engine.free_order_highs
    GenServer.stop(relay)
    restarted = start_relay(path)
    {:ok, session} = Relay.open_session(restarted, "device-b")
    assert view_for(restarted, session)["list_items"] == seen["list_items"]
    assert :sys.get_state(restarted).store.engine.free_order_highs == engine.free_order_highs
    add_reply(restarted, "older", 3008)
    assert hd(view_for(restarted, session)["list_items"])["kind"] == "reply"
    assert :sys.get_state(restarted).store.engine.free_order_highs == %{"person-a" => 4}
  end

  test "Free mixed-age threads return to Earlier messages when their common anchor resolves" do
    record = fn seq, order, thread ->
      %{
        accepted_seq: seq,
        free_order: order,
        terminal: nil,
        revision: 1,
        message: %{"thread" => thread}
      }
    end

    engine = %Engine{
      messages: %{
        {"p", "s", "a"} => record.(30, nil, "T"),
        {"p", "s", "b"} => record.(10, nil, "T"),
        {"p", "s", "c"} => record.(40, 2, "T"),
        {"p", "s", "u"} => record.(20, 1, "U")
      }
    }

    rows = project_list(engine, "p", [])
    assert Enum.map(rows, & &1["message_id"]) == ["c", "a", "b", "u"]
    assert Enum.all?(rows, &(&1["free_section"]["group"] == "common"))
    next = put_in(engine.messages[{"p", "s", "c"}].terminal, %{seq: 50})
    rows = project_list(next, "p", [])
    assert Enum.map(rows, & &1["message_id"]) == ["u", "a", "b", "c"]

    assert Enum.map(rows, & &1["free_section"]) == [
             %{"state" => "active", "group" => "common"},
             %{"state" => "active", "group" => "earlier_messages"},
             %{"state" => "active", "group" => "earlier_messages"},
             %{"state" => "history", "group" => "common"}
           ]

    assert next.messages[{"p", "s", "a"}].free_order == nil
  end

  test "Free common ties use kind then identity and legacy domains stay separate" do
    parent = fn n, k, terminal ->
      %{message: %{}, accepted_seq: n, free_order: k, terminal: terminal, revision: 1}
    end

    engine = %Engine{
      messages: %{
        {"p", "s", "a"} => parent.(30, 4, nil),
        {"p", "s", "b"} => parent.(20, 4, nil),
        {"p", "s", "old"} => parent.(900, nil, nil),
        {"p", "s", "done"} => parent.(1000, nil, %{seq: 1001})
      },
      conversation_events: %{
        {"p", 2} => %{
          key: {"p", "s", "a"},
          free_order: 4,
          payload: %{"kind" => "reply", "author" => "origin"}
        },
        {"p", 800} => %{
          key: {"p", "s", "old"},
          payload: %{"kind" => "reply", "author" => "origin"}
        }
      }
    }

    rows = project_list(engine, "p", [])

    assert Enum.map(rows, &{&1["kind"], &1["message_id"]}) ==
             [
               {"message", "a"},
               {"message", "b"},
               {"reply", "a"},
               {"message", "old"},
               {"reply", "old"},
               {"message", "done"}
             ]

    assert Enum.at(rows, 4)["free_section"]["group"] == "earlier_chat_replies"
    assert List.last(rows)["free_section"]["state"] == "history"
  end

  defp project_list(engine, person, entries) do
    {_, reply_summary} = Placement.ranking_projection(engine, person)
    Placement.list_items(engine, person, entries, reply_summary)
  end

  defp view_for(relay, session) do
    session_request(relay, session, %{"type" => "view_get", "ref" => "groups"})
    |> Enum.find(&(&1["ref"] == "groups"))
  end

  defp forward_chat(relay, id, n) do
    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => op(n + 1),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => 1,
                 "title" => "Parent",
                 "body" => "Context"
               }
             })
  end

  defp add_reply(relay, id, n) do
    discussion =
      request(relay, "device-b", %{
        "type" => "discussion",
        "op_id" => op(n),
        "sender_id" => "app:test",
        "message_id" => id,
        "request_revision" => 1,
        "text" => "Question"
      })

    assert discussion["type"] == "ok"

    reply =
      request(relay, "device-a", %{
        "type" => "conversation_reply",
        "op_id" => op(n + 1),
        "sender_id" => "app:test",
        "message_id" => id,
        "request_revision" => 1,
        "in_reply_to" => String.to_integer(discussion["event_id"]),
        "text" => "Reply"
      })

    assert reply["type"] == "ok"
    reply["seq"]
  end

  defp read_reply(relay, id, seq, n) do
    assert %{"type" => "ok"} =
             request(relay, "device-b", %{
               "type" => "conversation_read",
               "op_id" => op(n),
               "sender_id" => "app:test",
               "message_id" => id,
               "read_through_conversation_seq" => seq
             })
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

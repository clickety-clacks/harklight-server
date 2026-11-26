defmodule AttentionServer.PrioritizationTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{
    BounceDecision,
    BouncePolicy,
    ChangeLog,
    DashboardView,
    DeviceIdentity,
    Engine,
    JSON,
    JevClient,
    JevRanking,
    Message,
    Placement,
    PreChangeLogStore,
    Relay,
    RelayReaders,
    RouteBIndex,
    Store,
    TLSFixtures
  }

  alias AttentionServer.ClientAPI.{Reads, Resources}
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

  # The stored priority view, the rank input, in the string-keyed wire form
  # the protocol 0.1 view fetch served, without its revision.
  defp view_for(relay, person \\ "person-a") do
    view =
      :sys.get_state(relay).priority_views
      |> Map.fetch!(person)
      |> Map.take([:algorithm, :basis, :reason, :entries, :replies, :list_items])
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:ok, decoded} = JSON.decode_line(JSON.encode!(view) <> "\n")
    decoded
  end

  # The 0.1 view fetch rebuilt the view on read. A choice saved behind the
  # Relay's back (configure_jev_choice) reaches the view the way the Relay
  # publishes a Jev view it cannot rank yet: a pass that gives no scores
  # publishes the fallback. Its retry timer is cancelled; each test drives
  # its own later passes.
  defp publish_fallback_view(relay, person \\ "person-a") do
    job = start_synthetic_priority_pass(relay, person)
    complete_synthetic_priority_pass(job, {:error, :unavailable})

    :sys.replace_state(relay, fn state ->
      case Map.pop(state.priority_retry_timers, person) do
        {%{ref: ref}, timers} ->
          Process.cancel_timer(ref)
          %{state | priority_retry_timers: timers}

        {nil, _timers} ->
          state
      end
    end)

    assert view_for(relay, person)["basis"] == "fallback"
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
                barrier_result = RelayReaders.events(relay_pid, "device-a", 0)
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
    assert_receive {:priority_result_handled, ^expected_pid, {:ok, _events, _sequence}}, 2_000
  end

  defp setup_person(relay) do
    assert :ok = Relay.setup_person(relay, "person-a")
    assert :ok = Relay.setup_device(relay, "person-a", "device-a")
    assert :ok = Relay.setup_device(relay, "person-a", "device-b")
    assert :ok = Relay.setup_sender(relay, "device-a", "app:test")
  end

  defp persisted_rows(path) do
    alias AttentionServer.Store.RecordTable
    assert {:ok, db} = Sqlite3.open(path, mode: :readonly)
    assert {:ok, generation} = RecordTable.generation(db)

    assert {:ok, rows} =
             RecordTable.select(
               db,
               "SELECT key, payload, checksum FROM relay_records ORDER BY key"
             )

    assert :ok = Sqlite3.close(db)

    assert Enum.all?(rows, fn [key, payload, checksum] ->
             :crypto.hash(:sha256, [key, payload]) == checksum
           end)

    {generation, rows}
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

  test "basic order and Jev fallback schedule bounce from effective urgency", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)

    for {id, urgency, n} <- [
          {"A", "low", 1},
          {"B", "normal", 2},
          {"C", "now", 3},
          {"default", nil, 4}
        ] do
      message = %{
        "sender_id" => "app:test",
        "message_id" => id,
        "created_at" => n,
        "title" => id,
        "message_type" => "fyi",
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
        }
      }

      message = if urgency, do: Map.put(message, "urgency", urgency), else: message

      assert request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => op(100 + n),
               "message" => message
             })["type"] == "ok"
    end

    view = view_for(relay)
    assert view["algorithm"] == "baseline"
    entries = view["entries"]

    assert Enum.map(
             Enum.reject(entries, &(&1["message_id"] == "default")),
             &{&1["message_id"], &1["priority"]}
           ) == [{"C", "now"}, {"B", "normal"}, {"A", "low"}]

    assert Enum.find(entries, &(&1["message_id"] == "default"))["priority"] == "normal"
    refute Enum.any?(entries, &Map.has_key?(&1, "score"))
    state = :sys.get_state(relay)

    refute Enum.any?(
             state.priority_views["person-a"].engine_entries,
             &Map.has_key?(&1, "priority")
           )

    engine = state.store.engine
    refute Map.has_key?(engine.bounce_levels, "person-a")

    for {id, level, hours} <- [{"A", "Low", 168}, {"B", "Normal", 72},
                               {"C", "Urgent", 12}, {"default", "Normal", 72}] do
      key = {"person-a", "app:test", id}
      revision = engine.bounce_revisions[key].revisions[1]
      assert revision.selection["level_name"] == level
      assert revision.selection["due_at_ms"] == revision.accepted_at_ms + hours * 3_600_000
    end

    assert is_reference(state.bounce_deadline_timer)

    configure_jev_choice(relay)
    publish_fallback_view(relay)
    fallback = view_for(relay)
    assert fallback["basis"] == "fallback"

    assert Enum.map(
             Enum.reject(fallback["entries"], &(&1["message_id"] == "default")),
             & &1["priority"]
           ) == ["now", "normal", "low"]

    refute Enum.any?(fallback["entries"], &Map.has_key?(&1, "score"))
    fallback_engine = :sys.get_state(relay).store.engine

    fallback_unscored =
      BounceDecision.record_scores(fallback_engine, "person-a", fallback["entries"], 1_000)

    assert BounceDecision.next_deadline(fallback_unscored) == BounceDecision.next_deadline(engine)
  end

  test "startup backfills retained unscored items, bounces overdue items and logs once", %{path: path} do
    accepted = 1_794_900_000_000
    relay = start_relay(path, clock: fn -> accepted end)
    setup_person(relay)

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(1),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => "retained",
                 "title" => "Retained",
                 "created_at" => accepted,
                 "message_type" => "fyi",
                 "answer_ui" => %{
                   "schema" => "attention-client.actions/1",
                   "actions" => [%{"id" => "seen", "label" => "Seen", "kind" => "value",
                                    "value" => %{"acknowledgment" => "seen"}}]
                 }
               }
             })

    key = {"person-a", "app:test", "retained"}
    GenServer.stop(relay)

    {:ok, store} = Store.open(path)
    legacy =
      store.engine
      |> put_in([Access.key(:bounce_revisions), key, :revisions, 1, :selection], nil)
      |> BounceDecision.rebuild_indexes()

    {:ok, store} = Store.commit(store, legacy, %{person: "person-a", actor: "fixture", action: "legacy"})
    :ok = Store.close(store)

    original = File.read!(path)
    files = File.ls!(Path.dirname(path)) |> Enum.sort()

    assert ExUnit.CaptureIO.capture_io(fn ->
             AttentionServer.CLI.Operator.run(["bounce-preview", "--store", path])
           end) == ~s({"full":1,"soft":0}\n)

    assert File.read!(path) == original
    assert Enum.sort(File.ls!(Path.dirname(path))) == files
    File.write!(path <> "-wal", "uncheckpointed")
    assert Store.read_stopped_engine(path) == {:error, :store_not_checkpointed}
    File.rm!(path <> "-wal")

    due = accepted + 259_200_000
    opts = [clock: fn -> due end]

    log = ExUnit.CaptureLog.capture_log(fn ->
      restarted = start_relay(path, opts)
      engine = :sys.get_state(restarted).store.engine
      assert engine.messages[key]["full_bounced_at_ms"] == due
      GenServer.stop(restarted)
    end)

    assert log =~ "event=harklight_bounce_backfill selections=1 overdue_full_bounces=1"

    log = ExUnit.CaptureLog.capture_log(fn ->
      restarted = start_relay(path, opts)
      assert :sys.get_state(restarted).store.engine.messages[key]["full_bounced_at_ms"] == due
    end)

    refute log =~ "harklight_bounce_backfill"
  end

  test "an exact UTC cutoff excludes older deliveries and can be cleared", %{path: path} do
    setting = "ATTENTION_SERVER_BOUNCE_NOT_BEFORE"
    on_exit(fn -> System.delete_env(setting) end)
    System.put_env(setting, "2026-11-17T14:16:54Z")
    assert {:ok, 1_794_925_014_000 = cutoff} = BouncePolicy.cutoff()
    relay = start_relay(path, clock: fn -> cutoff - 1 end)
    setup_person(relay)

    for {id, at, n} <- [{"before", cutoff - 1, 1}, {"equal", cutoff, 2}] do
      :sys.replace_state(relay, &%{&1 | clock: fn -> at end})

      assert %{"type" => "ok"} =
               request(relay, "device-a", %{
                 "type" => "forward",
                 "op_id" => op(n),
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => id,
                   "title" => id,
                   "created_at" => at,
                   "message_type" => "fyi",
                   "answer_ui" => %{
                     "schema" => "attention-client.actions/1",
                     "actions" => [%{"id" => "seen", "label" => "Seen", "kind" => "value",
                                      "value" => %{"acknowledgment" => "seen"}}]
                   }
                 }
               })
    end

    engine = :sys.get_state(relay).store.engine
    older = {"person-a", "app:test", "before"}
    boundary = {"person-a", "app:test", "equal"}
    assert engine.bounce_revisions[older].revisions[1].selection == nil
    assert engine.bounce_revisions[boundary].revisions[1].selection["due_at_ms"] ==
             cutoff + 259_200_000

    retained =
      engine
      |> put_in([Access.key(:bounce_revisions), boundary, :revisions, 1, :selection], nil)
      |> BounceDecision.rebuild_indexes()

    now_ms = cutoff + 259_200_000
    {_, _, counts} = BounceDecision.startup(retained, now_ms, cutoff)
    assert %{full: 1, soft: 0, backfilled: 1} = counts

    {_, _, counts} = BounceDecision.startup(retained, now_ms, nil)
    assert %{full: 2, soft: 0, backfilled: 2} = counts

    System.put_env(setting, "2026-11-17")
    assert BouncePolicy.cutoff() == {:error, :invalid_bounce_cutoff}
  end

  test "Hosted accounts rank independently using the configured connection", %{
    path: path,
    dir: dir
  } do
    use_synthetic_jev_key(dir)
    test_process = self()
    record_path = Path.join(dir, "hosted-scores.jsonl")

    relay =
      start_relay(path,
        deployment_mode: :saas,
        jev_score_record_path: record_path,
        jev_endpoint: "https://provider.example.test",
        jev_model: "test-model",
        priority_evaluator: fn request ->
          send(test_process, {:hosted_jev_request, request})
          {:ok, Map.new(request.candidates, &{&1, 0.75})}
        end
      )

    assert {:ok, alice} = Relay.hosted_account_create(relay, "alice-account")
    assert {:ok, bob} = Relay.hosted_account_create(relay, "bob-account")

    for {person, device, message, n} <-
          [{alice, "alice-device", "alice-message", 1}, {bob, "bob-device", "bob-message", 2}] do
      assert :ok = Relay.setup_device(relay, person, device)
      assert :ok = Relay.setup_sender(relay, device, "app:test")

      assert %{"type" => "ok"} =
               request(relay, device, %{
                 "type" => "forward",
                 "op_id" => op(n),
                 "message" => %{
                   "sender_id" => "app:test",
                   "message_id" => message,
                   "title" => message,
                   "created_at" => n,
                   "message_type" => "fyi",
                   "answer_ui" => %{
                     "schema" => "attention-client.actions/1",
                     "actions" => [%{"id" => "seen", "label" => "Seen", "kind" => "value",
                                      "value" => %{"acknowledgment" => "seen"}}]
                   }
                 }
               })
    end

    for {person, message} <- [{alice, "alice-message"}, {bob, "bob-message"}] do
      assert_receive {:hosted_jev_request,
                      %{
                        model: "test-model",
                        uri: %{host: "provider.example.test"},
                        candidates: [{"app:test", ^message}]
                      }},
                     2_000

      state = await_state(relay, &(&1.priority_views[person].basis == "selected"))
      assert [%{"message_id" => ^message, "score" => 0.75}] = state.priority_views[person].entries
      revision = state.store.engine.bounce_revisions[{person, "app:test", message}].revisions[1]
      assert revision.selection["level_name"] == "High"
      assert revision.selection["due_at_ms"] == revision.accepted_at_ms + 86_400_000
    end

    assert :sys.get_state(relay).applied_score_record == nil
    refute File.exists?(record_path)
  end

  test "Jev selected views remain score-only after fallback display priorities", %{
    path: path,
    dir: dir
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
    setup_person(relay)

    for {id, urgency, n} <- [{"A", "low", 1}, {"B", "normal", 2}, {"C", "now", 3}] do
      assert request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(n),
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "title" => id,
                 "created_at" => n,
                 "urgency" => urgency
               }
             })["type"] == "ok"
    end

    configure_jev_choice(relay)
    publish_fallback_view(relay)
    fallback = view_for(relay)
    assert fallback["basis"] == "fallback"

    assert Enum.map(fallback["entries"], & &1["priority"]) == ["now", "normal", "low"]
    refute Enum.any?(fallback["entries"], &Map.has_key?(&1, "score"))

    job = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(
      job,
      {:ok, %{{"app:test", "C"} => 0.9, {"app:test", "B"} => 0.5, {"app:test", "A"} => 0.1}}
    )

    selected = view_for(relay)
    assert selected["basis"] == "selected"

    assert Enum.map(selected["entries"], & &1["score"]) == [
             %JSON.DecimalToken{raw: "0.9"},
             %JSON.DecimalToken{raw: "0.5"},
             %JSON.DecimalToken{raw: "0.1"}
           ]

    refute Enum.any?(selected["entries"], &Map.has_key?(&1, "priority"))
  end

  test "a retained reply event with a missing parent does not crash view projection", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)
    # Model an orphaned historical event: reply projection must not turn it
    # into an entry whose display metadata requires a missing parent.
    :sys.replace_state(relay, fn state ->
      engine = state.store.engine

      event = %{
        key: {"person-a", "app:test", "missing-parent"},
        payload: %{"kind" => "reply", "author" => "origin", "text" => "Retained reply"}
      }

      engine = %{
        engine
        | conversation_events: Map.put(engine.conversation_events, {"person-a", 1}, event),
          conversation_highs: Map.put(engine.conversation_highs, "person-a", 1),
          algorithm_choices: Map.put(engine.algorithm_choices, "person-a", "jev-set")
      }

      %{state | store: %{state.store | engine: engine}}
    end)

    # The store refuses such an event, so it is held in memory only. A pass
    # that cannot rank projects the view again over it.
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:error, :unavailable})
    view = view_for(relay)
    assert view["basis"] == "fallback"
    assert view["entries"] == []
    assert view["replies"] == []
    assert Process.alive?(relay)
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

  test "Progress stays stored but is excluded before baseline and JEV ranking", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    parent = self()

    evaluator = fn request ->
      send(parent, {:progress_candidates, request.candidates})
      {:ok, Map.new(request.candidates, &{&1, 0.7})}
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    setup_person(relay)
    forward(relay, 1, "ordinary-first", 20)

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

    progress = %{
      "sender_id" => "app:test",
      "message_id" => "progress-urgent",
      "created_at" => 2,
      "title" => "Build the inbox update",
      "message_type" => "fyi",
      "answer_ui" => seen,
      "urgency" => "urgent",
      "by_when" => 1,
      "progress" => %{"percent" => 100, "stage" => "done", "segments" => []}
    }

    assert %{"type" => "ok"} =
             request(relay, "device-a", %{
               "type" => "forward",
               "op_id" => op(2),
               "message" => progress
             })

    configure_jev_choice(relay)
    forward(relay, 3, "ordinary-second", 10)

    engine = :sys.get_state(relay).store.engine
    all_items = Placement.items(engine, "person-a")
    [progress_item] = Enum.filter(all_items, &Message.progress_message?(&1.message))
    assert progress_item.identity == {"app:test", "progress-urgent"}

    ordinary_items = Enum.reject(all_items, &Message.progress_message?(&1.message))

    assert MapSet.new(Enum.map(Placement.ranking_items(engine, "person-a"), & &1.identity)) ==
             MapSet.new(Enum.map(ordinary_items, & &1.identity))

    assert Placement.baseline(engine, "person-a") ==
             Placement.place("baseline", ordinary_items, [], nil)

    assert_receive {:progress_candidates, candidates}, 2_000

    assert MapSet.new(candidates) ==
             MapSet.new([{"app:test", "ordinary-first"}, {"app:test", "ordinary-second"}])
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

    assert %{"algorithm" => "jev-set", "basis" => "fallback", "entries" => entries} =
             view_for(relay, person)

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

    assert %{"basis" => "selected", "entries" => selected_entries} = view_for(relay, person)

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

    assert %{"basis" => "fallback", "entries" => fallback_entries} = view_for(relay, person)

    assert Enum.map(fallback_entries, & &1["message_id"]) == ["earlier", "later"]

    version = :sys.get_state(relay).store.version
    view = view_for(relay, person)

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
    assert view_for(relay, person) == view

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

  test "a baseline view carries display priority and due time for each entry", %{path: path} do
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "older")
    forward(relay, 2, "newer", 200)

    assert %{
             "algorithm" => "baseline",
             "basis" => "selected",
             "entries" => entries
           } = view_for(relay)

    assert entries == [
             %{
               "sender_id" => "app:test",
               "message_id" => "newer",
               "priority" => "normal",
               "due_at" => 200
             },
             %{"sender_id" => "app:test", "message_id" => "older", "priority" => "normal"}
           ]

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
    publish_fallback_view(relay)

    assert %{"algorithm" => "jev-set", "basis" => "fallback", "entries" => []} = view_for(relay)

    forward(relay, 1, "while-unavailable")

    assert %{"algorithm" => "jev-set", "basis" => "fallback", "entries" => entries} =
             view_for(relay)

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
    configure_jev_choice(relay)
    publish_fallback_view(relay)

    assert %{
             "algorithm" => "jev-set",
             "basis" => "fallback",
             "entries" => baseline_entries
           } = view_for(relay)

    assert baseline_entries ==
             Enum.map(
               [
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
               ],
               &Map.put(&1, "priority", "normal")
             )

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
             "algorithm" => "jev-set",
             "basis" => "selected",
             "entries" => selected_entries
           } = view_for(relay)

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

  test "a score-only change publishes a new view with the new number", %{
    path: path,
    dir: dir
  } do
    use_synthetic_jev_key(dir)
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "same-item")
    configure_jev_choice(relay)

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "same-item"} => 0.71}})
    first = view_for(relay)
    assert first["basis"] == "selected"

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "same-item"} => 0.72}})

    second = view_for(relay)
    assert second["basis"] == "selected"
    refute second == first

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
      configure_jev_choice(relay)
      job = start_synthetic_priority_pass(relay)
      complete_synthetic_priority_pass(job, result)

      assert %{
               "algorithm" => "jev-set",
               "basis" => "fallback",
               "entries" => fallback_entries
             } = view_for(relay)

      assert fallback_entries ==
               Enum.map(
                 [
                   %{"sender_id" => "app:test", "message_id" => "newer"},
                   %{"sender_id" => "app:test", "message_id" => "older"}
                 ],
                 &Map.put(&1, "priority", "normal")
               )

      assert Enum.all?(fallback_entries, &(not Map.has_key?(&1, "reason")))
      assert Process.alive?(relay)
      state = :sys.get_state(relay)
      assert state.priority_jobs == %{}
      assert state.priority_queued == %{}

      forward(relay, 3, "arrived-after-pass")

      assert %{
               "algorithm" => "jev-set",
               "basis" => "fallback",
               "entries" => updated_entries
             } = view_for(relay)

      assert updated_entries ==
               Enum.map(
                 [
                   %{
                     "sender_id" => "app:test",
                     "message_id" => "arrived-after-pass"
                   },
                   %{"sender_id" => "app:test", "message_id" => "newer"},
                   %{"sender_id" => "app:test", "message_id" => "older"}
                 ],
                 &Map.put(&1, "priority", "normal")
               )

      assert Enum.all?(updated_entries, &(not Map.has_key?(&1, "reason")))

      assert {:ok, events, 3} = RelayReaders.events(relay, "device-a", 0)
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

    assert %{
             "algorithm" => "jev-set",
             "basis" => "fallback",
             "entries" => entries
           } = view_for(restarted)

    assert entries ==
             Enum.map(
               [
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
               ],
               &Map.put(&1, "priority", "normal")
             )

    assert Enum.all?(entries, &(not Map.has_key?(&1, "reason")))

    forward(restarted, 4, "intake-after-restart")
    assert Process.alive?(restarted)
    assert {:ok, events, 4} = RelayReaders.events(restarted, "device-a", 0)
    assert length(events) == 4
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

    # A read appends no record and asks for no scoring.
    assert {:ok, %{"items" => [_, _]}} = Reads.inbox(relay, "device-a")
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

    rescored = persisted_bounce.revisions[1].selection
    assert rescored["level_name"] == "Urgent"
    assert rescored["due_at_ms"] == record.accepted_at + 345_600_000

    GenServer.stop(relay)
    restarted = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)

    restarted_bounce =
      Map.fetch!(
        :sys.get_state(restarted).store.engine.bounce_revisions,
        {"person-a", "app:test", "fixed-bounce-selection"}
      )

    assert restarted_bounce.revisions[1].selection == rescored
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

  test "self-hosted ranking leaves the score record off when no path is set", %{dir: dir, path: path} do
    use_synthetic_jev_key(dir)
    relay = start_relay(path)
    setup_person(relay)
    forward(relay, 1, "entry")
    configure_jev_choice(relay)

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "entry"} => 0.7}})

    state = :sys.get_state(relay)
    assert state.applied_score_record == nil
    assert state.priority_views["person-a"].basis == "selected"
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
        assert {:ok, events, 2} = RelayReaders.events(relay, "device-a", 0)
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
      assert {:ok, events, 2} = RelayReaders.events(relay, "device-a", 0)
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

  test "a deadline-only priority pass persists the reordered inbox without a Store commit", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    now = 1_793_290_000_000
    boundary = now + 10_000
    {:ok, clock} = Agent.start_link(fn -> now end)
    on_exit(fn -> if Process.alive?(clock), do: Agent.stop(clock) end)

    evaluator = fn request ->
      {:ok, %{"state" => %{"pending_items" => pending}}} = JSON.decode_line(request.body <> "\n")

      near_deadline? =
        Enum.any?(pending, &(is_number(&1["due_in_minutes"]) and &1["due_in_minutes"] < 60))

      {:ok,
       Map.new(request.candidates, fn
         {"app:test", "deadline"} = id -> {id, if(near_deadline?, do: 0.3, else: 0.9)}
         {"app:test", "steady"} = id -> {id, 0.6}
       end)}
    end

    relay =
      start_relay(path,
        clock: fn -> Agent.get(clock, & &1) end,
        priority_evaluator: evaluator
      )

    setup_person(relay)
    forward(relay, 1, "deadline", boundary + 3_600_000)
    configure_jev_choice(relay)
    forward(relay, 2, "steady")

    before =
      await_state(relay, &match?(%{due: ^boundary}, &1.priority_deadline_timers["person-a"]))

    assert Enum.map(before.priority_views["person-a"].entries, & &1["score"]) == [0.9, 0.6]
    refute MapSet.member?(before.store.change_log_touched, "person-a")

    assert {:ok, steady} =
             Resources.item_id(before.store.engine, {"person-a", "app:test", "steady"})

    assert {:ok, deadline} =
             Resources.item_id(before.store.engine, {"person-a", "app:test", "deadline"})

    assert {:ok, before_keys} = ChangeLog.rank_keys(before.store, "person-a")

    assert {:ok, %{"items" => [%{"item_id" => ^deadline}, %{"item_id" => ^steady}]}} =
             Reads.inbox(relay, "device-a")

    assert before_keys[deadline] < before_keys[steady]

    Agent.update(clock, fn _ -> boundary + 1_000 end)
    send(relay, {:priority_deadline, "person-a", boundary})

    after_pass =
      await_state(relay, fn state ->
        Enum.map(state.priority_views["person-a"].entries, & &1["score"]) == [0.6, 0.3]
      end)

    # Only the priority view changed. No normal change-log touch can trigger the rank write.
    assert after_pass.store.version == before.store.version
    assert after_pass.store.engine == before.store.engine
    refute MapSet.member?(after_pass.store.change_log_touched, "person-a")

    assert {:ok, keys} = ChangeLog.rank_keys(after_pass.store, "person-a")
    assert keys[steady] < keys[deadline]

    assert {:ok, %{"items" => [%{"item_id" => ^steady}, %{"item_id" => ^deadline}]}} =
             Reads.inbox(relay, "device-a")
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
    assert {:ok, events, 2} = RelayReaders.events(relay, "device-a", 0)
    assert length(events) == 2

    assert_receive {:ranking_waiting, next_worker}, 2_000
    refute Process.alive?(first_worker)
    assert Process.alive?(next_worker)
    send(next_worker, :release)
    await_state(relay, &(not Map.has_key?(&1.priority_jobs, "person-a")))
  end

  test "a mid-pass arrival cancels the old pass and ignores its stale result", %{
    dir: dir,
    path: path
  } do
    use_synthetic_jev_key(dir)
    parent = self()
    {:ok, gate} = Agent.start_link(fn -> true end)
    {:ok, calls} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(gate), do: Agent.stop(gate) end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)

    score_for = fn request -> {:ok, Map.new(request.candidates, &{&1, 0.3})} end

    evaluator = fn request ->
      Agent.update(calls, &(&1 + 1))
      send(parent, {:provider_request, self(), request.candidates})

      if Agent.get(gate, & &1) do
        receive do
          :release -> :ok
        end
      end

      score_for.(request)
    end

    relay = start_relay(path, priority_evaluator: evaluator)
    setup_person(relay)
    body = String.duplicate("x", 650)

    for n <- 1..100, do: forward(relay, n, "seed-#{n}", nil, body)
    configure_jev_choice(relay)

    seed_state = :sys.get_state(relay)

    seed_result =
      JevRanking.run(
        Placement.items(seed_state.store.engine, "person-a"),
        %{},
        seed_state.store.engine.jev_endpoint,
        seed_state.store.engine.jev_model,
        System.system_time(:millisecond),
        score_for
      )

    assert length(seed_result.cache.batches) > 1

    partial_cache = %{
      seed_result.cache
      | batches: List.update_at(seed_result.cache.batches, 0, &%{&1 | scores: nil, request: nil})
    }

    :sys.replace_state(relay, fn state ->
      %{state | priority_caches: Map.put(state.priority_caches, "person-a", partial_cache)}
    end)

    calls_before_first = Agent.get(calls, & &1)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        forward(relay, 101, "initial-trigger", nil, body)
        initial_state = :sys.get_state(relay)
        engine = initial_state.store.engine
        initial_cache = Map.get(initial_state.priority_caches, "person-a", %{})

        initial_result =
          JevRanking.run(
            Placement.items(engine, "person-a"),
            initial_cache,
            engine.jev_endpoint,
            engine.jev_model,
            System.system_time(:millisecond),
            score_for
          )

        expected_batches = initial_result.sent

        assert expected_batches > 0
        assert expected_batches < length(initial_result.cache.batches)

        workers =
          for _ <- 1..expected_batches do
            assert_receive {:provider_request, pid, _candidates}, 2_000
            pid
          end

        first_job = :sys.get_state(relay).priority_jobs["person-a"]
        assert is_map(first_job)
        assert Agent.get(calls, & &1) - calls_before_first == expected_batches

        calls_before_next = Agent.get(calls, & &1)
        forward(relay, 102, "mid-pass", nil, body)

        canceled_state =
          await_state(relay, fn state ->
            Map.get(state.priority_generations, "person-a", 0) > first_job.generation and
              not Map.has_key?(state.priority_jobs, "person-a") and
              Enum.all?(workers, &(not Process.alive?(&1)))
          end)

        canceled_engine = canceled_state.store.engine
        canceled_cache = Map.get(canceled_state.priority_caches, "person-a", %{})

        next_result =
          JevRanking.run(
            Placement.items(canceled_engine, "person-a"),
            canceled_cache,
            canceled_engine.jev_endpoint,
            canceled_engine.jev_model,
            System.system_time(:millisecond),
            score_for
          )

        next_batches = next_result.sent
        assert next_batches > 0
        assert next_batches < length(next_result.cache.batches)

        next_requests =
          for _ <- 1..next_batches do
            assert_receive {:provider_request, pid, candidates}, 2_000
            {pid, candidates}
          end

        assert Enum.any?(next_requests, fn {_pid, candidates} ->
                 {"app:test", "mid-pass"} in candidates
               end)

        next_job = :sys.get_state(relay).priority_jobs["person-a"]
        assert next_job.sequence > first_job.sequence
        assert next_job.generation > first_job.generation

        before_stale_result = :sys.get_state(relay)

        send(
          relay,
          {:prioritization_result, "person-a", first_job.sequence, first_job.generation,
           initial_result}
        )

        # A synchronous request from this process is ordered after the injected
        # stale result, so the following state read observes its handling.
        assert {:ok, _events, _high_water} = RelayReaders.events(relay, "device-a", 0)
        after_stale_result = :sys.get_state(relay)

        assert after_stale_result.priority_jobs["person-a"] == next_job
        assert after_stale_result.priority_views == before_stale_result.priority_views
        assert after_stale_result.priority_caches == before_stale_result.priority_caches

        Agent.update(gate, fn _ -> false end)
        Enum.each(next_requests, fn {pid, _candidates} -> send(pid, :release) end)

        await_state(relay, fn state ->
          current_cache = state.priority_caches["person-a"]

          is_map(current_cache) and not Map.has_key?(state.priority_jobs, "person-a") and
            Enum.any?(current_cache.batches, fn batch ->
              {"app:test", "mid-pass"} in batch.members and
                Map.get(batch.scores, {"app:test", "mid-pass"}) == 0.3
            end)
        end)

        assert Agent.get(calls, & &1) - calls_before_first == expected_batches + next_batches
        assert Agent.get(calls, & &1) - calls_before_next == next_batches
        refute_receive {:provider_request, _pid, _candidates}, 0
      end)

    assert length(Regex.scan(~r/event=harklight_priority_pass /, logs)) == 1
    assert logs =~ "finals=0"
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
    view = fn -> view_for(relay) end

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
    assert view.() == initial

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
    assert baseline["replies"] == [Map.put(expected, "priority", "high")]
    assert baseline["entries"] == initial["entries"]
    refute baseline == initial
    assert Map.delete(request(relay, "device-a", reply_frame), "duplicate") == reply
    assert view.() == baseline

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
    assert view.()["replies"] == [Map.put(expected, "priority", "high")]
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
    publish_fallback_view(relay)
    before_score = view.()
    assert before_score["replies"] == [Map.put(expected, "priority", "high")]
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{item.identity => 0.1}})
    scored = view.()
    assert [scored_reply] = scored["replies"]
    assert scored_reply["score"] == %JSON.DecimalToken{raw: "0.1"}
    refute Map.has_key?(scored_reply, "priority")
    assert scored["entries"] == []
    refute scored == before_score
    writer = :sys.get_state(relay).applied_score_record
    :sys.get_state(writer)
    assert {:ok, row} = JSON.decode_line(File.read!(record_path))
    assert row["kind"] == "reply"
    assert row["conversation_seq"] == seq
    assert row["title"] == "Parent title"
    assert row["score"] == %JSON.DecimalToken{raw: "0.1"}
    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:error, :unavailable})
    assert view.()["replies"] == [Map.put(expected, "priority", "high")]
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
    refute after_read == scored
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

    assert view_for(restarted)["list_items"] == after_read["list_items"]
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
      assert view_for(relay)["list_items"] == []
      forward_chat(relay, "history", 900)
      first = add_reply(relay, "history", 902)
      second = add_reply(relay, "history", 904)
      baseline = view_for(relay)
      assert [group] = Enum.filter(baseline["list_items"], &(&1["kind"] == "reply"))
      assert group["unseen_count"] == 2
      assert group["conversation_seq"] == second
      assert group["open_at_conversation_seq"] == first
      refute Map.has_key?(group, "score")
      refute Map.has_key?(group, "score_kind")
      configure_jev_choice(relay)
      publish_fallback_view(relay)
      assert Enum.all?(view_for(relay)["replies"], &(not Map.has_key?(&1, "score")))
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

      scored = view_for(relay)
      assert [%{"kind" => "reply"} = group, %{"kind" => "message"}] = scored["list_items"]
      assert group["score"] == %JSON.DecimalToken{raw: "0.9"}
      assert group["score_kind"] == "current"
      assert group["conversation_seq"] == second
      assert group["open_at_conversation_seq"] == first
      assert Enum.map(scored["replies"], & &1["conversation_seq"]) == [first, second]
      assert [%{"message_id" => "history"}] = scored["entries"]

      parent = :sys.get_state(relay).store.engine.messages[{"person-a", "app:test", "history"}]

      if @partial_read do
        read_reply(relay, "history", first, 906)
        partial = view_for(relay)
        [row] = Enum.filter(partial["list_items"], &(&1["kind"] == "reply"))
        assert row["unseen_count"] == 1
        assert row["open_at_conversation_seq"] == second
        assert row["score"] == %JSON.DecimalToken{raw: "0.4"}
        refute partial == scored
      end

      read_reply(relay, "history", second, 907)
      seen = view_for(relay)
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
      restored = view_for(restarted)
      assert List.last(restored["list_items"]) == history
      assert restored["replies"] == []

      assert :sys.get_state(restarted).store.engine.chat_read_positions[
               {"person-a", "app:test", "history"}
             ] == second

      third = add_reply(restarted, "history", 908)
      active = view_for(restarted)
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
               view_for(restarted)["list_items"]

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
    assert view_for(relay)["list_items"] == []

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

    # A protocol 0.1 origin's support record, as a 0.1 Server stored it.
    PreChangeLogStore.in_relay(relay, &PreChangeLogStore.origin_support(&1, "device-a", support))
    session_key = {"person-a", "device-a", op(2701)}
    declared = :sys.get_state(relay).store.engine.origin_sessions[session_key].support
    assert %{"route" => "claude-code"} = declared

    {support_version, support_rows} = persisted_rows(path)

    job = start_synthetic_priority_pass(relay)

    complete_synthetic_priority_pass(
      job,
      {:ok, %{{"app:test", "reply-history", reply_seq} => 0.9}}
    )

    [active] =
      view_for(relay)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert active["score"] == %JSON.DecimalToken{raw: "0.9"}
    assert active["score_kind"] == "current"

    {reply_version, reply_rows} = persisted_rows(path)

    assert reply_version == support_version + 1
    assert reply_rows != support_rows

    read_reply(relay, "reply-history", reply_seq, 2712)

    [historical] =
      view_for(relay)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert historical["unseen_count"] == 0
    assert historical["score"] == %JSON.DecimalToken{raw: "0.9"}
    assert historical["score_kind"] == "historical"

    GenServer.stop(relay)
    restarted = start_relay(path, priority_evaluator: fn _ -> {:error, :unavailable} end)
    restored = view_for(restarted)
    assert List.last(restored["list_items"]) == historical

    # The reply-history commits kept the stored support record.
    assert :sys.get_state(restarted).store.engine.origin_sessions[session_key].support ==
             declared

    next_reply_seq = add_reply(restarted, "reply-history", 2714)

    [reactivated] =
      view_for(restarted)["list_items"]
      |> Enum.filter(&(&1["kind"] == "reply"))

    assert reactivated["conversation_seq"] == next_reply_seq
    assert reactivated["unseen_count"] == 1
    refute Map.has_key?(reactivated, "score")
    refute Map.has_key?(reactivated, "score_kind")

    assert :sys.get_state(restarted).store.engine.reply_group_history[
             {"person-a", "app:test", "reply-history"}
           ].score == 0.9
  end

  test "an enabled score publication clears only its own active reply scores after rebuild" do
    published = {"person-a", "app:test", "published"}
    historical = {"person-a", "app:test", "historical"}
    other = {"person-b", "app:test", "other"}

    engine =
      %{
        Engine.new()
        | reply_group_history: %{
            published => %{score: 0.9, scores: %{3 => 0.9}},
            historical => %{score: 0.6, scores: %{}},
            other => %{score: 0.7, scores: %{5 => 0.7}}
          }
      }
      |> RouteBIndex.enable_agent_feed()
      |> Engine.rebuild_route_b_index()

    assert RouteBIndex.scored_reply_items(engine.route_b_index, "person-a") ==
             MapSet.new([published])

    assert {next, [{"person-a", 3}]} = Engine.retain_reply_scores(engine, "person-a", [])
    assert next.reply_group_history[published] == %{score: 0.9, scores: %{}}
    assert next.reply_group_history[historical] == engine.reply_group_history[historical]
    assert next.reply_group_history[other] == engine.reply_group_history[other]
    assert RouteBIndex.scored_reply_items(next.route_b_index, "person-a") == MapSet.new()
    assert RouteBIndex.scored_reply_items(next.route_b_index, "person-b") == MapSet.new([other])
  end

  test "publishing one reply score does not traverse unrelated accounts' histories" do
    key = {"person-a", "app:test", "reply"}

    entries = [
      %{
        "sender_id" => "app:test",
        "message_id" => "reply",
        "conversation_seq" => 3,
        "score" => 0.7
      }
    ]

    [small, large] =
      for size <- [0, 10_000] do
        history =
          for n <- 1..size//1, into: %{key => %{score: 0.9, scores: %{3 => 0.9}}} do
            {{"person-b", "app:test", Integer.to_string(n)}, %{score: 0.5, scores: %{1 => 0.5}}}
          end

        engine =
          %{Engine.new() | reply_group_history: history}
          |> RouteBIndex.enable_agent_feed()

        Enum.min(
          for _ <- 1..5 do
            {:reductions, start} = Process.info(self(), :reductions)
            result = Engine.retain_reply_scores(engine, "person-a", entries)
            {:reductions, finish} = Process.info(self(), :reductions)
            assert {_next, [{"person-a", 3}]} = result
            finish - start
          end
        )
      end

    assert large < max(small, 256) * 4
  end

  test "never-scored history stays unscored and a person's turn does not reactivate it", %{
    path: path
  } do
    relay = start_relay(path)
    setup_person(relay)
    forward_chat(relay, "plain", 920)
    seq = add_reply(relay, "plain", 922)
    read_reply(relay, "plain", seq, 924)
    before = view_for(relay)

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

    assert view_for(relay) == before
    GenServer.stop(relay)
    restarted = start_relay(path)
    assert view_for(restarted)["list_items"] == before["list_items"]
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
    publish_fallback_view(relay)
    before = view_for(relay)
    state = :sys.get_state(relay)

    assert :ok =
             Sqlite3.execute(
               state.store.db,
               "CREATE TRIGGER reject_history BEFORE UPDATE ON relay_record_generation BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END"
             )

    job = start_synthetic_priority_pass(relay)
    complete_synthetic_priority_pass(job, {:ok, %{{"app:test", "failure", seq} => 0.9}})
    assert view_for(relay)["list_items"] == before["list_items"]
    assert :sys.get_state(relay).store.engine.reply_group_history == %{}
    :sys.get_state(state.applied_score_record)
    assert not File.exists?(record_path) or File.read!(record_path) == ""
    assert :ok = Sqlite3.execute(state.store.db, "DROP TRIGGER reject_history")
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
               "CREATE TRIGGER reject_arrival BEFORE UPDATE ON relay_record_generation BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END"
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
    view = view_for(relay)
    assert view["algorithm"] == "baseline"

    assert Enum.map(view["list_items"], &{&1["kind"], &1["message_id"]}) ==
             [{"message", "newer"}, {"reply", "older"}, {"message", "older"}]

    engine = :sys.get_state(relay).store.engine
    assert engine.free_order_highs == %{"person-a" => 3}
    assert engine.messages[{"person-a", "app:test", "older"}].free_order == 1
    assert engine.conversation_events[{"person-a", seq}].free_order == 2
    forward_chat(relay, "older", 3000)
    assert view_for(relay) == view
    read_reply(relay, "older", seq, 3006)
    seen = view_for(relay)

    assert List.last(seen["list_items"])["free_section"] ==
             %{"state" => "history", "group" => "common"}

    refute seen == view
    assert :sys.get_state(relay).store.engine.free_order_highs == engine.free_order_highs
    GenServer.stop(relay)
    restarted = start_relay(path)
    assert view_for(restarted)["list_items"] == seen["list_items"]
    assert :sys.get_state(restarted).store.engine.free_order_highs == engine.free_order_highs
    add_reply(restarted, "older", 3008)
    assert hd(view_for(restarted)["list_items"])["kind"] == "reply"
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

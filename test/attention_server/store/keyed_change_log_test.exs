defmodule AttentionServer.Store.KeyedChangeLogTest do
  use ExUnit.Case, async: true

  # A keyed commit derives its client and agent change-log rows from the
  # touched addresses (ChangeLog.commit_log_touched/4). This sweep drives the
  # change-log-relevant transitions through a keyed store with the log on and
  # asserts, for every committed step, that each part equals the whole-map log
  # (ChangeLog.commit_log/3). Under keyed_validation: :both the commit itself
  # also raises on a mismatch. The final coverage check makes sure every
  # dependency expansion actually produced rows, so dropping one fails here.

  alias AttentionServer.{BounceDecision, ChangeLog, Engine, JSON, Store}

  @moduletag :tmp_dir
  @person "00000000-0000-4000-8000-0000000000aa"
  @owner String.duplicate("c", 64)
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
  @agent String.duplicate("e", 64)
  @agent_id "00000000-0000-4000-8000-0000000000ee"
  @sender "app:test/worker"
  @other_sender "app:test/other"
  @sessions ["00000000-0000-4000-8000-000000000101", "00000000-0000-4000-8000-000000000102"]
  @ids ~w(m1 m2 m3 m4 m5 m6)

  @required [
    {:op, :forward},
    {:op, :revise},
    {:op, :discussion},
    {:op, :reply},
    {:op, :answer},
    {:op, :lifecycle},
    {:op, :read},
    {:op, :open},
    {:op, :subscribe},
    {:op, :levels},
    {:op, :push_register},
    {:op, :push_remove},
    {:op, :focus},
    {:op, :session_end},
    {:op, :reply_scores},
    {:op, :bounce_due},
    {:op, :advisory_ack},
    {:op, :agent_ack},
    {:op, :file_use},
    :item,
    :entry,
    :counts,
    :settings,
    :subscription_items,
    :read_position_items,
    :summary_items,
    :session_items,
    :priority_entries,
    :projected_entries,
    :agent_message,
    :agent_entry,
    :advisory_created,
    :advisory_acknowledged,
    :seeded,
    :unseeded,
    {:closing, :device},
    {:closing, :agent},
    {:closing, :person},
    {:closing, :owner}
  ]

  test "seeded sweeps: every keyed commit's touched log equals the whole-map log", %{
    tmp_dir: directory
  } do
    seen =
      Enum.reduce(1..4, MapSet.new(), fn seed, seen ->
        state =
          Enum.reduce(1..160, start(Path.join(directory, "#{seed}")), &step(&2, &1, seed))

        :ok = Store.close(state.store)
        MapSet.union(seen, state.seen)
      end)

    # Every dependency expansion produced rows somewhere in the sweep.
    assert Enum.reject(@required, &MapSet.member?(seen, &1)) == []
  end

  test "an over-reported touch that changes nothing writes no row", %{tmp_dir: directory} do
    state = start(directory)
    state = forward(state, 1, "m1", hd(@sessions), false)
    engine = state.store.engine
    key = {@person, @sender, "m1"}

    touched =
      MapSet.new([
        {:row, :messages, key},
        {:row, :subscriptions, {@person, @reader}},
        {:row, :push_bindings, {@person, @reader}},
        {:row, :origin_sessions, {@person, @origin, hd(@sessions)}}
      ])

    assert %{pending: pending, agent_pending: agent} =
             ChangeLog.commit_log_touched(engine, engine, touched, [])

    assert pending == %{}
    assert agent == %{}
    :ok = Store.close(state.store)
  end

  # Every engine read also appends a read_position event, whose summary
  # change names the item too; this pins the read-position rule on its own.
  test "a read position change alone names its item", %{tmp_dir: directory} do
    state = start(directory)
    state = forward(state, 1, "m1", hd(@sessions), false)
    old = state.store.engine
    key = {@person, @sender, "m1"}
    next = %{old | chat_read_positions: Map.put(old.chat_read_positions, key, 1)}
    touched = MapSet.new([{:row, :chat_read_positions, key}])
    changed = ChangeLog.changed_resources_touched(old, next, touched, [])
    pending = ChangeLog.changes_touched(old, next, changed)
    assert pending == ChangeLog.changes(old, next, ChangeLog.changed_resources(old, next))
    assert pending == %{@person => [{:item, ChangeLog.item_resource(key)}]}
    :ok = Store.close(state.store)
  end

  test "a person's seed follows the delivery and conversation highs", %{tmp_dir: directory} do
    state = start(directory)
    refute ChangeLog.seeded_from_highs?(state.store.engine, @person)
    refute ChangeLog.seeded?(state.store.engine, @person)
    state = forward(state, 1, "m1", nil, false)
    assert ChangeLog.seeded_from_highs?(state.store.engine, @person)
    assert ChangeLog.seeded?(state.store.engine, @person)
    :ok = Store.close(state.store)
  end

  # --- fixture ---------------------------------------------------------------

  defp start(directory) do
    File.mkdir_p!(directory)
    path = Path.join(directory, "keyed.sqlite3")
    {:ok, store} = Store.open(path)
    engine = store.engine
    {:ok, engine, _} = Engine.hosted_account_create(engine, "acct-1", true, @person, @owner)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @other_sender)
    {:ok, engine} = Engine.setup_agent_fixture(engine, @person, @agent_id, @agent, [@sender])
    {:ok, store} = Store.commit(store, engine, audit(:setup))
    :ok = Store.close(store)
    {:ok, staged} = Store.open(path, :keyed_cutover)
    :ok = Store.close(staged)
    {:ok, keyed} = Store.open(path, :keyed)
    {:ok, keyed} = Store.enable_change_log(keyed)

    %{
      store: keyed,
      owner: @owner,
      spare_devices: [],
      spare_agents: [],
      revision: 1,
      seen: MapSet.new()
    }
  end

  defp audit(action), do: %{person: @person, actor: "fixture", action: inspect(action)}

  defp op_id(n), do: "00000000-0000-4000-8000-" <> String.pad_leading("#{n}", 12, "0")

  defp at(n), do: 1_000_000 + n * 50_000_000

  defp hex(n), do: :crypto.hash(:sha256, "key-#{inspect(n)}") |> Base.encode16(case: :lower)

  # --- one random step ---------------------------------------------------------

  defp step(state, n, seed) do
    if n == 1, do: :rand.seed(:exsss, {seed, 404, 1188})
    engine = state.store.engine

    case Enum.random(ops()) do
      :forward ->
        forward(
          state,
          n,
          Enum.random(@ids),
          Enum.random([nil | @sessions]),
          :rand.uniform(2) == 1
        )

      :revise ->
        with_message(state, fn key, record ->
          frame =
            common(key, %{
              "type" => "conversation_revise",
              "base_revision" => record.revision,
              "message" =>
                %{
                  "sender_id" => elem(key, 1),
                  "message_id" => elem(key, 2),
                  "created_at" => 1,
                  "title" => "r#{n}"
                }
                |> Map.merge(Enum.random([%{}, %{"urgency" => Enum.random(~w(low high now))}]))
            })

          wire(state, n, :revise, @origin, frame)
        end)

      :discussion ->
        with_message(state, fn key, record ->
          files =
            if :rand.uniform(2) == 1,
              do: %{
                "files" => [%{"name" => "f#{n}.txt", "data_base64" => Base.encode64("x#{n}")}]
              },
              else: %{}

          frame =
            common(key, %{
              "type" => "discussion",
              "request_revision" => record.revision,
              "text" => "p#{n}"
            })
            |> Map.merge(files)

          wire(state, n, :discussion, @reader, frame)
        end)

      :reply ->
        turns =
          for {seq, %{"kind" => "discussion", "author" => "person"} = entry} <-
                get_in(engine.origin_feeds, [{@person, @origin}, :entries]) || %{},
              do: {seq, entry}

        case turns do
          [] ->
            state

          _ ->
            {seq, entry} = Enum.random(turns)

            frame = %{
              "v" => 1,
              "type" => "conversation_reply",
              "sender_id" => entry["sender_id"],
              "message_id" => entry["message_id"],
              "request_revision" => entry["request_revision"],
              "in_reply_to" => seq,
              "text" => "o#{n}"
            }

            wire(state, n, :reply, @origin, frame)
        end

      :answer ->
        with_message(state, fn key, _record ->
          answer = Enum.random([nil, "yes"])
          wire(state, n, :answer, @reader, common(key, %{"type" => "answer", "answer" => answer}))
        end)

      :lifecycle ->
        with_message(state, fn key, _record ->
          frame =
            common(key, %{
              "type" => "lifecycle",
              "state" => Enum.random(~w(expired cleared)),
              "at" => at(n)
            })

          wire(state, n, :lifecycle, @origin, frame)
        end)

      :read ->
        replies =
          for {{@person, seq}, %{key: key, payload: %{"kind" => "reply", "author" => "origin"}}} <-
                engine.conversation_events,
              do: {key, seq}

        case replies do
          [] ->
            state

          _ ->
            {key, seq} = Enum.random(replies)

            frame =
              common(key, %{"type" => "conversation_read", "read_through_conversation_seq" => seq})

            wire(state, n, :read, @reader, frame)
        end

      :open ->
        with_message(state, fn key, record ->
          frame =
            common(key, %{"type" => "message_open", "request_revision" => record.revision})

          wire(state, n, :open, @reader, frame)
        end)

      :subscribe ->
        senders = Enum.take_random([@sender, @other_sender], :rand.uniform(2))

        frame = %{
          "v" => 1,
          "type" => "subscribe_remote",
          "revision" => state.revision,
          "sender_ids" => senders
        }

        %{wire(state, n, :subscribe, @reader, frame) | revision: state.revision + 1}

      :levels ->
        levels =
          Enum.random([
            [%{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}],
            [
              %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
              %{"name" => "Urgent", "lower_bound" => 0.5, "bounce_after_days" => 1}
            ]
          ])

        frame = %{"v" => 1, "type" => "levels_set", "ref" => "levels", "levels" => levels}
        wire(state, n, :levels, @reader, frame)

      :push_register ->
        frame = %{
          "v" => 1,
          "type" => "push_register",
          "revision" => state.revision,
          "profile_id" => "harklight-sandbox",
          "address_kind" => "apns_token",
          "address" => hex(n)
        }

        %{wire(state, n, :push_register, @reader, frame) | revision: state.revision + 1}

      :push_remove ->
        frame = %{"v" => 1, "type" => "push_remove", "revision" => state.revision}
        %{wire(state, n, :push_remove, @reader, frame) | revision: state.revision + 1}

      :focus ->
        apply_op(state, :focus, fn e ->
          Engine.owner_set_focus_permissions(
            e,
            @person,
            state.owner,
            @reader,
            :rand.uniform(2) == 1,
            :rand.uniform(2) == 1
          )
        end)

      :session_end ->
        handle = Enum.random(@sessions)

        apply_op(state, :session_end, fn e ->
          with {:ok, e} <- Engine.origin_session_gone(e, @origin, handle) do
            {:ok, Engine.origin_availability(e, @person, @origin, handle, "gone", at(n))}
          end
        end)

      :reply_scores ->
        entries =
          for {{@person, sender, id} = key, _} <- engine.messages,
              seq <- AttentionServer.RouteBIndex.origin_reply_positions(engine.route_b_index, key),
              :rand.uniform(2) == 1,
              do: %{
                "sender_id" => sender,
                "message_id" => id,
                "conversation_seq" => seq,
                "score" => Enum.random([0.25, 0.5, 0.75])
              }

        apply_op(state, :reply_scores, fn e ->
          {next, projected} = Engine.retain_reply_scores(e, @person, entries)
          {:ok, next, projected}
        end)

      :bounce_due ->
        with_message(state, fn {_, sender, id}, _record ->
          apply_op(state, :bounce_due, fn e ->
            scored =
              BounceDecision.record_scores(
                e,
                @person,
                [%{"sender_id" => sender, "message_id" => id, "score" => 0.9}],
                at(n)
              )

            {:ok, BounceDecision.evaluate_due(scored, at(n) + 3 * 86_400_000)}
          end)
        end)

      :advisory_ack ->
        acks =
          for {{@person, @origin, handle}, feed} <- engine.advisory_feeds,
              {_, %{"advisory_id" => id}} <- feed.entries,
              do: {handle, id}

        case acks do
          [] ->
            state

          _ ->
            {handle, id} = Enum.random(acks)

            apply_op(state, :advisory_ack, fn e ->
              case BounceDecision.advisory_ack(e, @origin, handle, id) do
                {:ok, next, _reply} -> {:ok, next}
                error -> error
              end
            end)
        end

      :agent_ack ->
        targets =
          for {{@person, seq}, %{payload: %{"kind" => "discussion", "author" => "person"}}} <-
                engine.conversation_events,
              do: {:entry, seq}

        case targets do
          [] ->
            state

          _ ->
            apply_op(state, :agent_ack, fn e ->
              Engine.agent_feed_delivered(e, @person, @origin, Enum.take_random(targets, 2))
            end)
        end

      :file_use ->
        files =
          for {seq, %{"files" => files}} <-
                get_in(engine.origin_feeds, [{@person, @origin}, :entries]) || %{},
              %{"name" => name} <- files,
              do: {seq, name}

        case files do
          [] ->
            state

          _ ->
            {seq, name} = Enum.random(files)
            report = Enum.random([%{"use" => %{"status" => "used"}}])

            apply_op(state, :file_use, fn e ->
              case Engine.origin_file_report(e, @origin, seq, name, report, op_id(n)) do
                {:ok, next, _reply} -> {:ok, next}
                error -> error
              end
            end)
        end

      :add_device ->
        key = hex({:device, n})
        state = apply_op(state, :add_device, &Engine.setup_device(&1, @person, key))

        if Map.has_key?(state.store.engine.devices, key),
          do: %{state | spare_devices: [key | state.spare_devices]},
          else: state

      :revoke_device ->
        case state.spare_devices do
          [] ->
            state

          [key | rest] ->
            state = %{state | spare_devices: rest}

            apply_op(state, :revoke_device, fn e ->
              if :rand.uniform(2) == 1,
                do: Engine.owner_revoke_device(e, @person, state.owner, key),
                else: with({:ok, next, _reply} <- Engine.device_remove(e, key), do: {:ok, next})
            end)
        end

      :add_agent ->
        key = hex({:agent, n})
        id = AttentionServer.ConversationFiles.new_upload_id()

        state =
          apply_op(
            state,
            :add_agent,
            &Engine.setup_agent_fixture(&1, @person, id, key, [@sender])
          )

        if Map.has_key?(state.store.engine.agents, key),
          do: %{state | spare_agents: [key | state.spare_agents]},
          else: state

      :revoke_agent ->
        case state.spare_agents do
          [] ->
            state

          [key | rest] ->
            state = %{state | spare_agents: rest}
            apply_op(state, :revoke_agent, &Engine.revoke_agent_fixture(&1, key))
        end

      :delivery ->
        target = Enum.random(["STOPPED", "RESTORED"])

        apply_op(state, :delivery, fn e ->
          case Engine.set_hosted_delivery_state(e, @person, target) do
            {:ok, next, _} -> {:ok, next}
            error -> error
          end
        end)

      :recover ->
        key = hex({:owner, n})
        state = apply_op(state, :recover, &Engine.operator_recover(&1, @person, key))

        if state.store.engine.owners[@person].key == key,
          do: %{state | owner: key},
          else: state
    end
  end

  defp ops do
    List.duplicate(:forward, 6) ++
      List.duplicate(:discussion, 4) ++
      List.duplicate(:reply, 4) ++
      List.duplicate(:reply_scores, 3) ++
      List.duplicate(:agent_ack, 3) ++
      List.duplicate(:file_use, 3) ++
      List.duplicate(:revise, 2) ++
      List.duplicate(:read, 2) ++
      List.duplicate(:bounce_due, 2) ++
      List.duplicate(:advisory_ack, 2) ++
      ~w(answer lifecycle open subscribe levels push_register push_remove focus session_end
         add_device revoke_device add_agent revoke_agent delivery recover)a
  end

  defp common({_person, sender, id}, fields),
    do: Map.merge(%{"v" => 1, "sender_id" => sender, "message_id" => id}, fields)

  defp with_message(state, fun) do
    case Enum.sort(Map.keys(state.store.engine.messages)) do
      [] ->
        state

      keys ->
        key = Enum.random(keys)
        fun.(key, Map.fetch!(state.store.engine.messages, key))
    end
  end

  defp forward(state, n, id, session, support?) do
    frame = %{
      "v" => 1,
      "type" => "forward",
      "message" => %{
        "sender_id" => Enum.random([@sender, @sender, @other_sender]),
        "message_id" => id,
        "created_at" => 1,
        "title" => id,
        "body" => "b#{n}",
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
    }

    frame = if session, do: Map.put(frame, "session_handle", session), else: frame

    frame =
      if session && support?,
        do:
          Map.merge(frame, %{
            "route" => "claude-code",
            "cli_version" => "fixture",
            "unsupported_reason" => nil,
            "advisory_v1" => true
          }),
        else: frame

    wire(state, n, :forward, @origin, frame)
  end

  defp wire(state, n, label, actor, frame) do
    raw = frame |> Map.put("op_id", op_id(n)) |> JSON.encode_line()

    {next, reply, touched} =
      Engine.request_line_with_touches(state.store.engine, actor, raw, at(n))

    case reply do
      {:ok, line} ->
        case JSON.decode_line(line) do
          {:ok, %{"type" => "ok"}} -> commit(state, label, next, touched, [])
          _ -> state
        end

      _ ->
        state
    end
  end

  defp apply_op(state, label, fun) do
    {next, result, touched} =
      Engine.with_touches(state.store.engine, fn engine ->
        case fun.(engine) do
          {:ok, %Engine{} = next} -> {next, {:ok, []}}
          {:ok, %Engine{} = next, projected} -> {next, {:ok, projected}}
          _ -> {engine, :skip}
        end
      end)

    case result do
      {:ok, projected} -> commit(state, label, next, touched, projected)
      :skip -> state
    end
  end

  defp commit(state, label, next, touched, projected) do
    old = state.store.engine

    case Store.commit(state.store, {next, touched}, audit(label), projected) do
      {:ok, store} ->
        if store.version > state.store.version do
          whole = ChangeLog.commit_log(old, next, projected)
          derived = ChangeLog.commit_log_touched(old, next, touched, projected)

          assert ChangeLog.commit_log_mismatches(derived, whole) == [],
                 "#{label}: #{inspect(Enum.sort(touched), limit: 40)}"

          seen =
            state.seen
            |> MapSet.put({:op, label})
            |> MapSet.union(observe(old, next, touched, projected, whole))

          %{state | store: store, seen: seen}
        else
          %{state | store: store}
        end

      {:error, :storage_unavailable, store} ->
        %{state | store: store}
    end
  end

  # What this commit's whole-map log shows, by the rule that produced it.
  defp observe(old, next, touched, projected, log) do
    rows = Enum.flat_map(log.pending, fn {_, rows} -> rows end)
    kinds = MapSet.new(rows, &elem(&1, 0))
    agent_rows = Enum.flat_map(log.agent_pending, fn {_, rows} -> rows end)
    items = for {kind, id} <- rows, kind in [:item, :item_removed], into: MapSet.new(), do: id
    entries = for {:entry, id} <- rows, into: MapSet.new(), do: id
    grouped = ChangeLog.group_touched(touched)
    touched_items = for {:row, :messages, key} <- touched, do: ChangeLog.item_resource(key)

    touched_entries =
      for {:row, :conversation_events, {_, seq}} <- touched, do: ChangeLog.entry_resource(seq)

    flags = [
      {:item, MapSet.member?(kinds, :item)},
      {:entry, MapSet.member?(kinds, :entry)},
      {:counts, MapSet.member?(kinds, :counts)},
      {:settings, MapSet.member?(kinds, :settings)},
      {:subscription_items,
       Map.has_key?(grouped, {:row, :subscriptions}) and
         not MapSet.subset?(items, MapSet.new(touched_items))},
      {:read_position_items,
       for({:row, :chat_read_positions, key} <- touched, do: key)
       |> Enum.any?(&(Map.get(old.chat_read_positions, &1) != next.chat_read_positions[&1]))},
      {:summary_items,
       Enum.any?(items, &(&1 not in touched_items)) and
         Map.has_key?(grouped, {:row, :conversation_events})},
      {:session_items,
       Map.has_key?(grouped, {:row, :origin_sessions}) and
         Enum.any?(agent_rows, &match?({:message, _, _}, &1))},
      {:priority_entries,
       projected == [] and not MapSet.subset?(entries, MapSet.new(touched_entries))},
      {:projected_entries, projected != [] and MapSet.size(entries) > 0},
      {:agent_message, Enum.any?(agent_rows, &match?({:message, _, _}, &1))},
      {:agent_entry, Enum.any?(agent_rows, &match?({:entry, _, _}, &1))},
      {:advisory_created,
       Enum.any?(touched, &match?({:advisory, _, {:entry, _}}, &1)) and
         Enum.any?(agent_rows, &match?({:advisory, _, _}, &1))},
      {:advisory_acknowledged,
       Enum.any?(touched, &match?({:advisory, _, {:receipt, _}}, &1)) and
         Enum.any?(agent_rows, &match?({:advisory, _, _}, &1))},
      {:seeded, Enum.any?(log.seeded, &elem(&1, 1))},
      {:unseeded, Enum.any?(log.seeded, &(not elem(&1, 1)))}
    ]

    closings = for {{kind, _}, _} <- log.closings, do: {:closing, kind}
    MapSet.new(for({flag, true} <- flags, do: flag) ++ closings)
  end
end

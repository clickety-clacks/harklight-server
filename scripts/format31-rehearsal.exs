# Run in a fresh BEAM with ATTENTION_SERVER_ENABLE=0. All inputs are synthetic.
defmodule Format31Rehearsal do
  alias AttentionServer.{BounceDecision, BouncePolicy, Engine, JSON, Relay, Store}
  alias AttentionServer.ClientAPI.{AgentWrites, Idempotency, Reads}
  alias Exqlite.Sqlite3

  @person "format31-rehearsal"
  @origin "fixture-origin"
  @reader "fixture-reader"
  @sender "app:rehearsal"
  @relay :format31_rehearsal
  def run(["seed", dir, messages, discussions, sessions, fyis, active]) do
    [messages, discussions, sessions, fyis, active] =
      Enum.map([messages, discussions, sessions, fyis, active], &String.to_integer/1)

    true = messages >= sessions and messages > active and active >= fyis and fyis >= 0
    :ok = File.mkdir(dir)
    :ok = File.write!(Path.join(dir, "synthetic-format31"), "")
    {:ok, store} = Store.open(path(dir))
    {:ok, store} = Store.enable_change_log(store)
    {:ok, engine} = Engine.setup_person(store.engine, @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)
    reference = System.fetch_env!("HARKLIGHT_FIXTURE_EPOCH_SECONDS") |> String.to_integer()
    now = reference * 1_000 - 7 * 86_400_000

    {:ok, engine} = Engine.set_jev_connection(engine, "https://127.0.0.1:1/v1/systemone", "synthetic")
    {:ok, engine} = Engine.set_algorithm_choice(engine, @person, "jev-set")

    {:ok, levels} = BouncePolicy.normalize_levels([
      %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil},
      %{"name" => "Urgent", "lower_bound" => 0.5, "bounce_after_days" => 1}
    ])
    {:ok, engine, _} = BounceDecision.configure_levels(engine, @person, levels, now)

    engine =
      Enum.reduce(1..messages, engine, fn i, engine ->
        frame = Map.put(forward(i, sessions), :agent_api_v2, true)
        frame =
          if i > messages - fyis do
            ui = %{"schema" => "attention-client.actions/1", "actions" => [
              %{"id" => "seen", "label" => "Seen", "kind" => "value", "value" => %{"acknowledgment" => "seen"}}
            ]}
            update_in(frame["message"], &Map.merge(&1, %{"message_type" => "fyi", "answer_ui" => ui}))
          else
            frame
          end
        engine = transition(engine, @origin, frame, now)
        record = engine.messages[{@person, @sender, "item-#{i}"}]

        engine =
          if i > messages - 300 do
            Engine.agent_feed_delivery_availability(engine, @person, record.accepted_seq, true)
          else
            engine
          end

        engine =
          if discussions > 0 and i <= 900 and rem(i, 3) == 0 do
            revised = %{
              frame["message"] |
              "body" => "Revised delivery scope for item #{i}. Include the follow-up steps and current project status.",
              "title" => "Revised synthetic request #{i}"
            }
            transition(engine, @origin, %{
              "type" => "conversation_revise", "op_id" => uuid(4_000_000 + i),
              "sender_id" => @sender, "message_id" => "item-#{i}", "base_revision" => 1, "message" => revised
            }, now)
          else
            engine
          end

        if i <= messages - active do
          transition(engine, @origin, %{
            "type" => "lifecycle", "op_id" => uuid(3_000_000 + i), "sender_id" => @sender,
            "message_id" => "item-#{i}", "state" => "cleared", "at" => now
          }, now)
        else
          engine
        end
      end)

    exchanges = div(max(0, discussions - map_size(engine.conversation_events)), 3)
    engine =
      Enum.reduce(1..exchanges//1, engine, fn i, engine ->
        target =
          if discussions > 500_000 and i > div(500_000, 3),
            do: messages - active + rem(i - 1, active) + 1,
            else: rem(i - 1, messages - fyis) + 1

        id = "item-#{target}"
        revision = engine.messages[{@person, @sender, id}].revision
        stamp = now + i
        {next, answer} = request_frame(engine, @reader, %{
          "type" => "discussion", "op_id" => uuid(1_000_000 + i), "sender_id" => @sender,
          "message_id" => id, "request_revision" => revision,
          "text" => "Can you clarify the expected result for step #{rem(i, 70) + 1}? " <>
            "Ticket #{i} has a follow-up and I'd like to verify it before moving on."
        }, stamp)

        entry = String.to_integer(answer["event_id"])
        transition(next, @origin, %{
          "type" => "conversation_reply", "op_id" => uuid(2_000_000 + i), "sender_id" => @sender,
          "message_id" => id, "request_revision" => revision, "in_reply_to" => entry,
          "text" => "Yes. I checked ticket #{i}. Use the updated result from the latest run, " <>
            "keep the existing notes, and flag any mismatch on the next pass."
        }, stamp)
      end)

    scores =
      for i <- (messages - 24)..messages do
        %{"sender_id" => @sender, "message_id" => "item-#{i}", "score" => 0.9}
      end

    engine = mark_long_chats_read(engine, messages, active, discussions, now + exchanges + 1)
    engine = BounceDecision.record_scores(engine, @person, scores, now)
    deadline = BounceDecision.next_deadline(engine)
    {:ok, store} =
      Store.commit(store, engine, %{person: @person, actor: @origin, action: "startup_benchmark_seed"})

    :ok = Store.close(store)
    report(:startup_fixture, %{
      messages: map_size(engine.messages),
      conversations: map_size(engine.conversation_events),
      sessions: map_size(engine.origin_sessions),
      fyis: Enum.count(engine.messages, fn {_, record} -> record.message["message_type"] == "fyi" end),
      pending_messages: Enum.count(engine.messages, fn {_, record} -> is_nil(record.terminal) end),
      revised_messages: Enum.count(engine.messages, fn {_, record} -> record.revision > 1 end),
      chat_read_positions: map_size(engine.chat_read_positions),
      bounce_due: deadline
    })
  end


  def run(["convert", dir]) do
    synthetic!(dir)
    before = File.stat!(path(dir)).size
    parent = self()
    sampler = spawn_link(fn -> sample_wal(path(dir) <> "-wal", parent, 0) end)
    {micros, {:ok, store}} = :timer.tc(fn -> Store.open(path(dir), :keyed_cutover) end)
    send(sampler, :finish)
    peak = receive do {:wal_peak, bytes} -> bytes after 5_000 -> raise "missing WAL sample" end
    report(:conversion, %{elapsed_us: micros, file_before_bytes: before, wal_peak_bytes: peak, memory: memory()})

    for table <- ~w(relay_records relay_items relay_entries relay_feed relay_advisories relay_sessions
                    relay_authority relay_upload_refs relay_large_data_refs client_resource_revs agent_changes) do
      report(:rows, %{table: table, count: query(store.db, "SELECT count(*) FROM " <> table)})
    end

    :ok = Store.close(store)
    report(:file_after_bytes, File.stat!(path(dir)).size)
  end

  def run(["inspect", dir]) do
    synthetic!(dir)
    {:ok, db} = Sqlite3.open(path(dir), mode: :readonly)
    [[length, prefix]] = query(db, "SELECT length(payload), substr(payload, 1, 256) FROM relay_state WHERE id = 1")
    alias AttentionServer.Store.ConversionPreflight
    {:ok, sizes} = ConversionPreflight.snapshot_sizes(length, prefix)
    {:ok, format} = ConversionPreflight.snapshot_format(prefix)
    report(:snapshot, Map.put(sizes, :format, format))
    :ok = Sqlite3.close(db)
  end

  def run(["measure", dir]) do
    synthetic!(dir)
    key_path = Path.join(dir, "synthetic-jev")
    File.write!(key_path, "synthetic-rehearsal-offline")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", key_path)
    evaluator = fn request -> {:ok, Map.new(request.candidates, &{&1, 0.9})} end

    {micros, {:ok, relay}} =
      :timer.tc(fn -> Relay.start_link(store_path: path(dir), name: @relay, priority_evaluator: evaluator) end)

    report(:startup_return, %{elapsed_us: micros, memory: memory()})
    Process.sleep(30_000)
    report(:startup_30s, %{memory: memory()})
    started_at = System.monotonic_time(:millisecond)
    await_quiet_startup(relay, started_at + 3_900_000, started_at)
    Process.sleep(2_000)
    report(:startup_rest, %{memory: memory()})

    {inbox_us, {:ok, _}} = :timer.tc(fn -> Reads.inbox(relay, @reader, %{"limit" => "50"}) end)
    {replied_us, :ok} = :timer.tc(fn -> first_reply(relay, %{"limit" => "50"}) end)
    report(:view_latency_us, %{inbox: inbox_us, first_replied_page: replied_us})
    measure_sends(relay)
    :ok = GenServer.stop(relay, :normal, 60_000)
  end

  defp await_quiet_startup(relay, deadline, next_sample) do
    observer = self()

    try do
      :sys.replace_state(relay, fn state ->
        next_bounce = BounceDecision.next_deadline(state.store.engine)

        pending = %{
          priority: map_size(state.priority_jobs) + map_size(state.priority_timers) +
            map_size(state.priority_retry_timers) + map_size(state.priority_queued),
          availability: MapSet.size(Map.get(state, :agent_availability_jobs, MapSet.new())),
          overdue_bounce: is_integer(next_bounce) and next_bounce <= state.clock.(),
          rank_retry: not is_nil(state.client_rank_retry)
        }

        send(observer, {:startup_pending, pending})
        throw(:startup_sampled)
      end, 120_000)
    catch
      :error, {:callback_failed, _, {:throw, :startup_sampled}} -> :ok
    end

    receive do
      {:startup_pending, %{priority: 0, availability: 0, overdue_bounce: false, rank_retry: false} = pending} ->
        report(:startup_drained, pending)

      {:startup_pending, pending} ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          raise "synthetic startup still has pending work: #{inspect(pending)}"
        end

        next_sample =
          if now >= next_sample do
            report(:startup_draining, %{pending: pending, memory: memory()})
            now + 120_000
          else
            next_sample
          end

        Process.sleep(1_000)
        await_quiet_startup(relay, deadline, next_sample)
    after
      5_000 -> raise "synthetic startup sample was not received"
    end
  end

  defp first_reply(relay, params) do
    {:ok, inbox} = Reads.inbox(relay, @reader, params)

    cond do
      Enum.any?(inbox["items"], &is_binary(get_in(&1, ["chat", "reply_rank_key"]))) -> :ok
      cursor = inbox["next_page"] -> first_reply(relay, Map.put(params, "page", cursor))
      true -> raise "synthetic fixture should have an unread reply"
    end
  end

  defp sample_wal(path, parent, peak) do
    peak =
      case File.stat(path) do
        {:ok, stat} -> max(peak, stat.size)
        _ -> peak
      end

    receive do
      :finish -> send(parent, {:wal_peak, peak})
    after
      20 -> sample_wal(path, parent, peak)
    end
  end

  defp memory do
    for [_, field, kib] <-
          Regex.scan(~r/^(VmRSS|VmHWM):\s+(\d+) kB$/m, File.read!("/proc/self/status")),
        into: %{},
        do: {field, String.to_integer(kib) * 1024}
  end

  defp query(db, sql) do
    {:ok, stmt} = Sqlite3.prepare(db, sql)
    try do
      {:ok, rows} = Sqlite3.fetch_all(db, stmt)
      rows
    after
      Sqlite3.release(db, stmt)
    end
  end

  defp synthetic!(dir) do
    true = File.regular?(Path.join(dir, "synthetic-format31"))
  end

  defp assert_ok(reply) do
    {:ok, %{"type" => "ok"}} = JSON.decode_line(reply)
  end

  defp path(dir), do: Path.join(dir, "relay.sqlite3")

  defp mark_long_chats_read(engine, messages, active, discussions, now) when discussions >= 900_000 do
    Enum.reduce((messages - active + 1)..messages, engine, fn i, state ->
      id = "item-#{i}"
      key = {@person, @sender, id}
      position = state.route_b_index.conversation_summary_by_item[key].latest_origin_reply_seq

      transition(state, @reader, %{
        "type" => "conversation_read", "op_id" => uuid(5_000_000 + i),
        "sender_id" => @sender, "message_id" => id,
        "read_through_conversation_seq" => position
      }, now)
    end)
  end

  defp mark_long_chats_read(engine, _messages, _active, _discussions, _now), do: engine

  defp uuid(i) do
    "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(i), 12, "0")
  end

  defp forward(i, sessions) do
    %{
      "v" => 1,
      "type" => "forward",
      "op_id" => uuid(i),
      "session_handle" => uuid(rem(i - 1, sessions) + 1),
      "message" => %{
        "sender_id" => @sender,
        "message_id" => "item-#{i}",
        "created_at" => i,
        "title" => "Synthetic request #{i}",
        "body" => String.duplicate("benchmark ", 16)
      }
    }
  end

  defp transition(engine, key, frame, now) do
    {engine, {:ok, response}} = Engine.request_frame(engine, key, Map.put_new(frame, "v", 1), now)
    assert_ok(response)
    engine
  end

  defp request_frame(engine, key, frame, now) do
    {next, {:ok, line}} = Engine.request_frame(engine, key, Map.put_new(frame, "v", 1), now)
    {:ok, %{"type" => "ok"} = result} = JSON.decode_line(line)
    {next, result}
  end

  defp measure_sends(relay) do
    times =
      for i <- 1..3 do
        op = uuid(9_000_000 + i)
        principal = %{kind: :device, person: @person, key: @origin}
        body = %{
          "sender" => @sender,
          "session" => uuid(1),
          "message" => %{
            "title" => "Ordinary synthetic send #{i}",
            "body" => "Confirm the next follow-up and reply with the status.",
            "created_at" => System.system_time(:millisecond)
          }
        }
        digest = Idempotency.digest("POST", ["v2", "agent", "messages"], body)
        input = {principal, :send, %{}, op, digest, nil, body}
        {micros, {:ok, 200, _, false}} = :timer.tc(fn ->
          GenServer.call(relay, {:client_api, AgentWrites, {:execute, input, op, []}}, :infinity)
        end)
        report(:ordinary_send_us, micros)

        micros
      end
    report(:ordinary_send_median_us, Enum.sort(times) |> Enum.at(1))

  end


  defp report(label, data), do: IO.puts("#{label} #{inspect(data, width: :infinity)}")
end

if System.get_env("ATTENTION_SERVER_ENABLE") != "0" do
  raise "rehearsal requires the server listener to be disabled"
end

{:ok, _} = Application.ensure_all_started(:attention_server)
Format31Rehearsal.run(System.argv())

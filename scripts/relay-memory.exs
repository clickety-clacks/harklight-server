# Run from the project root with ATTENTION_SERVER_ENABLE=0 and MIX_ENV=prod.
# Each scenario seeds a fresh store in one VM and measures it in a second VM:
#   mix run scripts/relay-memory.exs seed DIR MESSAGES DISCUSSIONS SESSIONS
#   mix run scripts/relay-memory.exs measure DIR MESSAGES SESSIONS
defmodule RelayMemory do
  alias AttentionServer.{Engine, JSON, Relay, Store}
  alias Exqlite.Sqlite3

  @person "memory-benchmark"
  @origin "benchmark-origin"
  @reader "benchmark-reader"
  @sender "app:benchmark"
  @relay :memory_benchmark_relay
  @words :erlang.system_info(:wordsize)
  @writes 20

  def run(["seed", dir, messages, discussions, sessions]) do
    [messages, discussions, sessions] =
      Enum.map([messages, discussions, sessions], &String.to_integer/1)

    true = messages > @writes and discussions >= 0 and sessions > 0 and sessions <= messages
    :ok = File.mkdir(dir)
    :ok = File.write!(Path.join(dir, "synthetic-relay-memory"), "")
    {:ok, store} = Store.open(path(dir))
    {:ok, store} = Store.enable_change_log(store)
    {:ok, engine} = Engine.setup_person(store.engine, @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)
    now = System.system_time(:millisecond)
    seed_messages = messages - @writes

    engine =
      Enum.reduce(1..seed_messages, engine, fn i, engine ->
        transition(engine, @origin, forward(i, sessions), now)
      end)

    engine =
      Enum.reduce(1..discussions//1, engine, fn i, engine ->
        id = rem(i - 1, seed_messages) + 1

        question = %{
          "type" => "discussion",
          "op_id" => uuid(1_000_000 + i),
          "sender_id" => @sender,
          "message_id" => "item-#{id}",
          "request_revision" => 1,
          "text" => "Synthetic follow-up #{i} " <> String.duplicate("benchmark ", 8)
        }

        transition(engine, @reader, question, now)
      end)

    {:ok, store} =
      Store.commit(store, engine, %{person: @person, actor: @origin, action: "benchmark_seed"})

    :ok = Store.close(store)

    report(:seed, %{
      messages: seed_messages,
      conversation_events: map_size(engine.conversation_events),
      sessions: map_size(engine.origin_sessions)
    })
  end

  def run(["measure", dir, messages, sessions]) do
    true = File.regular?(Path.join(dir, "synthetic-relay-memory"))
    [messages, sessions] = Enum.map([messages, sessions], &String.to_integer/1)
    sample(:empty_vm, nil)

    {:ok, relay} = Relay.start_link(store_path: path(dir), name: @relay)
    Process.sleep(1_000)
    sample(:cold_idle, relay)

    start = System.monotonic_time(:millisecond)

    for i <- (messages - @writes + 1)..messages do
      request(relay, forward(i, sessions))
      sample(:write, relay)
    end

    report(:write_elapsed_ms, System.monotonic_time(:millisecond) - start)
    Process.sleep(2_000)
    sample(:idle_2s, relay)

    frame = forward(messages, sessions)

    for round <- 1..3 do
      for _ <- 1..250 do
        request(relay, frame)
        {:ok, _} = Relay.acknowledged_cursor(relay, @reader)
        {:ok, _} = AttentionServer.ClientAPI.Reads.inbox(relay, @reader, %{"limit" => "10"})
      end

      sample({:fixed_size_replay, round}, relay)
    end

    Process.sleep(15_000)
    sample(:idle_15s, relay)
    true = :erlang.garbage_collect(relay)
    Process.sleep(250)
    sample(:relay_major_gc, relay)
    profile_state(relay)
    :ok = GenServer.stop(relay, :normal, 30_000)
    database(path(dir))
  end

  defp path(dir), do: Path.join(dir, "relay.sqlite3")

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

  defp request(relay, frame) do
    {:ok, response} = Relay.request_line(relay, @origin, JSON.encode!(frame) <> "\n")
    assert_ok(response)
  end

  defp assert_ok(response) do
    case JSON.decode_line(response) do
      {:ok, %{"type" => "ok"}} -> :ok
      error -> raise "fixture request failed: #{inspect(error)}"
    end
  end

  defp sample(label, relay) do
    # Read OS counters first. The process/ETS detail collected below can itself allocate.
    os =
      for [_, field, value] <-
            Regex.scan(~r/^(VmRSS|VmHWM|RssAnon):\s+(\d+) kB$/m, File.read!("/proc/self/status")),
          into: %{},
          do: {field, String.to_integer(value) * 1024}

    info =
      if relay,
        do: Process.info(relay, [:memory, :total_heap_size, :message_queue_len]),
        else: nil

    report(label, %{os_bytes: os, vm_bytes: :erlang.memory(), relay: info})
  end

  defp profile_state(relay) do
    ets =
      :ets.all()
      |> Enum.map(fn table ->
        [name, size, words, owner] =
          Enum.map([:name, :size, :memory, :owner], &:ets.info(table, &1))

        {name, size, if(is_integer(words), do: words * @words, else: 0), owner}
      end)
      |> Enum.sort_by(&elem(&1, 2), :desc)

    processes =
      Process.list()
      |> Enum.flat_map(fn pid ->
        case Process.info(pid, [
               :memory,
               :initial_call,
               :registered_name,
               :message_queue_len,
               :binary
             ]) do
          nil ->
            []

          info ->
            {binaries, info} = Keyword.pop!(info, :binary)
            bytes = binaries |> Enum.uniq_by(&elem(&1, 0)) |> Enum.reduce(0, &(elem(&1, 1) + &2))

            [
              {pid, info[:memory], bytes, info[:registered_name], info[:initial_call],
               info[:message_queue_len]}
            ]
        end
      end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(20)

    report(:ets, ets)
    report(:processes, processes)

    caller = self()

    # Measure in the owning process. Returning its state over :sys would
    # introduce a flattened copy and drown out the memory being diagnosed.
    try do
      :sys.replace_state(
        relay,
        fn state ->
          store = state.store
          engine = store.engine

          send(
            caller,
            {:footprint,
             %{
               whole_relay: size(state),
               relay_fields: fields(state),
               store_fields: fields(Map.from_struct(store)),
               encoding_cache_fields: fields(store.encoding_cache),
               engine_fields: fields(Map.from_struct(engine)),
               index_fields: fields(engine.route_b_index),
               process_dictionary: size(Process.get())
             }}
          )

          throw(:measurement_complete)
        end,
        30_000
      )
    catch
      :error, {:callback_failed, _, {:throw, :measurement_complete}} -> :ok
      kind, reason -> report(:profile_callback_result, {kind, reason})
    end

    receive do
      {:footprint, footprint} -> report(:state, footprint)
    after
      5_000 -> raise "Relay did not supply memory attribution"
    end
  end

  defp fields(map), do: Enum.map(map, fn {key, term} -> {key, size(term)} end) |> Map.new()

  defp size(term) do
    flat = :erts_debug.flat_size(term) * @words

    %{
      bytes: if(flat <= 65_536, do: :erts_debug.size(term) * @words, else: nil),
      flat_bytes: flat,
      count: count(term)
    }
  end

  defp count(%MapSet{} = term), do: MapSet.size(term)
  defp count(term) when is_map(term), do: map_size(term)
  defp count(_term), do: nil

  defp database(path) do
    {:ok, db} = Sqlite3.open(path)

    for table <-
          ~w(client_changes agent_changes client_idempotency relay_inbox_snapshots relay_audit) do
      case Sqlite3.prepare(db, "SELECT count(*) FROM " <> table) do
        {:ok, stmt} ->
          {:row, [count]} = Sqlite3.step(db, stmt)
          :ok = Sqlite3.release(db, stmt)
          report({:sqlite_rows, table}, count)

        {:error, "no such table: " <> ^table} ->
          report({:sqlite_rows, table}, :absent)
      end
    end

    :ok = Sqlite3.close(db)
    report(:sqlite_file_bytes, File.stat!(path).size)
  end

  defp report(label, data),
    do: IO.puts("#{inspect(label)} #{inspect(data, limit: :infinity, width: :infinity)}")
end

if Application.get_env(:attention_server, :server) ||
     System.get_env("ATTENTION_SERVER_ENABLE") == "1" do
  raise "run the benchmark with ATTENTION_SERVER_ENABLE=0"
end

RelayMemory.run(System.argv())

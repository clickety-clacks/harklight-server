defmodule AttentionServer.CommitProfile do
  @moduledoc """
  Bounded, opt-in profiling of a Relay and its persistence tasks.

  `capture/2` does not change the request path. It returns aggregate MFA-only
  samples and per-function runtime counters; it never reads messages, process
  dictionaries, function arguments, return values, or application state.
  Enable explicitly with `enabled: true` or the `:commit_profile` application
  setting (default false). Intended for isolated synthetic workload fixtures.
  """

  @targets [
    {AttentionServer.Relay, :handle_call, 3},
    {AttentionServer.Relay, :handle_info, 2},
    {AttentionServer.Store, :commit, 4},
    {AttentionServer.Store, :commit_keyed, 5},
    {AttentionServer.Store.Persistence, :run, 1},
    {AttentionServer.Store.Persistence, :await, 1},
    {Task, :async, 3},
    {AttentionServer.Store.RecordTable, :transaction, 2},
    {AttentionServer.Store.RecordTable, :write_generation, 4},
    {AttentionServer.ChangeLog, :notify, 1},
    {AttentionServer.ChangeLog, :read, 4},
    {AttentionServer.ClientAPI.FeedStep, :handle_call, 3}
  ]
  @max_stacks 128

  @doc "Capture for 1–90,000 ms at a 10–1,000 ms interval; default 10 seconds/20 ms."
  def capture(relay, opts \\ []) do
    enabled =
      Keyword.get(opts, :enabled, Application.get_env(:attention_server, :commit_profile, false))

    duration = Keyword.get(opts, :duration_ms, 10_000)
    interval = Keyword.get(opts, :interval_ms, 20)

    cond do
      enabled != true ->
        {:error, :disabled}

      not is_pid(relay) ->
        {:error, :invalid_target}

      not (is_integer(duration) and duration in 1..90_000 and
             is_integer(interval) and interval in 10..1_000) ->
        {:error, :invalid_limits}

      not Process.alive?(relay) ->
        {:error, :target_down}

      true ->
        collect(relay, duration, interval)
    end
  end

  defp collect(relay, duration, interval) do
    caller = self()
    token = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        Process.flag(:max_heap_size, %{size: 2_000_000, kill: true, error_logger: false})
        send(caller, {token, profile(relay, duration, interval)})
      end)

    receive do
      {^token, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, _reason} ->
        {:error, :collector_failed}
    after
      duration + 1_000 ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])
        {:error, :collector_timeout}
    end
  end

  defp profile(relay, duration, interval) do
    # A private OTP trace session avoids overwriting another diagnostic's hooks.
    # The VM also destroys the session if this tracer exits unexpectedly.
    session = :trace.session_create(__MODULE__, self(), [])

    try do
      coverage =
        for {module, _, _} = mfa <- @targets do
          Code.ensure_loaded(module)
          %{function: mfa_name(mfa), matches: :trace.function(session, mfa, true, [:call_time])}
        end

      :trace.process(session, relay, true, [:call, :set_on_spawn])
      start = System.monotonic_time(:millisecond)
      initial = %{samples: 0, queue_max: 0, queue_total: 0, stacks: %{}, dropped_stacks: 0}
      stats = sample(relay, start + duration, interval, initial)
      timers = Enum.map(@targets, &timer(session, &1))

      {:ok,
       Map.merge(stats, %{
         schema: 1,
         elapsed_ms: System.monotonic_time(:millisecond) - start,
         interval_ms: interval,
         coverage: coverage,
         timers: timers,
         queue_mean: stats.queue_total / max(stats.samples, 1)
       })}
    after
      :trace.session_destroy(session)
    end
  end

  defp sample(relay, deadline, interval, stats) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      stats
    else
      case Process.info(relay, [:current_stacktrace, :message_queue_len, :links]) do
        nil ->
          stats

        info ->
          queue = Keyword.fetch!(info, :message_queue_len)

          stats = %{
            stats
            | samples: stats.samples + 1,
              queue_max: max(stats.queue_max, queue),
              queue_total: stats.queue_total + queue
          }

          stats = stack(stats, "relay", Keyword.get(info, :current_stacktrace, []))

          stats =
            info
            |> Keyword.get(:links, [])
            |> Enum.filter(&is_pid/1)
            |> Enum.take(8)
            |> Enum.reduce(stats, fn pid, acc ->
              case Process.info(pid, :current_stacktrace) do
                {:current_stacktrace, frames} -> stack(acc, "linked", frames)
                _ -> acc
              end
            end)

          Process.sleep(min(interval, max(0, deadline - System.monotonic_time(:millisecond))))
          sample(relay, deadline, interval, stats)
      end
    end
  end

  defp stack(stats, scope, frames) do
    name =
      scope <>
        ":" <>
        Enum.map_join(Enum.take(frames, 8), ";", fn {m, f, a, _} ->
          mfa_name({m, f, if(is_integer(a), do: a, else: length(a))})
        end)

    if Map.has_key?(stats.stacks, name) or map_size(stats.stacks) < @max_stacks do
      %{stats | stacks: Map.update(stats.stacks, name, 1, &(&1 + 1))}
    else
      %{stats | dropped_stacks: stats.dropped_stacks + 1}
    end
  end

  defp timer(session, mfa) do
    rows =
      case :trace.info(session, mfa, :call_time) do
        {:call_time, values} when is_list(values) -> values
        _ -> []
      end

    %{
      function: mfa_name(mfa),
      processes: length(rows),
      calls: Enum.reduce(rows, 0, fn {_, count, _, _}, acc -> acc + count end),
      runtime_us:
        Enum.reduce(rows, 0, fn {_, _, seconds, micros}, acc ->
          acc + seconds * 1_000_000 + micros
        end)
    }
  end

  defp mfa_name({m, f, a}), do: "#{m}.#{f}/#{a}"
end

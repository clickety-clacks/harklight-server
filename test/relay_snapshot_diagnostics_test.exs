defmodule AttentionServer.RelaySnapshotDiagnosticsTest do
  use ExUnit.Case, async: false

  alias AttentionServer.Diagnostics

  @collector_name AttentionServer.Diagnostics.RelaySnapshotCollector
  @starting_name AttentionServer.Diagnostics.RelaySnapshotStarting
  @cooldown_ms 60_000

  test "snapshot output contains only the allowlist and caps normalized frames at eight" do
    frames =
      for _ <- 1..12 do
        {:erlang, :process_info, 2, [file: "ignored.ex", line: 1]}
      end

    info = [
      status: :waiting,
      current_function: {:erlang, :process_info, 2},
      current_stacktrace: frames,
      message_queue_len: 3,
      reductions: 123,
      memory: 456,
      heap_size: 78,
      garbage_collection: [minor_gcs: 4, major_gcs: 2, fullsweep_after: 99],
      messages: [:not_emitted],
      dictionary: [not_emitted: :value]
    ]

    {snapshot, message} = Diagnostics.relay_snapshot_message(info, 1_234_567)

    allowed =
      ~w(timestamp alive status current_function mfa_frames queue_length reductions memory heap gc_counters)

    assert snapshot |> Map.keys() |> Enum.map(&Atom.to_string/1) |> Enum.sort() ==
             Enum.sort(allowed)

    assert snapshot.alive
    assert snapshot.current_function == {"erlang", "process_info", 2}
    assert length(snapshot.mfa_frames) == 8
    assert snapshot.gc_counters == %{major_gcs: 2, minor_gcs: 4}

    ["relay_snapshot" | fields] = String.split(message)
    assert Enum.map(fields, &(&1 |> String.split("=", parts: 2) |> hd())) == allowed
    refute message =~ "messages="
    refute message =~ "dictionary="
    refute message =~ "not_emitted"
    refute message =~ "fullsweep_after"
    refute message =~ "ignored.ex"
    refute message =~ "line="
  end

  test "concurrent triggers admit only one collector spawn before registration" do
    parent = self()
    collector_name = :relay_snapshot_concurrent_test_collector
    trigger_count = 32

    collect_fun = fn _target ->
      send(parent, {:collector_entered, self()})

      receive do
        :release -> :ok
      end
    end

    spawn_fun = fn collector_fun ->
      send(parent, {:collector_spawn_attempt, self()})

      receive do
        :spawn -> spawn(collector_fun)
      end
    end

    callers =
      for _ <- 1..trigger_count do
        spawn(fn ->
          send(parent, {:trigger_ready, self()})

          receive do
            :trigger ->
              result =
                Diagnostics.start_relay_snapshot(
                  nil,
                  collector_name,
                  @cooldown_ms,
                  collect_fun,
                  spawn_fun
                )

              send(parent, {:trigger_result, result})

              receive do
                :stop -> :ok
              end
          end
        end)
      end

    try do
      Enum.each(callers, fn caller ->
        assert_receive {:trigger_ready, ^caller}, 1_000
      end)

      Enum.each(callers, &send(&1, :trigger))

      assert_receive {:collector_spawn_attempt, _winner}, 1_000

      for _ <- 1..(trigger_count - 1) do
        assert_receive {:trigger_result, :skipped}, 1_000
      end

      refute_receive {:collector_spawn_attempt, _caller}, 20

      Enum.each(callers, &send(&1, :spawn))
      assert_receive {:trigger_result, :started}, 1_000
      assert_receive {:collector_entered, collector}, 1_000
      assert Process.whereis(collector_name) == collector
    after
      Enum.each(callers, &send(&1, :trigger))
      Enum.each(callers, &send(&1, :spawn))
      Enum.each(callers, &send(&1, :stop))
      Process.sleep(10)
      stop_collector(collector_name)
    end
  end

  test "a second trigger is skipped while collection is active and during the 60-second cooldown" do
    parent = self()
    collector_name = :relay_snapshot_active_test_collector

    collect_fun = fn _target ->
      send(parent, {:collector_entered, self()})

      receive do
        :release -> send(parent, {:collector_finished, self()})
      end
    end

    assert :started =
             Diagnostics.start_relay_snapshot(nil, collector_name, @cooldown_ms, collect_fun)

    assert_receive {:collector_entered, collector}, 1_000

    try do
      assert Process.whereis(collector_name) == collector

      assert :skipped =
               Diagnostics.start_relay_snapshot(nil, collector_name, @cooldown_ms, fn _ ->
                 send(parent, :duplicate_collection)
               end)

      send(collector, :release)
      assert_receive {:collector_finished, ^collector}, 1_000
      assert await_cooldown(collector)
      assert Process.whereis(collector_name) == collector

      assert :skipped =
               Diagnostics.start_relay_snapshot(nil, collector_name, @cooldown_ms, fn _ ->
                 send(parent, :duplicate_collection)
               end)

      refute_receive :duplicate_collection, 20
    after
      stop_collector(collector_name)
    end
  end

  test "collector releases the cooldown slot after its interval while the listener stays alive" do
    parent = self()
    collector_name = :relay_snapshot_expiry_test_collector
    interval_ms = 40

    collect_fun = fn _target -> send(parent, :collection_finished) end

    listener =
      spawn(fn ->
        result =
          Diagnostics.start_relay_snapshot(nil, collector_name, interval_ms, collect_fun)

        send(parent, {:listener_trigger_result, result, self()})

        receive do
          :stop -> :ok
        end
      end)

    listener_monitor = Process.monitor(listener)

    try do
      assert_receive {:listener_trigger_result, :started, ^listener}, 1_000
      assert_receive :collection_finished, 1_000
      assert Process.whereis(@starting_name) == listener
      assert await_snapshot_names_released(collector_name)
      assert Process.alive?(listener)
    after
      send(listener, :stop)
      Process.exit(listener, :kill)

      receive do
        {:DOWN, ^listener_monitor, :process, ^listener, _reason} -> :ok
      after
        1_000 -> Process.demonitor(listener_monitor, [:flush])
      end
    end
  end

  test "an early collector exit keeps the cooldown slot until its listener exits" do
    parent = self()
    collector_name = :relay_snapshot_early_exit_test_collector

    collect_fun = fn _target ->
      send(parent, {:collector_entered, self()})

      receive do
        :release -> :ok
      end
    end

    listener =
      spawn(fn ->
        result = Diagnostics.start_relay_snapshot(nil, collector_name, @cooldown_ms, collect_fun)
        send(parent, {:listener_trigger_result, result, self()})

        receive do
          :stop -> :ok
        end
      end)

    listener_monitor = Process.monitor(listener)

    try do
      assert_receive {:listener_trigger_result, :started, ^listener}, 1_000
      assert_receive {:collector_entered, collector}, 1_000
      assert Process.whereis(@starting_name) == listener

      collector_monitor = Process.monitor(collector)
      Process.exit(collector, :kill)
      assert_receive {:DOWN, ^collector_monitor, :process, ^collector, :killed}, 1_000
      assert Process.whereis(collector_name) == nil

      assert :skipped =
               Diagnostics.start_relay_snapshot(nil, collector_name, @cooldown_ms, fn _ ->
                 send(parent, :duplicate_collection)
               end)

      refute_receive :duplicate_collection, 20
      assert Process.whereis(@starting_name) == listener
    after
      send(listener, :stop)
      Process.exit(listener, :kill)

      receive do
        {:DOWN, ^listener_monitor, :process, ^listener, _reason} -> :ok
      after
        1_000 -> Process.demonitor(listener_monitor, [:flush])
      end

      stop_collector(collector_name)
    end
  end

  test "Relay-boundary request returns while the named collector is blocked" do
    parent = self()

    blocked_target =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    :erlang.suspend_process(blocked_target)

    collect_fun = fn _target ->
      send(parent, {:collector_blocked, self()})

      receive do
        :release -> :ok
      end
    end

    assert :started =
             Diagnostics.start_relay_snapshot(
               blocked_target,
               @collector_name,
               @cooldown_ms,
               collect_fun
             )

    try do
      assert_receive {:collector_blocked, collector}, 1_000

      task =
        Task.async(fn ->
          Diagnostics.failure(
            :origin_https,
            :relay_boundary,
            :timeout,
            :outcome_unknown,
            nil,
            blocked_target
          )
        end)

      assert {:ok, :ok} = Task.yield(task, 500)
      assert Process.whereis(@collector_name) == collector
    after
      if collector = Process.whereis(@collector_name), do: send(collector, :release)
      stop_collector(@collector_name)
      :erlang.resume_process(blocked_target)
      Process.exit(blocked_target, :kill)
    end
  end

  defp await_cooldown(collector, attempts \\ 100)

  defp await_cooldown(collector, attempts) when attempts > 0 do
    case Process.info(collector, :current_function) do
      {:current_function, {Process, :sleep, 1}} ->
        true

      _ ->
        Process.sleep(1)
        await_cooldown(collector, attempts - 1)
    end
  end

  defp await_cooldown(_collector, _attempts), do: false

  defp await_snapshot_names_released(collector_name, attempts \\ 100)

  defp await_snapshot_names_released(collector_name, attempts) when attempts > 0 do
    if Process.whereis(collector_name) == nil and Process.whereis(@starting_name) == nil do
      true
    else
      Process.sleep(1)
      await_snapshot_names_released(collector_name, attempts - 1)
    end
  end

  defp await_snapshot_names_released(_collector_name, _attempts), do: false

  defp stop_collector(name) do
    case Process.whereis(name) do
      pid when is_pid(pid) ->
        monitor = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
        after
          1_000 -> Process.demonitor(monitor, [:flush])
        end

      _ ->
        :ok
    end
  end
end

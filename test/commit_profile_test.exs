defmodule AttentionServer.CommitProfileTest do
  use ExUnit.Case, async: false
  alias AttentionServer.{CommitProfile, Relay}

  test "default-off capture leaves a suspended target alone and rejects invalid bounds" do
    target =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    :erlang.suspend_process(target)

    try do
      assert {:error, :disabled} = CommitProfile.capture(target)

      assert {:error, :invalid_limits} =
               CommitProfile.capture(target, enabled: true, duration_ms: 90_001)
    after
      :erlang.resume_process(target)
      send(target, :stop)
    end
  end

  test "capture measures real Relay calls without emitting their synthetic payload" do
    dir = Path.join(System.tmp_dir!(), "commit-profile-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    {:ok, relay} = Relay.start_link(store_path: Path.join(dir, "relay.sqlite3"))

    try do
      load =
        Task.async(fn ->
          for n <- 1..250 do
            :ok = Relay.setup_person(relay, "private-synthetic-payload-#{n}")
            Process.sleep(2)
          end
        end)

      assert {:ok, report} =
               CommitProfile.capture(relay, enabled: true, duration_ms: 200, interval_ms: 10)

      Task.await(load)
      assert report.samples > 0
      assert report.elapsed_ms >= 200
      assert report.elapsed_ms < 1_200
      assert Enum.any?(report.timers, &(&1.calls > 0))
      assert Enum.all?(report.coverage, &(&1.matches == 1))
      refute inspect(report) =~ "private-synthetic-payload"
      assert :ok = Relay.setup_person(relay, "after-capture")
      assert :trace.session_info(relay) == []
    after
      GenServer.stop(relay)
      File.rm_rf!(dir)
    end
  end

  test "a stalled target is bounded and leaves another trace session intact" do
    target =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    session = :trace.session_create(:existing_profile_test, self(), [])
    :trace.process(session, target, true, [:procs])
    :erlang.suspend_process(target)

    try do
      started = System.monotonic_time(:millisecond)
      result = CommitProfile.capture(target, enabled: true, duration_ms: 10)
      # Some OTP schedulers can answer process_info while the process is suspended.
      assert match?({:ok, _}, result) or result == {:error, :collector_timeout}
      assert System.monotonic_time(:millisecond) - started < 2_000
      assert {:flags, [:procs]} = :trace.info(session, target, :flags)
      assert :trace.session_info(target) == [elem(session, 1)]
    after
      :erlang.resume_process(target)
      :trace.session_destroy(session)
      send(target, :stop)
    end
  end

  test "timing storage stays bounded through 1000 exited workers" do
    target =
      spawn(fn ->
        receive do
          {:churn, caller} ->
            for _ <- 1..1_000, do: Task.async(__MODULE__, :churn_worker, []) |> Task.await()
            send(caller, :churn_done)

            receive do
              :stop -> :ok
            end
        end
      end)

    try do
      capture =
        Task.async(fn -> CommitProfile.capture(target, enabled: true, duration_ms: 2_000) end)

      session = await_session(target)
      assert {:flags, [:call]} = :trace.info(session, target, :flags)
      send(target, {:churn, self()})
      assert_receive :churn_done, 1_500
      {:call_time, rows} = :trace.info(session, {Task, :async, 3}, :call_time)
      assert [{^target, calls, _, _}] = rows
      assert calls >= 1_000
      assert {:ok, report} = Task.await(capture, 3_500)
      assert Enum.all?(report.timers, &(&1.processes <= 1))
      assert :trace.session_info(target) == []
    after
      send(target, :stop)
    end
  end

  test "collector failure and forced timeout remove private hooks and preserve other sessions" do
    target =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    other = :trace.session_create(:other_profile, self(), [])
    :trace.process(other, target, true, [:procs])

    try do
      for failure <- [:kill, :suspend] do
        capture =
          Task.async(fn -> CommitProfile.capture(target, enabled: true, duration_ms: 200) end)

        session = await_session(target)
        {:tracer, collector} = :trace.info(session, target, :tracer)

        expected =
          case failure do
            :kill ->
              Process.exit(collector, :kill)
              :collector_failed

            :suspend ->
              :erlang.suspend_process(collector)
              :collector_timeout
          end

        assert {:error, ^expected} = Task.await(capture, 2_000)
        assert eventually(fn -> :trace.session_info(target) == [elem(other, 1)] end)
        assert {:flags, [:procs]} = :trace.info(other, target, :flags)
      end
    after
      :trace.session_destroy(other)
      send(target, :stop)
    end
  end

  def churn_worker, do: Task.async(Kernel, :is_atom, [:synthetic]) |> Task.await()

  defp await_session(target) do
    assert eventually(fn ->
             Enum.any?(:trace.session_info(target), &match?({CommitProfile, _}, &1))
           end)

    Enum.find(:trace.session_info(target), &match?({CommitProfile, _}, &1))
  end

  defp eventually(fun, remaining \\ 100)
  defp eventually(_, 0), do: false

  defp eventually(fun, remaining) do
    if fun.(),
      do: true,
      else:
        (
          Process.sleep(5)
          eventually(fun, remaining - 1)
        )
  end

  test "a dead target has an explicit result" do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert {:error, :target_down} = CommitProfile.capture(pid, enabled: true)
  end
end

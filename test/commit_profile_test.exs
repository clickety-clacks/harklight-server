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
      assert {:flags, []} = :erlang.trace_info(relay, :flags)
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
    after
      :erlang.resume_process(target)
      :trace.session_destroy(session)
      send(target, :stop)
    end
  end

  test "a dead target has an explicit result" do
    {pid, ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert {:error, :target_down} = CommitProfile.capture(pid, enabled: true)
  end
end

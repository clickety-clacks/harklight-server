defmodule AttentionServer.FeatureFlagsTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias AttentionServer.{JSON, Relay, Store}

  setup do
    dir = Path.join(System.tmp_dir!(), "feature-flags-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir, path: Path.join(dir, "flags.json"), store: Path.join(dir, "state.sqlite3")}
  end

  test "reconnect reads changed flags and keeps persisted server identity", ctx do
    File.write!(ctx.path, ~s({"test_on":true,"test_off":false,"ignored":"true"}))
    relay = start_relay(ctx)
    server = :sys.get_state(relay).store.engine.relay_instance_id
    assert server =~ ~r/\A[0-9a-f]{32}\z/
    first = connect(relay)
    assert first["server"] == server
    assert first["feature_flags"] == %{"test_on" => true, "test_off" => false}
    # Replacement rather than merge: test_on disappears and test_off flips.
    File.write!(ctx.path, ~s({"test_off":true}))
    second = connect(relay)
    assert second["server"] == server
    assert second["feature_flags"] == %{"test_off" => true}
    File.write!(ctx.path, "{}")
    assert connect(relay)["feature_flags"] == %{}
    GenServer.stop(relay)
    {:ok, stored} = Store.open(ctx.store)
    assert stored.engine.relay_instance_id == server
    Store.close(stored)
    {:ok, restarted} = Relay.start_link(store_path: ctx.store, feature_flags_path: ctx.path)
    assert connect(restarted)["server"] == server
    GenServer.stop(restarted)
  end

  test "missing, unreadable and malformed files default without repeated warnings or restart",
       ctx do
    relay = start_relay(ctx)

    log =
      capture_log(fn ->
        assert connect(relay)["feature_flags"] == %{}
        File.mkdir!(ctx.path)
        assert connect(relay)["feature_flags"] == %{}
        File.rmdir!(ctx.path)

        for contents <- ["not-json", "null", "[]", "{\"test_on\":"] do
          File.write!(ctx.path, contents)
          assert connect(relay)["feature_flags"] == %{}
        end

        File.write!(ctx.path, ~s({"test_on":true,"number":1,"nested":{},"null":null}))
        assert connect(relay)["feature_flags"] == %{"test_on" => true}
      end)

    assert length(Regex.scan(~r/event=harklight_feature_flags/, log)) == 1
    refute log =~ ctx.path
    assert Process.alive?(relay)
    GenServer.stop(relay)
  end

  test "no configured file uses client defaults", ctx do
    {:ok, relay} = Relay.start_link(store_path: ctx.store)
    :ok = Relay.setup_person(relay, "test-person")
    :ok = Relay.setup_device(relay, "test-person", "test-device")
    assert connect(relay)["feature_flags"] == %{}
    GenServer.stop(relay)
  end

  defp start_relay(ctx) do
    {:ok, relay} = Relay.start_link(store_path: ctx.store, feature_flags_path: ctx.path)
    :ok = Relay.setup_person(relay, "test-person")
    :ok = Relay.setup_device(relay, "test-person", "test-device")
    relay
  end

  defp connect(relay) do
    {:ok, session} = Relay.open_session(relay, "test-device")
    request(relay, session, %{"type" => "resync", "since" => 0}) |> List.last()
  end

  defp request(relay, session, frame) do
    {:ok, lines} =
      Relay.session_request_line(relay, session, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    Enum.map(lines, fn line ->
      {:ok, decoded} = JSON.decode_line(line)
      decoded
    end)
  end
end

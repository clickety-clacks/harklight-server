defmodule AttentionServer.FeatureFlagsTest do
  # The operator's flags file as GET /v2/me reads it (Reads.me in-process).
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias AttentionServer.{Relay, Store}
  alias AttentionServer.ClientAPI.Reads

  setup do
    dir = Path.join(System.tmp_dir!(), "feature-flags-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir, path: Path.join(dir, "flags.json"), store: Path.join(dir, "state.sqlite3")}
  end

  test "each read takes the changed flags file and the server identity persists", ctx do
    File.write!(
      ctx.path,
      ~s({"server_matched_push_routing":true,"priority_level_editing":false,"ignored":"true"})
    )

    relay = start_relay(ctx)
    server = :sys.get_state(relay).store.engine.relay_instance_id
    assert server =~ ~r/\A[0-9a-f]{32}\z/
    first = me(relay)
    assert first["server"] == %{"id" => server}
    assert first["features"] == features(true, false)
    # Replacement rather than merge: the first flag goes back to off.
    File.write!(ctx.path, ~s({"priority_level_editing":true}))
    second = me(relay)
    assert second["server"] == %{"id" => server}
    assert second["features"] == features(false, true)
    File.write!(ctx.path, "{}")
    assert me(relay)["features"] == features(false, false)
    GenServer.stop(relay)
    {:ok, stored} = Store.open(ctx.store)
    assert stored.engine.relay_instance_id == server
    Store.close(stored)
    {:ok, restarted} = Relay.start_link(store_path: ctx.store, feature_flags_path: ctx.path)
    assert me(restarted)["server"] == %{"id" => server}
    GenServer.stop(restarted)
  end

  test "missing, unreadable and malformed files give defaults with one warning per Relay lifetime",
       ctx do
    relay = start_relay(ctx)

    log =
      capture_log(fn ->
        assert me(relay)["features"] == features(false, false)
        File.mkdir!(ctx.path)
        assert me(relay)["features"] == features(false, false)
        File.rmdir!(ctx.path)

        for contents <- ["not-json", "null", "[]", "{\"priority_level_editing\":"] do
          File.write!(ctx.path, contents)
          assert me(relay)["features"] == features(false, false)
        end

        File.write!(
          ctx.path,
          ~s({"server_matched_push_routing":true,"priority_level_editing":1,"nested":{},"null":null})
        )

        assert me(relay)["features"] == features(true, false)
      end)

    assert length(Regex.scan(~r/event=harklight_feature_flags/, log)) == 1
    refute log =~ ctx.path
    assert Process.alive?(relay)
    GenServer.stop(relay)

    # A new Relay lifetime warns again, once.
    File.write!(ctx.path, "not-json")
    {:ok, restarted} = Relay.start_link(store_path: ctx.store, feature_flags_path: ctx.path)

    log =
      capture_log(fn ->
        assert me(restarted)["features"] == features(false, false)
        assert me(restarted)["features"] == features(false, false)
      end)

    assert length(Regex.scan(~r/event=harklight_feature_flags/, log)) == 1
    GenServer.stop(restarted)
  end

  test "no configured file uses client defaults without a warning", ctx do
    {:ok, relay} = Relay.start_link(store_path: ctx.store)
    :ok = Relay.setup_person(relay, "test-person")
    :ok = Relay.setup_device(relay, "test-person", "test-device")
    log = capture_log(fn -> assert me(relay)["features"] == features(false, false) end)
    refute log =~ "harklight_feature_flags"
    GenServer.stop(relay)
  end

  defp start_relay(ctx) do
    {:ok, relay} = Relay.start_link(store_path: ctx.store, feature_flags_path: ctx.path)
    :ok = Relay.setup_person(relay, "test-person")
    :ok = Relay.setup_device(relay, "test-person", "test-device")
    relay
  end

  defp me(relay) do
    assert {:ok, body} = Reads.me(relay, "test-device")
    body
  end

  defp features(push_routing, level_editing),
    do: %{
      "server_matched_push_routing" => push_routing,
      "priority_level_editing" => level_editing
    }
end

defmodule AttentionServer.APNSRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Relay, Server, TLSFixtures}

  @config Path.expand("../config/runtime.exs", __DIR__)
  @server_env %{
    "ATTENTION_SERVER_ENABLE" => "1",
    "ATTENTION_SERVER_JEV_ENDPOINT" => nil,
    "ATTENTION_SERVER_JEV_MODEL" => nil,
    "ATTENTION_SERVER_JEV_KEY_PATH" => nil,
    "ATTENTION_SERVER_JEV_SCORE_RECORD_PATH" => nil,
    "ATTENTION_SERVER_FEATURE_FLAGS_PATH" => nil,
    "ATTENTION_SERVER_STORE_PATH" => "/tmp/harklight-config-test/relay.sqlite3",
    "ATTENTION_SERVER_CERTFILE" => "/tmp/harklight-config-test/server.pem",
    "ATTENTION_SERVER_KEYFILE" => "/tmp/harklight-config-test/server.key",
    "ATTENTION_SERVER_CACERTFILE" => "/tmp/harklight-config-test/ca.pem",
    "ATTENTION_SERVER_PORT" => "18443",
    "ATTENTION_SERVER_ENROLL_PORT" => "18444",
    "ATTENTION_SERVER_OWNER_PORT" => "18445",
    "ATTENTION_SERVER_APNS_KEY_PATH" => "/protected/sandbox-key",
    "ATTENTION_SERVER_APNS_KEY_ID" => "SANDBOX001",
    "ATTENTION_SERVER_APNS_TEAM_ID" => "TESTTEAMID",
    "ATTENTION_SERVER_PUSH_RELAY_URL" => nil,
    "ATTENTION_SERVER_APNS_ATTEMPT_TIMEOUT_MS" => nil,
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => nil,
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID" => nil,
    "ATTENTION_SERVER_DEPLOYMENT_MODE" => nil,
    "ATTENTION_SERVER_UPLOAD_CAP_BYTES" => nil,
    "ATTENTION_SERVER_PUBLIC_BASE_URL" => nil,
    "ATTENTION_SERVER_HOSTED_PORT" => nil,
    "ATTENTION_SERVER_HOSTED_SIGN_IN_URL" => nil,
    "ATTENTION_SERVER_HOSTED_SIGN_OUT_URL" => nil,
    "ATTENTION_SERVER_HOSTED_ACCOUNT_URL" => nil,
    "ATTENTION_SERVER_HOSTED_TEST_ACCOUNT" => nil,
    "ATTENTION_SERVER_DASHBOARD_PORT" => nil,
    "ATTENTION_SERVER_DASHBOARD_BIND" => nil,
    "ATTENTION_SERVER_DASHBOARD_OWNER_PERSON" => nil,
    "ATTENTION_SERVER_DASHBOARD_SKILL_PATH" => nil,
    "ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SEND_LISTEN" => nil,
    "ATTENTION_SERVER_DASHBOARD_CERTFILE" => nil,
    "ATTENTION_SERVER_DASHBOARD_KEYFILE" => nil,
    "ATTENTION_SERVER_ENROLL_BIND" => nil,
    "ATTENTION_SERVER_BIND" => nil,
    "ATTENTION_SERVER_OWNER_BIND" => nil,
    "ATTENTION_SERVER_JOIN_NAME" => nil
  }

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "optional score record path reaches the self-hosted relay", %{certs: certs} do
    with_env(
      Map.put(@server_env, "ATTENTION_SERVER_JEV_SCORE_RECORD_PATH", "/unused/scores.jsonl"),
      fn ->
        opts = runtime_server_opts(certs)
        assert opts[:jev_score_record_path] == "/unused/scores.jsonl"
        server = start_server(opts)

        {_, relay, _, _} =
          Enum.find(Supervisor.which_children(server), fn {id, _, _, _} -> id == :relay end)

        writer = :sys.get_state(relay).applied_score_record
        assert :sys.get_state(writer).path == "/unused/scores.jsonl"
      end
    )
  end

  test "Hosted uses the startup Jev connection for new and existing accounts", %{certs: certs} do
    env =
      Map.merge(@server_env, %{
        "ATTENTION_SERVER_DEPLOYMENT_MODE" => "saas",
        "ATTENTION_SERVER_JEV_ENDPOINT" => "https://provider.example.test",
        "ATTENTION_SERVER_JEV_MODEL" => "test-model",
        "ATTENTION_SERVER_JEV_KEY_PATH" => "/unused/test-jev-key"
      })

    with_env(env, fn ->
      opts = runtime_server_opts(certs)
      assert opts[:jev_endpoint] == "https://provider.example.test"
      assert opts[:jev_model] == "test-model"

      server = start_server(Keyword.drop(opts, [:jev_endpoint, :jev_model]))
      relay = Server.relay(server)
      assert {:ok, existing} = Relay.hosted_account_create(relay, "existing-account")
      assert :sys.get_state(relay).priority_views[existing].algorithm == "baseline"
      Supervisor.stop(server)

      server = start_server(opts)
      relay = Server.relay(server)
      assert {:ok, new} = Relay.hosted_account_create(relay, "new-account")

      state = :sys.get_state(relay)
      assert state.hosted_jev_connection == {"https://provider.example.test", "test-model"}
      assert state.store.engine.jev_endpoint == nil
      assert %{algorithm: "jev-set", basis: "fallback"} = state.priority_views[existing]
      assert %{algorithm: "jev-set", basis: "fallback"} = state.priority_views[new]

      assert {:ok, %{algorithm_id: "baseline"}} =
               Relay.hosted_set_algorithm(relay, existing, "baseline")

      Supervisor.stop(server)

      server = start_server(opts)
      relay = Server.relay(server)
      assert %{algorithm: "baseline"} = :sys.get_state(relay).priority_views[existing]
      assert %{algorithm: "jev-set"} = :sys.get_state(relay).priority_views[new]
    end)
  end

  test "optional feature flag path reaches the relay without reading it at startup", %{
    certs: certs
  } do
    with_env(
      Map.put(@server_env, "ATTENTION_SERVER_FEATURE_FLAGS_PATH", "/unused/test-flags.json"),
      fn ->
        opts = runtime_server_opts(certs)
        assert opts[:feature_flags_path] == "/unused/test-flags.json"
        server = start_server(opts)

        assert :sys.get_state(Server.relay(server)).feature_flags_path ==
                 "/unused/test-flags.json"
      end
    )
  end

  test "runtime config keeps production and sandbox key settings separate" do
    with_env(@server_env, fn ->
      sandbox_only = runtime_apns_config()

      assert sandbox_only[:profiles]["harklight-sandbox"] ==
               [key_path: "/protected/sandbox-key", key_id: "SANDBOX001"]

      refute Map.has_key?(sandbox_only[:profiles], "harklight-production")

      with_env(
        %{
          "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => "/protected/production-key",
          "ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID" => "PRODKEY001"
        },
        fn ->
          both_profiles = runtime_apns_config()

          assert both_profiles[:profiles]["harklight-sandbox"] ==
                   [key_path: "/protected/sandbox-key", key_id: "SANDBOX001"]

          assert both_profiles[:profiles]["harklight-production"] ==
                   [key_path: "/protected/production-key", key_id: "PRODKEY001"]
        end
      )
    end)
  end

  test "runtime APNs attempt timeout is optional and accepts positive milliseconds" do
    variable = "ATTENTION_SERVER_APNS_ATTEMPT_TIMEOUT_MS"

    with_env(@server_env, fn ->
      refute Keyword.has_key?(runtime_apns_config(), :attempt_timeout_ms)

      with_env(%{variable => "1500"}, fn ->
        assert runtime_apns_config()[:attempt_timeout_ms] == 1_500
      end)

      with_env(%{variable => "0"}, fn ->
        refute Keyword.has_key?(runtime_apns_config(), :attempt_timeout_ms)
      end)
    end)
  end

  test "push relay URL is opt-in and read independently of Apple keys", %{certs: certs} do
    with_env(@server_env, fn ->
      refute Keyword.has_key?(runtime_server_opts(certs), :push_relay_url)

      with_env(%{"ATTENTION_SERVER_PUSH_RELAY_URL" => "https://relay.example"}, fn ->
        opts = runtime_server_opts(certs)
        assert opts[:push_relay_url] == "https://relay.example"
        assert opts[:apns][:team_id] == "TESTTEAMID"

        with_env(
          %{
            "ATTENTION_SERVER_APNS_KEY_PATH" => nil,
            "ATTENTION_SERVER_APNS_KEY_ID" => nil,
            "ATTENTION_SERVER_APNS_TEAM_ID" => nil
          },
          fn ->
            opts = runtime_server_opts(certs)
            refute Keyword.has_key?(opts, :apns)
            assert opts[:push_relay_url] == "https://relay.example"
          end
        )
      end)
    end)
  end

  test "partial APNs settings disable push without stopping core listeners", %{certs: certs} do
    env =
      Map.put(
        @server_env,
        "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH",
        "/protected/production-key"
      )

    with_env(env, fn ->
      server_opts = runtime_server_opts(certs)
      refute Keyword.has_key?(server_opts, :apns)

      server = start_server(server_opts)
      refute_child(server, :push_dispatcher)
      assert_core_listeners(server)
    end)
  end

  test "missing dashboard settings disable only the dashboard listener", %{certs: certs} do
    env =
      @server_env
      |> Map.put("ATTENTION_SERVER_DASHBOARD_PORT", "18448")
      |> Map.put("ATTENTION_SERVER_APNS_KEY_PATH", nil)
      |> Map.put("ATTENTION_SERVER_APNS_KEY_ID", nil)
      |> Map.put("ATTENTION_SERVER_APNS_TEAM_ID", nil)

    with_env(env, fn ->
      server = start_server(runtime_server_opts(certs))
      refute_child(server, :dashboard_https)
      assert_core_listeners(server)
    end)
  end

  defp runtime_apns_config do
    config = Config.Reader.read!(@config, env: :test)
    server = config |> Keyword.fetch!(:attention_server) |> Keyword.fetch!(:server)
    Keyword.fetch!(server, :apns)
  end

  defp runtime_server_opts(certs) do
    dir_name = "harklight-runtime-config-#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), dir_name)
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    config = Config.Reader.read!(@config, env: :test)
    server = config |> Keyword.fetch!(:attention_server) |> Keyword.fetch!(:server)

    Keyword.merge(server,
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      enroll_port: 0,
      owner_port: 0,
      relay_name: String.to_atom("runtime_config_#{System.unique_integer([:positive])}")
    )
  end

  defp start_server(opts) do
    {:ok, server} = Server.start_link(opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    server
  end

  defp assert_core_listeners(server) do
    assert Server.port(server) > 0
    assert Server.enroll_port(server) > 0
    assert Server.owner_port(server) > 0
    assert is_pid(Server.relay(server))
  end

  defp refute_child(server, id) do
    refute Enum.any?(Supervisor.which_children(server), fn {child_id, pid, _, _} ->
             child_id == id and is_pid(pid)
           end)
  end

  defp with_env(values, fun) do
    previous = Map.new(values, fn {key, _value} -> {key, System.get_env(key)} end)

    Enum.each(values, fn
      {key, nil} -> System.delete_env(key)
      {key, value} -> System.put_env(key, value)
    end)

    try do
      fun.()
    after
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end
  end
end

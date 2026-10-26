defmodule AttentionServer.APNSRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Server, TLSFixtures}

  @config Path.expand("../config/runtime.exs", __DIR__)
  @server_env %{
    "ATTENTION_SERVER_ENABLE" => "1",
    "ATTENTION_SERVER_STORE_PATH" => "/tmp/harklight-config-test/relay.sqlite3",
    "ATTENTION_SERVER_CERTFILE" => "/tmp/harklight-config-test/server.pem",
    "ATTENTION_SERVER_KEYFILE" => "/tmp/harklight-config-test/server.key",
    "ATTENTION_SERVER_CACERTFILE" => "/tmp/harklight-config-test/ca.pem",
    "ATTENTION_SERVER_PORT" => "18443",
    "ATTENTION_SERVER_ENROLL_PORT" => "18444",
    "ATTENTION_SERVER_OWNER_PORT" => "18445",
    "ATTENTION_SERVER_AGENT_PORT" => "18446",
    "ATTENTION_SERVER_APNS_KEY_PATH" => "/protected/sandbox-key",
    "ATTENTION_SERVER_APNS_KEY_ID" => "SANDBOX001",
    "ATTENTION_SERVER_APNS_TEAM_ID" => "TESTTEAMID",
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => nil,
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID" => nil,
    "ATTENTION_SERVER_DEPLOYMENT_MODE" => nil,
    "ATTENTION_SERVER_UPLOAD_CAP_BYTES" => nil,
    "ATTENTION_SERVER_ORIGIN_PORT" => "18447",
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
    "ATTENTION_SERVER_AGENT_BIND" => nil,
    "ATTENTION_SERVER_JOIN_NAME" => nil
  }

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
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
      agent_port: 0,
      origin_port: 0,
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
    assert Server.agent_port(server) > 0
    assert Server.origin_port(server) > 0
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

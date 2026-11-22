defmodule AttentionServer.RetiredListenersTest do
  # The protocol 0.1 agent (/agent/v1) and origin (/origin/v1) listeners are
  # retired: their configuration is ignored, nothing binds their ports, and the
  # shared device port serves only client API v2.
  use ExUnit.Case, async: false

  alias AttentionServer.{DeviceIdentity, HeadlessTLS, Relay, Server, TLSFixtures}

  @config Path.expand("../config/runtime.exs", __DIR__)
  @server_env %{
    "ATTENTION_SERVER_ENABLE" => "1",
    "ATTENTION_SERVER_STORE_PATH" => "/tmp/harklight-retired-listeners/relay.sqlite3",
    "ATTENTION_SERVER_CERTFILE" => "/tmp/harklight-retired-listeners/server.pem",
    "ATTENTION_SERVER_KEYFILE" => "/tmp/harklight-retired-listeners/server.key",
    "ATTENTION_SERVER_CACERTFILE" => "/tmp/harklight-retired-listeners/ca.pem",
    "ATTENTION_SERVER_PORT" => "18443",
    "ATTENTION_SERVER_ENROLL_PORT" => "18444",
    "ATTENTION_SERVER_OWNER_PORT" => "18445",
    "ATTENTION_SERVER_BIND" => nil,
    "ATTENTION_SERVER_ENROLL_BIND" => nil,
    "ATTENTION_SERVER_OWNER_BIND" => nil,
    "ATTENTION_SERVER_JOIN_NAME" => nil,
    "ATTENTION_SERVER_JEV_SCORE_RECORD_PATH" => nil,
    "ATTENTION_SERVER_FEATURE_FLAGS_PATH" => nil,
    "ATTENTION_SERVER_APNS_KEY_PATH" => nil,
    "ATTENTION_SERVER_APNS_KEY_ID" => nil,
    "ATTENTION_SERVER_APNS_TEAM_ID" => nil,
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
    "ATTENTION_SERVER_DASHBOARD_KEYFILE" => nil
  }
  @retired_keys [:agent_port, :agent_ip, :origin_port, :origin_ip]

  setup_all do
    certs = TLSFixtures.create!()
    TLSFixtures.make_self_signed_client!(certs, "retired-agent")
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "runtime configuration ignores retired agent and origin listener variables" do
    retired = %{
      "ATTENTION_SERVER_AGENT_PORT" => "18446",
      "ATTENTION_SERVER_AGENT_BIND" => "127.0.0.1",
      "ATTENTION_SERVER_ORIGIN_PORT" => "18447",
      "ATTENTION_SERVER_ORIGIN_BIND" => "127.0.0.1"
    }

    malformed = Map.new(retired, fn {name, _value} -> {name, "not-a-port"} end)
    unset = Map.new(retired, fn {name, _value} -> {name, nil} end)

    for retired_env <- [retired, malformed, unset] do
      opts = with_env(Map.merge(@server_env, retired_env), &runtime_server_opts/0)
      assert opts[:port] == 18_443
      assert opts[:owner_port] == 18_445
      assert Enum.filter(@retired_keys, &Keyword.has_key?(opts, &1)) == []
    end
  end

  test "retired port options start no agent or origin listener and bind neither port",
       %{certs: certs} do
    agent_port = free_port()
    origin_port = free_port()

    server =
      start_server(
        server_opts(certs,
          agent_port: agent_port,
          agent_ip: {127, 0, 0, 1},
          origin_port: origin_port,
          origin_ip: {127, 0, 0, 1}
        )
      )

    ids = for {id, pid, _, _} <- Supervisor.which_children(server), is_pid(pid), do: id
    assert Enum.sort(ids) == [:client_api, :client_api_feed_shutdown, :owner_tls, :relay]
    assert Code.ensure_loaded?(Server)
    refute function_exported?(Server, :agent_port, 1)
    refute function_exported?(Server, :origin_port, 1)

    for port <- [agent_port, origin_port] do
      assert {:ok, socket} = :gen_tcp.listen(port, ip: {127, 0, 0, 1}, reuseaddr: true)
      :gen_tcp.close(socket)
    end
  end

  test "the device port answers retired agent and origin routes with not_found",
       %{certs: certs} do
    server = start_server(server_opts(certs, []))
    relay = Server.relay(server)
    {:ok, person} = Relay.operator_bootstrap(relay, TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, owner} = DeviceIdentity.from_certificate(TLSFixtures.certificate_der!(certs, "a1"))
    {:ok, _} = Relay.setup_device_certificate(relay, person, der(certs, "a2"))
    {:ok, agent} = DeviceIdentity.from_certificate(der(certs, "retired-agent"))

    assert :ok =
             Relay.owner_register_agent(
               relay,
               person,
               owner,
               "00000000-0000-4000-8000-0000000000c1",
               agent
             )

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: Server.port(server)
    }

    routes = [
      {"POST", "/agent/v1", ~s({"v":1,"type":"search"})},
      {"POST", "/origin/v1", ~s({"v":1,"type":"coverage"})},
      {"POST", "/origin/v1/upload", "bytes"},
      {"GET", "/origin/v1/files/u1", nil},
      {"GET", "/origin/v1/stream?session=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", nil}
    ]

    for name <- ["a2", "retired-agent"],
        {method, path, body} <- routes do
      identity = {Path.join(certs, "#{name}.pem"), Path.join(certs, "#{name}.key")}
      headers = [{"content-type", "application/json"}]

      assert {:ok, 404, response_headers, %{"error" => "not_found"}} =
               HeadlessTLS.v2_request(endpoint, identity, method, path, headers, body),
             "#{name} #{method} #{path}"

      assert List.keymember?(response_headers, "request-id", 0)
    end

    assert {:ok, 200, %{"items" => []}} =
             HeadlessTLS.v2_get(
               endpoint,
               {Path.join(certs, "retired-agent.pem"), Path.join(certs, "retired-agent.key")},
               "/v2/search"
             )
  end

  defp runtime_server_opts do
    @config
    |> Config.Reader.read!(env: :test)
    |> Keyword.fetch!(:attention_server)
    |> Keyword.fetch!(:server)
  end

  defp server_opts(certs, extra) do
    dir = Path.join(System.tmp_dir!(), "harklight-retired-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    [
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      owner_port: 0,
      enrollment_enabled: false,
      relay_name: :"retired_listeners_#{System.unique_integer([:positive])}"
    ] ++ extra
  end

  defp start_server(opts) do
    {:ok, server} = Server.start_link(opts)
    Process.unlink(server)
    on_exit(fn -> if Process.alive?(server), do: Supervisor.stop(server) end)
    server
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp der(certs, name), do: TLSFixtures.certificate_der!(certs, name)

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

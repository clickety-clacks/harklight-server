defmodule AttentionServer.OptionalStartupTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{Server, TLSFixtures}

  @public_origin "https://relay.test"

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  test "hosted routes stay off when SaaS has no finite upload cap", %{certs: certs} do
    opts =
      server_opts(certs)
      |> Keyword.merge(
        deployment_mode: :saas,
        upload_cap: :none,
        public_base_url: @public_origin,
        hosted_port: 0,
        hosted_account_url: "/account"
      )

    server = start_server(opts)

    refute_child(server, :hosted_http)
    assert_core_listeners(server)
  end

  test "an unreadable APNs key disables push without stopping core listeners", %{certs: certs} do
    key_path =
      Path.join(
        System.tmp_dir!(),
        "harklight-missing-apns-key-#{System.unique_integer([:positive])}.p8"
      )

    refute File.exists?(key_path)

    apns = [
      team_id: "TESTTEAMID",
      profiles: %{
        "harklight-sandbox" => [
          key_path: key_path,
          key_id: "SANDBOX001"
        ]
      }
    ]

    server = start_server(Keyword.put(server_opts(certs), :apns, apns))

    refute_child(server, :push_dispatcher)
    refute Map.fetch!(:sys.get_state(Server.relay(server)), :push_enabled)
    assert_core_listeners(server)
  end

  test "invalid guided-join settings leave the core listeners available", %{certs: certs} do
    server = start_server(Keyword.put(server_opts(certs), :join_name, "bad/name"))

    assert is_nil(Map.get(:sys.get_state(Server.relay(server)), :join_configuration))
    assert_core_listeners(server)
  end

  test "enrollment listener startup failure leaves other listeners available", %{certs: certs} do
    opts = Keyword.put(server_opts(certs), :enroll_ip, :invalid)
    server = start_server(opts)

    refute_child(server, :enrollment_https)
    assert Server.port(server) > 0
    assert Server.owner_port(server) > 0
    assert Server.agent_port(server) > 0
    assert Server.origin_port(server) > 0
    assert is_pid(Server.relay(server))
  end

  defp server_opts(certs) do
    dir_name = "harklight-optional-startup-#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), dir_name)
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    [
      store_path: Path.join(dir, "relay.sqlite3"),
      certfile: Path.join(certs, "server.pem"),
      keyfile: Path.join(certs, "server.key"),
      cacertfile: Path.join(certs, "ca.pem"),
      port: 0,
      enroll_port: 0,
      owner_port: 0,
      agent_port: 0,
      origin_port: 0,
      relay_name: String.to_atom("optional_startup_#{System.unique_integer([:positive])}")
    ]
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
end

defmodule AttentionServer.HeadlessTLSWaitTest do
  # The owner, agent and device CLIs wait for the server's real answer. A slow
  # commit must not be reported as a failure while the server is still working.
  use ExUnit.Case, async: true

  alias AttentionServer.{HeadlessTLS, TLSFixtures}

  @delay_ms 3_500

  setup_all do
    certs = TLSFixtures.create!()
    on_exit(fn -> File.rm_rf!(certs) end)
    %{certs: certs}
  end

  defp slow_server(certs, reply) do
    {:ok, listen} =
      :ssl.listen(0,
        versions: [:"tlsv1.3"],
        certfile: String.to_charlist(Path.join(certs, "server.pem")),
        keyfile: String.to_charlist(Path.join(certs, "server.key")),
        cacertfile: String.to_charlist(Path.join(certs, "ca.pem")),
        verify: :verify_peer,
        fail_if_no_peer_cert: true,
        active: false,
        mode: :binary,
        reuseaddr: true
      )

    {:ok, {_, port}} = :ssl.sockname(listen)

    server =
      Task.async(fn ->
        {:ok, transport} = :ssl.transport_accept(listen)
        {:ok, socket} = :ssl.handshake(transport)
        {:ok, _request} = :ssl.recv(socket, 0)
        Process.sleep(@delay_ms)
        :ok = :ssl.send(socket, reply)
        :ssl.close(socket)
        :ssl.close(listen)
      end)

    endpoint = %{
      address: {127, 0, 0, 1},
      name: "relay.test",
      ca_file: Path.join(certs, "ca.pem"),
      port: port
    }

    {endpoint, server}
  end

  test "an owner request receives an answer that arrives after several seconds",
       %{certs: certs} do
    {endpoint, server} = slow_server(certs, ~s({"type":"ok","late":true}\n))

    assert {:ok, %{"type" => "ok", "late" => true}} =
             HeadlessTLS.owner_request(
               endpoint,
               Path.join(certs, "a1.pem"),
               Path.join(certs, "a1.key"),
               %{"type" => "agent_list"}
             )

    Task.await(server, @delay_ms + 10_000)
  end

  test "a client API v2 request receives a response that arrives after several seconds",
       %{certs: certs} do
    body = ~s({"items":[]})

    {endpoint, server} =
      slow_server(
        certs,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" <>
          "Content-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n" <> body
      )

    assert {:ok, 200, %{"items" => []}} =
             HeadlessTLS.v2_get(
               endpoint,
               {Path.join(certs, "a1.pem"), Path.join(certs, "a1.key")},
               "/v2/search"
             )

    Task.await(server, @delay_ms + 10_000)
  end
end

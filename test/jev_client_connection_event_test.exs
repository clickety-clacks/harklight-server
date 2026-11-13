defmodule AttentionServer.JevClientConnectionEventTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{JevClient, TLSFixtures}

  setup_all do
    certs = TLSFixtures.create!(server_san: "DNS:localhost")
    :ok = :public_key.cacerts_load(Path.join(certs, "ca.pem"))
    previous = System.get_env("ATTENTION_SERVER_JEV_KEY_PATH")
    path = Path.join(certs, "synthetic-provider-token")
    File.write!(path, "synthetic-test-token")
    System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", path)

    on_exit(fn ->
      if previous,
        do: System.put_env("ATTENTION_SERVER_JEV_KEY_PATH", previous),
        else: System.delete_env("ATTENTION_SERVER_JEV_KEY_PATH")

      :public_key.cacerts_clear()
      :public_key.cacerts_load()
      File.rm_rf!(certs)
    end)

    %{certs: certs}
  end

  @tag timeout: 10_000
  test "a peer closing during TLS connection setup ends the ranking call" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    parent = self()

    peer =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        send(parent, {:tcp_accepted, self()})

        receive do
          :close -> :gen_tcp.close(socket)
        end
      end)

    on_exit(fn ->
      :gen_tcp.close(listener)
      Process.exit(peer, :kill)
    end)

    task = Task.async(fn -> call(port) end)
    assert_receive {:tcp_accepted, ^peer}, 2_000
    send(peer, :close)
    assert Task.await(task, 2_000) == {:error, :unavailable}
  end

  @tag timeout: 10_000
  test "a peer closing after receiving a ranking request ends the call", %{certs: certs} do
    parent = self()

    {:ok, listener} =
      :ssl.listen(0,
        ip: {127, 0, 0, 1},
        active: false,
        mode: :binary,
        packet: :raw,
        reuseaddr: true,
        versions: [:"tlsv1.3"],
        alpn_preferred_protocols: ["h2"],
        certfile: String.to_charlist(Path.join(certs, "server.pem")),
        keyfile: String.to_charlist(Path.join(certs, "server.key"))
      )

    {:ok, {_, port}} = :ssl.sockname(listener)

    peer =
      spawn(fn ->
        {:ok, transport} = :ssl.transport_accept(listener, 5_000)
        {:ok, socket} = :ssl.handshake(transport, 5_000)
        {:ok, _request_bytes} = :ssl.recv(socket, 0, 5_000)
        send(parent, {:ranking_request_received, self()})

        receive do
          :close -> :ssl.close(socket)
        end
      end)

    on_exit(fn ->
      :ssl.close(listener)
      Process.exit(peer, :kill)
    end)

    task = Task.async(fn -> call(port) end)
    assert_receive {:ranking_request_received, ^peer}, 5_000
    send(peer, :close)
    assert Task.await(task, 2_000) == {:error, :unavailable}
  end

  defp call(port) do
    item = %{
      identity: {"app:test", "connection-event"},
      message: %{"title" => "Synthetic peer close"}
    }

    {:ok, request} =
      JevClient.compose([item], "https://localhost:#{port}/v1/systemone", "test-model")

    JevClient.evaluate(request)
  end
end

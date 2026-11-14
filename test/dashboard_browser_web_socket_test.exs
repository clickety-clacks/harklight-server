defmodule AttentionServer.DashboardBrowserWebSocketTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{DashboardBrowserWebSocket, DashboardHTTPS, JSON, Relay, TLSFixtures}

  defmodule InactiveBackend do
    def open_browser_connection(_state, _request), do: {:error, :inactive_account}
  end

  defmodule InactiveFrameBackend do
    def open_browser_connection(_state, _request),
      do: {:ok, %{relay: self(), browser_session_id: <<0::256>>, session_id: make_ref()}}

    def browser_session_request_line(_state, _request), do: {:error, :inactive_account}
  end

  test "same-origin upgrade needs no subprotocol and ignores one that is offered", _context do
    with_dashboard(fn port ->
      for offered <- [nil, "attention-relay/1"] do
        {response, close} =
          upgrade(port, "https://browser.example.test:#{port}",
            path: "/browser/relay?recipient=00000000-0000-4000-8000-000000000001",
            subprotocol: offered
          )

        assert response =~ "HTTP/1.1 101 Switching Protocols"
        refute String.downcase(response) =~ "sec-websocket-protocol:"
        assert close == {4401, "session_ended"}
      end
    end)
  end

  test "wrong origin is refused before the WebSocket handshake", _context do
    with_dashboard(fn port ->
      {response, _tail} =
        upgrade(port, "https://elsewhere.example",
          path: "/browser/relay?recipient=00000000-0000-4000-8000-000000000001"
        )

      assert response =~ "HTTP/1.1 403"
      refute response =~ "101 Switching Protocols"
    end)
  end

  test "a missing recipient closes an accepted upgrade with policy code 1008", _context do
    with_dashboard(fn port ->
      {response, close} =
        upgrade(port, "https://browser.example.test:#{port}", path: "/browser/relay")

      assert response =~ "HTTP/1.1 101 Switching Protocols"
      assert close == {1008, "invalid_recipient"}
    end)
  end

  defp with_dashboard(fun) do
    certs = TLSFixtures.create!()
    port = available_port()
    {:ok, relay} = Relay.start_link()

    {:ok, listener} =
      DashboardHTTPS.start_link(
        relay: relay,
        deployment_mode: :saas,
        port: port,
        ip: {127, 0, 0, 1},
        certfile: Path.join(certs, "server.pem"),
        keyfile: Path.join(certs, "server.key"),
        skill_path: __ENV__.file,
        public_origin: "https://browser.example.test:#{port}"
      )

    Process.unlink(listener)

    try do
      fun.(port)
    after
      if Process.alive?(listener), do: Supervisor.stop(listener)
      if Process.alive?(relay), do: GenServer.stop(relay)
      File.rm_rf!(certs)
    end
  end

  test "inactive account outcomes use the contract close and frame reason" do
    opts = [
      backend: {InactiveBackend, nil},
      cookie: "signed-in",
      recipient: "00000000-0000-4000-8000-000000000001"
    ]

    assert {:stop, :normal, {4403, "inactive_account"}, _state} =
             DashboardBrowserWebSocket.init(opts)

    frame_opts = Keyword.put(opts, :backend, {InactiveFrameBackend, nil})
    assert {:ok, state} = DashboardBrowserWebSocket.init(frame_opts)

    assert {:push, {:text, reply}, _state} =
             DashboardBrowserWebSocket.handle_in(
               {JSON.encode!(%{"v" => 1, "type" => "answer", "ref" => "frame-1"}), opcode: :text},
               state
             )

    assert {:ok,
            %{
              "v" => 1,
              "type" => "error",
              "reason" => "inactive_account",
              "ref" => "frame-1"
            }} = JSON.decode_line(reply)
  end

  test "idle browser connections send a ping inside the hosted proxy window" do
    state = %{backend: nil}

    assert {:push, {:ping, <<>>}, ^state} =
             DashboardBrowserWebSocket.handle_info(:browser_keepalive, state)
  end

  test "a fenced browser session closes with the contract reason" do
    state = %{backend: nil}

    assert {:stop, :normal, {4401, "session_ended"}, ^state} =
             DashboardBrowserWebSocket.handle_info({:attention_fenced, make_ref()}, state)
  end

  defp upgrade(port, origin, opts) do
    {:ok, socket} =
      :ssl.connect(
        {127, 0, 0, 1},
        port,
        [verify: :verify_none, active: false, mode: :binary],
        5_000
      )

    path = Keyword.fetch!(opts, :path)

    protocol_header =
      case Keyword.get(opts, :subprotocol) do
        nil -> []
        offered -> ["Sec-WebSocket-Protocol: ", offered, "\r\n"]
      end

    request = [
      "GET ",
      path,
      " HTTP/1.1\r\nHost: browser.example.test:",
      Integer.to_string(port),
      "\r\nOrigin: ",
      origin,
      "\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n",
      "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
      protocol_header,
      "\r\n"
    ]

    :ok = :ssl.send(socket, request)
    {response, tail} = receive_headers(socket, <<>>)

    close =
      if response =~ "101 Switching Protocols" do
        read_close(socket, tail)
      else
        tail
      end

    :ssl.close(socket)
    {response, close}
  end

  defp receive_headers(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {offset, 4} ->
        header_size = offset + 4
        <<headers::binary-size(header_size), remainder::binary>> = buffer
        {headers, remainder}

      :nomatch ->
        {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
        receive_headers(socket, buffer <> bytes)
    end
  end

  defp read_close(socket, buffer) do
    case buffer do
      <<0x88, length, rest::binary>> when length < 126 ->
        if byte_size(rest) >= length do
          <<payload::binary-size(length), _tail::binary>> = rest
          <<code::16, reason::binary>> = payload
          {code, reason}
        else
          {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
          read_close(socket, buffer <> bytes)
        end

      _ ->
        {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
        read_close(socket, buffer <> bytes)
    end
  end

  defp available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :gen_tcp.close(socket)
    port
  end
end

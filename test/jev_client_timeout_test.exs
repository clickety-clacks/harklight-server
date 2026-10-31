defmodule AttentionServer.JevClientTimeoutTest.SilentProvider do
  def init(test_pid), do: test_pid

  def call(conn, test_pid) do
    {:ok, _body, conn} = Plug.Conn.read_body(conn)
    send(test_pid, :request_received_over_tls)

    receive do
      :respond -> Plug.Conn.send_resp(conn, 200, "{}")
    end
  end
end

defmodule AttentionServer.JevClientTimeoutTest do
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

  @tag timeout: 15_000
  test "a TCP peer that never completes TLS reaches the five second connect ceiling" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    parent = self()

    peer =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        send(parent, :tcp_accepted)

        receive do
          :stop -> :gen_tcp.close(socket)
        end
      end)

    on_exit(fn ->
      :gen_tcp.close(listener)
      Process.exit(peer, :kill)
    end)

    task = Task.async(fn -> timed_call(port) end)
    assert_receive :tcp_accepted, 2_000
    {result, elapsed} = Task.await(task, 8_000)
    IO.puts("jev_timeout phase=connect elapsed_ms=#{elapsed} outcome=unavailable")
    assert result == {:error, :unavailable}
    assert elapsed >= 4_500 and elapsed < 7_000
    assert Process.alive?(peer)
  end

  @tag timeout: 40_000
  test "a TLS-complete request without an answer reaches the separate thirty second ceiling", %{
    certs: certs
  } do
    provider =
      start_supervised!(
        {Bandit,
         plug: {AttentionServer.JevClientTimeoutTest.SilentProvider, self()},
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: 0,
         certfile: Path.join(certs, "server.pem"),
         keyfile: Path.join(certs, "server.key"),
         startup_log: false}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(provider)
    task = Task.async(fn -> timed_call(port) end)
    assert_receive :request_received_over_tls, 3_000
    received = System.monotonic_time(:millisecond)
    {result, elapsed} = Task.await(task, 34_000)
    answer_wait = System.monotonic_time(:millisecond) - received

    IO.puts(
      "jev_timeout phase=answer elapsed_ms=#{elapsed} after_request_ms=#{answer_wait} outcome=unavailable"
    )

    assert result == {:error, :unavailable}
    assert answer_wait >= 29_000 and answer_wait < 33_000
    assert elapsed >= 29_000 and elapsed < 34_000
  end

  defp timed_call(port) do
    item = %{identity: {"app:test", "timeout"}, message: %{"title" => "Synthetic timeout"}}

    {:ok, request} =
      JevClient.compose([item], "https://localhost:#{port}/v1/systemone", "test-model")

    started = System.monotonic_time(:millisecond)
    result = JevClient.evaluate(request)
    {result, System.monotonic_time(:millisecond) - started}
  end
end

defmodule AttentionServer.TLSRelay do
  @moduledoc """
  Bounded TLS 1.3 newline-frame entry for enrolled device relay sessions.

  This adapter proves S1 identity and D4 socket fencing over the existing
  serialized relay core. It is not yet the contracted `/relay` WebSocket or
  owner-approved enrollment interface. The configured CA validates the peer
  certificate chain; current enrollment of the proven leaf SPKI is a second,
  mandatory authorization check before any application frame is processed.
  """

  use GenServer

  alias AttentionServer.{DeviceIdentity, EnrolledLeafVerify, Relay}

  @handshake_timeout 3_000
  @poll_ms 100

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <- Keyword.fetch(opts, :relay),
         {:ok, certfile} <- Keyword.fetch(opts, :certfile),
         {:ok, keyfile} <- Keyword.fetch(opts, :keyfile),
         {:ok, cacertfile} <- Keyword.fetch(opts, :cacertfile),
         {:ok, listener} <- listen(opts, relay, certfile, keyfile, cacertfile),
         {:ok, {_ip, port}} <- :ssl.sockname(listener) do
      case start_workers(listener, relay, self()) do
        {:ok, supervisor} ->
          {:ok, %{listener: listener, supervisor: supervisor, relay: relay, port: port}}

        error ->
          :ssl.close(listener)
          {:stop, error}
      end
    else
      error -> {:stop, error}
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def terminate(_reason, state) do
    # Admission is fenced in Relay before any worker can complete a late
    # handshake. The task supervisor then waits for all accepted workers.
    try do
      Relay.unregister_tls_listener(state.relay, self())
    catch
      :exit, _ -> :ok
    end

    :ssl.close(state.listener)
    Supervisor.stop(state.supervisor, :normal)
  end

  defp start_workers(listener, relay, listener_id) do
    case Relay.register_tls_listener(relay, listener_id) do
      :ok ->
        case Task.Supervisor.start_link() do
          {:ok, supervisor} ->
            case Task.Supervisor.start_child(supervisor, fn ->
                   accept_loop(listener, relay, listener_id, supervisor)
                 end) do
              {:ok, _acceptor} ->
                {:ok, supervisor}

              error ->
                Supervisor.stop(supervisor, :normal)
                Relay.unregister_tls_listener(relay, listener_id)
                error
            end

          error ->
            Relay.unregister_tls_listener(relay, listener_id)
            error
        end

      error ->
        error
    end
  end

  defp listen(opts, relay, certfile, keyfile, cacertfile) do
    :ssl.listen(Keyword.get(opts, :port, 0),
      ip: Keyword.get(opts, :ip, {127, 0, 0, 1}),
      active: false,
      mode: :binary,
      packet: :line,
      packet_size: 2 * 1024 * 1024,
      send_timeout: 1_000,
      send_timeout_close: true,
      reuseaddr: true,
      versions: [:"tlsv1.3"],
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.device(relay),
      fail_if_no_peer_cert: true,
      cacertfile: String.to_charlist(cacertfile),
      certfile: String.to_charlist(certfile),
      keyfile: String.to_charlist(keyfile),
      session_tickets: :disabled,
      early_data: :disabled
    )
  end

  defp accept_loop(listener, relay, listener_id, supervisor) do
    case :ssl.transport_accept(listener) do
      {:ok, socket} ->
        case Task.Supervisor.start_child(supervisor, fn -> await_socket(relay, listener_id) end) do
          {:ok, worker} ->
            case :ssl.controlling_process(socket, worker) do
              :ok ->
                send(worker, {:socket, socket})

              _ ->
                send(worker, :abort)
                :ssl.close(socket)
            end

          _ ->
            :ssl.close(socket)
        end

        accept_loop(listener, relay, listener_id, supervisor)

      {:error, :closed} ->
        :ok

      {:error, _} ->
        Process.sleep(100)
        accept_loop(listener, relay, listener_id, supervisor)
    end
  end

  defp await_socket(relay, listener_id) do
    receive do
      {:socket, socket} -> serve(socket, relay, listener_id)
      :abort -> :ok
    end
  end

  defp serve(socket, relay, listener_id) do
    try do
      with {:ok, tls_socket} <- :ssl.handshake(socket, @handshake_timeout),
           {:ok, certificate} <- :ssl.peercert(tls_socket),
           {:ok, key} <- DeviceIdentity.from_certificate(certificate),
           {:ok, session} <- Relay.open_tls_session(relay, key, tls_socket, listener_id) do
        read_loop(tls_socket, relay, session)
        Relay.close_session(relay, session)
      end
    catch
      # A stopped Relay ends its socket workers during shutdown/restart.
      :exit, _ -> :ok
    after
      :ssl.close(socket)
    end
  end

  defp read_loop(socket, relay, session) do
    receive do
      {:attention_fenced, ^session} ->
        :ok
    after
      0 ->
        case :ssl.recv(socket, 0, @poll_ms) do
          {:ok, line} ->
            case Relay.session_request_line(relay, session, line) do
              {:ok, frames} ->
                case Relay.session_send(relay, session, frames) do
                  :ok -> drain_then_read(socket, relay, session)
                  _ -> :ok
                end

              _ ->
                :ok
            end

          {:error, :timeout} ->
            drain_then_read(socket, relay, session)

          _ ->
            :ok
        end
    end
  end

  defp drain_then_read(socket, relay, session) do
    case Relay.session_drain(relay, session) do
      {:ok, frames} ->
        case Relay.session_send(relay, session, frames) do
          :ok -> read_loop(socket, relay, session)
          _ -> :ok
        end

      {:error, :resync_required} ->
        read_loop(socket, relay, session)

      _ ->
        :ok
    end
  end
end

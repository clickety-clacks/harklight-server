defmodule AttentionServer.AgentTLS do
  @moduledoc """
  One-request TLS 1.3 channel for separately enrolled service keys. A device,
  owner, or unknown certificate never becomes an agent by supplying wire IDs.
  The Relay validates current authority and sends each reply in one serialized
  boundary, so queued content cannot escape after a grant change or revocation.
  """

  use GenServer

  alias AttentionServer.{DeviceIdentity, Diagnostics, EnrolledLeafVerify, JSON, Relay}

  @timeout 5_000
  @max_line 2_400_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :agent_tls,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :agent_tls,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :agent_tls,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :agent_tls,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :agent_tls,
             :listener_start,
             listen(opts, relay, certfile, keyfile, cacertfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :agent_tls,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ),
         {:ok, supervisor} <-
           Diagnostics.result(
             :agent_tls,
             :worker_start,
             Task.Supervisor.start_link(),
             :unavailable
           ) do
      case Diagnostics.result(
             :agent_tls,
             :worker_start,
             Task.Supervisor.start_child(supervisor, fn ->
               accept_loop(listener, relay, supervisor)
             end),
             :unavailable
           ) do
        {:ok, _} ->
          {:ok, %{listener: listener, supervisor: supervisor, port: port}}

        error ->
          :ssl.close(listener)
          Supervisor.stop(supervisor, :normal)
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
    :ssl.close(state.listener)
    Supervisor.stop(state.supervisor, :normal)
  end

  defp listen(opts, relay, certfile, keyfile, cacertfile) do
    :ssl.listen(Keyword.get(opts, :port, 0),
      ip: Keyword.get(opts, :ip, {127, 0, 0, 1}),
      active: false,
      mode: :binary,
      packet: :line,
      packet_size: @max_line,
      send_timeout: 1_000,
      send_timeout_close: true,
      reuseaddr: true,
      versions: [:"tlsv1.3"],
      certfile: String.to_charlist(certfile),
      keyfile: String.to_charlist(keyfile),
      cacertfile: String.to_charlist(cacertfile),
      verify: :verify_peer,
      verify_fun: EnrolledLeafVerify.agent(relay),
      fail_if_no_peer_cert: true,
      session_tickets: :disabled,
      early_data: :disabled
    )
  end

  defp accept_loop(listener, relay, supervisor) do
    case :ssl.transport_accept(listener) do
      {:ok, socket} ->
        case Task.Supervisor.start_child(supervisor, fn -> await_socket(relay) end) do
          {:ok, worker} ->
            case :ssl.controlling_process(socket, worker) do
              :ok ->
                send(worker, {:socket, socket})

              {:error, reason} ->
                Diagnostics.failure(:agent_tls, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:agent_tls, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:agent_tls, :accept, reason, :retry_100ms)
        Process.sleep(100)
        accept_loop(listener, relay, supervisor)
    end
  end

  defp await_socket(relay) do
    receive do
      {:socket, socket} -> serve(socket, relay)
      :abort -> :ok
    end
  end

  defp serve(socket, relay) do
    try do
      with {:ok, tls} <-
             Diagnostics.result(
               :agent_tls,
               :tls_handshake,
               :ssl.handshake(socket, @timeout),
               :closed
             ),
           {:ok, certificate} <-
             Diagnostics.result(:agent_tls, :peer_certificate, :ssl.peercert(tls), :closed),
           {:ok, key} <-
             Diagnostics.result(
               :agent_tls,
               :identity,
               DeviceIdentity.from_certificate(certificate),
               :closed
             ),
           {:ok, _person, _id, revision} <-
             Diagnostics.result(
               :agent_tls,
               :authentication,
               Relay.agent_identity(relay, key),
               :closed
             ),
           {:ok, line} <-
             Diagnostics.result(:agent_tls, :request_read, :ssl.recv(tls, 0, @timeout), :closed) do
        request =
          case JSON.decode_line(line) do
            {:ok, value} when is_map(value) ->
              value

            {:error, reason} ->
              Diagnostics.failure(:agent_tls, :request_read, reason, :rejected)
              %{}

            other ->
              Diagnostics.failure(:agent_tls, :request_read, other, :rejected)
              %{}
          end

        _ =
          Diagnostics.result(
            :agent_tls,
            :request_dispatch,
            Relay.agent_send_request(relay, key, revision, request, tls),
            :automatic,
            Diagnostics.operation_id(request)
          )
      end
    rescue
      error ->
        Diagnostics.failure(:agent_tls, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:agent_tls, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end
end

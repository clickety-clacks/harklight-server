defmodule AttentionServer.OwnerTLS do
  @moduledoc """
  One-request TLS 1.3 owner-management channel for the headless owner tool.

  OTP proves the client certificate and validates its configured CA before an
  application line is read. Relay resolves the proven SPKI to the *current*
  owner/person/revision, serializes every mutation, and releases the response
  under that same revision fence. This channel never accepts a pending-device
  certificate as owner authority.
  """

  use GenServer

  alias AttentionServer.{DeviceIdentity, Diagnostics, EnrolledLeafVerify, GuidedJoin, JSON, Relay}

  @timeout 3_000
  @max_line 32 * 1024

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    with {:ok, relay} <-
           Diagnostics.result(
             :owner_tls,
             :configuration,
             Keyword.fetch(opts, :relay),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :owner_tls,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :owner_tls,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :owner_tls,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ),
         {:ok, listener} <-
           Diagnostics.result(
             :owner_tls,
             :listener_start,
             listen(opts, relay, certfile, keyfile, cacertfile),
             :unavailable
           ),
         {:ok, {_ip, port}} <-
           Diagnostics.result(
             :owner_tls,
             :listener_address,
             :ssl.sockname(listener),
             :unavailable
           ),
         {:ok, supervisor} <-
           Diagnostics.result(
             :owner_tls,
             :worker_start,
             Task.Supervisor.start_link(),
             :unavailable
           ) do
      case Diagnostics.result(
             :owner_tls,
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
      verify_fun: EnrolledLeafVerify.owner(relay),
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
                Diagnostics.failure(:owner_tls, :socket_control, reason, :closed)
                send(worker, :abort)
                :ssl.close(socket)
            end

          {:error, reason} ->
            Diagnostics.failure(:owner_tls, :worker_start, reason, :closed)
            :ssl.close(socket)
        end

        accept_loop(listener, relay, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Diagnostics.failure(:owner_tls, :accept, reason, :retry_100ms)
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
               :owner_tls,
               :tls_handshake,
               :ssl.handshake(socket, @timeout),
               :closed
             ),
           {:ok, certificate} <-
             Diagnostics.result(:owner_tls, :peer_certificate, :ssl.peercert(tls), :closed),
           {:ok, key} <-
             Diagnostics.result(
               :owner_tls,
               :identity,
               DeviceIdentity.from_certificate(certificate),
               :closed
             ),
           {:ok, person, revision} <-
             Diagnostics.result(
               :owner_tls,
               :authentication,
               Relay.owner_identity(relay, key),
               :closed
             ),
           {:ok, line} <-
             Diagnostics.result(:owner_tls, :request_read, :ssl.recv(tls, 0, @timeout), :closed) do
        reply =
          case JSON.decode_line(line) do
            {:ok, request} ->
              result = dispatch(relay, person, key, request)

              case result do
                %{"error" => reason} ->
                  Diagnostics.failure(
                    :owner_tls,
                    :request_dispatch,
                    safe_error(reason),
                    :rejected,
                    Diagnostics.operation_id(request)
                  )

                _ ->
                  :ok
              end

              result

            {:error, reason} ->
              Diagnostics.failure(:owner_tls, :request_read, reason, :rejected)
              %{"error" => "invalid_request"}
          end

        operation_id =
          case JSON.decode_line(line) do
            {:ok, request} -> Diagnostics.operation_id(request)
            _ -> nil
          end

        _ =
          Diagnostics.result(
            :owner_tls,
            :response_write,
            Relay.owner_send_line(relay, person, key, revision, tls, JSON.encode!(reply) <> "\n"),
            :outcome_unknown,
            operation_id
          )
      end
    rescue
      error ->
        Diagnostics.failure(:owner_tls, :relay_boundary, error, :outcome_unknown)
        :ok
    catch
      :exit, reason ->
        Diagnostics.failure(:owner_tls, :relay_boundary, reason, :outcome_unknown)
        :ok
    after
      :ssl.close(socket)
    end
  end

  defp dispatch(relay, person, key, %{"v" => 1, "person_id" => person} = request) do
    case request do
      %{"type" => "browser_sign_in_create"} ->
        if fields?(request, ~w(v type person_id)) do
          case owner_result(request, Relay.owner_browser_sign_in_create(relay, person, key)) do
            {:ok, credential} -> %{"ok" => true, "browser_sign_in" => credential}
            {:error, :capacity} -> %{"error" => "capacity"}
            {:error, :clock_unavailable} -> %{"error" => "unavailable"}
            {:error, :storage_unavailable} -> %{"error" => "unavailable"}
            _ -> not_found()
          end
        else
          invalid()
        end

      %{"type" => "invite_create", "request_id" => request_id} ->
        if fields?(request, ~w(v type person_id request_id)) do
          case owner_result(request, Relay.owner_invite_create(relay, person, key, request_id)) do
            {:ok, invitation} ->
              invitation_reply(relay, invitation)

            {:error, reason}
            when reason in [:expired, :used, :request_conflict, :capacity, :invalid_request] ->
              %{"error" => Atom.to_string(reason)}

            {:error, :storage_unavailable} ->
              %{"error" => "unavailable"}

            _ ->
              not_found()
          end
        else
          invalid()
        end

      %{"type" => "invite_create"} ->
        if fields?(request, ~w(v type person_id)) do
          case owner_result(request, Relay.owner_invite_create(relay, person, key)) do
            {:ok, invitation} -> invitation_reply(relay, invitation)
            {:error, :unavailable} -> %{"error" => "unavailable"}
            {:error, :capacity} -> %{"error" => "capacity"}
            _ -> not_found()
          end
        else
          invalid()
        end

      %{
        "type" => "pending_decline",
        "approval_code" => code,
        "expected_device_id" => expected_key,
        "expected_expires_at" => deadline
      } ->
        if fields?(
             request,
             ~w(v type person_id approval_code expected_device_id expected_expires_at)
           ) do
          case owner_result(
                 request,
                 Relay.owner_pending_decline(relay, person, key, code, expected_key, deadline)
               ) do
            :ok -> %{"ok" => true}
            _ -> not_found()
          end
        else
          invalid()
        end

      %{"type" => "pending_lookup", "approval_code" => code} ->
        if fields?(request, ~w(v type person_id approval_code)) do
          case owner_result(request, Relay.owner_pending_lookup(relay, person, key, code)) do
            {:ok, pending} ->
              %{
                "ok" => true,
                "label" => pending.label,
                "device_id" => pending.key,
                "expires_at" => pending.expires_at
              }

            _ ->
              not_found()
          end
        else
          invalid()
        end

      %{
        "type" => "pending_approve",
        "approval_code" => code,
        "expected_device_id" => expected_key,
        "expected_expires_at" => expected_expires_at,
        "sender_grants" => grants
      } ->
        if fields?(
             request,
             ~w(v type person_id approval_code expected_device_id expected_expires_at sender_grants)
           ) do
          case owner_result(
                 request,
                 Relay.owner_pending_approve(
                   relay,
                   person,
                   key,
                   code,
                   expected_key,
                   expected_expires_at,
                   grants
                 )
               ) do
            {:ok, device} -> %{"ok" => true, "device_id" => device}
            _ -> not_found()
          end
        else
          invalid()
        end

      %{"type" => "device_revoke", "device_id" => device} ->
        mutation(request, ~w(v type person_id device_id), fn ->
          Relay.owner_revoke_device(relay, person, key, device)
        end)

      %{"type" => "sender_grant", "device_id" => device, "sender_id" => sender} ->
        mutation(request, ~w(v type person_id device_id sender_id), fn ->
          Relay.owner_grant_sender(relay, person, key, device, sender)
        end)

      %{"type" => "sender_revoke", "device_id" => device, "sender_id" => sender} ->
        mutation(request, ~w(v type person_id device_id sender_id), fn ->
          Relay.owner_revoke_sender(relay, person, key, device, sender)
        end)

      %{
        "type" => "enrollment_read_grant",
        "connector_device_id" => connector,
        "grant_id" => grant,
        "source_instance_id" => source,
        "target" => target
      } ->
        mutation(
          request,
          ~w(v type person_id connector_device_id grant_id source_instance_id target),
          fn ->
            Relay.owner_grant_enrollment_read(
              relay,
              person,
              key,
              connector,
              grant,
              source,
              target
            )
          end
        )

      %{"type" => "enrollment_read_revoke", "grant_id" => grant} ->
        mutation(request, ~w(v type person_id grant_id), fn ->
          Relay.owner_revoke_enrollment_read(relay, person, key, grant)
        end)

      %{
        "type" => "maintainer_grant",
        "grant_id" => grant,
        "grantee_person_id" => grantee,
        "scopes" => scopes
      } ->
        mutation(request, ~w(v type person_id grant_id grantee_person_id scopes), fn ->
          Relay.owner_grant_maintainer(relay, person, key, grant, grantee, scopes)
        end)

      %{"type" => "maintainer_revoke", "grant_id" => grant} ->
        mutation(request, ~w(v type person_id grant_id), fn ->
          Relay.owner_revoke_maintainer(relay, person, key, grant)
        end)

      %{"type" => "maintainer_use"} ->
        %{"error" => "not_supported"}

      %{"type" => "agent_register", "agent_id" => id, "key_fingerprint" => agent_key} ->
        mutation(request, ~w(v type person_id agent_id key_fingerprint), fn ->
          Relay.owner_register_agent(relay, person, key, id, agent_key)
        end)

      %{
        "type" => "agent_grants_replace",
        "key_fingerprint" => agent_key,
        "expected_revision" => revision,
        "read_grants" => read,
        "respond_grants" => respond
      } ->
        mutation(
          request,
          ~w(v type person_id key_fingerprint expected_revision read_grants respond_grants),
          fn ->
            Relay.owner_replace_agent_grants(
              relay,
              person,
              key,
              agent_key,
              revision,
              read,
              respond
            )
          end
        )

      %{"type" => "agent_revoke", "key_fingerprint" => agent_key} ->
        mutation(request, ~w(v type person_id key_fingerprint), fn ->
          Relay.owner_revoke_agent(relay, person, key, agent_key)
        end)

      %{"type" => "agent_list"} ->
        if fields?(request, ~w(v type person_id)) do
          case owner_result(request, Relay.owner_list_agents(relay, person, key)) do
            {:ok, agents} -> %{"ok" => true, "agents" => agents}
            _ -> not_found()
          end
        else
          invalid()
        end

      _ ->
        invalid()
    end
  end

  defp dispatch(_relay, _person, _key, _request), do: not_found()

  defp invitation_reply(relay, invitation) do
    base = %{"ok" => true, "invitation" => invitation}

    case Diagnostics.result(
           :owner_tls,
           :request_dispatch,
           Relay.dashboard_origin(relay),
           :unavailable
         ) do
      origin when is_binary(origin) ->
        Map.put(
          base,
          "tappable_url",
          GuidedJoin.bridge_link(origin, invitation["invite_token"])
        )

      _ ->
        base
    end
  end

  defp mutation(request, names, action) do
    if fields?(request, names) do
      case action.() do
        :ok ->
          %{"ok" => true}

        {:error, reason} ->
          owner_result(request, {:error, reason})

          not_found()

        other ->
          Diagnostics.failure(
            :owner_tls,
            :request_dispatch,
            other,
            :rejected,
            Diagnostics.operation_id(request)
          )

          not_found()
      end
    else
      invalid()
    end
  end

  defp owner_result(request, result) do
    Diagnostics.result(
      :owner_tls,
      :request_dispatch,
      result,
      :automatic,
      Diagnostics.operation_id(request)
    )
  end

  defp fields?(request, names), do: Map.keys(request) |> Enum.sort() == Enum.sort(names)
  defp not_found, do: %{"error" => "not_found"}
  defp invalid, do: %{"error" => "invalid_request"}

  defp safe_error("not_found"), do: :not_found
  defp safe_error("invalid_request"), do: :invalid_request
  defp safe_error("not_supported"), do: :not_supported
  defp safe_error("unavailable"), do: :storage_unavailable
  defp safe_error("capacity"), do: :capacity
  defp safe_error("expired"), do: :expired
  defp safe_error("used"), do: :used
  defp safe_error("request_conflict"), do: :request_conflict
  defp safe_error(_), do: :unknown
end

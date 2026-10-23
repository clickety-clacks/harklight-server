defmodule AttentionServer.Server do
  @moduledoc """
  Opt-in supervised Relay, `/relay` WebSocket and separate owner/agent TLS listeners.

  Certificate paths and a durable store path are explicit operator config.
  Production deployment remains separate work.
  """

  use Supervisor

  alias AttentionServer.{
    AgentHTTPS,
    DashboardHTTPS,
    EnrollmentHTTPS,
    Diagnostics,
    HostedHTTP,
    OriginHTTPS,
    OwnerTLS,
    PushDispatcher,
    Relay,
    WebSocketRelay
  }

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  def relay(server), do: child(server, :relay)
  def port(server), do: server |> child(:web_socket) |> WebSocketRelay.port()
  def enroll_port(server), do: server |> child(:enrollment_https) |> EnrollmentHTTPS.port()
  def owner_port(server), do: server |> child(:owner_tls) |> OwnerTLS.port()
  def agent_port(server), do: server |> child(:agent_https) |> AgentHTTPS.port()
  def origin_port(server), do: server |> child(:origin_https) |> OriginHTTPS.port()
  def hosted_port(server), do: server |> child(:hosted_http) |> HostedHTTP.port()

  @impl true
  def init(opts) do
    relay_name = Keyword.get(opts, :relay_name, AttentionServer.SupervisedRelay)

    with {:ok, store_path} <-
           Diagnostics.result(
             :server_startup,
             :configuration,
             Keyword.fetch(opts, :store_path),
             :unavailable
           ),
         {:ok, certfile} <-
           Diagnostics.result(
             :server_startup,
             :configuration,
             Keyword.fetch(opts, :certfile),
             :unavailable
           ),
         {:ok, keyfile} <-
           Diagnostics.result(
             :server_startup,
             :configuration,
             Keyword.fetch(opts, :keyfile),
             :unavailable
           ),
         {:ok, cacertfile} <-
           Diagnostics.result(
             :server_startup,
             :configuration,
             Keyword.fetch(opts, :cacertfile),
             :unavailable
           ) do
      deployment_mode = Keyword.get(opts, :deployment_mode, :self_hosted)
      upload_cap = Keyword.get(opts, :upload_cap, :none)
      public_origin = public_origin(Keyword.get(opts, :public_base_url))
      hosted_port = Keyword.get(opts, :hosted_port)

      hosted_addresses =
        Map.new([:sign_in, :sign_out, :account], fn key ->
          {key, hosted_address(Keyword.get(opts, :"hosted_#{key}_url"), public_origin)}
        end)

      cond do
        deployment_mode not in [:self_hosted, :saas] ->
          raise ArgumentError, "invalid :deployment_mode"

        public_origin == :invalid ->
          raise ArgumentError, "invalid :public_base_url (ATTENTION_SERVER_PUBLIC_BASE_URL)"

        not is_nil(hosted_port) and deployment_mode != :saas ->
          raise ArgumentError,
                "the hosted listener (:hosted_port) requires :deployment_mode :saas"

        not is_nil(hosted_port) and not (is_integer(hosted_port) and hosted_port in 0..65_535) ->
          raise ArgumentError, "invalid :hosted_port (ATTENTION_SERVER_HOSTED_PORT)"

        deployment_mode != :saas and
            Enum.any?([:hosted_sign_in_url, :hosted_sign_out_url, :hosted_account_url], fn key ->
              not is_nil(Keyword.get(opts, key))
            end) ->
          raise ArgumentError, "hosted addresses require :deployment_mode :saas"

        Enum.any?(hosted_addresses, &(elem(&1, 1) == :invalid)) ->
          raise ArgumentError,
                "invalid hosted address (ATTENTION_SERVER_HOSTED_SIGN_IN_URL, _SIGN_OUT_URL or " <>
                  "_ACCOUNT_URL): use https://host/path or a path under the public base address"

        upload_cap != :none and not (is_integer(upload_cap) and upload_cap > 0) ->
          raise ArgumentError, "invalid :upload_cap (ATTENTION_SERVER_UPLOAD_CAP_BYTES)"

        deployment_mode == :saas and upload_cap == :none ->
          raise ArgumentError,
                "SaaS requires finite ATTENTION_SERVER_UPLOAD_CAP_BYTES (:upload_cap)"

        true ->
          :ok
      end

      apns_opts = Keyword.get(opts, :apns)
      push_enabled = not is_nil(apns_opts)

      relay_opts = [
        name: relay_name,
        store_path: store_path,
        push_enabled: push_enabled,
        upload_cap: upload_cap
      ]

      relay_opts =
        case Keyword.get(opts, :hosted_test_account) do
          nil -> relay_opts
          account_ref -> Keyword.put(relay_opts, :hosted_test_account, account_ref)
        end

      dashboard_opts = Keyword.get(opts, :dashboard)

      relay_opts =
        cond do
          is_binary(public_origin) ->
            Keyword.put(relay_opts, :dashboard_origin, public_origin)

          dashboard_opts ->
            host = Keyword.fetch!(opts, :join_name)
            port = Keyword.fetch!(dashboard_opts, :port)

            unless is_binary(host) and Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}\z/, host) and
                     is_integer(port) and port in 1..65_535 do
              raise ArgumentError, "invalid dashboard host or port"
            end

            Keyword.put(relay_opts, :dashboard_origin, "https://#{host}:#{port}")

          true ->
            relay_opts
        end

      dashboard_opts =
        if dashboard_opts do
          dashboard_opts
          |> Keyword.put(:deployment_mode, deployment_mode)
          |> Keyword.put(:public_origin, public_origin)
          |> Keyword.put(:hosted_addresses, hosted_addresses)
        end

      relay_opts =
        case Keyword.get(opts, :join_name) do
          nil ->
            relay_opts

          name ->
            with {:ok, ca} <-
                   Diagnostics.result(
                     :server_startup,
                     :configuration,
                     File.read(cacertfile),
                     :unavailable
                   ),
                 {:ok, configuration} <-
                   Diagnostics.result(
                     :server_startup,
                     :configuration,
                     AttentionServer.GuidedJoin.configuration(
                       name,
                       Keyword.get(opts, :enroll_port),
                       Keyword.get(opts, :port),
                       ca
                     ),
                     :unavailable
                   ) do
              Keyword.put(relay_opts, :join_configuration, configuration)
            else
              _ -> raise ArgumentError, "invalid configured join endpoint or public trust"
            end
        end

      web_opts = [
        relay: relay_name,
        certfile: certfile,
        keyfile: keyfile,
        cacertfile: cacertfile,
        port: Keyword.get(opts, :port, 0),
        ip: Keyword.get(opts, :ip, {127, 0, 0, 1})
      ]

      enroll_opts = [
        relay: relay_name,
        certfile: certfile,
        keyfile: keyfile,
        cacertfile: cacertfile,
        port: Keyword.get(opts, :enroll_port, 0),
        ip: Keyword.get(opts, :enroll_ip, {127, 0, 0, 1})
      ]

      owner_opts = [
        relay: relay_name,
        certfile: certfile,
        keyfile: keyfile,
        cacertfile: cacertfile,
        port: Keyword.get(opts, :owner_port, 0),
        ip: Keyword.get(opts, :owner_ip, {127, 0, 0, 1})
      ]

      agent_opts = [
        relay: relay_name,
        certfile: certfile,
        keyfile: keyfile,
        cacertfile: cacertfile,
        port: Keyword.get(opts, :agent_port, 0),
        ip: Keyword.get(opts, :agent_ip, {127, 0, 0, 1})
      ]

      origin_opts = [
        relay: relay_name,
        certfile: certfile,
        keyfile: keyfile,
        cacertfile: cacertfile,
        port: Keyword.get(opts, :origin_port, 0),
        ip: Keyword.get(opts, :origin_ip, {127, 0, 0, 1})
      ]

      children = [
        Supervisor.child_spec({Relay, relay_opts}, id: :relay),
        Supervisor.child_spec({WebSocketRelay, web_opts}, id: :web_socket),
        Supervisor.child_spec({EnrollmentHTTPS, enroll_opts}, id: :enrollment_https),
        Supervisor.child_spec({OwnerTLS, owner_opts}, id: :owner_tls),
        Supervisor.child_spec({AgentHTTPS, agent_opts}, id: :agent_https)
      ]

      children =
        if Keyword.has_key?(opts, :origin_port) do
          children ++ [Supervisor.child_spec({OriginHTTPS, origin_opts}, id: :origin_https)]
        else
          children
        end

      children =
        if apns_opts do
          children ++
            [
              Supervisor.child_spec(
                {PushDispatcher, relay: relay_name, apns: apns_opts},
                id: :push_dispatcher
              )
            ]
        else
          children
        end

      children =
        if dashboard_opts do
          children ++
            [
              Supervisor.child_spec(
                {DashboardHTTPS, Keyword.put(dashboard_opts, :relay, relay_name)},
                id: :dashboard_https
              )
            ]
        else
          children
        end

      children =
        if hosted_port do
          children ++
            [
              Supervisor.child_spec(
                {HostedHTTP, relay: relay_name, port: hosted_port, ip: {127, 0, 0, 1}},
                id: :hosted_http
              )
            ]
        else
          children
        end

      Supervisor.init(children, strategy: :rest_for_one)
    else
      _ -> raise ArgumentError, "missing server certificate or store configuration"
    end
  rescue
    error ->
      Diagnostics.failure(:server_startup, :configuration, error, :unavailable)
      reraise error, __STACKTRACE__
  catch
    kind, reason ->
      Diagnostics.failure(:server_startup, :configuration, {kind, reason}, :unavailable)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # The configured public base address, normalized to an origin, or nil when unset.
  defp public_origin(nil), do: nil

  defp public_origin(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: "https", host: host, port: port, userinfo: nil, query: nil, fragment: nil} =
          uri
      when is_binary(host) and is_integer(port) and port in 1..65_535 and
             uri.path in [nil, "", "/"] ->
        if Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}\z/, host) do
          if port == 443, do: "https://#{host}", else: "https://#{host}:#{port}"
        else
          :invalid
        end

      _ ->
        :invalid
    end
  end

  defp public_origin(_), do: :invalid

  # A hosted address is an absolute https URL or a path resolved against the
  # public base address; nil when unset.
  defp hosted_address(nil, _origin), do: nil

  defp hosted_address("/" <> _ = path, origin) when is_binary(origin) do
    if String.contains?(path, [" ", "#", "//"]), do: :invalid, else: origin <> path
  end

  defp hosted_address("https://" <> _ = url, _origin) do
    case URI.parse(url) do
      %URI{host: host, userinfo: nil, fragment: nil} when is_binary(host) and host != "" -> url
      _ -> :invalid
    end
  end

  defp hosted_address(_, _), do: :invalid

  defp child(server, id) do
    case Enum.find(Supervisor.which_children(server), fn {child_id, _, _, _} -> child_id == id end) do
      {^id, pid, _, _} when is_pid(pid) -> pid
      _ -> raise "#{id} child is unavailable"
    end
  end
end

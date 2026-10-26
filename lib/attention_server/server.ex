defmodule AttentionServer.Server do
  @moduledoc """
  Opt-in supervised Relay, `/relay` WebSocket and separate owner/agent TLS listeners.

  Certificate paths and a durable store path are explicit operator config.
  Production deployment remains separate work.
  """

  use Supervisor
  require Logger

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
      deployment_mode =
        normalize_deployment_mode(Keyword.get(opts, :deployment_mode, :self_hosted))
      upload_cap = normalize_upload_cap(Keyword.get(opts, :upload_cap, :none))
      public_origin = normalize_public_origin(Keyword.get(opts, :public_base_url))

      {hosted_port, hosted_addresses, hosted_test_account} =
        hosted_configuration(opts, deployment_mode, upload_cap, public_origin)

      apns_opts = Keyword.get(opts, :apns)

      relay_opts = [
        name: relay_name,
        store_path: store_path,
        push_enabled: false,
        deployment_mode: deployment_mode,
        upload_cap: upload_cap
      ]

      relay_opts =
        case hosted_test_account do
          nil -> relay_opts
          account_ref -> Keyword.put(relay_opts, :hosted_test_account, account_ref)
        end

      {dashboard_opts, dashboard_origin} =
        dashboard_configuration(
          Keyword.get(opts, :dashboard),
          Keyword.get(opts, :join_name),
          public_origin,
          deployment_mode,
          certfile,
          keyfile
        )

      relay_opts =
        case {public_origin, dashboard_origin} do
          {origin, _dashboard_origin} when is_binary(origin) ->
            Keyword.put(relay_opts, :dashboard_origin, origin)

          {_public_origin, origin} when is_binary(origin) ->
            Keyword.put(relay_opts, :dashboard_origin, origin)

          _ ->
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
        maybe_add_join_configuration(relay_opts, opts, cacertfile)

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

      children = [Supervisor.child_spec({Relay, relay_opts}, id: :relay)]

      children =
        if apns_opts do
          children ++
            [
              optional_child_spec(
                :push_dispatcher,
                PushDispatcher,
                [relay: relay_name, apns: apns_opts],
                :apns_dispatch,
                :configuration,
                :worker
              )
            ]
        else
          children
        end

      children = children ++ [Supervisor.child_spec({WebSocketRelay, web_opts}, id: :web_socket)]

      children =
        if Keyword.get(opts, :enrollment_enabled, true) do
          children ++
            [
              optional_child_spec(
                :enrollment_https,
                EnrollmentHTTPS,
                enroll_opts,
                :enrollment_https,
                :listener_start,
                :worker
              )
            ]
        else
          children
        end

      children =
        children ++
          [
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
        if dashboard_opts do
          children ++
            [
              optional_child_spec(
                :dashboard_https,
                DashboardHTTPS,
                Keyword.put(dashboard_opts, :relay, relay_name),
                :dashboard_http,
                :listener_start,
                :supervisor
              )
            ]
        else
          children
        end

      children =
        if hosted_port do
          children ++
            [
              optional_child_spec(
                :hosted_http,
                HostedHTTP,
                [relay: relay_name, port: hosted_port, ip: {127, 0, 0, 1}],
                :hosted_http,
                :listener_start,
                :supervisor
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

  defp normalize_deployment_mode(mode) when mode in [:self_hosted, :saas], do: mode

  defp normalize_deployment_mode(_mode) do
    optional_warning("hosted_account", "invalid_deployment_mode")
    :invalid
  end

  defp normalize_upload_cap(:none), do: :none
  defp normalize_upload_cap(cap) when is_integer(cap) and cap > 0, do: cap

  defp normalize_upload_cap(_cap) do
    optional_warning("hosted_account", "invalid_upload_cap")
    :none
  end

  defp normalize_public_origin(value) do
    public_origin(value)
  end

  defp hosted_configuration(opts, deployment_mode, upload_cap, public_origin) do
    hosted_port = Keyword.get(opts, :hosted_port)
    hosted_test_account = Keyword.get(opts, :hosted_test_account)

    hosted_addresses =
      Map.new(
        [
          {:sign_in, :hosted_sign_in_url},
          {:sign_out, :hosted_sign_out_url},
          {:account, :hosted_account_url}
        ],
        fn {key, option} ->
          {key, hosted_address(Keyword.get(opts, option), public_origin)}
        end
      )

    requested? =
      not is_nil(hosted_port) or not is_nil(hosted_test_account) or
        Enum.any?(hosted_addresses, fn {_key, value} -> not is_nil(value) end)

    finite_cap? = is_integer(upload_cap) and upload_cap > 0

    cond do
      deployment_mode == :saas and not finite_cap? ->
        optional_warning("hosted_account", "finite_upload_cap_required")
        {nil, %{}, nil}

      deployment_mode == :invalid ->
        {nil, %{}, nil}

      deployment_mode != :saas and requested? ->
        optional_warning("hosted_account", "saas_mode_required")
        {nil, %{}, nil}

      requested? and public_origin == :invalid ->
        optional_warning("hosted_account", "invalid_public_origin")
        {nil, %{}, nil}

      not is_nil(hosted_port) and
          not (is_integer(hosted_port) and hosted_port in 0..65_535) ->
        optional_warning("hosted_account", "invalid_listener_port")
        {nil, %{}, nil}

      Enum.any?(hosted_addresses, fn {_key, value} -> value == :invalid end) ->
        optional_warning("hosted_account", "invalid_address")
        {nil, %{}, nil}

      true ->
        {hosted_port, hosted_addresses, hosted_test_account}
    end
  end

  defp dashboard_configuration(nil, _join_name, _origin, _mode, _cert, _key), do: {nil, nil}

  defp dashboard_configuration(opts, join_name, origin, mode, certfile, keyfile)
       when is_list(opts) do
    if Keyword.keyword?(opts) do
      dashboard_port = Keyword.get(opts, :port)
      skill_path = Keyword.get(opts, :skill_path)
      owner_person = Keyword.get(opts, :owner_person)

      dashboard_origin =
        cond do
          is_binary(origin) -> origin
          origin == :invalid -> nil
          valid_join_name?(join_name) and is_integer(dashboard_port) and
              dashboard_port in 1..65_535 -> "https://#{join_name}:#{dashboard_port}"
          true -> nil
        end

      configured? =
        mode in [:self_hosted, :saas] and origin != :invalid and
          is_integer(dashboard_port) and dashboard_port in 0..65_535 and
          is_binary(skill_path) and skill_path != "" and is_binary(dashboard_origin) and
          (mode == :saas or (is_binary(owner_person) and owner_person != ""))

      if configured? do
        {Keyword.merge(opts,
           certfile: Keyword.get(opts, :certfile, certfile),
           keyfile: Keyword.get(opts, :keyfile, keyfile)
         ), dashboard_origin}
      else
        optional_warning("dashboard", "invalid_or_incomplete_configuration")
        {nil, nil}
      end
    else
      optional_warning("dashboard", "invalid_configuration")
      {nil, nil}
    end
  end

  defp dashboard_configuration(_opts, _join_name, _origin, _mode, _cert, _key) do
    optional_warning("dashboard", "invalid_configuration")
    {nil, nil}
  end

  defp valid_join_name?(name) when is_binary(name) do
    Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}\z/, name)
  end

  defp valid_join_name?(_), do: false

  defp maybe_add_join_configuration(relay_opts, opts, cacertfile) do
    case Keyword.get(opts, :join_name) do
      nil ->
        relay_opts

      name ->
        result =
          with true <- Keyword.get(opts, :enrollment_enabled, true),
               {:ok, ca} <- File.read(cacertfile),
               {:ok, configuration} <-
                 AttentionServer.GuidedJoin.configuration(
                   name,
                   Keyword.get(opts, :enroll_port),
                   Keyword.get(opts, :port, 0),
                   ca
                 ) do
            {:ok, configuration}
          else
            _ -> {:error, :invalid_join_configuration}
          end

        case result do
          {:ok, configuration} ->
            Keyword.put(relay_opts, :join_configuration, configuration)

          {:error, _reason} ->
            optional_warning("guided_join", "invalid_or_unreadable_configuration")
            relay_opts
        end
    end
  rescue
    _error ->
      optional_warning("guided_join", "invalid_configuration")
      relay_opts
  end

  defp optional_child_spec(id, module, opts, operation, phase, type) do
    %{
      id: id,
      start: {__MODULE__, :start_optional_child, [module, opts, operation, phase]},
      restart: :temporary,
      shutdown: 5_000,
      type: type,
      modules: [module]
    }
  end

  @doc false
  def start_optional_child(module, opts, operation, phase) do
    case apply(module, :start_link, [opts]) do
      {:ok, _pid} = started ->
        started

      {:ok, _pid, _state} = started ->
        started

      :ignore ->
        :ignore

      {:error, reason} ->
        Diagnostics.failure(operation, phase, reason, :unavailable)
        :ignore

      _other ->
        Diagnostics.failure(operation, phase, :invalid_configuration, :unavailable)
        :ignore
    end
  rescue
    error ->
      Diagnostics.failure(operation, phase, error, :unavailable)
      :ignore
  catch
    kind, reason ->
      Diagnostics.failure(operation, phase, {kind, reason}, :unavailable)
      :ignore
  end

  defp optional_warning(feature, reason) do
    Logger.warning(
      "event=harklight_optional_feature_disabled feature=#{feature} reason=#{reason}"
    )
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

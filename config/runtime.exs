import Config
require Logger

# Keep styles withheld from every delivery route until operators verify that
# the installed clients tolerate the top-level choice_styles member.
config :attention_server,
  choice_styles_held: System.get_env("ATTENTION_SERVER_CHOICE_STYLES_HELD", "true") != "false"

optional_warning = fn feature, reason ->
  Logger.warning("event=harklight_optional_feature_disabled feature=#{feature} reason=#{reason}")
end

parse_optional_port = fn variable ->
  case System.get_env(variable) do
    nil ->
      :not_set

    value ->
      case Integer.parse(value) do
        {port, ""} when port in 0..65_535 -> {:ok, port}
        _ -> {:error, :invalid}
      end
  end
end

parse_optional_bind = fn variable ->
  value = System.get_env(variable, "127.0.0.1")

  case :inet.parse_address(String.to_charlist(value)) do
    {:ok, address} -> {:ok, address}
    _ -> {:error, :invalid}
  end
end

# Opt-in operator configuration. Tests and an unconfigured application do not
# start a listener or create an account. Bind addresses and ports are explicit.
if System.get_env("ATTENTION_SERVER_ENABLE") == "1" do
  bind = fn variable ->
    value = System.get_env(variable, "127.0.0.1")

    case :inet.parse_address(String.to_charlist(value)) do
      {:ok, address} -> address
      _ -> raise "invalid #{variable}"
    end
  end

  sandbox_key_path = System.get_env("ATTENTION_SERVER_APNS_KEY_PATH")
  sandbox_key_id = System.get_env("ATTENTION_SERVER_APNS_KEY_ID")
  production_key_path = System.get_env("ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH")
  production_key_id = System.get_env("ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID")
  team_id = System.get_env("ATTENTION_SERVER_APNS_TEAM_ID")

  sandbox_values = [sandbox_key_path, sandbox_key_id]
  production_values = [production_key_path, production_key_id]
  sandbox_set = Enum.count(sandbox_values, &is_binary/1)
  production_set = Enum.count(production_values, &is_binary/1)

  profiles =
    %{"harklight-sandbox" => [key_path: sandbox_key_path, key_id: sandbox_key_id]}
    |> then(fn profiles ->
      if production_set == 2 do
        Map.put(profiles, "harklight-production",
          key_path: production_key_path,
          key_id: production_key_id
        )
      else
        profiles
      end
    end)

  apns =
    cond do
      sandbox_set == 0 and production_set == 0 and is_nil(team_id) ->
        nil

      sandbox_set == 2 and production_set in [0, 2] and is_binary(team_id) ->
        [team_id: team_id, profiles: profiles]

      true ->
        optional_warning.("push_notifications", "incomplete_configuration")
        nil
    end

  {deployment_mode, deployment_mode_valid?} =
    case System.get_env("ATTENTION_SERVER_DEPLOYMENT_MODE", "self_hosted") do
      "self_hosted" ->
        {:self_hosted, true}

      "saas" ->
        {:saas, true}

      _ ->
        optional_warning.("hosted_account", "invalid_deployment_mode")
        {:invalid, false}
    end

  {upload_cap, upload_cap_valid?} =
    case System.get_env("ATTENTION_SERVER_UPLOAD_CAP_BYTES", "none") do
      "none" ->
        {:none, true}

      value ->
        case Integer.parse(value) do
          {cap, ""} when cap > 0 ->
            {cap, true}

          _ ->
            optional_warning.("hosted_account", "invalid_upload_cap")
            {:none, false}
        end
    end

  enrollment =
    with {:ok, port} <- parse_optional_port.("ATTENTION_SERVER_ENROLL_PORT"),
         {:ok, ip} <- parse_optional_bind.("ATTENTION_SERVER_ENROLL_BIND") do
      {:enabled, port, ip}
    else
      _ ->
        optional_warning.("device_enrollment", "invalid_listener_configuration")
        :disabled
    end

  server_opts = [
    store_path: System.fetch_env!("ATTENTION_SERVER_STORE_PATH"),
    certfile: System.fetch_env!("ATTENTION_SERVER_CERTFILE"),
    keyfile: System.fetch_env!("ATTENTION_SERVER_KEYFILE"),
    cacertfile: System.fetch_env!("ATTENTION_SERVER_CACERTFILE"),
    port: System.fetch_env!("ATTENTION_SERVER_PORT") |> String.to_integer(),
    ip: bind.("ATTENTION_SERVER_BIND"),
    owner_port: System.fetch_env!("ATTENTION_SERVER_OWNER_PORT") |> String.to_integer(),
    owner_ip: bind.("ATTENTION_SERVER_OWNER_BIND"),
    agent_port: System.fetch_env!("ATTENTION_SERVER_AGENT_PORT") |> String.to_integer(),
    agent_ip: bind.("ATTENTION_SERVER_AGENT_BIND"),
    join_name: System.get_env("ATTENTION_SERVER_JOIN_NAME"),
    deployment_mode: deployment_mode,
    upload_cap: upload_cap
  ]

  server_opts =
    case enrollment do
      {:enabled, port, ip} ->
        Keyword.merge(server_opts, enrollment_enabled: true, enroll_port: port, enroll_ip: ip)

      :disabled ->
        Keyword.put(server_opts, :enrollment_enabled, false)
    end

  server_opts =
    case System.get_env("ATTENTION_SERVER_ORIGIN_PORT") do
      nil ->
        server_opts

      port ->
        server_opts
        |> Keyword.put(:origin_port, String.to_integer(port))
        |> Keyword.put(:origin_ip, bind.("ATTENTION_SERVER_ORIGIN_BIND"))
    end

  server_opts =
    case System.get_env("ATTENTION_SERVER_PUBLIC_BASE_URL") do
      nil -> server_opts
      url -> Keyword.put(server_opts, :public_base_url, url)
    end

  hosted_port = parse_optional_port.("ATTENTION_SERVER_HOSTED_PORT")

  hosted_address_vars = [
    hosted_sign_in_url: "ATTENTION_SERVER_HOSTED_SIGN_IN_URL",
    hosted_sign_out_url: "ATTENTION_SERVER_HOSTED_SIGN_OUT_URL",
    hosted_account_url: "ATTENTION_SERVER_HOSTED_ACCOUNT_URL"
  ]

  hosted_addresses_requested? =
    Enum.any?(hosted_address_vars, fn {_key, variable} ->
      not is_nil(System.get_env(variable))
    end)

  hosted_test_account = System.get_env("ATTENTION_SERVER_HOSTED_TEST_ACCOUNT")

  hosted_settings_requested? =
    hosted_port != :not_set or hosted_addresses_requested? or not is_nil(hosted_test_account)

  hosted_settings_available? =
    deployment_mode_valid? and deployment_mode == :saas and upload_cap_valid? and
      is_integer(upload_cap) and upload_cap > 0 and hosted_port != {:error, :invalid}

  server_opts =
    if hosted_settings_requested? and not hosted_settings_available? do
      reason =
        cond do
          not deployment_mode_valid? -> "invalid_deployment_mode"
          deployment_mode != :saas -> "saas_mode_required"
          not upload_cap_valid? -> "invalid_upload_cap"
          hosted_port == {:error, :invalid} -> "invalid_listener_port"
          true -> "finite_upload_cap_required"
        end

      optional_warning.("hosted_account", reason)
      server_opts
    else
      case hosted_port do
        {:ok, port} ->
          server_opts
          |> Keyword.put(:hosted_port, port)
          |> Keyword.put(:hosted_test_account, hosted_test_account)

        :not_set ->
          server_opts

        {:error, :invalid} ->
          server_opts
      end
      |> then(fn opts ->
        if hosted_settings_available? do
          Enum.reduce(hosted_address_vars, opts, fn {key, variable}, acc ->
            case System.get_env(variable) do
              nil -> acc
              url -> Keyword.put(acc, key, url)
            end
          end)
        else
          opts
        end
      end)
    end

  dashboard_port = parse_optional_port.("ATTENTION_SERVER_DASHBOARD_PORT")

  dashboard =
    if not deployment_mode_valid? do
      if dashboard_port != :not_set,
        do: optional_warning.("dashboard", "invalid_deployment_mode")

      nil
    else
      case dashboard_port do
        :not_set ->
          nil

        {:ok, port} ->
          owner_person =
            if deployment_mode == :saas do
              {:ok, nil}
            else
              case System.get_env("ATTENTION_SERVER_DASHBOARD_OWNER_PERSON") do
                value when is_binary(value) and value != "" -> {:ok, value}
                _ -> {:error, :missing_owner}
              end
            end

          skill_path = System.get_env("ATTENTION_SERVER_DASHBOARD_SKILL_PATH")

          with {:ok, ip} <- parse_optional_bind.("ATTENTION_SERVER_DASHBOARD_BIND"),
               {:ok, owner_person} <- owner_person,
               true <- is_binary(skill_path) and skill_path != "" do
            [
              port: port,
              ip: ip,
              owner_person: owner_person,
              certfile:
                System.get_env("ATTENTION_SERVER_DASHBOARD_CERTFILE") ||
                  Keyword.fetch!(server_opts, :certfile),
              keyfile:
                System.get_env("ATTENTION_SERVER_DASHBOARD_KEYFILE") ||
                  Keyword.fetch!(server_opts, :keyfile),
              skill_path: skill_path,
              skill_path_send_listen:
                System.get_env("ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SEND_LISTEN")
            ]
          else
            _ ->
              optional_warning.("dashboard", "invalid_or_incomplete_configuration")
              nil
          end

        {:error, :invalid} ->
          optional_warning.("dashboard", "invalid_listener_port")
          nil
      end
    end

  server_opts =
    if dashboard, do: Keyword.put(server_opts, :dashboard, dashboard), else: server_opts

  config :attention_server,
         :server,
         if(apns, do: Keyword.put(server_opts, :apns, apns), else: server_opts)
end

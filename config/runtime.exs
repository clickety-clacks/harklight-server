import Config

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
        raise "incomplete APNs configuration"
    end

  server_opts = [
    store_path: System.fetch_env!("ATTENTION_SERVER_STORE_PATH"),
    certfile: System.fetch_env!("ATTENTION_SERVER_CERTFILE"),
    keyfile: System.fetch_env!("ATTENTION_SERVER_KEYFILE"),
    cacertfile: System.fetch_env!("ATTENTION_SERVER_CACERTFILE"),
    port: System.fetch_env!("ATTENTION_SERVER_PORT") |> String.to_integer(),
    ip: bind.("ATTENTION_SERVER_BIND"),
    enroll_port: System.fetch_env!("ATTENTION_SERVER_ENROLL_PORT") |> String.to_integer(),
    enroll_ip: bind.("ATTENTION_SERVER_ENROLL_BIND"),
    owner_port: System.fetch_env!("ATTENTION_SERVER_OWNER_PORT") |> String.to_integer(),
    owner_ip: bind.("ATTENTION_SERVER_OWNER_BIND"),
    agent_port: System.fetch_env!("ATTENTION_SERVER_AGENT_PORT") |> String.to_integer(),
    agent_ip: bind.("ATTENTION_SERVER_AGENT_BIND"),
    join_name: System.get_env("ATTENTION_SERVER_JOIN_NAME")
  ]

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
    case System.get_env("ATTENTION_SERVER_DEPLOYMENT_MODE", "self_hosted") do
      "self_hosted" -> Keyword.put(server_opts, :deployment_mode, :self_hosted)
      "saas" -> Keyword.put(server_opts, :deployment_mode, :saas)
      _ -> raise "invalid ATTENTION_SERVER_DEPLOYMENT_MODE"
    end

  server_opts =
    case System.get_env("ATTENTION_SERVER_PUBLIC_BASE_URL") do
      nil -> server_opts
      url -> Keyword.put(server_opts, :public_base_url, url)
    end

  server_opts =
    Enum.reduce(
      [
        hosted_sign_in_url: "ATTENTION_SERVER_HOSTED_SIGN_IN_URL",
        hosted_sign_out_url: "ATTENTION_SERVER_HOSTED_SIGN_OUT_URL",
        hosted_account_url: "ATTENTION_SERVER_HOSTED_ACCOUNT_URL"
      ],
      server_opts,
      fn {key, variable}, acc ->
        case System.get_env(variable) do
          nil -> acc
          url -> Keyword.put(acc, key, url)
        end
      end
    )

  server_opts =
    case System.get_env("ATTENTION_SERVER_HOSTED_PORT") do
      nil ->
        server_opts

      hosted_port ->
        server_opts
        |> Keyword.put(:hosted_port, String.to_integer(hosted_port))
        |> Keyword.put(
          :hosted_test_account,
          System.get_env("ATTENTION_SERVER_HOSTED_TEST_ACCOUNT")
        )
    end

  server_opts =
    case System.get_env("ATTENTION_SERVER_UPLOAD_CAP_BYTES", "none") do
      "none" ->
        Keyword.put(server_opts, :upload_cap, :none)

      value ->
        case Integer.parse(value) do
          {cap, ""} when cap > 0 -> Keyword.put(server_opts, :upload_cap, cap)
          _ -> raise "invalid ATTENTION_SERVER_UPLOAD_CAP_BYTES"
        end
    end

  server_opts =
    case System.get_env("ATTENTION_SERVER_DASHBOARD_PORT") do
      nil ->
        server_opts

      dashboard_port ->
        dashboard = [
          port: String.to_integer(dashboard_port),
          ip: bind.("ATTENTION_SERVER_DASHBOARD_BIND"),
          owner_person:
            if(server_opts[:deployment_mode] == :saas,
              do: System.get_env("ATTENTION_SERVER_DASHBOARD_OWNER_PERSON"),
              else: System.fetch_env!("ATTENTION_SERVER_DASHBOARD_OWNER_PERSON")
            ),
          certfile:
            System.get_env("ATTENTION_SERVER_DASHBOARD_CERTFILE") ||
              System.fetch_env!("ATTENTION_SERVER_CERTFILE"),
          keyfile:
            System.get_env("ATTENTION_SERVER_DASHBOARD_KEYFILE") ||
              System.fetch_env!("ATTENTION_SERVER_KEYFILE"),
          skill_path: System.fetch_env!("ATTENTION_SERVER_DASHBOARD_SKILL_PATH"),
          skill_path_send_listen:
            System.get_env("ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SEND_LISTEN")
        ]

        Keyword.put(server_opts, :dashboard, dashboard)
    end

  config :attention_server,
         :server,
         if(apns, do: Keyword.put(server_opts, :apns, apns), else: server_opts)
end

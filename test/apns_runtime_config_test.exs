defmodule AttentionServer.APNSRuntimeConfigTest do
  use ExUnit.Case, async: false

  @config Path.expand("../config/runtime.exs", __DIR__)
  @server_env %{
    "ATTENTION_SERVER_ENABLE" => "1",
    "ATTENTION_SERVER_STORE_PATH" => "/tmp/harklight-config-test/relay.sqlite3",
    "ATTENTION_SERVER_CERTFILE" => "/tmp/harklight-config-test/server.pem",
    "ATTENTION_SERVER_KEYFILE" => "/tmp/harklight-config-test/server.key",
    "ATTENTION_SERVER_CACERTFILE" => "/tmp/harklight-config-test/ca.pem",
    "ATTENTION_SERVER_PORT" => "18443",
    "ATTENTION_SERVER_ENROLL_PORT" => "18444",
    "ATTENTION_SERVER_OWNER_PORT" => "18445",
    "ATTENTION_SERVER_AGENT_PORT" => "18446",
    "ATTENTION_SERVER_APNS_KEY_PATH" => "/protected/sandbox-key",
    "ATTENTION_SERVER_APNS_KEY_ID" => "SANDBOX001",
    "ATTENTION_SERVER_APNS_TEAM_ID" => "TESTTEAMID",
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => nil,
    "ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID" => nil
  }

  test "runtime config keeps production and sandbox key settings separate" do
    with_env(@server_env, fn ->
      sandbox_only = runtime_apns_config()

      assert sandbox_only[:profiles]["harklight-sandbox"] ==
               [key_path: "/protected/sandbox-key", key_id: "SANDBOX001"]

      refute Map.has_key?(sandbox_only[:profiles], "harklight-production")

      with_env(
        %{
          "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH" => "/protected/production-key",
          "ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID" => "PRODKEY001"
        },
        fn ->
          both_profiles = runtime_apns_config()

          assert both_profiles[:profiles]["harklight-sandbox"] ==
                   [key_path: "/protected/sandbox-key", key_id: "SANDBOX001"]

          assert both_profiles[:profiles]["harklight-production"] ==
                   [key_path: "/protected/production-key", key_id: "PRODKEY001"]
        end
      )
    end)
  end

  test "partial production key settings refuse runtime configuration" do
    env =
      Map.put(
        @server_env,
        "ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH",
        "/protected/production-key"
      )

    with_env(env, fn ->
      assert_raise RuntimeError, "incomplete APNs configuration", fn ->
        runtime_apns_config()
      end
    end)
  end

  defp runtime_apns_config do
    config = Config.Reader.read!(@config, env: :test)
    server = config |> Keyword.fetch!(:attention_server) |> Keyword.fetch!(:server)
    Keyword.fetch!(server, :apns)
  end

  defp with_env(values, fun) do
    previous = Map.new(values, fn {key, _value} -> {key, System.get_env(key)} end)

    Enum.each(values, fn
      {key, nil} -> System.delete_env(key)
      {key, value} -> System.put_env(key, value)
    end)

    try do
      fun.()
    after
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end
  end
end

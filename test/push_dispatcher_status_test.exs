defmodule AttentionServer.PushDispatcherStatusTest do
  use ExUnit.Case, async: true

  alias AttentionServer.PushDispatcher.Worker

  test "crash status omits cached token and APNs configuration while preserving other state" do
    cached_token = "synthetic-apns-provider-token-sentinel"
    private_key = "synthetic-apns-private-key-sentinel"
    key_id = "synthetic-apns-profile-key-id-sentinel"

    status = %{
      state: %{
        apns: %{
          profiles: %{
            "harklight-sandbox" => %{
              private_key: private_key,
              key_id: key_id,
              team_id: "SYNTHETICTEAM",
              topic: "co.clicketyclacks.Harklight"
            }
          }
        },
        cached_token: cached_token,
        retry_attempt: 3
      },
      reason: :synthetic_crash
    }

    formatted = Worker.format_status(status)

    assert formatted == %{
             state: %{retry_attempt: 3},
             reason: :synthetic_crash
           }

    refute inspect(formatted) =~ cached_token
    refute inspect(formatted) =~ private_key
    refute inspect(formatted) =~ key_id
  end
end

defmodule AttentionServer.PushDispatcherStatusTest do
  use ExUnit.Case, async: true

  alias AttentionServer.PushDispatcher.Worker

  test "crash status omits the cached provider token and preserves other state" do
    sentinel = "synthetic-apns-provider-token-sentinel"
    status = %{state: %{cached_token: sentinel, retry_attempt: 3}, reason: :synthetic_crash}

    assert Worker.format_status(status) == %{
             state: %{retry_attempt: 3},
             reason: :synthetic_crash
           }
  end
end

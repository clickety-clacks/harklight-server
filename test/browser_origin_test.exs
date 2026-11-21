defmodule AttentionServer.BrowserOriginTest do
  # The exact origin browser writes must come from: the configured public
  # origin, otherwise this listener's own scheme, host and port.
  use ExUnit.Case, async: true

  alias AttentionServer.BrowserOrigin
  alias AttentionServer.ClientAPI.Auth

  @configured "https://relay.test:9443"

  defp conn(method, host, port, origins) do
    Enum.reduce(origins, %{Plug.Test.conn(method, "/v2/me") | host: host, port: port}, fn
      origin, conn -> Plug.Conn.prepend_req_headers(conn, [{"origin", origin}])
    end)
  end

  test "a configured origin is the only accepted one" do
    opts = %{expected_origin: @configured}
    assert BrowserOrigin.expected(conn(:post, "other.test", 443, []), opts) == @configured

    for {origins, accepted} <- [
          {[@configured], true},
          {["https://relay.test"], false},
          {["https://relay.test:9443/"], false},
          {["http://relay.test:9443"], false},
          {[], false},
          {[@configured, @configured], false}
        ] do
      assert BrowserOrigin.exact?(conn(:post, "relay.test", 9443, origins), opts) == accepted,
             inspect(origins)
    end
  end

  test "without a configured origin the listener's own host and port are expected" do
    for {port, expected} <- [{443, "https://relay.test"}, {18448, "https://relay.test:18448"}] do
      assert BrowserOrigin.expected(conn(:get, "relay.test", port, []), %{}) == expected
      assert BrowserOrigin.exact?(conn(:post, "relay.test", port, [expected]), %{})
      refute BrowserOrigin.exact?(conn(:post, "relay.test", port, ["https://other.test"]), %{})
    end
  end

  test "the client API checks the origin on writes only" do
    for method <- [:get, :head] do
      assert Auth.browser_origin_ok?(conn(method, "relay.test", 9443, []), @configured)
    end

    for method <- [:post, :put, :patch, :delete] do
      refute Auth.browser_origin_ok?(conn(method, "relay.test", 9443, []), @configured)
      assert Auth.browser_origin_ok?(conn(method, "relay.test", 9443, [@configured]), @configured)
    end
  end
end

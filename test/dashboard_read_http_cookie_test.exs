defmodule AttentionServer.DashboardReadHTTPCookieTest do
  use ExUnit.Case, async: true

  alias AttentionServer.DashboardReadHTTP

  defmodule Backend do
    def redeem_sign_in(_token), do: {:ok, "opaque-session-cookie", nil}
  end

  test "sign-in creates a browser session cookie without a fixed max age" do
    origin = "https://account.example.test"

    opts =
      DashboardReadHTTP.init(
        backend: Backend,
        skill_path: Path.join(System.tmp_dir!(), "unused-dashboard-skill.md"),
        hosted: true,
        expected_origin: origin
      )

    token = "hbs1_" <> String.duplicate("a", 64)

    conn = %{
      Plug.Test.conn(:post, "/sign-in", token)
      | scheme: :https,
        port: 443
    }

    response =
      conn
      |> Plug.Conn.put_req_header("origin", origin)
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> DashboardReadHTTP.call(opts)

    assert response.status == 204

    assert %{max_age: nil, secure: true, http_only: true, same_site: "Lax"} =
             response.resp_cookies["__Host-harklight"]
  end
end

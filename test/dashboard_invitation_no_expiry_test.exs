defmodule AttentionServer.DashboardInvitationNoExpiryTest do
  use ExUnit.Case, async: true

  alias AttentionServer.DashboardReadHTTP

  defmodule Backend do
  end

  test "hosted and self-hosted join scripts label invitations with no expiry" do
    for hosted <- [false, true] do
      opts =
        DashboardReadHTTP.init(
          backend: Backend,
          skill_path: "/unused/skill.md",
          hosted: hosted
        )

      conn =
        Plug.Test.conn("GET", "/join.js")
        |> Map.put(:scheme, :https)

      response = DashboardReadHTTP.call(conn, opts)

      assert response.status == 200
      assert response.resp_body =~ "invitation.expires_at == null"
      assert response.resp_body =~ "No expiry"
      refute response.resp_body =~ "; expires "
    end
  end
end

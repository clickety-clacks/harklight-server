defmodule AttentionServer.ClientAPISharedCapabilityTest do
  use ExUnit.Case, async: true

  alias AttentionServer.ClientAPI.SharedCapability

  test "person SSE merges matching header and query values" do
    conn =
      Plug.Test.conn(:get, "/v2/changes?features=shared_messages&continuation=opaque&after=5")
      |> Plug.Conn.put_req_header("harklight-features", "shared_messages")
      |> Plug.Conn.put_req_header("harklight-continuation", "opaque")

    assert {:ok, %{shared?: true, continuation: "opaque"}} =
             SharedCapability.parse(conn, :person_sse)

    conflicting = Plug.Conn.put_req_header(conn, "harklight-continuation", "different")
    assert {:error, "invalid_field", "continuation"} =
             SharedCapability.parse(conflicting, :person_sse)
  end

  test "rejects repeated SSE aliases and ignores them in JSON mode" do
    for field <- ["features", "continuation"] do
      conn = Plug.Test.conn(:get, "/v2/changes?#{field}=value&#{field}=value")
      assert {:error, "invalid_field", ^field} = SharedCapability.parse(conn, :person_sse)
      assert {:ok, %{shared?: false, continuation: nil}} = SharedCapability.parse(conn)
    end
  end
end

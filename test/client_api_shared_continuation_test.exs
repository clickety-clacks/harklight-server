defmodule AttentionServer.ClientAPISharedContinuationTest do
  use ExUnit.Case, async: true

  alias AttentionServer.ClientAPI.SharedContinuation

  test "allows advancing from a baseline only in its original scope" do
    key = :crypto.strong_rand_bytes(32)
    principal = %{kind: :device, person: "person-a", key: "device-a"}
    filters = %{session: "chat-a"}
    token = SharedContinuation.issue(key, principal, :agent_changes, filters, 20, 1_000)
    assert byte_size(token) <= 128

    assert :ok =
             SharedContinuation.validate(token, key, principal, :agent_changes, filters, 25, 1_100)

    for {who, route, scope, cursor, now} <- [
          {%{principal | key: "device-b"}, :agent_changes, filters, 25, 1_100},
          {principal, :person_changes, filters, 25, 1_100},
          {principal, :agent_changes, %{session: nil}, 25, 1_100},
          {principal, :agent_changes, filters, 19, 1_100},
          {principal, :agent_changes, filters, 25, 3_000_000_000}
        ] do
      assert {:error, :cursor_expired} =
               SharedContinuation.validate(token, key, who, route, scope, cursor, now)
    end

    assert {:error, :cursor_expired} =
             SharedContinuation.validate(nil, key, principal, :agent_changes, filters, 25, 1_100)
  end
end

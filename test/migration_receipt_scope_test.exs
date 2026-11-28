defmodule AttentionServer.Migration.ReceiptScopeTest do
  use ExUnit.Case, async: true
  alias AttentionServer.ClientAPI.Idempotency
  alias AttentionServer.Migration.ImportReceipts

  test "person and existing Agent receipt scopes retain their distinct identity" do
    assert {:ok, {"person", ""}} = ImportReceipts.scope("person")
    encoded = Idempotency.agent_scope("person", "agent")
    assert {:ok, {"person", "agent"}} = ImportReceipts.scope(encoded)
    assert {:error, :invalid_receipt_scope} = ImportReceipts.scope("agent:invalid")
    assert {:error, :invalid_receipt_scope} = ImportReceipts.scope("")
  end
end

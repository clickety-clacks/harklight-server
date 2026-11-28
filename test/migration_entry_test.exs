defmodule AttentionServer.Migration.EntryTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.ImportEntries

  test "original positions distinguish reused payload event IDs and absent read-position revisions" do
    event = %{kind: :conversation, key: {"person", "sender", "message"},
      payload: %{"kind" => "file_use", "request_revision" => 3, "event_id" => "same"}}
    assert {:ok, {"10", 10, "file_use", 3}} = ImportEntries.fields({"person", 10}, event)
    acted = put_in(event, [:payload, "kind"], "acted")
    assert {:ok, {"11", 11, "acted", 3}} = ImportEntries.fields({"person", 11}, acted)
    read = %{event | payload: %{"kind" => "read_position", "through" => 10}}
    assert {:ok, {"12", 12, "read_position", nil}} = ImportEntries.fields({"person", 12}, read)
    assert {:error, :invalid_conversation_entry} = ImportEntries.fields({"foreign", 10}, event)
    assert {:error, :invalid_conversation_entry} = ImportEntries.fields({"person", 0}, event)
    assert {:error, :invalid_conversation_entry} = ImportEntries.fields({"person", 10},
      %{event | payload: Map.delete(event.payload, "request_revision")})
  end
end

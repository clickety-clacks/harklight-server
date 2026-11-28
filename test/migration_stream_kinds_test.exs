defmodule AttentionServer.Migration.StreamKindsTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.ImportStreams

  test "person revision kinds use canonical identity, including non-feed writer markers" do
    for {id, kind} <- [
      {"item:encoded", "item"}, {"shared-item:id", "item"},
      {"entry:2", "entry"}, {"shared-entry:id", "entry"},
      {"counts", "counts"}, {"shared-counts", "counts"}, {"group-counts", "counts"},
      {"settings", "settings"}, {"availability-writer:item:encoded", "availability_writer"}
    ] do
      assert ImportStreams.person_kind(id) == {:ok, kind}
    end
    assert {:error, :unknown_person_stream_resource} = ImportStreams.person_kind("future:id")
  end

  test "Agent revision kinds match its decoder while retaining full resource IDs" do
    for {id, kind} <- [
      {"item:encoded", "message"}, {"entry:2", "entry"},
      {"advisory:id", "advisory"}, {"item-advisory:id:item:encoded", "advisory"},
      {"shared-message:id", "shared_message"}, {"shared-entry:id", "shared_entry"},
      {"shared-seen:id", "shared_seen"}, {"shared-answer:id", "shared_answer"},
      {"shared-advisory:id:advisory", "shared_advisory"}
    ] do
      assert ImportStreams.agent_kind(id) == {:ok, kind}
    end
    assert {:error, :unknown_agent_stream_resource} = ImportStreams.agent_kind("future:id")
  end
end

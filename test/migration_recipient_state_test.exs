defmodule AttentionServer.Migration.RecipientStateTest do
  use ExUnit.Case, async: true
  alias AttentionServer.{Engine, ClientAPI.Resources}
  alias AttentionServer.Migration.ImportRecipients

  test "current substantive membership matches source seen semantics through raw revisions and fallback" do
    key = {"person", "app:fixture", "message"}
    mapped = %{raw_to_substantive: %{1 => 1, 2 => 1, 3 => 2}, current_substantive_revision: 2}
    cases = [{2, mapped, [1], 1, true}, {3, mapped, [1], 2, false},
      {3, mapped, [2], 2, true}, {3, %{mapped | raw_to_substantive: %{}}, [2], 2, true},
      {3, nil, [1, 2], 3, false}, {3, nil, [3], 3, true},
      {3, %{raw_to_substantive: %{}}, [3], 3, true}]
    for {raw, state, opened, expected, seen} <- cases do
      record = %{revision: raw}
      members = MapSet.new(opened, fn n -> {"person", "app:fixture", "message", n} end)
      engine = %Engine{opened_revisions: members, bounce_revisions: if(state, do: %{key => state}, else: %{})}
      assert {:ok, ^expected} = ImportRecipients.substantive(record, state)
      assert Resources.seen?(engine, key, record) == seen
      assert MapSet.member?(members, {"person", "app:fixture", "message", expected}) == seen
    end
    assert {:error, :invalid_substantive_revision} = ImportRecipients.substantive(%{revision: 1},
      %{raw_to_substantive: %{1 => nil}})
  end
end

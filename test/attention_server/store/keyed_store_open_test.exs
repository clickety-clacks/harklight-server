defmodule AttentionServer.Store.KeyedStoreOpenTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store}
  alias AttentionServer.Store.KeyedMigration

  @moduletag :tmp_dir

  test "only explicit cutover converts and cold keyed reopen preserves the generation", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "state.sqlite3")
    audit = %{person: "A", actor: "device", action: "prepare"}
    assert {:ok, legacy} = Store.open(path)
    assert {:ok, engine} = Engine.setup_person(legacy.engine, "A")
    assert {:ok, engine} = Engine.setup_device(engine, "A", "device")
    assert {:ok, legacy} = Store.commit(legacy, engine, audit)
    opened = Engine.put_member(legacy.engine, :conversation_opened, {"A", "device"})
    assert {:ok, legacy} = Store.commit(legacy, opened, audit)
    assert legacy.progress_version == 1
    assert :ok = Store.close(legacy)

    assert {:error, {:storage_unavailable, {:error, :not_keyed}}} = Store.open(path, :keyed)
    assert {:ok, default} = Store.open(path)
    assert default.mode == :legacy
    assert {:error, :not_keyed} = KeyedMigration.load(default.db)
    assert :ok = Store.close(default)

    assert {:ok, keyed} = Store.open(path, :keyed_cutover)
    assert keyed.mode == :keyed
    generation = legacy.version + 1
    assert keyed.version == generation
    assert keyed.progress_version == 0
    assert MapSet.member?(keyed.engine.conversation_opened, {"A", "device"})
    assert keyed.engine.route_b_index.request_keys_by_id == %{}

    assert {:ok, proposal} = Engine.setup_person(keyed.engine, "another")
    assert {:error, :storage_unavailable, ^keyed} = Store.commit(keyed, proposal, audit)
    assert :ok = Store.close(keyed)

    assert {:error, {:storage_unavailable, {:error, :format31_requires_keyed_mode}}} =
             Store.open(path, :legacy)

    for mode <- [:keyed, :keyed_cutover] do
      assert {:ok, reopened} = Store.open(path, mode)
      assert reopened.version == generation
      assert reopened.engine == keyed.engine
      assert :ok = Store.close(reopened)
    end
  end
end

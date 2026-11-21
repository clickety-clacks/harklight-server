defmodule AttentionServer.Store.KeyedReferenceTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, LargeData, Store.KeyedReference}

  test "prepares reference markers and exact legacy paths from touched records" do
    directory = Path.join(System.tmp_dir!(), "keyed-refs-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    key = {"person", "sender", "item"}
    address = {:row, :messages, key}
    old = Engine.new()
    body = :binary.copy("x", LargeData.inline_limit() + 1)
    next = %{old | messages: %{key => %{message: %{"body" => body}}}}

    assert {:ok, changes} = KeyedReference.prepare(old, next, MapSet.new([address]), directory)
    assert [] == changes.delete_paths
    assert [%LargeData.Ref{path: path} = ref] = Map.values(LargeData.refs(changes.engine))
    assert File.read!(path) == body
    assert :ok = LargeData.verify(ref)
    assert changes.reference_rows == LargeData.persisted_references(changes.engine)

    assert [{:put, {_, persisted, _}}] = changes.records

    assert :erlang.binary_to_term(persisted, [:safe]) ==
             LargeData.persist_refs(changes.engine.messages[key])
  end

  @tag :tmp_dir
  test "rekeying removes the old reference path and inserts the new one", %{tmp_dir: directory} do
    one = {"person", "sender", "one"}
    two = {"person", "sender", "two"}
    body = :binary.copy("x", LargeData.inline_limit() + 1)
    old = %{Engine.new() | messages: %{one => %{"body" => body}}}
    assert {:ok, old, _} = LargeData.externalize(old, directory)
    next = %{old | messages: %{two => old.messages[one]}}
    addresses = MapSet.new([{:row, :messages, one}, {:row, :messages, two}])

    assert {:ok, change} = KeyedReference.prepare(old, next, addresses, directory)
    assert change.reference_rows == LargeData.persisted_references(next)
    assert change.delete_paths == Enum.map(LargeData.persisted_references(old), &hd/1)
    assert Enum.sort(Enum.map(change.records, &elem(&1, 0))) == [:delete, :put]

    assert_raise FunctionClauseError, fn ->
      KeyedReference.prepare(
        old,
        next,
        [{:row, :messages, one}, {:row, :messages, one}],
        directory
      )
    end
  end

  test "leaves an unrelated row's reference out of the write set" do
    directory = Path.join(System.tmp_dir!(), "keyed-refs-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    one = {"person", "sender", "one"}
    two = {"person", "sender", "two"}
    body = :binary.copy("x", LargeData.inline_limit() + 1)
    old = %{Engine.new() | messages: %{one => %{"body" => body}, two => %{"body" => body}}}
    assert {:ok, old, _} = LargeData.externalize(old, directory)
    next = %{old | messages: Map.delete(old.messages, one)}

    assert {:ok, changes} =
             KeyedReference.prepare(old, next, MapSet.new([{:row, :messages, one}]), directory)

    assert [{:delete, _}] = changes.records
    assert [] == changes.reference_rows
    assert [_path] = changes.delete_paths
    assert [id] = changes.orphan_candidates
    assert %{^id => ref} = LargeData.refs(changes.engine)
    assert File.exists?(ref.path)
  end
end

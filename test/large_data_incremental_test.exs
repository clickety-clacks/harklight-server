defmodule AttentionServer.LargeDataIncrementalTest do
  # The incremental commit path must produce exactly what the full path
  # (externalize, persist_refs, persisted_references) produces for the same
  # next state, while walking only what changed since the committed base.
  use ExUnit.Case, async: true

  alias AttentionServer.LargeData

  defmodule Holder do
    defstruct [:a, :b, :c]
  end

  @large 2 * 1024 * 1024 + 17

  setup do
    dir = Path.join(System.tmp_dir!(), "large-incremental-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp big(seed), do: :binary.copy(<<seed>>, @large)

  defp full(next, dir) do
    {:ok, durable, _created} = LargeData.externalize(next, dir)
    true = LargeData.valid_refs?(durable)
    {durable, LargeData.persist_refs(durable), LargeData.persisted_references(durable)}
  end

  defp base(term, dir) do
    {durable, persisted, _rows} = full(term, dir)
    {durable, persisted, LargeData.reference_index(durable)}
  end

  defp assert_equivalent(next, {base, persisted, index}, dir) do
    assert {:ok, durable, inc_persisted, inc_index, _created} =
             LargeData.commit_incremental(next, base, persisted, index, dir)

    {full_durable, full_persisted, full_rows} = full(next, dir)
    assert durable == full_durable
    assert inc_persisted == full_persisted
    assert LargeData.reference_index_rows(inc_index) == full_rows
    assert LargeData.reference_index_ids(inc_index) == LargeData.refs(full_durable)
    {durable, inc_persisted, inc_index}
  end

  defp initial do
    %Holder{
      a: %{
        "m1" => %{"body" => big(1), "title" => "one", "files" => [%{"n" => 1}, {:x, 2}]},
        "m2" => %{"body" => "small", "answer" => big(2), "list" => [1, 2, %{"title" => big(3)}]},
        "m3" => %{"nested" => %{"deep" => %{"body" => big(4)}}}
      },
      b: [%{"body" => big(5)}, "plain", {:t, %{"answer" => big(6)}}],
      c: %{counter: 0, tags: MapSet.new([:a])}
    }
  end

  test "attachment restoration keeps unrelated maps shared and rebinds retained references", %{dir: dir} do
    history = Map.new(1..1_000, fn id -> {id, %{"body" => "inline #{id}"}} end)
    inline = %{history: history}
    assert {:ok, unchanged} = LargeData.attach_paths(inline, dir, [])
    assert :erts_debug.same(unchanged, inline)

    {durable, persisted, rows} = full(%{history: history, content: [%{"body" => big(1)}]}, dir)
    assert {:ok, restored} = LargeData.attach_paths(persisted, dir, rows)
    assert restored == durable
    assert :erts_debug.same(restored.history, history)

    moved = Path.join(dir, "new")
    assert {:ok, rebound} = LargeData.attach_paths(durable, moved, [])
    [ref] = Map.values(LargeData.refs(rebound))
    assert ref.path == Path.join(moved, ref.id)
    assert :erts_debug.same(rebound.history, history)
  end

  test "an unchanged state reuses the base without walking it", %{dir: dir} do
    {durable, persisted, index} = base(initial(), dir)

    assert {:ok, ^durable, ^persisted, ^index, []} =
             LargeData.commit_incremental(durable, durable, persisted, index, dir)
  end

  test "edits, additions, removals, list and tuple changes and new large data match the full",
       %{dir: dir} do
    state = base(initial(), dir)
    {b, _, _} = state

    steps = [
      fn s -> put_in(s.c.counter, 1) end,
      fn s -> put_in(s.a["m1"]["title"], "renamed") end,
      fn s -> put_in(s.a["m4"], %{"body" => big(7), "title" => "new"}) end,
      fn s -> update_in(s.a, &Map.delete(&1, "m2")) end,
      fn s -> put_in(s.a["m3"]["nested"]["deep"], %{"body" => "now small"}) end,
      fn s -> update_in(s.b, &[%{"body" => big(8)} | &1]) end,
      fn s -> put_in(s.a["m1"]["files"], [{:x, 3}, %{"answer" => big(9)}]) end,
      fn s -> put_in(s.a["m5"], s.a["m1"]) end,
      fn s -> put_in(s.c, %Holder{a: %{"body" => big(10)}}) end,
      fn s -> put_in(s.b, "replaced") end,
      fn s -> %{s | a: %{"only" => %{"title" => big(11)}}} end
    ]

    Enum.reduce(steps, {b, state}, fn step, {current, state} ->
      next = step.(current)
      {durable, _, _} = state = assert_equivalent(next, state, dir)
      {durable, state}
    end)
  end

  test "randomized mutations match the full path", %{dir: dir} do
    :rand.seed(:exsss, {7, 11, 13})
    state = base(initial(), dir)
    {b, _, _} = state

    Enum.reduce(1..150, {b, state}, fn _, {current, state} ->
      next = mutate(current)
      {durable, _, _} = state = assert_equivalent(next, state, dir)
      {durable, state}
    end)
  end

  test "a retained durable file that disappeared fails the commit like the full path",
       %{dir: dir} do
    {durable, persisted, index} = base(initial(), dir)
    [{_path, {_row, ref}} | _] = Enum.to_list(index)
    File.rm!(Path.join(dir, ref.id))
    next = put_in(durable.c.counter, 5)

    assert {:error, :storage_unavailable} =
             LargeData.commit_incremental(next, durable, persisted, index, dir)

    assert {:error, _} = LargeData.externalize(next, dir)
  end

  defp mutate(%Holder{} = s) do
    case :rand.uniform(7) do
      1 -> put_in(s.c.counter, :rand.uniform(1000))
      2 -> put_in(s.a["r#{:rand.uniform(5)}"], %{"title" => "t#{:rand.uniform(9)}"})
      3 -> put_in(s.a["r#{:rand.uniform(5)}"], %{"body" => big(20 + :rand.uniform(3))})
      4 -> update_in(s.a, &Map.delete(&1, "r#{:rand.uniform(5)}"))
      5 -> update_in(s.b, fn list -> if is_list(list), do: Enum.drop(list, 1), else: [] end)
      6 -> update_in(s.b, &[%{"answer" => big(30 + :rand.uniform(2))} | List.wrap(&1)])
      7 -> put_in(s.a["m1"], %{"nested" => {:t, [%{"body" => big(40)}, :rand.uniform(3)]}})
    end
  end
end

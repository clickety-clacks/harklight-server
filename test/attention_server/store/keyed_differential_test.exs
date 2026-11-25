defmodule AttentionServer.Store.KeyedDifferentialTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON, LargeData, Store}
  alias AttentionServer.Store.RecordCodec

  @person "a"
  @sender "app:test/worker"
  @origin String.duplicate("a", 64)
  @reader String.duplicate("b", 64)
  @derived [:touched_keys, :advisory_feeds, :origin_feeds | RecordCodec.derived_fields()]

  for seed <- 1..8 do
    @tag :tmp_dir
    test "seed #{seed} compares keyed commits and cold readback with legacy validation", %{
      tmp_dir: directory
    } do
      sweep(unquote(seed), 64, directory)
    end
  end

  @tag :tmp_dir
  test "alert choices commit and reopen in both storage modes", %{tmp_dir: directory} do
    for mode <- [:legacy, :keyed] do
      path = Path.join(directory, "#{mode}.sqlite3")
      {:ok, legacy} = Store.open(path)
      {:ok, engine} = Engine.setup_person(legacy.engine, @person)
      {:ok, engine} = Engine.setup_device(engine, @person, @reader)
      {:ok, legacy} = Store.commit(legacy, engine, audit("setup"))
      assert :ok = Store.close(legacy)

      if mode == :keyed do
        assert {:ok, keyed} = Store.open(path, :keyed_cutover)
        assert :ok = Store.close(keyed)
      end

      for {alerts, n} <- [{"all", 1}, {"important", 2}] do
        assert {:ok, store} = Store.open(path, mode)
        assert {:ok, store} = Store.enable_change_log(store)

        frame = %{
          "v" => 1,
          "type" => "alerts_set",
          "ref" => "client_api_v2",
          "op_id" => "00000000-0000-4000-8000-00000000000#{n}",
          "alerts" => alerts
        }

        {next, {:ok, reply}, touched} =
          Engine.request_line_with_touches(store.engine, @reader, JSON.encode_line(frame), 1_000)

        assert {:ok, %{"alerts" => ^alerts}} = JSON.decode_line(reply)
        assert MapSet.member?(touched, {:row, :alert_choices, @person})
        transition = if mode == :keyed, do: {next, touched}, else: next
        assert {:ok, committed} = Store.commit(store, transition, audit("alerts"))
        assert :ok = Store.close(committed)

        assert {:ok, reopened} = Store.open(path, mode)
        assert Engine.alert_choice(reopened.engine, @person) == alerts
        assert :ok = Store.close(reopened)
      end
    end
  end

  test "the touch oracle requires the old address when a record is rekeyed" do
    old_key = {@person, @sender, "old"}
    new_key = {@person, @sender, "new"}
    record = %{message: %{"body" => :binary.copy("x", LargeData.inline_limit() + 1)}}
    old = %{Engine.new() | messages: %{old_key => record}}
    next = %{old | messages: %{new_key => record}}
    touched = MapSet.new([{:row, :messages, new_key}])

    assert_raise ExUnit.AssertionError, fn ->
      assert_complete_touches(old, next, touched, "rekey")
    end

    assert_complete_touches(old, next, MapSet.put(touched, {:row, :messages, old_key}), "rekey")
  end

  defp sweep(seed, steps, dir) do
    :rand.seed(:exsss, {seed, 77, 991})

    legacy_path = Path.join(dir, "legacy.sqlite3")
    keyed_path = Path.join(dir, "keyed.sqlite3")

    {:ok, store} = Store.open(legacy_path)
    {:ok, engine} = Engine.setup_person(store.engine, @person)
    {:ok, engine} = Engine.setup_device(engine, @person, @origin)
    {:ok, engine} = Engine.setup_device(engine, @person, @reader)
    {:ok, engine} = Engine.setup_sender(engine, @origin, @sender)
    {:ok, store} = Store.commit(store, engine, audit("setup"))
    :ok = Store.close(store)
    copy_db(legacy_path, keyed_path)
    {:ok, staged} = Store.open(keyed_path, :keyed_cutover)
    :ok = Store.close(staged)

    for step <- 0..(steps - 1) do
      backup(legacy_path, keyed_path)
      # The keyed log runs, so every keyed commit also checks its touched
      # change log against the whole-map one (keyed_validation: :both). The
      # keyed commit validates first; the legacy commit finds its log before
      # validating, which malformed mutations cannot survive.
      {:ok, legacy} = Store.open(legacy_path)
      {:ok, keyed} = Store.open(keyed_path, :keyed)
      {:ok, keyed} = Store.enable_change_log(keyed)
      {kind, label, legacy_next, keyed_next, touched, responses} = proposal(legacy, keyed, step)
      detail = "step #{step}, #{inspect(label)}"
      assert responses == :same, detail
      assert codec_diff(legacy_next, keyed_next) == MapSet.new(), detail

      if kind == :wire do
        assert_complete_touches(keyed.engine, keyed_next, touched, detail)
      end

      old_result = Store.commit(legacy, legacy_next, audit(label))
      new_result = Store.commit(keyed, {keyed_next, touched}, audit(label))
      old_ok = match?({:ok, _}, old_result)
      new_ok = match?({:ok, _}, new_result)
      :ok = Store.close(store_of(old_result))
      :ok = Store.close(store_of(new_result))

      if kind == :wire do
        assert old_ok == new_ok, detail
      end

      # Keyed history also rejects rewrites of published facts and malformed
      # bounce rows that the legacy incremental validator can permit.
      if old_ok and not new_ok do
        restore(legacy_path, keyed_path)
      else
        assert old_ok == new_ok, detail
        assert {:ok, cold_legacy} = Store.open(legacy_path), detail
        assert {:ok, cold_keyed} = Store.open(keyed_path, :keyed), detail
        assert codec_diff(cold_legacy.engine, cold_keyed.engine) == MapSet.new(), detail
        assert :ok = Store.close(cold_legacy)
        assert :ok = Store.close(cold_keyed)
      end
    end
  end

  defp store_of({:ok, store}), do: store
  defp store_of({:error, _, store}), do: store

  defp audit(action),
    do: %{
      person: @person,
      actor: @origin,
      action: if(is_binary(action), do: action, else: inspect(action))
    }

  defp copy_db(from, to) do
    for suffix <- ["", "-wal", "-shm"] do
      if File.exists?(from <> suffix),
        do: File.cp!(from <> suffix, to <> suffix),
        else: File.rm(to <> suffix)
    end

    if File.dir?(from <> ".uploads") do
      File.rm_rf!(to <> ".uploads")
      File.cp_r!(from <> ".uploads", to <> ".uploads")
    end
  end

  defp backup(legacy, keyed) do
    copy_db(legacy, legacy <> ".bak")
    copy_db(keyed, keyed <> ".bak")
  end

  defp restore(legacy, keyed) do
    copy_db(legacy <> ".bak", legacy)
    copy_db(keyed <> ".bak", keyed)
  end

  defp encode(engine) do
    Map.new(RecordCodec.encode(LargeData.persist_refs(engine)), fn {key, bytes, _} ->
      {key, bytes}
    end)
  end

  defp assert_complete_touches(old, next, touched, detail) do
    assert MapSet.subset?(codec_diff(old, next), touched), detail
  end

  defp codec_diff(old, next) do
    a = encode(old)
    b = encode(next)

    MapSet.union(MapSet.new(Map.keys(a)), MapSet.new(Map.keys(b)))
    |> Enum.reject(&(Map.fetch(a, &1) === Map.fetch(b, &1)))
    |> MapSet.new(&elem(:erlang.binary_to_term(&1), 1))
  end

  # --- proposals -------------------------------------------------------------

  defp proposal(legacy, keyed, n) do
    if :rand.uniform() < 0.45, do: wire(legacy, keyed, n), else: mutation(legacy, keyed)
  end

  @ids for i <- 1..5, do: "m#{i}"

  defp wire(legacy, keyed, n) do
    id = Enum.random(@ids)
    common = %{"v" => 1, "sender_id" => @sender, "message_id" => id}
    op = "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n + 1), 12, "0")

    {actor, frame} =
      case :rand.uniform(10) do
        k when k <= 3 ->
          {@origin,
           %{
             "v" => 1,
             "type" => "forward",
             "session_handle" => "00000000-0000-4000-8000-00000000000#{:rand.uniform(2)}",
             "message" => %{
               "sender_id" => @sender,
               "message_id" => id,
               "created_at" => 1,
               "title" => id,
               "body" => "b#{n}"
             }
           }}

        4 ->
          {@reader,
           Map.merge(common, %{"type" => "message_open", "request_revision" => :rand.uniform(2)})}

        5 ->
          {@reader,
           Map.merge(common, %{"type" => "discussion", "request_revision" => 1, "text" => "p#{n}"})}

        6 ->
          {@origin,
           Map.merge(common, %{
             "type" => "conversation_reply",
             "request_revision" => 1,
             "text" => "o#{n}"
           })}

        7 ->
          {@origin,
           Map.merge(common, %{
             "type" => "conversation_revise",
             "base_revision" => :rand.uniform(2),
             "message" => %{
               "sender_id" => @sender,
               "message_id" => id,
               "created_at" => 1,
               "title" => "r#{n}"
             }
           })}

        8 ->
          {@reader,
           Map.merge(common, %{"type" => "answer", "answer" => Enum.random([nil, "yes"])})}

        9 ->
          {@origin,
           Map.merge(common, %{
             "type" => "lifecycle",
             "state" => Enum.random(~w(expired cleared)),
             "at" => 100 + n
           })}

        10 ->
          {@reader,
           Map.merge(common, %{
             "type" => "conversation_read",
             "read_through_conversation_seq" => :rand.uniform(40)
           })}
      end

    raw = frame |> Map.put("op_id", op) |> JSON.encode_line()
    at = 1_000 + n
    {plain, old_response} = Engine.request_line(legacy.engine, actor, raw, at)

    {tracked, new_response, touched} =
      Engine.request_line_with_touches(keyed.engine, actor, raw, at)

    responses = if old_response == new_response, do: :same, else: {old_response, new_response}
    {:wire, frame["type"], plain, tracked, touched, responses}
  end

  defp mutation(legacy, keyed) do
    engine = keyed.engine
    # Keep the seeded mutation sequences stable. Alert choice writes are
    # exercised directly above, including removal of the keyed row.
    fields =
      engine
      |> Map.from_struct()
      |> Map.drop([:alert_choices | @derived])
      |> Map.keys()
      |> Enum.sort()
    choice = :rand.uniform()

    {label, fun} =
      cond do
        choice < 0.12 -> origin_mutation(engine)
        true -> field_mutation(engine, Enum.random(fields))
      end

    next_legacy = fun.(legacy.engine)
    next_keyed = fun.(keyed.engine)
    touched = codec_diff(keyed.engine, next_keyed)
    {:mutation, label, next_legacy, next_keyed, touched, :same}
  end

  defp origin_mutation(engine) do
    case Map.keys(engine.origin_feeds) do
      [] ->
        {:noop, & &1}

      scopes ->
        scope = Enum.random(scopes)
        feed = engine.origin_feeds[scope]

        case {:rand.uniform(3), Map.keys(feed.entries)} do
          {1, _} ->
            v = perturb(feed.high, 0)
            {{:origin_high, v}, fn e -> put_in(e.origin_feeds[scope].high, v) end}

          {_, []} ->
            {:noop, & &1}

          {2, seqs} ->
            seq = Enum.random(seqs)
            v = perturb(feed.entries[seq], 0)
            {{:origin_entry, seq}, fn e -> put_in(e.origin_feeds[scope].entries[seq], v) end}

          {3, seqs} ->
            seq = Enum.random(seqs)

            {{:origin_entry_delete, seq},
             fn e -> update_in(e.origin_feeds[scope].entries, &Map.delete(&1, seq)) end}
        end
    end
  end

  defp field_mutation(engine, field) do
    value = Map.fetch!(engine, field)

    cond do
      is_struct(value, MapSet) and MapSet.size(value) > 0 and :rand.uniform(2) == 1 ->
        member = Enum.random(value)
        {{field, :member_delete}, fn e -> Map.update!(e, field, &MapSet.delete(&1, member)) end}

      is_struct(value, MapSet) ->
        member =
          if MapSet.size(value) > 0,
            do: perturb(Enum.random(value), 0),
            else: Enum.random([{@person, @sender, "m1", 1}, {@person, @reader}, "x"])

        {{field, :member_add}, fn e -> Map.update!(e, field, &MapSet.put(&1, member)) end}

      is_map(value) and map_size(value) > 0 ->
        key = Enum.random(Map.keys(value))

        case :rand.uniform(10) do
          1 ->
            {{field, :row_delete}, fn e -> Map.update!(e, field, &Map.delete(&1, key)) end}

          2 ->
            new_key = perturb(key, 0)
            row = Map.fetch!(value, key)
            {{field, :row_copy}, fn e -> Map.update!(e, field, &Map.put(&1, new_key, row)) end}

          _ ->
            row = perturb(Map.fetch!(value, key), 0)
            {{field, :row_perturb}, fn e -> Map.update!(e, field, &Map.put(&1, key, row)) end}
        end

      is_map(value) ->
        row = perturb(nil, 0)

        {{field, :empty_map_insert},
         fn e -> Map.update!(e, field, &Map.put(&1, @person, row)) end}

      true ->
        v = perturb(value, 0)
        {{field, :field}, fn e -> Map.put(e, field, v) end}
    end
  end

  defp perturb(value, depth) when is_map(value) and not is_struct(value) do
    cond do
      map_size(value) > 0 and depth < 5 and :rand.uniform() < 0.8 ->
        key = Enum.random(Map.keys(value))
        Map.put(value, key, perturb(Map.fetch!(value, key), depth + 1))

      map_size(value) > 0 and :rand.uniform(2) == 1 ->
        Map.delete(value, Enum.random(Map.keys(value)))

      true ->
        Map.put(value, :review_extra, 1)
    end
  end

  defp perturb(%MapSet{} = value, _depth) do
    if MapSet.size(value) > 0 and :rand.uniform(2) == 1,
      do: MapSet.delete(value, Enum.random(value)),
      else: MapSet.put(value, "review")
  end

  defp perturb(value, depth) when is_tuple(value) and tuple_size(value) > 0 and depth < 5 do
    i = :rand.uniform(tuple_size(value)) - 1
    put_elem(value, i, perturb(elem(value, i), depth + 1))
  end

  defp perturb(value, _) when is_integer(value),
    do: Enum.random([value + 1, value - 1, -1, 0, value * 1000 + 7, value + 1])

  defp perturb(value, _) when is_float(value), do: Enum.random([value + 0.1, -0.5, 1.5, 0.0, 1])

  defp perturb(value, _) when is_binary(value),
    do: Enum.random(["", "missing", value <> "x", <<0>>, @reader, @origin])

  defp perturb(nil, _), do: Enum.random([1, "x", %{}, true, 0])
  defp perturb(value, _) when is_boolean(value), do: Enum.random([not value, nil])
  defp perturb(value, _) when is_atom(value), do: Enum.random([nil, :review_other, "x"])
  defp perturb([], _), do: Enum.random([[1], ["x"], nil])
  defp perturb([_ | rest] = list, _), do: Enum.random([rest, list ++ list, nil])
  defp perturb(_value, _), do: nil
end

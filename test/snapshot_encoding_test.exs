defmodule AttentionServer.Store.SnapshotEncodingTest do
  use ExUnit.Case, async: true
  alias AttentionServer.{Engine, Store, Store.SnapshotEncoding}
  alias Exqlite.Sqlite3

  defp expected(engine) do
    persisted =
      engine
      |> Map.from_struct()
      |> Map.drop([:open_default_index, :advisory_id_index, :route_b_index, :touched_keys])

    {:attention_relay_state, 30, Map.put(persisted, :__struct__, Engine)}
  end

  test "cached independent blocks decode as ordinary FORMAT30 ETF across record mutations" do
    engine = Engine.new()
    {:ok, engine} = Engine.setup_person(engine, "person")
    {:ok, engine} = Engine.setup_device(engine, "person", "device")
    {payload, cache} = SnapshotEncoding.encode(engine, 30)
    assert :erlang.binary_to_term(payload, [:safe]) == expected(engine)

    {:ok, next} = Engine.setup_device(engine, "person", "second")
    {changed, next_cache} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(changed, [:safe]) == expected(next)
    assert next_cache.persons === cache.persons

    {:ok, next} = Engine.revoke_device(next, "device")
    {changed, _cache} = SnapshotEncoding.encode(next, 30, next_cache)
    assert :erlang.binary_to_term(changed, [:safe]) == expected(next)
  end

  test "opaque record data keeps ETF values and supports map additions and removals" do
    opaque = %{
      <<0, 255>> => [nil, true, false, -9, 1_208_925_819_614_629_174_706_176, 1.25, "λ", {1, 2}],
      "blob" => :crypto.strong_rand_bytes(100_000)
    }

    engine = %{Engine.new() | messages: %{{"p", "s", "m"} => opaque}}
    {payload, cache} = SnapshotEncoding.encode(engine, 30)
    assert :erlang.binary_to_term(payload) == expected(engine)
    next = %{engine | messages: Map.put(engine.messages, {"p", "s", "n"}, %{text: "new"})}
    {payload, cache} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
    next = %{next | messages: Map.delete(next.messages, {"p", "s", "m"})}
    {payload, _cache} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
  end

  @tag :tmp_dir
  test "the snapshot opener rejects earlier and future formats", %{tmp_dir: directory} do
    path = Path.join(directory, "state.sqlite3")
    assert {:ok, store} = Store.open(path)

    for format <- [1, 29, 31] do
      {payload, _cache} = SnapshotEncoding.encode(store.engine, format)
      checksum = :crypto.hash(:sha256, payload)
      assert {:ok, statement} =
               Sqlite3.prepare(store.db, "UPDATE relay_state SET payload = ?, checksum = ?")

      assert :ok = Sqlite3.bind(statement, [{:blob, payload}, {:blob, checksum}])
      assert :done = Sqlite3.step(store.db, statement)
      assert :ok = Sqlite3.release(store.db, statement)

      assert {:error, {:storage_unavailable, {:error, {:unsupported_store_format, ^format}}}} =
               Store.open(path)
    end

    assert :ok = Store.close(store)
  end

  test "FORMAT30 preserves alert choices, delivery state and focus permissions, excluding transient fields" do
    {:ok, engine} = Engine.setup_person(Engine.new(), "person")
    {:ok, engine} = Engine.setup_device(engine, "person", "device")

    engine = %{
      engine
      | alert_choices: %{"person" => "all"},
        delivery_states: %{"person" => "STOPPED"},
        devices:
          Map.update!(
            engine.devices,
            "device",
            &Map.merge(&1, %{see_focus: true, push_view: true})
          ),
        route_b_index: %{transient: "must be rebuilt"},
        touched_keys: MapSet.new([{:row, :devices, "device"}])
    }

    {payload, _cache} = SnapshotEncoding.encode(engine, 30)
    assert {:attention_relay_state, 30, persisted} = :erlang.binary_to_term(payload, [:safe])
    assert persisted.alert_choices == %{"person" => "all"}
    assert persisted.delivery_states == %{"person" => "STOPPED"}
    assert persisted.devices["device"].see_focus
    assert persisted.devices["device"].push_view
    refute Map.has_key?(persisted, :route_b_index)
    refute Map.has_key?(persisted, :touched_keys)
  end

  test "origin history remains readable across entry append, delivery update and removal" do
    key = {"person", "origin"}
    entry = %{"text" => :crypto.strong_rand_bytes(100_000), "delivered" => false}
    engine = %{Engine.new() | origin_feeds: %{key => %{high: 1, entries: %{1 => entry}}}}
    {payload, cache} = SnapshotEncoding.encode(engine, 30)
    assert :erlang.binary_to_term(payload) == expected(engine)

    next = put_in(engine.origin_feeds[key], %{high: 2, entries: %{1 => entry, 2 => entry}})
    {payload, cache} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(payload) == expected(next)

    next = put_in(next.origin_feeds[key].entries[1]["delivered"], true)
    {payload, cache} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(payload) == expected(next)

    next = %{next | origin_feeds: %{}}
    {payload, _} = SnapshotEncoding.encode(next, 30, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
  end
end

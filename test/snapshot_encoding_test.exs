defmodule AttentionServer.Store.SnapshotEncodingTest do
  use ExUnit.Case, async: true
  alias AttentionServer.{Engine, Store.SnapshotEncoding}

  defp expected(engine) do
    persisted = engine |> Map.from_struct() |> Map.drop([:open_default_index, :advisory_id_index])
    {:attention_relay_state, 27, Map.put(persisted, :__struct__, Engine)}
  end

  test "cached independent blocks decode as ordinary FORMAT27 ETF across record mutations" do
    engine = Engine.new()
    {:ok, engine} = Engine.setup_person(engine, "person")
    {:ok, engine} = Engine.setup_device(engine, "person", "device")
    {payload, cache} = SnapshotEncoding.encode(engine, 27)
    assert :erlang.binary_to_term(payload, [:safe]) == expected(engine)

    {:ok, next} = Engine.setup_device(engine, "person", "second")
    {changed, next_cache} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(changed, [:safe]) == expected(next)
    assert next_cache.persons === cache.persons

    {:ok, next} = Engine.revoke_device(next, "device")
    {changed, _cache} = SnapshotEncoding.encode(next, 27, next_cache)
    assert :erlang.binary_to_term(changed, [:safe]) == expected(next)
  end

  test "opaque record data keeps ETF values and supports map additions and removals" do
    opaque = %{
      <<0, 255>> => [nil, true, false, -9, 1_208_925_819_614_629_174_706_176, 1.25, "λ", {1, 2}],
      "blob" => :crypto.strong_rand_bytes(100_000)
    }

    engine = %{Engine.new() | messages: %{{"p", "s", "m"} => opaque}}
    {payload, cache} = SnapshotEncoding.encode(engine, 27)
    assert :erlang.binary_to_term(payload) == expected(engine)
    next = %{engine | messages: Map.put(engine.messages, {"p", "s", "n"}, %{text: "new"})}
    {payload, cache} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
    next = %{next | messages: Map.delete(next.messages, {"p", "s", "m"})}
    {payload, _cache} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
  end

  test "origin history remains readable across entry append, delivery update and removal" do
    key = {"person", "origin"}
    entry = %{"text" => :crypto.strong_rand_bytes(100_000), "delivered" => false}
    engine = %{Engine.new() | origin_feeds: %{key => %{high: 1, entries: %{1 => entry}}}}
    {payload, cache} = SnapshotEncoding.encode(engine, 27)
    assert :erlang.binary_to_term(payload) == expected(engine)

    next = put_in(engine.origin_feeds[key], %{high: 2, entries: %{1 => entry, 2 => entry}})
    {payload, cache} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(payload) == expected(next)

    next = put_in(next.origin_feeds[key].entries[1]["delivered"], true)
    {payload, cache} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(payload) == expected(next)

    next = %{next | origin_feeds: %{}}
    {payload, _} = SnapshotEncoding.encode(next, 27, cache)
    assert :erlang.binary_to_term(payload) == expected(next)
  end
end

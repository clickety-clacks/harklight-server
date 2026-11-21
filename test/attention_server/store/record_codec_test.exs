defmodule AttentionServer.Store.RecordCodecTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, Store.RecordCodec}

  test "round trips canonical fields, keyed records and set members" do
    key = {"person", "sender", "message"}

    engine = %{
      Engine.new()
      | messages: %{key => %{body: "retained"}, :nil_value => nil},
        opened_revisions: MapSet.new([{"person", "sender", "message", 1}]),
        pending_clock_high_water: 18
    }

    expected = %{engine | route_b_index: %Engine{}.route_b_index}
    assert {:ok, ^expected} = engine |> RecordCodec.encode() |> RecordCodec.decode()
  end

  test "changing one message changes only its encoded row" do
    key = {"person", "sender", "message"}
    value = %{body: "updated"}
    previous = %{Engine.new() | messages: %{key => %{body: "old"}, :another => %{body: "keep"}}}
    next = %{previous | messages: Map.put(previous.messages, key, value)}

    previous_rows = MapSet.new(RecordCodec.encode(previous))
    next_rows = MapSet.new(RecordCodec.encode(next))

    assert MapSet.to_list(MapSet.difference(next_rows, previous_rows)) ==
             [RecordCodec.encode_row({:row, :messages, key}, value)]
  end

  test "rejects damaged, duplicate and incomplete records on load" do
    rows = Engine.new() |> RecordCodec.encode() |> Enum.to_list()
    [{key, payload, checksum} | rest] = rows

    assert {:error, :invalid_state} =
             RecordCodec.decode([{key, payload <> <<0>>, checksum} | rest])

    assert {:error, :invalid_state} = RecordCodec.decode(rest)
    assert {:error, :invalid_state} = RecordCodec.decode([hd(rows) | rows])
  end

  test "derived indexes are rebuilt by the caller" do
    baseline = Engine.new()
    with_index = %{baseline | open_default_index: %{{"person", "sender", "message"} => 27}}

    assert Enum.to_list(RecordCodec.encode(baseline)) ==
             Enum.to_list(RecordCodec.encode(with_index))
  end
end

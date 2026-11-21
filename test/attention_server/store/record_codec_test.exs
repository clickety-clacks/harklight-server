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

  test "encodes advisory counters, entries and receipts as separate records" do
    scope = {"person", "origin", "session"}
    feed = %{high: 2, entries: %{1 => :old, 2 => :new}, receipts: MapSet.new([:old_id])}
    engine = %{Engine.new() | advisory_feeds: %{scope => feed}}
    rows = engine |> RecordCodec.encode() |> Enum.to_list()
    entry = RecordCodec.encode_row({:advisory, scope, {:entry, 2}}, :new)
    receipt = RecordCodec.encode_row({:advisory, scope, {:receipt, :old_id}}, true)
    high = RecordCodec.encode_row({:advisory, scope, :high}, 2)

    assert entry in rows
    assert receipt in rows
    assert high in rows
    assert {:ok, ^engine} = RecordCodec.decode(Enum.reverse(rows))
    assert {:error, :invalid_state} = RecordCodec.decode(List.delete(rows, high))
    assert {:error, :invalid_state} = RecordCodec.decode([entry | rows])
  end

  test "record identity includes the application codec version" do
    address = {:row, :messages, {"person", "sender", "msg"}}

    expected =
      Base.decode16!(
        "836802770F72656C61795F7265636F72645F763168037703726F7777086D65737361676573" <>
          "68036D00000006706572736F6E6D0000000673656E6465726D000000036D7367"
      )

    assert RecordCodec.encode_key(address) == expected
    assert {^expected, _, _} = RecordCodec.encode_row(address, nil)
    assert :erlang.binary_to_term(expected, [:safe]) == {:relay_record_v1, address}
  end

  test "retains event rows and decimal values" do
    key = {"person", 7}
    value = %{payload: %{"answer" => %AttentionServer.JSON.DecimalToken{raw: "-2.4e100"}}}
    engine = %{Engine.new() | conversation_events: %{key => value}}

    rows = engine |> RecordCodec.encode() |> Enum.to_list()
    assert RecordCodec.encode_row({:row, :conversation_events, key}, value) in rows
    assert {:ok, ^engine} = RecordCodec.decode(rows)
  end

  test "fresh VMs encode the same record bytes across atom insertion orders" do
    contents = """
    numbers = if hd(System.argv()) == "asc", do: 1..80, else: 80..1
    map = Map.new(numbers, fn n -> {String.to_atom("codec_test_atom_\#{n}"), n} end)
    {_, _, digest} = AttentionServer.Store.RecordCodec.encode_row({:row, :messages, "msg"}, map)
    IO.puts(Base.encode16(digest))
    """

    assert {first, 0} = cold_elixir(contents, ["asc"])
    assert {second, 0} = cold_elixir(contents, ["desc"])
    assert first == second
  end

  test "fresh VM decodes a row with a persisted decimal token" do
    value = %AttentionServer.JSON.DecimalToken{raw: "1.234e567"}
    engine = %{Engine.new() | conversation_events: %{{"person", 7} => value}}

    fixture =
      engine
      |> RecordCodec.encode()
      |> Enum.to_list()
      |> :erlang.term_to_binary()
      |> Base.encode64()

    contents = """
    rows = System.argv() |> hd() |> Base.decode64!() |> :erlang.binary_to_term([:safe])
    {:ok, engine} = AttentionServer.Store.RecordCodec.decode(rows)
    IO.puts(engine.conversation_events[{"person", 7}].raw)
    """

    assert {"1.234e567\n", 0} = cold_elixir(contents, [fixture])
  end

  test "origin history persists as independent high, entry and delivery rows" do
    scope = {"person", "origin"}

    entries =
      Map.new(
        1..300,
        &{&1, %{"session_handle" => "session", "event_id" => Integer.to_string(&1)}}
      )

    feed = %{high: 300, entries: entries, delivery_acks: %{1 => %{ack1: 4}}}
    engine = %{Engine.new() | origin_feeds: %{scope => feed}}
    normalized = AttentionServer.Conversation.index_origin_feeds(engine)
    before = MapSet.new(RecordCodec.encode(normalized))
    next = put_in(normalized.origin_feeds[scope].delivery_acks[1], %{ack1: 4, ack2: 7})
    after_rows = MapSet.new(RecordCodec.encode(next))

    assert MapSet.to_list(MapSet.difference(after_rows, before)) ==
             [
               RecordCodec.encode_row({:origin_feed, scope, {:delivery_ack, 1}}, %{
                 ack1: 4,
                 ack2: 7
               })
             ]

    assert {:ok, decoded} = RecordCodec.decode(after_rows |> Enum.to_list() |> Enum.reverse())
    refute Map.has_key?(decoded.origin_feeds[scope], :entry_seqs_by_handle)
    assert AttentionServer.Conversation.index_origin_feeds(decoded) == next
  end

  defp cold_elixir(contents, args) do
    beam_dir = :code.which(RecordCodec) |> List.to_string() |> Path.dirname()

    System.cmd(System.find_executable("elixir"), ["-pa", beam_dir, "-e", contents, "--" | args],
      stderr_to_stdout: true
    )
  end
end

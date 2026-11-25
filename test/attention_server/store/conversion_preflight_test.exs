defmodule AttentionServer.Store.ConversionPreflightTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Store.ConversionPreflight

  test "reports the compressed snapshot length and expanded length from six bytes" do
    # A sentinel body ensures this test never passes by inflating the snapshot.
    payload = <<131, 80, 100_000_000::32, 0, 0, 0>>

    assert {:ok, %{stored_bytes: 9, expanded_bytes: 100_000_000}} =
             ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
  end

  test "uses the length of an uncompressed snapshot" do
    payload = :erlang.term_to_binary({:attention_relay_state, 30, %{}})
    length = byte_size(payload)

    assert {:ok, %{stored_bytes: ^length, expanded_bytes: ^length}} =
             ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
  end

  test "refuses an unreadable or incomplete ETF header" do
    for payload <- [<<>>, <<130, 104, 3>>, <<131, 80, 1>>, <<131, 80, 0::32>>] do
      assert {:error, :fit_not_established} =
               ConversionPreflight.snapshot_sizes(byte_size(payload), payload)
    end
  end

  test "reads the revision from the snapshot's own compressed or uncompressed prefix" do
    for format <- [27, 29, 30], flags <- [[], [{:compressed, 6}]] do
      term = {:attention_relay_state, format, %{history: :binary.copy("synthetic", 10_000)}}
      payload = :erlang.term_to_binary(term, flags)
      assert {:ok, ^format} = ConversionPreflight.snapshot_format(payload)
    end
  end

  test "accepts the older atom tag encodings without loading an Engine" do
    tag = "attention_relay_state"
    size = byte_size(tag)

    for atom <- [
          <<100, size::16, tag::binary>>,
          <<118, size::16, tag::binary>>,
          <<119, size, tag::binary>>
        ] do
      payload = <<131, 104, 3, atom::binary, 97, 30, 116, 0::32>>
      assert {:ok, 30} = ConversionPreflight.snapshot_format(payload)
    end
  end

  test "skips a store that already has a FORMAT31 row anchor" do
    payload = :erlang.term_to_binary({:attention_relay_keyed_state, 31, {:fixture, 27}})

    assert {:already_converted, 31} = ConversionPreflight.snapshot_format(payload)
  end

  test "refuses a wrong anchor, unsupported revision, and truncated compressed prefix" do
    for payload <- [
          :erlang.term_to_binary({:attention_relay_keyed_state, 30, %{}}),
          :erlang.term_to_binary({:attention_relay_state, 28, %{}}),
          <<131, 80, 100_000_000::32, 0, 0, 0>>
        ] do
      assert {:error, :fit_not_established} = ConversionPreflight.snapshot_format(payload)
    end
  end
end

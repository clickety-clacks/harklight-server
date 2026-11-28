defmodule AttentionServer.Migration.SourceFormatTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Migration.SourceFormat
  alias AttentionServer.Store.RecordCodec

  defp anchor(format, identity) do
    payload = :erlang.term_to_binary({:attention_relay_keyed_state, format, identity})
    {payload, :crypto.hash(:sha256, payload)}
  end

  test "accepts only a verified current codec anchor without changing generations" do
    {payload, checksum} = anchor(32, RecordCodec.identity())

    assert {:ok, %{format: 32, version: 7, generation: 9}} =
             SourceFormat.validate(payload, checksum, 7, 9)

    assert {:error, :anchor_checksum_mismatch} = SourceFormat.validate(payload, <<0>>, 7, 9)
    assert {:error, :invalid_source_anchor} = SourceFormat.validate(payload, checksum, 9, 7)
  end

  test "older formats and incompatible codecs cannot enter an import" do
    for format <- [29, 30, 31, 33] do
      {payload, checksum} = anchor(format, RecordCodec.identity())
      assert {:error, :unsupported_source_format} = SourceFormat.validate(payload, checksum, 0, 0)
    end

    {payload, checksum} = anchor(32, {:relay_record_v1, "other"})
    assert {:error, :source_otp_mismatch} = SourceFormat.validate(payload, checksum, 0, 0)
    {payload, checksum} = anchor(32, :unknown_codec)
    assert {:error, :unsupported_record_codec} = SourceFormat.validate(payload, checksum, 0, 0)
  end

  test "malformed and oversized anchors fail without decoding a snapshot" do
    for payload <- [<<0>>, :binary.copy(<<0>>, 257)] do
      assert {:error, :invalid_source_anchor} =
               SourceFormat.validate(payload, :crypto.hash(:sha256, payload), 0, 0)
    end

    assert SourceFormat.upgrade_instruction() =~ "Never rerun bootstrap"
  end
end

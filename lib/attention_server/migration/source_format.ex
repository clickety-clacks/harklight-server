defmodule AttentionServer.Migration.SourceFormat do
  @moduledoc false

  alias AttentionServer.Store.RecordCodec

  # This inspects only the small FORMAT32 anchor. It never opens a Store,
  # bootstraps state, decodes an older snapshot, or changes the source.
  @maximum_anchor_bytes 256

  def validate(<<131, 104, 3, _::binary>> = payload, checksum, version, generation)
      when is_binary(payload) and is_binary(checksum) and
             byte_size(payload) <= @maximum_anchor_bytes and
             is_integer(version) and version >= 0 and
             is_integer(generation) and generation >= version do
    identity = RecordCodec.identity()

    if :crypto.hash(:sha256, payload) == checksum do
      case :erlang.binary_to_term(payload, [:safe]) do
        {:attention_relay_keyed_state, 32, ^identity} ->
          {:ok, %{format: 32, codec: identity, version: version, generation: generation}}

        {:attention_relay_keyed_state, 32, {:relay_record_v1, _major}} ->
          {:error, :source_otp_mismatch}

        {:attention_relay_keyed_state, 32, _codec} ->
          {:error, :unsupported_record_codec}

        _ ->
          {:error, :unsupported_source_format}
      end
    else
      {:error, :anchor_checksum_mismatch}
    end
  rescue
    ArgumentError -> {:error, :invalid_source_anchor}
  end

  def validate(_, _, _, _), do: {:error, :invalid_source_anchor}

  def upgrade_instruction do
    "Import requires a stopped, checkpointed FORMAT32 backup. " <>
      "For older supported snapshots, use the documented v0.2.16–v0.2.20 bridge " <>
      "to FORMAT30, then the packaged stopped-store converter from a FORMAT32 release. " <>
      "Keep database, uploads, configuration and keys together. Never rerun bootstrap. " <>
      "Unknown formats require a supported upgrade path or a readable backup."
  end
end

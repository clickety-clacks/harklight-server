defmodule AttentionServer.Store.ConversionPreflight do
  @moduledoc false

  alias AttentionServer.Store.RecordTable

  @snapshot_formats [27, 29, 30]
  @row_format 31
  @prefix_bytes 256
  @prefix_overhead 32 * 1024 * 1024

  # SQLite can materialize the whole BLOB for substr. length reads its size
  # without doing so. The caller holds the conversion transaction across read.
  def read(db, available_bytes) when is_integer(available_bytes) do
    with {:ok, [[stored_bytes]]} when is_integer(stored_bytes) <-
           RecordTable.select(db, "SELECT length(payload) FROM relay_state WHERE id = 1"),
         true <- stored_bytes + @prefix_overhead <= available_bytes,
         {:ok, [[prefix]]} <-
           RecordTable.select(db, "SELECT substr(payload, 1, ?) FROM relay_state WHERE id = 1", [
             @prefix_bytes
           ]) do
      inspect_prefix(stored_bytes, prefix)
    else
      false -> {:error, :fit_not_established}
      _ -> {:error, :unsupported_snapshot}
    end
  end

  defp inspect_prefix(stored_bytes, prefix) do
    with {:ok, sizes} <- snapshot_sizes(stored_bytes, prefix) do
      case snapshot_format(prefix) do
        {:ok, format} -> {:ok, Map.put(sizes, :format, format)}
        {:already_converted, _} = converted -> converted
        {:error, _} -> {:error, :unsupported_snapshot}
      end
    else
      {:error, _} -> {:error, :unsupported_snapshot}
    end
  end

  def snapshot_sizes(stored_bytes, <<131, 80, expanded_bytes::32, _::binary>>)
      when expanded_bytes > 0 and stored_bytes > 6 do
    {:ok, %{stored_bytes: stored_bytes, expanded_bytes: expanded_bytes}}
  end

  def snapshot_sizes(stored_bytes, <<131, tuple, _::binary>>)
      when tuple in [104, 105] and stored_bytes > 2 do
    {:ok, %{stored_bytes: stored_bytes, expanded_bytes: stored_bytes}}
  end

  def snapshot_sizes(_, _), do: {:error, :fit_not_established}

  def snapshot_format(<<131, 80, _expanded_bytes::32, compressed::binary>>) do
    stream = :zlib.open()

    try do
      :ok = :zlib.inflateInit(stream)
      {_status, first} = :zlib.safeInflate(stream, compressed)
      first |> IO.iodata_to_binary() |> parse_tuple()
    rescue
      ErlangError -> {:error, :fit_not_established}
    after
      :zlib.close(stream)
    end
  end

  def snapshot_format(<<131, tuple::binary>>), do: parse_tuple(tuple)
  def snapshot_format(_), do: {:error, :fit_not_established}

  defp parse_tuple(<<104, 3, encoding, size::16, bytes::binary>>)
       when encoding in [100, 118],
       do: parse_tag(bytes, size)

  defp parse_tuple(<<104, 3, 119, size, bytes::binary>>), do: parse_tag(bytes, size)
  defp parse_tuple(_), do: {:error, :fit_not_established}

  defp parse_tag(bytes, size) do
    case bytes do
      <<"attention_relay_state", 97, format, _::binary>>
      when size == byte_size("attention_relay_state") and format in @snapshot_formats ->
        {:ok, format}

      <<"attention_relay_keyed_state", 97, @row_format, _::binary>>
      when size == byte_size("attention_relay_keyed_state") ->
        {:already_converted, @row_format}

      _ ->
        {:error, :fit_not_established}
    end
  end
end

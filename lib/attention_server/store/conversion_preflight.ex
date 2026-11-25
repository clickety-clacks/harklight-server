defmodule AttentionServer.Store.ConversionPreflight do
  @moduledoc false

  @snapshot_formats [27, 29, 30]
  @row_format 31

  # SQL substr returns a small result but can materialize the entire SQLite BLOB.
  # The packaged reader uses SQLite's incremental BLOB API.
  def read(path, executable \\ Application.app_dir(:attention_server, "priv/snapshot-prefix")) do
    with {<<stored_bytes::32, prefix::binary>>, 0} <- System.cmd(executable, [path]),
         {:ok, sizes} <- snapshot_sizes(stored_bytes, prefix) do
      case snapshot_format(prefix) do
        {:ok, format} -> {:ok, Map.put(sizes, :format, format)}
        other -> other
      end
    else
      _ -> {:error, :fit_not_established}
    end
  rescue
    ErlangError -> {:error, :fit_not_established}
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

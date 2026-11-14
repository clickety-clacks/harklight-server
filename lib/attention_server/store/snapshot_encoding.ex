defmodule AttentionServer.Store.SnapshotEncoding do
  @moduledoc false

  # This is still one standard compressed Erlang external term, readable by
  # binary_to_term in the old release. Full-flush DEFLATE blocks have no shared
  # dictionary, so unchanged records can reuse their compressed bytes. The cache
  # is ephemeral; SQLite still gets a complete, checksummed snapshot per commit.
  def encode(engine, format, previous \\ %{}) do
    fields = engine |> Map.from_struct() |> Map.drop([:open_default_index, :advisory_id_index])
    fields = Map.put(fields, :__struct__, engine.__struct__)

    header =
      block([
        <<104, 3>>,
        term(:attention_relay_state),
        term(format),
        <<116, map_size(fields)::32>>
      ])

    {parts, cache} =
      Enum.map_reduce(Map.to_list(fields), %{}, fn {key, value}, cache ->
        entry = field(key, value, Map.get(previous, key))
        {elem(entry, 1), Map.put(cache, key, entry)}
      end)

    {bytes, size, checksum} = join([header | parts])
    # RFC1950 zlib header (DEFLATE, 32K window), final empty DEFLATE block,
    # and the checksum of the complete uncompressed ETF body.
    payload =
      IO.iodata_to_binary([<<131, 80, size::32, 0x78, 0x9C>>, bytes, <<3, 0, checksum::32>>])

    {payload, cache}
  end

  defp field(_key, value, {old, _part, _children} = previous) when value === old,
    do: previous

  defp field(key, value, previous) when is_map(value) do
    old_children =
      case previous do
        {_, _, children} when is_map(children) -> children
        _ -> %{}
      end

    header = block([term(key), <<116, map_size(value)::32>>])

    {parts, children} =
      Enum.map_reduce(Map.to_list(value), %{}, fn {record_key, record}, children ->
        entry =
          case Map.get(old_children, record_key) do
            {old, _part} = cached when old === record -> cached
            _ -> {record, block([term(record_key), term(record)])}
          end

        {elem(entry, 1), Map.put(children, record_key, entry)}
      end)

    {value, join([header | parts]), children}
  end

  defp field(key, value, _previous), do: {value, block([term(key), term(value)]), nil}

  defp term(value) do
    <<131, body::binary>> = :erlang.term_to_binary(value)
    body
  end

  defp block(raw) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, 6, :deflated, -15, 8, :default)
      bytes = :zlib.deflate(z, raw, :full) |> IO.iodata_to_binary()
      # Complete the temporary compressor solely to release it normally. Its
      # final block is not part of the reusable, byte-aligned fragment.
      _ = :zlib.deflate(z, [], :finish)
      :ok = :zlib.deflateEnd(z)
      {bytes, :erlang.iolist_size(raw), :erlang.adler32(raw)}
    after
      :zlib.close(z)
    end
  end

  defp join(parts) do
    {bytes, {size, checksum}} =
      Enum.map_reduce(parts, {0, 1}, fn {bytes, size, checksum}, {total, sum} ->
        {bytes, {total + size, :erlang.adler32_combine(sum, checksum, size)}}
      end)

    {bytes, size, checksum}
  end
end

defmodule AttentionServer.Store.ConversionPreflight do
  @moduledoc false

  alias AttentionServer.Store.RecordTable

  @snapshot_format 30
  @row_format 31
  @prefix_bytes 256
  @prefix_overhead 32 * 1024 * 1024

  # The 2,400-message FORMAT30 rehearsal included 99,999 conversation
  # facts, 1,300 sessions and up to 43 references to 16 retained files.
  # Round the worst peak ratio up with 20% headroom and reserve 512 MiB.
  # Extra disk includes database growth and peak WAL with 25% headroom.
  @common_budget %{
    memory_fixed: 512 * 1024 * 1024,
    disk_fixed: 128 * 1024 * 1024,
    disk_per_byte: 5
  }
  @shape_a Map.merge(@common_budget, %{
    expanded_bytes: 123_901_842,
    memory_per_byte: 23,
    revisions: 104_801,
    changes: 69_166,
    references: 43,
    uploads: 33_624_064
  })

  # The retained-data extension was rehearsed with these counts and the
  # original coefficients under a 4 GiB limit. Keep the original large band
  # available for denser snapshots in this new interval.
  @shape_a_extension %{
    @shape_a
    | expanded_bytes: 141_033_503,
      revisions: 119_801,
      changes: 79_166
  }
  @shape_fields [:upload_bytes, :client_resource_revs, :agent_changes, :large_data_references]

  # Both 50,000-message fixtures were rehearsed with this upload allocation
  # and no retained large-data references. Round each measured memory ratio
  # up with 20% headroom and apply the fixed reserve. Budget each band at its
  # upper end so smaller payloads can still carry its densest admitted shape.
  @large_budget Map.merge(@common_budget, %{references: 0, uploads: 8_392_704})
  @envelopes [
    @shape_a,
    Map.put(@shape_a_extension, :extension, true),
    Map.merge(@large_budget, %{
      expanded_bytes: 771_569_498,
      bound_bytes: 771_569_498,
      memory_per_byte: 13,
      revisions: 600_000,
      changes: 383_432
    }),
    Map.merge(@large_budget, %{
      expanded_bytes: 1_232_830_635,
      bound_bytes: 1_232_830_635,
      memory_per_byte: 10,
      revisions: 1_000_402,
      changes: 650_100
    })
  ]

  def check(db, budget, expected_format, envelopes \\ @envelopes) do
    with {:ok, info} <- read(db, budget.memory, budget.disk),
         :ok <- check_format(info.format, expected_format),
         :ok <- check_envelopes(db, info, budget, envelopes) do
      {:ok, info}
    end
  end

  defp check_envelopes(db, info, budget, envelopes) do
    expanded = info.expanded_bytes

    case Enum.drop_while(envelopes, &(expanded > &1.expanded_bytes)) do
      [] ->
        limit = envelopes |> Enum.map(& &1.expanded_bytes) |> Enum.max(fn -> 0 end)
        within_limit(expanded, limit, :expanded_snapshot)

      [%{extension: true} = extension, larger | _] ->
        case check_envelope(db, info, budget, extension) do
          {:error, {:fit_not_established, field, _, _}} = refused when field in @shape_fields ->
            case check_envelope(db, info, budget, larger) do
              {:error, {:fit_not_established, field, _, _}} when field in @shape_fields -> refused
              result -> result
            end

          result -> result
        end

      [envelope | _] ->
        check_envelope(db, info, budget, envelope)
    end
  end

  defp check_envelope(db, info, budget, envelope) do
    {memory, disk} = upper_bound(info, envelope)

    with :ok <- within_limit(memory, budget.memory, :memory),
         :ok <- within_limit(disk, budget.disk, :disk) do
      check_shape(db, budget.uploads, envelope)
    end
  end

  defp within_limit(value, limit, _field) when value <= limit, do: :ok

  defp within_limit(value, limit, field),
    do: {:error, {:fit_not_established, field, value, limit}}

  defp upper_bound(info, envelope) do
    # Coefficients are integer ratios rounded up from the worst rehearsal.
    # Keep the measured fixed margin in addition to that rounding.
    size = Map.get(envelope, :bound_bytes, info.expanded_bytes)

    {envelope.memory_fixed + envelope.memory_per_byte * size,
     envelope.disk_fixed + envelope.disk_per_byte * size}
  end

  defp check_format(actual, expected) when is_nil(expected) or actual == expected, do: :ok
  defp check_format(_, _), do: {:error, :source_format_mismatch}

  defp check_shape(db, uploads, envelope) do
    with :ok <- within_limit(uploads, envelope.uploads, :upload_bytes),
         :ok <- check_count(db, "client_resource_revs", envelope.revisions, :client_resource_revs),
         :ok <- check_count(db, "agent_changes", envelope.changes, :agent_changes) do
      check_count(db, "relay_large_data_refs", envelope.references, :large_data_references)
    end
  end

  defp check_count(db, table, limit, field) do
    case count_rows(db, table) do
      {:ok, count} -> within_limit(count, limit, field)
      _ -> {:error, {:fit_not_established, :measurement_unavailable, field}}
    end
  end

  defp count_rows(db, table) do
    with {:ok, [[exists]]} <-
           RecordTable.select(
             db,
             """
             SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?)
             """,
             [table]
           ) do
      if exists == 1 do
        with {:ok, [[count]]} <- RecordTable.select(db, "SELECT count(*) FROM #{table}"),
             do: {:ok, count}
      else
        {:ok, 0}
      end
    end
  end

  # SQLite can materialize the whole BLOB for substr. length reads its size
  # without doing so. The caller holds the conversion transaction across read.
  def read(db, available_bytes, disk_bytes)
      when is_integer(available_bytes) and is_integer(disk_bytes) do
    with {:ok, [[stored_bytes]]} when is_integer(stored_bytes) <-
           RecordTable.select(db, "SELECT length(payload) FROM relay_state WHERE id = 1"),
         :ok <- within_limit(stored_bytes + @prefix_overhead, available_bytes, :prefix_memory),
         :ok <- within_limit(stored_bytes + @prefix_overhead, disk_bytes, :prefix_disk),
         {:ok, [[prefix]]} <-
           RecordTable.select(db, "SELECT substr(payload, 1, ?) FROM relay_state WHERE id = 1", [
             @prefix_bytes
           ]) do
      inspect_prefix(db, stored_bytes, prefix)
    else
      {:error, {:fit_not_established, _, _, _}} = error -> error
      _ -> {:error, :unsupported_snapshot}
    end
  end

  defp inspect_prefix(db, stored_bytes, prefix) do
    with {:ok, sizes} <- snapshot_sizes(stored_bytes, prefix) do
      case snapshot_format(prefix) do
        {:ok, format} -> {:ok, Map.put(sizes, :format, format)}
        {:already_converted, _} -> confirm_row_anchor(db, stored_bytes, prefix)
        {:error, _} -> {:error, :unsupported_snapshot}
      end
    else
      {:error, _} -> {:error, :unsupported_snapshot}
    end
  end

  defp confirm_row_anchor(db, stored_bytes, prefix) when stored_bytes == byte_size(prefix) do
    with {:ok, [[checksum]]} <-
           RecordTable.select(db, "SELECT checksum FROM relay_state WHERE id = 1"),
         true <- :crypto.hash(:sha256, prefix) == checksum,
         {:ok, [[3]]} <-
           RecordTable.select(db, """
           SELECT count(*) FROM sqlite_schema WHERE type = 'table'
             AND name IN ('relay_records', 'relay_record_generation', 'relay_index_state')
           """),
         {:ok, _} <- RecordTable.generation(db),
         {:ok, [[1]]} <- RecordTable.select(db, "SELECT id FROM relay_index_state WHERE id = 1") do
      {:already_converted, @row_format}
    else
      _ -> {:error, :unsupported_snapshot}
    end
  end

  defp confirm_row_anchor(_db, _stored_bytes, _prefix), do: {:error, :unsupported_snapshot}

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
      <<"attention_relay_state", 97, @snapshot_format, _::binary>>
      when size == byte_size("attention_relay_state") ->
        {:ok, @snapshot_format}

      <<"attention_relay_keyed_state", 97, @row_format, _::binary>>
      when size == byte_size("attention_relay_keyed_state") ->
        {:already_converted, @row_format}

      _ ->
        {:error, :fit_not_established}
    end
  end
end

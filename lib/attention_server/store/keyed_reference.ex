defmodule AttentionServer.Store.KeyedReference do
  @moduledoc false

  alias AttentionServer.LargeData
  alias AttentionServer.Store.RecordCodec

  def blob_owners(engine) do
    engine
    |> Map.from_struct()
    |> Map.drop(RecordCodec.derived_fields())
    |> LargeData.count_blob_owners()
  end

  def update_blob_owners(owners, delta) do
    Enum.reduce_while(delta, {:ok, owners}, fn {id, change}, {:ok, current} ->
      case Map.get(current, id, 0) + change do
        count when count > 0 -> {:cont, {:ok, Map.put(current, id, count)}}
        0 -> {:cont, {:ok, Map.delete(current, id)}}
        _ -> {:halt, {:error, :invalid_blob_owners}}
      end
    end)
  end

  # For a rekey, pass both the removed and inserted address so both paths
  # are updated. A set ensures each record contributes to the delta once.
  def prepare(old, next, %MapSet{} = touched, directory) do
    changes = %{
      engine: next,
      records: [],
      delete_paths: [],
      reference_rows: [],
      orphan_candidates: [],
      blob_delta: %{},
      created: []
    }

    Enum.reduce_while(touched, {:ok, changes}, fn address, {:ok, changes} ->
      previous = refs_at(old, address)

      case prepare_record(changes, address, directory) do
        {:ok, updated} ->
          rows = refs_at(updated.engine, address)
          deleted = Enum.map(previous, &hd/1)
          candidates = Enum.map(previous, fn [_, _, id, _, _] -> id end)

          blob_delta =
            old
            |> count_record_blobs(address, updated.blob_delta, -1)
            |> then(&count_record_blobs(updated.engine, address, &1, 1))

          {:cont,
           {:ok,
            %{
              updated
              | delete_paths: deleted ++ updated.delete_paths,
                reference_rows: rows ++ updated.reference_rows,
                orphan_candidates: candidates ++ updated.orphan_candidates,
                blob_delta: blob_delta
            }}}

        error ->
          LargeData.cleanup_created(changes.created)
          {:halt, error}
      end
    end)
  end

  defp count_record_blobs(engine, address, counts, direction) do
    case RecordCodec.lookup(engine, address) do
      {:ok, value} -> LargeData.count_blob_owners(value, counts, direction)
      :error -> counts
    end
  end

  defp prepare_record(changes, address, directory) do
    case RecordCodec.lookup(changes.engine, address) do
      {:ok, value} ->
        with {:ok, durable, created} <- LargeData.externalize(value, directory) do
          row = RecordCodec.encode_row(address, LargeData.persist_refs(durable))

          engine =
            if value === durable,
              do: changes.engine,
              else: replace(changes.engine, address, durable)

          {:ok,
           %{
             changes
             | engine: engine,
               records: [{:put, row} | changes.records],
               created: created ++ changes.created
           }}
        end

      :error ->
        {:ok,
         %{changes | records: [{:delete, RecordCodec.encode_key(address)} | changes.records]}}
    end
  end

  defp refs_at(engine, address) do
    with path when is_list(path) <- path(address),
         {:ok, value} <- RecordCodec.lookup(engine, address) do
      LargeData.reference_rows_at(value, path)
    else
      _ -> []
    end
  end

  defp path({:field, field}), do: [{:map, field}]
  defp path({:row, field, key}), do: [{:map, field}, {:map, key}]

  defp path({:origin_feed, scope, {:entry, seq}}),
    do: feed_path(:origin_feeds, scope, :entries, seq)

  defp path({:origin_feed, scope, {:delivery_ack, seq}}),
    do: feed_path(:origin_feeds, scope, :delivery_acks, seq)

  defp path({:advisory, scope, {:entry, seq}}),
    do: feed_path(:advisory_feeds, scope, :entries, seq)

  defp path(_), do: nil

  defp feed_path(field, scope, collection, key),
    do: [{:map, field}, {:map, scope}, {:map, collection}, {:map, key}]

  defp replace(engine, {:field, field}, value), do: Map.replace!(engine, field, value)

  defp replace(engine, {:row, field, key}, value),
    do: Map.update!(engine, field, &Map.replace!(&1, key, value))

  defp replace(engine, {:origin_feed, scope, {:entry, seq}}, value),
    do: put_in(engine.origin_feeds[scope].entries[seq], value)

  defp replace(engine, {:origin_feed, scope, {:delivery_ack, seq}}, value),
    do: put_in(engine.origin_feeds[scope].delivery_acks[seq], value)

  defp replace(engine, {:advisory, scope, {:entry, seq}}, value),
    do: put_in(engine.advisory_feeds[scope].entries[seq], value)
end

defmodule AttentionServer.Migration.VerifyFeeds do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, VerifyAccounts}
  alias AttentionServer.Repository.Postgres.Attributes

  @tables ~w(origin_feed_entries origin_delivery_acks advisory_feed_entries advisory_feed_receipts)

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyAccounts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           Map.new(@tables, &{&1, 0}), fn row, acc -> verify(db, row, acc) end),
         :ok <- counts(db, counts) do
      {:ok, counts}
    end
  end

  defp verify(db, {{:origin_feed, {person, origin}, {:entry, position}} = address, value}, counts) do
    compare(db, "origin_feed_entries", address, counts,
      "SELECT session_handle,body FROM harklight.origin_feed_entries WHERE person_id=$1 AND origin_key=$2 AND position=$3",
      [person, origin, position], [Map.get(value, "session_handle"), Attributes.encode(value)])
  end
  defp verify(db, {{:origin_feed, {person, origin}, {:delivery_ack, position}} = address, value}, counts) do
    compare(db, "origin_delivery_acks", address, counts,
      "SELECT body FROM harklight.origin_delivery_acks WHERE person_id=$1 AND origin_key=$2 AND position=$3",
      [person, origin, position], [Attributes.encode(value)])
  end
  defp verify(db, {{:advisory, {person, origin, handle}, {:entry, position}} = address, value}, counts) do
    compare(db, "advisory_feed_entries", address, counts,
      "SELECT body FROM harklight.advisory_feed_entries WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND position=$4",
      [person, origin, handle, position], [Attributes.encode(value)])
  end
  defp verify(db, {{:advisory, {person, origin, handle}, {:receipt, id}} = address, true}, counts) do
    compare(db, "advisory_feed_receipts", address, counts,
      "SELECT true FROM harklight.advisory_feed_receipts WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND advisory_id=$4",
      [person, origin, handle, id], [true])
  end
  defp verify(_, _, counts), do: {:ok, counts}

  defp compare(db, table, address, counts, sql, params, expected) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{rows: [^expected]}} -> {:ok, Map.update!(counts, table, &(&1 + 1))}
      {:error, _} = error -> error
      _ -> {:error, {:feed_state_mismatch, table,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(address)), case: :lower)}}
    end
  end
  defp counts(db, counts) do
    Enum.reduce_while(counts, :ok, fn {table, expected}, :ok ->
      case ImportSession.query(db, "SELECT count(*) FROM harklight." <> table, []) do
        {:ok, %{rows: [[^expected]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, {:feed_count_mismatch, table}}}
      end
    end)
  end
end

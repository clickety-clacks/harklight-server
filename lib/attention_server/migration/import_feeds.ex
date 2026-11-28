defmodule AttentionServer.Migration.ImportFeeds do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  # Feed allocation highs are independent of client/Agent change-log positions.
  # Preserve facts separately, including delivery stages and advisory receipts.
  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root) do
      Enum.reduce_while([:highs, :entries, :receipts], :ok, fn phase, :ok ->
        case ImportRows.run(db, identity, "feeds/" <> Atom.to_string(phase) <> "/v1",
               Path.join(root, "records.frames"), manifest.data.records,
               &select?(&1, phase), &write/2) do
          {:ok, _} -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp select?({{family, _, part}, _}, phase) when family in [:origin_feed, :advisory] do
    case {phase, part} do
      {:highs, :high} -> true
      {:entries, {:entry, _}} -> true
      {:receipts, {:delivery_ack, _}} -> true
      {:receipts, {:receipt, _}} -> true
      _ -> false
    end
  end
  defp select?(_, _), do: false

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn entry, :ok ->
      case row(db, entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp row(db, {{:origin_feed, {person, origin}, :high}, high}),
    do: high(db, person, origin, "", "origin_feed", high)
  defp row(db, {{:advisory, {person, origin, handle}, :high}, high}),
    do: high(db, person, origin, handle, "advisory_feed", high)

  defp row(db, {{:origin_feed, {person, origin}, {:entry, position}}, body}) do
    with :ok <- within_high(db, person, origin, "", "origin_feed", position) do
      execute(db,
        "INSERT INTO harklight.origin_feed_entries(person_id,origin_key,session_handle,position,body) VALUES($1,$2,$3,$4,$5::text::jsonb)",
        [person, origin, Map.get(body, "session_handle"), position, encode(body)])
    end
  end

  defp row(db, {{:origin_feed, {person, origin}, {:delivery_ack, position}}, body}) do
    with {:ok, %{rows: [[1]]}} <- ImportSession.query(db,
           "SELECT 1 FROM harklight.origin_feed_entries WHERE person_id=$1 AND origin_key=$2 AND position=$3", [person, origin, position]) do
      execute(db,
        "INSERT INTO harklight.origin_delivery_acks(person_id,origin_key,position,body) VALUES($1,$2,$3,$4::text::jsonb)",
        [person, origin, position, encode(body)])
    else
      {:ok, _} -> {:error, :feed_reference_mismatch}
      error -> error
    end
  end

  defp row(db, {{:advisory, {person, origin, handle}, {:entry, position}}, body}) do
    with :ok <- within_high(db, person, origin, handle, "advisory_feed", position) do
      execute(db,
        "INSERT INTO harklight.advisory_feed_entries(person_id,origin_key,session_handle,position,body) VALUES($1,$2,$3,$4,$5::text::jsonb)",
        [person, origin, handle, position, encode(body)])
    end
  end

  defp row(db, {{:advisory, {person, origin, handle}, {:receipt, id}}, true}) do
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT body FROM harklight.advisory_feed_entries WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3", [person, origin, handle]),
         true <- Enum.any?(rows, fn [body] ->
           case Attributes.decode(body) do
             {:ok, %{"advisory_id" => ^id}} -> true
             _ -> false
           end
         end) do
      execute(db,
        "INSERT INTO harklight.advisory_feed_receipts(person_id,origin_key,session_handle,advisory_id) VALUES($1,$2,$3,$4)",
        [person, origin, handle, id])
    else
      false -> {:error, :feed_reference_mismatch}
      error -> error
    end
  end

  defp row(_, _), do: {:error, :unsupported_feed_fact}

  defp high(db, person, origin, handle, kind, high),
    do: execute(db,
      "INSERT INTO harklight.allocation_highs(person_id,origin_key,session_handle,kind,high) VALUES($1,$2,$3,$4,$5)",
      [person, origin, handle, kind, high])

  defp within_high(db, person, origin, handle, kind, position) do
    case ImportSession.query(db,
           "SELECT 1 FROM harklight.allocation_highs WHERE person_id=$1 AND origin_key=$2 AND session_handle=$3 AND kind=$4 AND high >= $5", [person, origin, handle, kind, position]) do
      {:ok, %{rows: [[1]]}} -> :ok
      {:ok, _} -> {:error, :feed_reference_mismatch}
      error -> error
    end
  end

  defp encode(value), do: JSON.encode!(Attributes.encode(value))
  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :feed_reference_mismatch}
      error -> error
    end
  end
end

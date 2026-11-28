defmodule AttentionServer.Migration.ImportReceipts do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession, ImportTables}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.{JSON, ClientAPI.Idempotency}

  @tables ~w(client_idempotency harklight_app_receipts)

  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         :ok <- ImportTables.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root) do
      Enum.reduce_while(@tables, :ok, fn table, :ok ->
        case Map.fetch(manifest.data.tables, table) do
          :error -> {:cont, :ok}
          {:ok, source} ->
            case ImportRows.run(db, identity, "receipts/" <> table <> "/v1",
                   Path.join(root, source.file), source.summary, fn _ -> true end,
                   fn tx, rows -> each(rows, &row(tx, table, &1)) end) do
              {:ok, _} -> {:cont, :ok}
              error -> {:halt, error}
            end
        end
      end)
    end
  end

  def scope("agent:" <> encoded) do
    with {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         {person, origin} when is_binary(person) and is_binary(origin) <- :erlang.binary_to_term(bytes, [:safe]),
         true <- person != "" and origin != "" do
      {:ok, {person, origin}}
    else
      _ -> {:error, :invalid_receipt_scope}
    end
  rescue
    _ -> {:error, :invalid_receipt_scope}
  end

  def scope(person) when is_binary(person) and person != "", do: {:ok, {person, ""}}
  def scope(_), do: {:error, :invalid_receipt_scope}

  defp row(db, "client_idempotency" = table, [scope, key, digest, status, body, at]),
    do: insert(db, table, scope, key, digest, status, body, at, at + Idempotency.retention_ms(), false)

  defp row(db, "harklight_app_receipts" = table, [scope, key, digest, status, body, at, original])
       when original in [0, 1],
    do: insert(db, table, scope, key, digest, status, body, at, nil, original == 1)

  defp row(_, _, _), do: {:error, :unsupported_receipt_row}

  defp insert(db, namespace, source_scope, key, digest, status, body, at, expires, original) do
    with {:ok, {person, origin}} <- scope(source_scope),
         {:ok, hash} when byte_size(hash) == 32 <- Base.decode16(digest, case: :lower),
         {:ok, decoded} when is_map(decoded) <- JSON.decode_line(body <> "\n"),
         {:ok, %{num_rows: 1}} <- ImportSession.query(db,
           """
           INSERT INTO harklight.operation_receipts
             (person_id,origin_key,namespace,operation_key,digest,status,response,created_at_ms,expires_at_ms,original_response)
           VALUES ($1,$2,$3,$4,$5,$6,$7::text::jsonb,$8,$9,$10)
           """,
           [person, origin, namespace, key, hash, status, JSON.encode!(Attributes.encode(decoded)), at, expires, original]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_receipt}
    end
  end

  defp each(entries, fun),
    do: Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case fun.(entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
end

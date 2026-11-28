defmodule AttentionServer.Migration.ImportUploads do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportAccounts, ImportBytes, ImportMessages,
    ImportRows, ImportSession, ImportTables}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         :ok <- ImportTables.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root),
         :ok <- manifests(db, identity, root, manifest),
         :ok <- files(db, identity, root, manifest),
         :ok <- ImportMessages.run(db, identity, root),
         {:ok, _} <- ImportRows.run(db, identity, "uploads/message-links/v1",
           Path.join(root, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &links/2) do
      :ok
    end
  end

  defp manifests(db, identity, root, manifest) do
    with {:ok, _} <- ImportRows.run(db, identity, "uploads/manifests/v1",
           Path.join(root, "records.frames"), manifest.data.records,
           &match?({{:row, :uploads, _}, _}, &1), fn tx, rows ->
             each(rows, fn {{:row, :uploads, id}, source} ->
               insert(tx, id, source.person, source.uploader, source.size, source.sha256,
                 Attributes.encode(source))
             end)
           end),
         :ok <- staged(db, identity, root, manifest) do
      :ok
    end
  end

  defp staged(db, identity, root, manifest) do
    case Map.fetch(manifest.data.tables, "client_staged_files") do
      :error -> :ok
      {:ok, table} ->
        case ImportRows.run(db, identity, "uploads/staged-manifests/v1",
               Path.join(root, table.file), table.summary, fn _ -> true end, fn tx, rows ->
                 each(rows, fn [id, person, _name, _type, size, hash, at] = source ->
                   with {:ok, owner} <- staged_owner(tx, id) do
                     attrs = source |> Attributes.encode() |> Map.put("reservation_expires_at_ms", at + 30 * 86_400_000)
                     insert(tx, id, person, owner, size, hash, attrs)
                   end
                 end)
               end) do
          {:ok, _} -> :ok
          error -> error
        end
    end
  end

  defp staged_owner(db, id) do
    case ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='client_staged_file_owners' AND address=$1",
           [Attributes.key(id)]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} ->
        case Attributes.decode(body) do
          {:ok, [^id, owner]} -> {:ok, owner}
          _ -> {:error, :invalid_staged_owner}
        end
      error -> error
    end
  end

  defp insert(db, id, person, owner, size, hex, attrs) do
    with {:ok, hash} <- decode_hash(hex) do
      execute(db,
        "INSERT INTO harklight.uploads(id,person_id,owner_key,size_bytes,sha256,attributes) VALUES($1,$2,$3,$4,$5,$6::text::jsonb)",
        [id, person, owner, size, hash, JSON.encode!(attrs)])
    end
  end

  defp files(db, identity, root, manifest) do
    with {:ok, :ok} <- Frames.fold(Path.join(root, "records.frames"), manifest.data.records, :ok, fn
           {{:row, :uploads, id}, source}, :ok ->
             transfer(db, identity, root, manifest, id, "files/uploads/" <> id,
               %{size: source.size, sha256: source.sha256}, source.last_activity_ms)
           _, :ok -> {:ok, :ok}
         end) do
      case Map.fetch(manifest.data.tables, "client_staged_files") do
        :error -> :ok
        {:ok, table} ->
          case Frames.fold(Path.join(root, table.file), table.summary, :ok, fn
                 [id, _, _, _, size, hash, at], :ok ->
                   transfer(db, identity, root, manifest, id,
                     "files/uploads/client-api-v2-staged/" <> id, %{size: size, sha256: hash}, at)
               end) do
            {:ok, :ok} -> :ok
            error -> error
          end
      end
    end
  end

  defp transfer(db, identity, root, manifest, id, relative, expected, at) do
    with {:ok, ^expected} <- Map.fetch(manifest.data.files, relative),
         {:ok, result} <- ImportBytes.run(db, identity, id, Path.join(root, relative), expected),
         :ok <- complete(db, identity, id, expected, result.chunks, at) do
      {:ok, :ok}
    else
      :error -> {:error, :missing_exported_upload}
      {:ok, _} -> {:error, :upload_manifest_mismatch}
      error -> error
    end
  end

  defp complete(db, identity, id, expected, chunks, at) when is_integer(at) and at >= 0 do
    family = "uploads/complete/" <> id <> "/v1"
    entry = {id, expected, chunks, at}
    with {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, family) do
      case checkpoint.ordinal do
        0 ->
          case ImportSession.batch(db, identity, family, checkpoint, [entry], fn tx, _ ->
                 with :ok <- verify_bytes(tx, id, expected, chunks) do
                   execute(tx, "UPDATE harklight.uploads SET completed_at_ms=$2 WHERE id=$1 AND completed_at_ms IS NULL", [id, at])
                 end
               end) do
            {:ok, _} -> :ok
            error -> error
          end
        1 ->
          first = %{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)}
          if ImportSession.advance(first, [entry]) == checkpoint,
            do: :ok, else: {:error, :checkpoint_source_mismatch}
        _ -> {:error, :checkpoint_source_mismatch}
      end
    end
  end
  defp complete(_, _, _, _, _, _), do: {:error, :invalid_upload_clock}

  defp verify_bytes(db, id, expected, chunks) do
    ordinals = if chunks == 0, do: [], else: 0..(chunks - 1)
    with {:ok, {size, ctx}} <- Enum.reduce_while(ordinals, {:ok, {0, :crypto.hash_init(:sha256)}}, fn ordinal, {:ok, {size, ctx}} ->
           case ImportSession.query(db, "SELECT bytes FROM harklight.upload_chunks WHERE upload_id=$1 AND chunk_number=$2", [id, ordinal]) do
             {:ok, %{rows: [[bytes]]}} -> {:cont, {:ok, {size + byte_size(bytes), :crypto.hash_update(ctx, bytes)}}}
             {:ok, _} -> {:halt, {:error, :upload_chunk_mismatch}}
             error -> {:halt, error}
           end
         end),
         {:ok, %{rows: [[^chunks]]}} <- ImportSession.query(db, "SELECT count(*) FROM harklight.upload_chunks WHERE upload_id=$1", [id]),
         {:ok, hash} <- decode_hash(expected.sha256),
         true <- size == expected.size and :crypto.hash_final(ctx) == hash do
      :ok
    else
      false -> {:error, :upload_content_mismatch}
      {:ok, _} -> {:error, :upload_chunk_mismatch}
      error -> error
    end
  end

  defp links(db, rows) do
    each(rows, fn {{:row, :messages, key = {person, _, _}}, record} ->
      each(Map.get(record, :files_by_revision, %{}), fn {revision, files} ->
        each(Enum.with_index(files), fn {file, ordinal} ->
          case file do
            %{"tier" => "inline"} -> :ok
            %{"upload_id" => upload, "size" => size, "sha256" => hex} ->
              with {:ok, hash} <- decode_hash(hex) do
                execute(db,
                  """
                  INSERT INTO harklight.message_uploads(message_id,revision,ordinal,upload_id,attributes)
                  SELECT $1,$2,$3,id,$7::text::jsonb FROM harklight.uploads
                  WHERE id=$4 AND person_id=$5 AND size_bytes=$6 AND sha256=$8 AND completed_at_ms IS NOT NULL
                  """, [ImportMessages.id(key, record), revision, ordinal, upload, person, size,
                    JSON.encode!(Attributes.encode(file)), hash])
              end
            _ -> {:error, :unsupported_attachment_reference}
          end
        end)
      end)
    end)
  end

  defp decode_hash(hex) do
    case Base.decode16(hex, case: :lower) do
      {:ok, hash} when byte_size(hash) == 32 -> {:ok, hash}
      _ -> {:error, :invalid_upload_hash}
    end
  end
  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :upload_reference_mismatch}
      error -> error
    end
  end
end

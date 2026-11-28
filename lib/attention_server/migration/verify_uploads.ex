defmodule AttentionServer.Migration.VerifyUploads do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, VerifyFacts}
  alias AttentionServer.Repository.Postgres.{Attributes, Uploads}

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
           {{:row, :uploads, id}, source}, count ->
             with :ok <- file(db, bundle, manifest, id, "files/uploads/" <> id,
               source.person, source.uploader, source.size, source.sha256,
               source.last_activity_ms, Attributes.encode(source)), do: {:ok, count + 1}
           _, count -> {:ok, count}
         end),
         {:ok, count} <- staged(db, bundle, manifest, count),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db, "SELECT count(*) FROM harklight.uploads", []) do
      {:ok, %{uploads: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, :upload_count_mismatch}
    end
  end

  defp staged(db, bundle, manifest, count) do
    case Map.fetch(manifest.data.tables, "client_staged_files") do
      :error -> {:ok, count}
      {:ok, table} ->
        Frames.fold(Path.join(bundle, table.file), table.summary, count, fn
          [id, person, _name, _type, size, hash, at] = source, count ->
            with {:ok, owner} <- owner(db, id),
                 attrs = Map.put(Attributes.encode(source), "reservation_expires_at_ms", at + 30 * 86_400_000),
                 :ok <- file(db, bundle, manifest, id, "files/uploads/client-api-v2-staged/" <> id,
                   person, owner, size, hash, at, attrs), do: {:ok, count + 1}
        end)
    end
  end

  # VerifyFacts has already compared this staged owner row with the export.
  defp owner(db, id) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family='client_staged_file_owners' AND address=$1", [Attributes.key(id)]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} ->
        case Attributes.decode(body) do
          {:ok, [^id, owner]} -> {:ok, owner}
          _ -> {:error, :upload_owner_mismatch}
        end
      error -> error
    end
  end

  defp file(db, bundle, manifest, id, relative, person, owner, size, hex, at, attrs) do
    with {:ok, %{size: ^size, sha256: ^hex}} <- Map.fetch(manifest.data.files, relative),
         {:ok, hash} <- Base.decode16(hex, case: :lower),
         {:ok, %{rows: [[^person, ^owner, ^size, ^hash, ^at, ^attrs]]}} <- ImportSession.query(db,
           "SELECT person_id,owner_key,size_bytes,sha256,completed_at_ms,attributes FROM harklight.uploads WHERE id=$1", [id]),
         {:ok, chunks} <- chunks(db, id, Path.join(bundle, relative)),
         {:ok, %{rows: [[^chunks]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.upload_chunks WHERE upload_id=$1", [id]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :upload_source_mismatch}
    end
  end

  defp chunks(db, id, path) do
    path |> File.stream!([], Uploads.chunk_bytes()) |> Enum.reduce_while({:ok, 0}, fn expected, {:ok, ordinal} ->
      case ImportSession.query(db,
        "SELECT bytes FROM harklight.upload_chunks WHERE upload_id=$1 AND chunk_number=$2", [id, ordinal]) do
        {:ok, %{rows: [[^expected]]}} -> {:cont, {:ok, ordinal + 1}}
        {:ok, _} -> {:halt, {:error, :upload_chunk_source_mismatch}}
        error -> {:halt, error}
      end
    end)
  rescue
    _ -> {:error, :upload_source_unreadable}
  end
end

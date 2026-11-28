defmodule AttentionServer.Migration.ImportBytes do
  @moduledoc false
  alias AttentionServer.Migration.ImportSession
  alias AttentionServer.Repository.Postgres.Uploads

  # The caller has verified the immutable bundle and inserted an upload manifest.
  # Each chunk and its source-prefix checkpoint commit together. A failed or
  # interrupted file remains unpublished; completion is a separate validated step.
  def run(db, identity, id, path, %{size: size, sha256: hex}) do
    family = "upload-bytes/" <> id <> "/v1"
    with {:ok, hash} when byte_size(hash) == 32 <- Base.decode16(hex, case: :lower),
         {:ok, %{rows: [[^size, ^hash, completed]]}} <- ImportSession.query(db,
           "SELECT size_bytes,sha256,completed_at_ms FROM harklight.uploads WHERE id=$1", [id]),
         {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, family) do
      initial = %{position: %{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)},
        checkpoint: checkpoint, completed: not is_nil(completed), bytes: 0, hash: :crypto.hash_init(:sha256)}
      result = path |> File.stream!([], Uploads.chunk_bytes())
        |> Enum.reduce_while({:ok, initial}, fn bytes, {:ok, state} ->
          case consume(db, identity, family, id, bytes, state) do
            {:ok, next} -> {:cont, {:ok, next}}
            error -> {:halt, error}
          end
        end)
      with {:ok, final} <- result,
           true <- final.position.ordinal >= checkpoint.ordinal,
           true <- final.bytes == size and :crypto.hash_final(final.hash) == hash do
        {:ok, %{chunks: final.position.ordinal, bytes: final.bytes}}
      else
        false -> {:error, :upload_source_mismatch}
        error -> error
      end
    else
      :error -> {:error, :invalid_upload_hash}
      {:ok, _} -> {:error, :upload_manifest_mismatch}
      error -> error
    end
  rescue
    File.Error -> {:error, :upload_source_read_failed}
  end

  defp consume(db, identity, family, id, bytes, state) do
    entry = {state.position.ordinal, bytes}
    next = ImportSession.advance(state.position, [entry])
    updated = %{state | position: next, bytes: state.bytes + byte_size(bytes),
      hash: :crypto.hash_update(state.hash, bytes)}
    cond do
      next.ordinal < state.checkpoint.ordinal -> {:ok, updated}
      next.ordinal == state.checkpoint.ordinal ->
        if next == state.checkpoint, do: {:ok, updated}, else: {:error, :checkpoint_source_mismatch}
      state.completed -> {:error, :completed_upload_checkpoint_mismatch}
      true ->
        case ImportSession.batch(db, identity, family, state.checkpoint, [entry], fn tx, [{ordinal, data}] ->
          case ImportSession.query(tx,
                 "INSERT INTO harklight.upload_chunks(upload_id,chunk_number,bytes) VALUES($1,$2,$3)",
                 [id, ordinal, data]) do
            {:ok, %{num_rows: 1}} -> :ok
            {:ok, _} -> {:error, :upload_chunk_mismatch}
            error -> error
          end
        end) do
          {:ok, checkpoint} -> {:ok, %{updated | checkpoint: checkpoint}}
          error -> error
        end
    end
  end
end

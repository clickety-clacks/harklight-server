defmodule AttentionServer.Repository.Postgres.Uploads do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Repo, Transaction}
  alias Ecto.Adapters.SQL

  @chunk_bytes 1_048_576

  def chunk_bytes, do: @chunk_bytes

  def begin_upload(person, owner, id, size, sha256, metadata, limit)
      when is_integer(size) and size >= 0 and is_binary(sha256) and byte_size(sha256) == 32 do
    if limit != :none and size > limit do
      {:error, :upload_cap_exceeded}
    else
      with {:ok, %{num_rows: 1}} <-
             Transaction.query(
               """
               INSERT INTO harklight.uploads
                 (id, person_id, owner_key, size_bytes, sha256, attributes)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT DO NOTHING
               """,
               [id, person, owner, size, sha256, Attributes.encode(metadata)]
             ) do
        {:ok, id}
      else
        {:ok, _} -> {:error, :upload_conflict}
        error -> error
      end
    end
  end

  def put_chunk({person, owner, id}, ordinal, bytes)
      when is_integer(ordinal) and ordinal >= 0 and is_binary(bytes) and byte_size(bytes) > 0 do
    Transaction.run([], fn _ ->
      with {:ok, size, _sha256, nil} <- lock_manifest(person, owner, id),
           expected = min(@chunk_bytes, size - ordinal * @chunk_bytes),
           true <- byte_size(bytes) == expected,
           {:ok, %{num_rows: 1}} <-
             Transaction.query(
               """
               INSERT INTO harklight.upload_chunks (upload_id, chunk_number, bytes)
               VALUES ($1, $2, $3)
               ON CONFLICT (upload_id, chunk_number) DO UPDATE SET bytes = excluded.bytes
               WHERE harklight.upload_chunks.bytes = excluded.bytes
               """,
               [id, ordinal, bytes]
             ) do
        {:ok, :stored}
      else
        {:ok, %{num_rows: 0}} -> {:error, :upload_conflict}
        {:ok, _, _, _} -> {:error, :upload_completed}
        false -> {:error, :invalid_chunk}
        error -> error
      end
    end)
  end

  def complete({person, owner, id}, completed_at_ms) when is_integer(completed_at_ms) do
    Transaction.run([], fn _ ->
      case lock_manifest(person, owner, id) do
        {:ok, size, sha256, nil} ->
          with :ok <- verify_content(id, size, sha256),
               {:ok, %{num_rows: 1}} <-
                 Transaction.query(
                   "UPDATE harklight.uploads SET completed_at_ms = $2 WHERE id = $1",
                   [id, completed_at_ms]
                 ) do
            {:ok, id}
          end

        {:ok, _, _, _} ->
          {:ok, id}

        error ->
          error
      end
    end)
  end

  def read_chunk(person, id, ordinal) when is_integer(ordinal) and ordinal >= 0 do
    case Transaction.query(
           """
           SELECT chunks.bytes
           FROM harklight.uploads AS upload
           JOIN harklight.upload_chunks AS chunks ON chunks.upload_id = upload.id
           WHERE upload.person_id = $1 AND upload.id = $2 AND chunks.chunk_number = $3
             AND upload.completed_at_ms IS NOT NULL
           """,
           [person, id, ordinal]
         ) do
      {:ok, %{rows: [[bytes]]}} -> {:ok, bytes}
      {:ok, %{rows: []}} -> {:error, :not_found}
      error -> error
    end
  end

  defp lock_manifest(person, owner, id) do
    case Transaction.query(
           """
           SELECT size_bytes, sha256, completed_at_ms
           FROM harklight.uploads
           WHERE person_id = $1 AND owner_key IS NOT DISTINCT FROM $2 AND id = $3
           FOR UPDATE
           """,
           [person, owner, id]
         ) do
      {:ok, %{rows: [[size, sha256, completed]]}} -> {:ok, size, sha256, completed}
      {:ok, %{rows: []}} -> {:error, :not_found}
      error -> error
    end
  end

  defp verify_content(id, size, sha256) do
    stream =
      SQL.stream(
        Repo,
        "SELECT chunk_number, bytes FROM harklight.upload_chunks WHERE upload_id = $1 ORDER BY chunk_number",
        [id],
        max_rows: 1,
        timeout: 5_000,
        log: false
      )

    result =
      Enum.reduce_while(stream, {0, 0, :crypto.hash_init(:sha256)}, fn
        %{rows: [[ordinal, bytes]]}, {ordinal, total, hash} ->
          {:cont, {ordinal + 1, total + byte_size(bytes), :crypto.hash_update(hash, bytes)}}

        %{rows: []}, state ->
          {:cont, state}

        _, _ ->
          {:halt, :invalid_chunk}
      end)

    case result do
      {_, ^size, hash} ->
        if :crypto.hash_final(hash) == sha256, do: :ok, else: {:error, :upload_digest_mismatch}

      _ ->
        {:error, :incomplete_upload}
    end
  end
end

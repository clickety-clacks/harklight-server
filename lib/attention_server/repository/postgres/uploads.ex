defmodule AttentionServer.Repository.Postgres.Uploads do
  @moduledoc false

  alias AttentionServer.Repository.Postgres.{Attributes, Repo, Transaction}
  alias Ecto.Adapters.SQL

  @chunk_bytes 1_048_576
  @cleanup_batch 32

  def chunk_bytes, do: @chunk_bytes

  def begin_upload(person, owner, id, size, sha256, metadata, %{
        file_limit: file_limit,
        account_limit: account_limit,
        expires_at_ms: expires_at_ms
      })
      when is_integer(size) and size >= 0 and is_binary(sha256) and byte_size(sha256) == 32 and
             (file_limit == :none or (is_integer(file_limit) and file_limit >= 0)) and
             is_integer(account_limit) and account_limit >= 0 and is_integer(expires_at_ms) do
    attrs = Attributes.encode(metadata)

    Transaction.run([], fn _ ->
      with :ok <- lock_account(person) do
        case existing_manifest({person, owner, id}, size, sha256, attrs) do
          {:ok, :matches} ->
            {:ok, id}

          {:ok, :missing} ->
            with :ok <- check_capacity(person, size, file_limit, account_limit) do
              insert_manifest({person, owner, id}, size, sha256, attrs, expires_at_ms)
            end

          {:error, _} = error ->
            error
        end
      end
    end)
  end

  defp lock_account(person) do
    # Serialize admissions per person while allowing unrelated people and
    # foreign-key inserts to proceed.
    case Transaction.query("SELECT id FROM harklight.people WHERE id = $1 FOR NO KEY UPDATE", [
           person
         ]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  defp existing_manifest({person, owner, id}, size, sha256, attrs) do
    case Transaction.query(
           """
           SELECT owner_key IS NOT DISTINCT FROM $3 AND size_bytes = $4
                    AND sha256 = $5 AND (attributes - 'reservation_expires_at_ms') = $6
           FROM harklight.uploads WHERE person_id = $1 AND id = $2 FOR UPDATE
           """,
           [person, id, owner, size, sha256, attrs]
         ) do
      {:ok, %{rows: [[true]]}} -> {:ok, :matches}
      {:ok, %{rows: [[false]]}} -> {:error, :upload_conflict}
      {:ok, %{rows: []}} -> {:ok, :missing}
      {:error, _} = error -> error
    end
  end

  defp check_capacity(_person, size, file_limit, _account_limit)
       when is_integer(file_limit) and size > file_limit,
       do: {:error, :upload_cap_exceeded}

  defp check_capacity(person, size, _file_limit, account_limit) do
    case Transaction.query(
           """
           SELECT coalesce(sum(size_bytes), 0) + $2::bigint <= $3::bigint
           FROM harklight.uploads WHERE person_id = $1
           """,
           [person, size, account_limit]
         ) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> {:error, :upload_cap_exceeded}
      {:error, _} = error -> error
    end
  end

  defp insert_manifest({person, owner, id}, size, sha256, attrs, expires_at_ms) do
    case Transaction.query(
           """
           INSERT INTO harklight.uploads
             (id, person_id, owner_key, size_bytes, sha256, attributes)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT DO NOTHING
           """,
           [id, person, owner, size, sha256, Map.put(attrs, "reservation_expires_at_ms", expires_at_ms)]
         ) do
      {:ok, %{num_rows: 1}} -> {:ok, id}
      {:ok, %{num_rows: 0}} -> {:error, :upload_conflict}
      {:error, _} = error -> error
    end
  end

  def cleanup(person, now_ms) when is_integer(now_ms) do
    Transaction.run([], fn _ ->
      with {:ok, %{rows: rows}} <-
             Transaction.query(
               """
               SELECT upload.id FROM harklight.uploads AS upload
               WHERE upload.person_id = $1
                 AND NOT EXISTS (
                   SELECT 1 FROM harklight.import_state WHERE phase <> 'active'
                 )
                 AND (upload.attributes->>'reservation_expires_at_ms')::bigint <= $2
                 AND NOT EXISTS (
                   SELECT 1 FROM harklight.message_uploads WHERE upload_id = upload.id
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM harklight.conversation_entry_uploads WHERE upload_id = upload.id
                 )
               ORDER BY (upload.attributes->>'reservation_expires_at_ms')::bigint, upload.id
               FOR UPDATE OF upload SKIP LOCKED LIMIT $3
               """,
               [person, now_ms, @cleanup_batch]
             ) do
        delete_orphans(Enum.map(rows, fn [id] -> id end))
      end
    end)
  end

  defp delete_orphans([]), do: {:ok, 0}

  defp delete_orphans(ids) do
    # Read references again with a fresh snapshot once the manifest locks
    # are held. This also protects links committed during candidate selection.
    with {:ok, %{num_rows: count}} <-
           Transaction.query(
             """
             DELETE FROM harklight.uploads AS upload WHERE upload.id = ANY($1::text[])
               AND NOT EXISTS (
                 SELECT 1 FROM harklight.message_uploads WHERE upload_id = upload.id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM harklight.conversation_entry_uploads WHERE upload_id = upload.id
               )
             """,
             [ids]
           ) do
      {:ok, count}
    end
  end

  # Called by an authorized message write on its checked-out transaction.
  def link({person, owner, id}, message_id, revision, ordinal, attrs) do
    if Repo.in_transaction?() do
      with {:ok, _size, _sha256, completed} <- lock_manifest(person, owner, id),
           true <- is_integer(completed),
           {:ok, %{num_rows: 1}} <-
             Transaction.query(
               """
               INSERT INTO harklight.message_uploads (message_id, revision, ordinal, upload_id, attributes)
               SELECT message.id, $3, $4, $5, $6
               FROM harklight.messages AS message WHERE person_id = $1 AND id = $2
               """,
               [person, message_id, revision, ordinal, id, Attributes.encode(attrs)]
             ) do
        {:ok, id}
      else
        false -> {:error, :incomplete_upload}
        {:ok, %{num_rows: 0}} -> {:error, :not_found}
        {:error, _} = error -> error
      end
    else
      {:error, :transaction_required}
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

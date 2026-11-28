defmodule AttentionServer.Repository.Postgres.LargeText.Ref do
  @moduledoc false
  @enforce_keys [:sha256, :size]
  defstruct [:sha256, :size]
end

defmodule AttentionServer.Repository.Postgres.LargeText do
  @moduledoc false

  alias __MODULE__.Ref
  alias AttentionServer.LargeData
  alias AttentionServer.Repository.Postgres.{Transaction, Uploads}

  @chunk_bytes Uploads.chunk_bytes()
  @max_size 9_223_372_036_854_775_807

  def chunk_bytes, do: @chunk_bytes

  def reference(sha256, size) do
    ref = %Ref{sha256: sha256, size: size}

    with {:ok, _} <- address(ref), do: {:ok, ref}
  end

  # Import callers can supply the query function of their checked-out
  # transaction to commit each chunk and its checkpoint together.
  def stage(%Ref{size: size} = ref, query \\ &Transaction.query/2) do
    with {:ok, hash} <- address(ref),
         {:ok, _} <-
           query.(
             """
             INSERT INTO harklight.large_texts (sha256, size_bytes) VALUES ($1, $2)
             ON CONFLICT DO NOTHING
             """,
             [hash, size]
           ),
         {:ok, %{rows: [[^size]]}} <-
           query.("SELECT size_bytes FROM harklight.large_texts WHERE sha256 = $1", [hash]) do
      :ok
    else
      {:ok, _} -> {:error, :large_text_conflict}
      {:error, _} = error -> error
    end
  end

  def put_chunk(ref, number, bytes, query \\ &Transaction.query/2)
      when is_integer(number) and number >= 0 and is_binary(bytes) do
    with {:ok, hash} <- address(ref),
         true <- valid_length?(ref, number, bytes),
         {:ok, _} <-
           query.(
             """
             INSERT INTO harklight.large_text_chunks (sha256, chunk_number, bytes)
             SELECT sha256, $3, $4 FROM harklight.large_texts
             WHERE sha256 = $1 AND size_bytes = $2 AND NOT verified
             ON CONFLICT DO NOTHING
             """,
             [hash, ref.size, number, bytes]
           ),
         {:ok, %{rows: [[true]]}} <-
           query.(
             """
             SELECT chunk.bytes = $4 FROM harklight.large_text_chunks AS chunk
             JOIN harklight.large_texts AS text USING (sha256)
             WHERE sha256 = $1 AND text.size_bytes = $2 AND chunk.chunk_number = $3
             """,
             [hash, ref.size, number, bytes]
           ) do
      :ok
    else
      false -> {:error, :invalid_large_text_chunk}
      {:ok, _} -> {:error, :large_text_conflict}
      {:error, _} = error -> error
    end
  end

  # Check immutable, already committed chunks one at a time without opening
  # a whole-object transaction. Publication is a single statement.
  def complete(ref, query \\ &Transaction.query/2) do
    with {:ok, hash} <- address(ref),
         {:ok, digest, total} <- verify_chunks(ref, hash, 0, :crypto.hash_init(:sha256), query),
         true <- digest == hash,
         {:ok, %{num_rows: 1}} <-
           query.(
             """
             UPDATE harklight.large_texts AS text SET verified = true
             WHERE sha256 = $1 AND size_bytes = $2 AND NOT EXISTS (
               SELECT 1 FROM harklight.large_text_chunks
               WHERE sha256 = text.sha256 AND chunk_number >= $3
             )
             """,
             [hash, ref.size, total]
           ) do
      :ok
    else
      false -> {:error, :large_text_checksum_mismatch}
      {:ok, _} -> {:error, :invalid_large_text_manifest}
      {:error, _} = error -> error
    end
  end

  # Called after the parent resource authorizes this reference. The only
  # returned payload is the requested chunk, at most one MiB.
  def read_chunk(ref, number, query \\ &Transaction.query/2)
      when is_integer(number) and number >= 0 do
    with {:ok, hash} <- address(ref),
         true <- number * @chunk_bytes < ref.size do
      fetch_chunk(ref, hash, number, true, query)
    else
      false -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  # Enumerate after the parent resource's authorization transaction returns.
  # Each fetch borrows a connection only for that chunk's indexed query.
  def escaped_stream(ref) do
    Stream.resource(
      fn -> open_stream(ref) end,
      &stream_chunk(ref, &1),
      fn _ -> :ok end
    )
    |> Stream.flat_map(&LargeData.escape_json_chunks/1)
  end

  defp open_stream(ref) do
    with {:ok, hash} <- address(ref),
         {:ok, %{rows: [[true]]}} <-
           Transaction.query(
             "SELECT verified FROM harklight.large_texts WHERE sha256 = $1 AND size_bytes = $2",
             [hash, ref.size]
           ) do
      {hash, 0, :crypto.hash_init(:sha256)}
    else
      _ -> raise ArgumentError, "large text unavailable"
    end
  end

  defp stream_chunk(%Ref{size: size}, {hash, number, context} = state)
       when number * @chunk_bytes >= size do
    if :crypto.hash_final(context) == hash,
      do: {:halt, state},
      else: raise(ArgumentError, "large text checksum mismatch")
  end

  defp stream_chunk(ref, {hash, number, context}) do
    case fetch_chunk(ref, hash, number, true, &Transaction.query/2) do
      {:ok, bytes} ->
        {[bytes], {hash, number + 1, :crypto.hash_update(context, bytes)}}

      {:error, _} ->
        raise ArgumentError, "large text unavailable"
    end
  end

  defp verify_chunks(%Ref{size: size}, _hash, number, context, _query)
       when number * @chunk_bytes >= size,
       do: {:ok, :crypto.hash_final(context), number}

  defp verify_chunks(ref, hash, number, context, query) do
    case fetch_chunk(ref, hash, number, false, query) do
      {:ok, bytes} ->
        verify_chunks(ref, hash, number + 1, :crypto.hash_update(context, bytes), query)

      {:error, :not_found} ->
        {:error, :incomplete_large_text}

      {:error, _} = error ->
        error
    end
  end

  defp fetch_chunk(ref, hash, number, verified, query) do
    case query.(
           """
           SELECT chunk.bytes FROM harklight.large_text_chunks AS chunk
           JOIN harklight.large_texts AS text USING (sha256)
           WHERE sha256 = $1 AND text.size_bytes = $2 AND chunk.chunk_number = $3
             AND (NOT $4::boolean OR text.verified)
           """,
           [hash, ref.size, number, verified]
         ) do
      {:ok, %{rows: [[bytes]]}} ->
        if valid_length?(ref, number, bytes), do: {:ok, bytes}, else: {:error, :invalid_large_text_chunk}

      {:ok, %{rows: []}} ->
        {:error, :not_found}

      {:error, _} = error ->
        error
    end
  end

  defp address(%Ref{sha256: hex, size: size})
       when is_binary(hex) and byte_size(hex) == 64 and is_integer(size) and size in 0..@max_size do
    case Base.decode16(hex, case: :lower) do
      {:ok, hash} -> {:ok, hash}
      :error -> {:error, :invalid_large_text_reference}
    end
  end

  defp address(_), do: {:error, :invalid_large_text_reference}

  defp valid_length?(ref, number, bytes) do
    expected = min(@chunk_bytes, ref.size - number * @chunk_bytes)
    expected > 0 and byte_size(bytes) == expected
  end
end

defmodule AttentionServer.Migration.Frames do
  @moduledoc false

  @magic "HARKLIGHT-FORMAT32-FRAMES-1\n"
  @term_options [:deterministic, {:minor_version, 2}]

  # The enclosing bundle directory must be private. Files are created
  # exclusively: retries must never overwrite an earlier export.
  def write(path, values) do
    with {:ok, file} <- File.open(path, [:write, :binary, :exclusive]) do
      try do
        with :ok <- File.chmod(path, 0o600),
             :ok <- IO.binwrite(file, @magic),
             {:ok, summary} <- write_values(file, values),
             :ok <- :file.sync(file) do
          {:ok, summary}
        end
      after
        File.close(file)
      end
    end
  end

  # Validate the entire sequence before invoking the consumer. An interrupted
  # export must not look like a valid shorter stream to the importer.
  def fold(path, expected, acc, consume) do
    with {:ok, ^expected, _} <- read(path, nil, fn _, acc -> {:ok, acc} end),
         {:ok, ^expected, result} <- read(path, acc, consume) do
      {:ok, result}
    else
      {:ok, _, _} -> {:error, :frame_manifest_mismatch}
      {:error, _} = error -> error
    end
  end

  defp write_values(file, values) do
    initial = {0, byte_size(@magic), :crypto.hash_update(:crypto.hash_init(:sha256), @magic)}

    Enum.reduce_while(values, {:ok, initial}, fn value, {:ok, {count, size, hash}} ->
      payload = :erlang.term_to_binary(value, @term_options)
      frame = [<<byte_size(payload)::64>>, :crypto.hash(:sha256, payload), payload]

      case IO.binwrite(file, frame) do
        :ok ->
          {:cont,
           {:ok, {count + 1, size + 40 + byte_size(payload), :crypto.hash_update(hash, frame)}}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, {count, size, hash}} -> {:ok, summary(count, size, hash)}
      error -> error
    end
  end

  defp read(path, acc, consume) do
    with {:ok, %{type: :regular, size: size}} <- File.stat(path),
         {:ok, file} <- File.open(path, [:read, :binary]) do
      try do
        if IO.binread(file, byte_size(@magic)) == @magic do
          hash = :crypto.hash_update(:crypto.hash_init(:sha256), @magic)
          read_values(file, size, byte_size(@magic), 0, hash, acc, consume)
        else
          {:error, :invalid_frame_header}
        end
      after
        File.close(file)
      end
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_frame_file}
    end
  end

  defp read_values(file, size, offset, count, hash, acc, consume) do
    case IO.binread(file, 8) do
      :eof when offset == size ->
        {:ok, summary(count, size, hash), acc}

      <<length::64>> when length > 0 and length <= size - offset - 40 ->
        checksum = IO.binread(file, 32)
        payload = IO.binread(file, length)

        with true <- is_binary(payload) and byte_size(payload) == length,
             true <- :crypto.hash(:sha256, payload) == checksum,
             {:ok, value} <- decode(payload),
             {:ok, next} <- consume.(value, acc) do
          hash = :crypto.hash_update(hash, [<<length::64>>, checksum, payload])
          read_values(file, size, offset + 40 + length, count + 1, hash, next, consume)
        else
          {:error, _} = error -> error
          _ -> {:error, :invalid_frame_checksum}
        end

      _ ->
        {:error, :truncated_frame_file}
    end
  end

  defp decode(payload) do
    {:ok, :erlang.binary_to_term(payload, [:safe])}
  rescue
    ArgumentError -> {:error, :invalid_frame_term}
  end

  defp summary(count, size, hash),
    do: %{
      count: count,
      bytes: size,
      sha256: Base.encode16(:crypto.hash_final(hash), case: :lower)
    }
end

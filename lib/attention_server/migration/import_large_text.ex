defmodule AttentionServer.Migration.ImportLargeText do
  @moduledoc false
  alias AttentionServer.Migration.{ImportFacts, ImportSession, LargeTextSource}
  alias AttentionServer.Repository.Postgres.{Attributes, LargeText}
  alias AttentionServer.JSON

  # Bundle identity and exact source-path closure precede every publication.
  # Chunk/checkpoint transactions are bounded; complete rehashes committed chunks
  # outside a whole-content transaction. The database remains loading throughout.
  def run(db, identity, bundle) do
    with {:ok, _} <- ImportFacts.run(db, identity, bundle),
         {:ok, count} <- LargeTextSource.scan(bundle, &import_reference(db, identity, &1)) do
      {:ok, count}
    end
  end

  defp import_reference(db, identity, source) do
    with {:ok, ref} <- LargeText.reference(source.sha256, source.size),
         :ok <- manifest(db, identity, ref),
         :ok <- chunks(db, identity, source, ref),
         :ok <- LargeText.complete(ref, &ImportSession.query(db, &1, &2)),
         :ok <- path_fact(db, identity, source, ref) do
      :ok
    end
  end

  defp manifest(db, identity, ref) do
    once(db, identity, "large-text-manifest/" <> ref.sha256, {ref.sha256, ref.size}, fn tx ->
      LargeText.stage(ref, &ImportSession.query(tx, &1, &2))
    end)
  end

  defp chunks(db, identity, source, ref) do
    family = "large-text-bytes/" <> ref.sha256 <> "/v1"
    with {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, family) do
      initial = %{position: zero(), checkpoint: checkpoint, bytes: 0, hash: :crypto.hash_init(:sha256)}
      result = source.file |> File.stream!([], LargeText.chunk_bytes())
        |> Enum.reduce_while({:ok, initial}, fn bytes, {:ok, state} ->
          entry = {state.position.ordinal, bytes}
          next = ImportSession.advance(state.position, [entry])
          check = cond do
            next.ordinal < checkpoint.ordinal -> {:ok, state.checkpoint}
            next.ordinal == checkpoint.ordinal ->
              if next == checkpoint, do: {:ok, checkpoint}, else: {:error, :checkpoint_source_mismatch}
            true -> ImportSession.batch(db, identity, family, state.checkpoint, [entry], fn tx, [{ordinal, chunk}] ->
              LargeText.put_chunk(ref, ordinal, chunk, &ImportSession.query(tx, &1, &2))
            end)
          end
          case check do
            {:ok, committed} -> {:cont, {:ok, %{state | position: next, checkpoint: committed,
              bytes: state.bytes + byte_size(bytes), hash: :crypto.hash_update(state.hash, bytes)}}}
            error -> {:halt, error}
          end
        end)
      with {:ok, final} <- result,
           true <- final.position.ordinal >= checkpoint.ordinal,
           true <- final.bytes == ref.size,
           true <- Base.encode16(:crypto.hash_final(final.hash), case: :lower) == ref.sha256 do
        :ok
      else
        false -> {:error, :large_text_source_mismatch}
        error -> error
      end
    end
  rescue
    File.Error -> {:error, :large_text_source_read_failed}
  end

  defp path_fact(db, identity, source, ref) do
    family = family(source.address)
    address = Attributes.key(source.path)
    body = %{path: source.path, marker: source.marker, reference: ref}
    checkpoint_family = family <> "/" <> hex(address)
    once(db, identity, checkpoint_family, {source.address, body}, fn tx ->
      case ImportSession.query(tx,
        "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)",
        [family, address, JSON.encode!(Attributes.encode(body))]) do
        {:ok, %{num_rows: 1}} -> :ok
        {:error, _} = error -> error
        _ -> {:error, :large_text_path_mismatch}
      end
    end)
  end

  # Only explicit, verified source references are replaced. Literal marker-like
  # text has no path fact and remains text. Raw FORMAT32 facts stay unchanged.
  def project(db, address, value) do
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family=$1 ORDER BY address", [family(address)]) do
      Enum.reduce_while(rows, {:ok, value}, fn [body], {:ok, current} ->
        with {:ok, %{path: path, marker: marker, reference: %LargeText.Ref{} = ref}} <- Attributes.decode(body),
             {:ok, next} <- replace(current, path, marker, ref) do
          {:cont, {:ok, next}}
        else
          _ -> {:halt, {:error, :large_text_projection_mismatch}}
        end
      end)
    end
  end

  defp once(db, identity, family, entry, write) do
    expected = ImportSession.advance(zero(), [entry])
    with {:ok, checkpoint} <- ImportSession.checkpoint(db, identity, family) do
      cond do
        checkpoint == expected -> :ok
        checkpoint == zero() ->
          case ImportSession.batch(db, identity, family, checkpoint, [entry], fn tx, [_] -> write.(tx) end) do
            {:ok, ^expected} -> :ok
            error -> error
          end
        true -> {:error, :checkpoint_source_mismatch}
      end
    end
  end

  defp replace(value, [], marker, ref) when value == marker, do: {:ok, ref}
  defp replace(value, [{:map, key} | rest], marker, ref) when is_map(value) do
    with {:ok, child} <- Map.fetch(value, key), {:ok, next} <- replace(child, rest, marker, ref),
      do: {:ok, Map.put(value, key, next)}
  end
  defp replace(value, [{:list, index} | rest], marker, ref) when is_list(value) and is_integer(index) and index >= 0 do
    with {:ok, child} <- Enum.fetch(value, index), {:ok, next} <- replace(child, rest, marker, ref),
      do: {:ok, List.replace_at(value, index, next)}
  end
  defp replace(value, [{:tuple, index} | rest], marker, ref) when is_tuple(value) and is_integer(index) and index >= 0 and index < tuple_size(value) do
    with {:ok, next} <- replace(elem(value, index), rest, marker, ref), do: {:ok, put_elem(value, index, next)}
  end
  defp replace(_, _, _, _), do: {:error, :large_text_projection_mismatch}
  defp family(address), do: "large-text-paths/" <> hex(Attributes.key(address))
  defp hex(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  defp zero, do: %{ordinal: 0, digest: :crypto.hash(:sha256, <<>>)}
end

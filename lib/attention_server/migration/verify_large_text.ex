defmodule AttentionServer.Migration.VerifyLargeText do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportSession, LargeTextSource, VerifyFacts}
  alias AttentionServer.Repository.Postgres.{Attributes, LargeText}
  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  # Independently read every referenced byte and exact source-path projection.
  # A completed checkpoint or publication flag alone is not validation evidence.
  # This does not publish content, change lifecycle state, or validate consumers.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyFacts.run(db, identity, bundle),
         {:ok, count} <- LargeTextSource.scan(bundle, &reference(db, &1)),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.compatibility_facts WHERE family LIKE 'large-text-paths/%'", []),
         :ok <- hash_closure(db, bundle) do
      {:ok, %{references: count}}
    else
      {:error, _} = error -> error
      _ -> {:error, :large_text_path_count_mismatch}
    end
  end

  # Every referenced hash was checked above. Equality with the source's distinct
  # hash count therefore also excludes unrelated destination objects, including
  # when the source has no references. SQLite computes deduplication on disk.
  defp hash_closure(db, bundle) do
    with {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, count} <- source_hash_count(bundle, manifest),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.large_texts", []) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :large_text_hash_count_mismatch}
    end
  end

  defp source_hash_count(bundle, manifest) do
    if Map.has_key?(manifest.data.tables, "relay_large_data_refs") do
      with {:ok, sqlite} <- Sqlite3.open(Path.join(bundle, "source.sqlite3"), mode: :readonly) do
        try do
          case RecordTable.select(sqlite, "SELECT count(DISTINCT sha256) FROM relay_large_data_refs") do
            {:ok, [[count]]} -> {:ok, count}
            error -> error
          end
        after
          Sqlite3.close(sqlite)
        end
      end
    else
      {:ok, 0}
    end
  end

  defp reference(db, source) do
    family = "large-text-paths/" <>
      Base.encode16(:crypto.hash(:sha256, Attributes.key(source.address)), case: :lower)
    with {:ok, ref} <- LargeText.reference(source.sha256, source.size),
         hash = Base.decode16!(source.sha256, case: :lower),
         expected = Attributes.encode(%{path: source.path, marker: source.marker, reference: ref}),
         {:ok, %{rows: [[^expected]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
           [family, Attributes.key(source.path)]),
         {:ok, %{rows: [[size, true]]}} when size == source.size <- ImportSession.query(db,
           "SELECT size_bytes,verified FROM harklight.large_texts WHERE sha256=$1", [hash]),
         {:ok, count} <- chunks(db, hash, source.file),
         {:ok, %{rows: [[^count]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.large_text_chunks WHERE sha256=$1", [hash]) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :large_text_source_mismatch}
    end
  end

  defp chunks(db, hash, file) do
    file |> File.stream!([], LargeText.chunk_bytes()) |> Enum.reduce_while({:ok, 0}, fn expected, {:ok, ordinal} ->
      case ImportSession.query(db,
        "SELECT bytes FROM harklight.large_text_chunks WHERE sha256=$1 AND chunk_number=$2", [hash, ordinal]) do
        {:ok, %{rows: [[^expected]]}} -> {:cont, {:ok, ordinal + 1}}
        {:ok, _} -> {:halt, {:error, :large_text_chunk_source_mismatch}}
        error -> {:halt, error}
      end
    end)
  rescue
    File.Error -> {:error, :large_text_source_unreadable}
  end
end

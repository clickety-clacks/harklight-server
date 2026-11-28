defmodule AttentionServer.Migration.VerifyFileFacts do
  @moduledoc false
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Migration.{Bundle, Frames, ImportSession, ImportStatus}
  alias AttentionServer.Repository.Postgres.Attributes

  # Compare the bounded reader projections. Upload content and relational link
  # closure remain the responsibility of VerifyUploads/VerifyMessageUploads.
  def run(db, identity, bundle) do
    with {:ok, manifest, summary} <- Bundle.verify(bundle),
         true <- identity.source == Base.decode16!(summary.sha256, case: :lower),
         {:ok, %{"phase" => phase}} when phase in ["loading", "validated"] <- ImportStatus.read(db, identity),
         {:ok, uploads} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records, 0, fn
           {{:row, :uploads, _} = address, value}, n ->
             with :ok <- fact(db, "uploads", address, value), do: {:ok, n + 1}
           _, n -> {:ok, n}
         end),
         :ok <- count(db, "uploads", uploads),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{items: 0, revisions: 0}, fn
             {{:row, :messages, {person, _, _}}, record}, counts -> message(db, person, record, counts)
             _, counts -> {:ok, counts}
           end),
         :ok <- count(db, "ordinary_item_files", counts.items),
         :ok <- count(db, "ordinary_revision_files", counts.revisions),
         {:ok, ^manifest, ^summary} <- Bundle.verify(bundle) do
      {:ok, counts}
    else
      false -> {:error, :import_source_mismatch}
      {:error, _} = error -> error
      _ -> {:error, :file_verification_requires_staged_import}
    end
  end

  defp message(db, person, record, counts) do
    revisions = Map.get(record, :revisions, %{1 => record.message})
    files_by_revision = Map.get(record, :files_by_revision, %{})
    with {:ok, uploads} <- metadata(db, person, files_by_revision),
         :ok <- fact(db, "ordinary_item_files", {person, Resources.item_id(record), record.revision},
           Resources.item_file_refs(record, uploads)),
         :ok <- each(Map.keys(revisions), fn revision ->
           expected = Resources.revision_file_refs(record.accepted_seq, revision,
             Map.get(files_by_revision, revision, []), uploads)
           fact(db, "ordinary_revision_files", {person, Resources.item_id(record), revision}, expected)
         end) do
      {:ok, %{items: counts.items + 1, revisions: counts.revisions + map_size(revisions)}}
    end
  end

  # Only metadata referenced by this source record is materialized; source upload
  # values and family closure were already compared before these keyed lookups.
  defp metadata(db, person, files_by_revision) do
    files_by_revision |> Map.values() |> Stream.flat_map(& &1)
    |> Enum.reduce_while({:ok, %{}}, fn
      %{"tier" => "upload", "upload_id" => id}, {:ok, acc} ->
        case ImportSession.query(db,
          "SELECT body FROM harklight.compatibility_facts WHERE family='uploads' AND address=$1",
          [Attributes.key({:row, :uploads, id})]) do
          {:ok, %{rows: []}} -> {:cont, {:ok, acc}}
          {:ok, %{rows: [[body]]}} ->
            case Attributes.decode(body) do
              {:ok, %{person: ^person} = value} -> {:cont, {:ok, Map.put(acc, id, value)}}
              _ -> {:halt, {:error, :file_fact_owner_mismatch}}
            end
          {:error, _} = error -> {:halt, error}
          _ -> {:halt, {:error, :file_fact_source_mismatch}}
        end
      _, acc -> {:cont, acc}
    end)
  end

  defp fact(db, family, key, value) do
    expected = Attributes.encode(value)
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
      [family, Attributes.key(key)]) do
      {:ok, %{rows: [[^expected]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:file_fact_mismatch, family,
        Base.encode16(:crypto.hash(:sha256, Attributes.key(key)), case: :lower)}}
    end
  end
  defp count(db, family, n) do
    case ImportSession.query(db,
      "SELECT count(*) FROM harklight.compatibility_facts WHERE family=$1", [family]) do
      {:ok, %{rows: [[^n]]}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, {:file_fact_count_mismatch, family}}
    end
  end
  defp each(rows, fun), do: Enum.reduce_while(rows, :ok, fn row, :ok ->
    case fun.(row) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end)
end

defmodule AttentionServer.Migration.ImportItemFacts do
  @moduledoc false
  alias AttentionServer.{JSON, ClientAPI.Resources, AgentAPI.FeedActivity}
  alias AttentionServer.Migration.{Bundle, ImportChatSummary, ImportMessages, ImportRevisionFiles, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  # Requires the shared FORMAT32 selectors introduced with complete Item assembly.
  def run(db, identity, bundle) do
    with :ok <- ImportRevisionFiles.run(db, identity, bundle),
         :ok <- ImportChatSummary.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "messages/item-facts/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &write/2) do
      :ok
    end
  end

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, {person, _, _} = key}, record}, :ok ->
      result = with {:ok, uploads} <- metadata(db, person, record),
                    :ok <- fact(db, "ordinary_item_files", {person, Resources.item_id(record), record.revision},
                      Resources.item_file_refs(record, uploads)),
                    :ok <- availability(db, key, record) do
        :ok
      end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp availability(db, {person, _, _} = key, %{session_handle: handle} = record)
       when is_binary(handle) do
    with {:ok, session} <- source(db, :origin_sessions, {person, record.origin, handle}),
         {:ok, raw} <- source(db, :item_availability, key),
         {:ok, legacy} <- legacy(db, key, record) do
      fact(db, "ordinary_item_availability", {person, Resources.item_id(record)},
        FeedActivity.committed_availability(record, session, raw, legacy))
    end
  end
  defp availability(_, _, _), do: :ok

  defp legacy(db, {person, _, _} = key, record) do
    with {:ok, %{rows: rows}} <- ImportSession.query(db,
           "SELECT body FROM harklight.conversation_entries WHERE conversation_id=$1 AND kind='availability' ORDER BY position DESC LIMIT 1",
           [ImportMessages.id(key, record)]) do
      case rows do
        [[body]] ->
          case Attributes.decode(body) do
            {:ok, %{payload: payload}} -> {:ok, payload}
            _ -> {:error, :item_availability_event_mismatch}
          end
        [] ->
          case source(db, :events, {person, record.accepted_seq}) do
            {:ok, nil} -> {:ok, %{}}
            {:ok, %{payload: payload}} -> {:ok, payload}
            {:error, _} = error -> error
            _ -> {:error, :item_availability_event_mismatch}
          end
      end
    end
  end

  defp metadata(db, person, record) do
    files = record |> Map.get(:files_by_revision, %{}) |> Map.values() |> List.flatten()
    Enum.reduce_while(files, {:ok, %{}}, fn
      %{"tier" => "upload", "upload_id" => id}, {:ok, acc} ->
        case source(db, :uploads, id) do
          {:ok, nil} -> {:cont, {:ok, acc}}
          {:ok, %{person: ^person} = value} -> {:cont, {:ok, Map.put(acc, id, value)}}
          {:error, _} = error -> {:halt, error}
          _ -> {:halt, {:error, :item_file_owner_mismatch}}
        end
      _, result -> {:cont, result}
    end)
  end

  defp source(db, family, key) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2",
      [Atom.to_string(family), Attributes.key({:row, family, key})]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :item_fact_source_mismatch}
    end
  end

  defp fact(db, family, key, value) do
    case ImportSession.query(db,
      "INSERT INTO harklight.compatibility_facts(family,address,body) VALUES($1,$2,$3::text::jsonb)",
      [family, Attributes.key(key), JSON.encode!(Attributes.encode(value))]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :item_fact_projection_mismatch}
    end
  end
end

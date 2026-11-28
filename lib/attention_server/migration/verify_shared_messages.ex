defmodule AttentionServer.Migration.VerifySharedMessages do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportSession, VerifyLargeText}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyLargeText.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, counts} <- Frames.fold(Path.join(bundle, "records.frames"), manifest.data.records,
           %{messages: 0, revisions: 0, recipients: 0}, fn
             {{:row, :shared_messages, id} = address, source}, counts ->
               with {:ok, record} <- ImportLargeText.project(db, address, source),
                    {:ok, revisions} <- message(db, id, record) do
                 {:ok, %{counts | messages: counts.messages + 1, revisions: counts.revisions + revisions}}
               end
             {{:row, :shared_recipients, {id, person}} = address, source}, counts ->
               with {:ok, record} <- ImportLargeText.project(db, address, source),
                    :ok <- recipient(db, id, person, record) do
                 {:ok, %{counts | recipients: counts.recipients + 1}}
               end
             _, counts -> {:ok, counts}
           end),
         :ok <- counts(db, counts) do
      {:ok, counts}
    end
  end

  defp message(db, id, record) do
    with {:ok, group} <- group(db, Map.get(record, :group_id)) do
      expected = ["shared", record.sender_person, record.sender_key, record.sender_id, id,
        nil, group, record.revision, record["created_at_ms"], nil, nil, nil,
        optional(Map.get(record, :terminal)), Attributes.encode(record)]
      versions = Map.put(Map.get(record, :history, %{}), record.revision, %{message: record.message})
      with {:ok, %{rows: [^expected]}} <- ImportSession.query(db,
             "SELECT kind,person_id,origin_key,sender_id,message_id,session_handle,group_id,revision,accepted_at_ms,accepted_position,free_order,principal_origin_key,terminal,attributes FROM harklight.messages WHERE id=$1", [id]),
           :ok <- revisions(db, id, record, versions) do
        {:ok, map_size(versions)}
      else
        {:error, _} = error -> error
        _ -> mismatch(:message, id)
      end
    end
  end

  # Source group presence, not destination group presence, controls the FK.
  defp group(_, nil), do: {:ok, nil}
  defp group(db, id) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family='groups' AND address=$1",
      [Attributes.key({:row, :groups, id})]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[_]]}} -> {:ok, id}
      {:error, _} = error -> error
    end
  end

  defp recipient(db, id, person, record) do
    expected = [record.item_id, Map.get(record, :seen_revision), Attributes.encode(record)]
    case ImportSession.query(db,
      "SELECT item_id,seen_revision,attributes FROM harklight.message_recipients WHERE message_id=$1 AND person_id=$2", [id, person]) do
      {:ok, %{rows: [^expected]}} -> :ok
      {:error, _} = error -> error
      _ -> mismatch(:recipient, id)
    end
  end

  defp revisions(db, id, record, versions) do
    Enum.reduce_while(versions, :ok, fn {revision, %{message: message}}, :ok ->
      at = cond do
        revision == 1 -> record["created_at_ms"]
        revision == record.revision -> Map.get(record, :revision_accepted_at_ms)
        true -> nil
      end
      body = Attributes.encode(message)
      case ImportSession.query(db,
        "SELECT accepted_at_ms,body FROM harklight.message_revisions WHERE message_id=$1 AND revision=$2", [id, revision]) do
        {:ok, %{rows: [[^at, ^body]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, mismatch({:revision, revision}, id)}
      end
    end)
  end

  defp counts(db, expected) do
    queries = [messages: "SELECT count(*) FROM harklight.messages m WHERE m.kind='shared'",
      revisions: "SELECT count(*) FROM harklight.message_revisions r JOIN harklight.messages m ON m.id=r.message_id WHERE m.kind='shared'",
      recipients: "SELECT count(*) FROM harklight.message_recipients r JOIN harklight.messages m ON m.id=r.message_id WHERE m.kind='shared'"]
    Enum.reduce_while(queries, :ok, fn {field, sql}, :ok ->
      count = Map.fetch!(expected, field)
      case ImportSession.query(db, sql, []) do
        {:ok, %{rows: [[^count]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, {:shared_count_mismatch, field}}}
      end
    end)
  end
  defp optional(nil), do: nil
  defp optional(value), do: Attributes.encode(value)
  defp mismatch(field, id), do: {:error, {:shared_message_mismatch, field,
    Base.encode16(:crypto.hash(:sha256, id), case: :lower)}}
end

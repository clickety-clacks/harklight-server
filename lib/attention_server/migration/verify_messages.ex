defmodule AttentionServer.Migration.VerifyMessages do
  @moduledoc false
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Migration.{Bundle, Frames, ImportLargeText, ImportMessages, ImportSession, VerifyLargeText}
  alias AttentionServer.Repository.Postgres.Attributes

  # Ordinary record projection only. Recipient state, conversations, answers,
  # file links and shared history retain their own validation obligations.
  def run(db, identity, bundle) do
    with {:ok, _} <- VerifyLargeText.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, {messages, revisions}} <- Frames.fold(Path.join(bundle, "records.frames"),
           manifest.data.records, {0, 0}, fn
             {{:row, :messages, key} = address, source}, {messages, revisions} ->
               with {:ok, record} <- ImportLargeText.project(db, address, source),
                    {:ok, count} <- message(db, key, record) do
                 {:ok, {messages + 1, revisions + count}}
               end
             _, counts -> {:ok, counts}
           end),
         :ok <- counts(db, messages, revisions) do
      {:ok, %{ordinary_messages: messages, ordinary_revisions: revisions}}
    end
  end

  defp message(db, {person, sender, external} = key, record) do
    id = ImportMessages.id(key, record)
    current = Map.get(record, :revision, 1)
    terminal = if is_nil(record.terminal), do: nil, else: Attributes.encode(record.terminal)
    attributes = record |> Map.drop([:message, :origin, :session_handle, :revision,
      :accepted_seq, :free_order, :accepted_at, :terminal]) |> Attributes.encode()
    expected = [person, record.origin, sender, external, Map.get(record, :session_handle),
      current, record.accepted_at, record.accepted_seq, Map.get(record, :free_order), terminal, attributes]
    versions = Map.get(record, :revisions, %{1 => record.message})
    item = Resources.item_id(record)
    with {:ok, %{rows: [^expected]}} <- ImportSession.query(db,
           "SELECT person_id,origin_key,sender_id,message_id,session_handle,revision,accepted_at_ms,accepted_position,free_order,terminal,attributes FROM harklight.messages WHERE id=$1", [id]),
         {:ok, %{rows: [[^person, ^item]]}} <- ImportSession.query(db,
           "SELECT person_id,item_id FROM harklight.message_recipients WHERE message_id=$1", [id]),
         :ok <- revisions(db, id, current, record, versions) do
      {:ok, map_size(versions)}
    else
      {:error, _} = error -> error
      _ -> mismatch(:message, id)
    end
  end

  defp revisions(db, id, current, record, versions) do
    Enum.reduce_while(versions, :ok, fn {revision, body}, :ok ->
      at = cond do
        revision == 1 -> record.accepted_at
        revision == current -> Map.get(record, :revision_accepted_at)
        true -> nil
      end
      expected = Attributes.encode(body)
      case ImportSession.query(db,
        "SELECT accepted_at_ms,body FROM harklight.message_revisions WHERE message_id=$1 AND revision=$2", [id, revision]) do
        {:ok, %{rows: [[^at, ^expected]]}} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, mismatch({:revision, revision}, id)}
      end
    end)
  end

  defp counts(db, messages, revisions) do
    with {:ok, %{rows: [[version]]}} when is_integer(version) <- ImportSession.query(db,
           "SELECT max(version) FROM harklight.schema_migrations", []),
         filter = if(version >= 5, do: " WHERE m.kind='ordinary'", else: ""),
         {:ok, %{rows: [[^messages]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.messages m" <> filter, []),
         {:ok, %{rows: [[^messages]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.message_recipients r JOIN harklight.messages m ON m.id=r.message_id" <> filter, []),
         {:ok, %{rows: [[^revisions]]}} <- ImportSession.query(db,
           "SELECT count(*) FROM harklight.message_revisions r JOIN harklight.messages m ON m.id=r.message_id" <> filter, []) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :ordinary_message_count_mismatch}
    end
  end

  defp mismatch(field, id) do
    {:error, {:ordinary_message_mismatch, field, Base.encode16(:crypto.hash(:sha256, id), case: :lower)}}
  end
end

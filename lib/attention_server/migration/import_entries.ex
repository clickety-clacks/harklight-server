defmodule AttentionServer.Migration.ImportEntries do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportConversations, ImportLargeText, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportConversations.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "conversations/entries/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :conversation_events, _}, _}, &1), &write/2) do
      :ok
    end
  end

  # Event IDs inside payloads can repeat (for example acted and file_use).
  # The original person's journal position is the internal row identity.
  def fields({person, position}, %{kind: :conversation, key: {person, _, _}, payload: payload})
      when is_integer(position) and position > 0 and is_map(payload) do
    kind = payload["kind"]
    revision = payload["request_revision"]
    if is_binary(kind) and kind != "" and
         ((is_integer(revision) and revision > 0) or (kind == "read_position" and is_nil(revision))),
      do: {:ok, {Integer.to_string(position), position, kind, revision}},
      else: {:error, :invalid_conversation_entry}
  end
  def fields(_, _), do: {:error, :invalid_conversation_entry}

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :conversation_events, scope} = address, event}, :ok ->
      result = with {:ok, {id, position, kind, revision}} <- fields(scope, event),
                    {person, sender, external} = event.key,
                    {:ok, %{rows: [[conversation]]}} <- ImportSession.query(db,
                      "SELECT c.id FROM harklight.conversations c JOIN harklight.messages m ON m.id=c.message_id WHERE m.kind='ordinary' AND m.person_id=$1 AND m.sender_id=$2 AND m.message_id=$3",
                      [person, sender, external]),
                    {:ok, projected} <- ImportLargeText.project(db, address, event),
                    {:ok, %{num_rows: 1}} <- ImportSession.query(db,
                      "INSERT INTO harklight.conversation_entries(id,conversation_id,position,request_revision,kind,body) VALUES($1,$2,$3,$4,$5,$6::text::jsonb)",
                      [id, conversation, position, revision, kind, JSON.encode!(Attributes.encode(projected))]) do
        :ok
      else
        {:error, _} = error -> error
        _ -> {:error, :conversation_entry_reference_mismatch}
      end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

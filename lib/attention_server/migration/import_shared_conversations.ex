defmodule AttentionServer.Migration.ImportSharedConversations do
  @moduledoc false
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportLargeText, ImportRows, ImportSession, ImportSharedMessages}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportSharedMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         :ok <- phase(db, identity, bundle, manifest, :shared_messages),
         :ok <- phase(db, identity, bundle, manifest, :shared_entries),
         :ok <- phase(db, identity, bundle, manifest, :shared_recipients),
         :ok <- phase(db, identity, bundle, manifest, :shared_answers) do
      :ok
    end
  end

  defp phase(db, identity, bundle, manifest, field) do
    case ImportRows.run(db, identity, "shared/conversations-" <> Atom.to_string(field) <> "/v1",
      Path.join(bundle, "records.frames"), manifest.data.records,
      fn {{:row, source, _}, _} -> source == field; _ -> false end,
      fn tx, rows -> Enum.reduce_while(rows, :ok, fn row, :ok ->
        case write(tx, row) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end) end) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp write(db, {{:row, :shared_messages, id}, _}) do
    execute(db,
      "INSERT INTO harklight.conversations(id,message_id,attributes) SELECT id,id,$2::text::jsonb FROM harklight.messages WHERE id=$1 AND kind='shared'",
      [id, encode(%{shared_id: id})])
  end

  defp write(db, {{:row, :shared_entries, id} = address, %{id: id} = source}) do
    with {:ok, record} <- ImportLargeText.project(db, address, source) do
      execute(db, """
        INSERT INTO harklight.conversation_entries(id,conversation_id,position,request_revision,kind,body)
        SELECT $1,c.id,$3,$4,'reply',$5::text::jsonb
        FROM harklight.conversations c JOIN harklight.messages m ON m.id=c.message_id
        JOIN harklight.message_revisions r ON r.message_id=m.id AND r.revision=$4
        WHERE m.id=$2 AND m.kind='shared'
        """, [id, record.shared_id, record.position, record.request_revision, encode(record)])
    end
  end

  defp write(db, {{:row, :shared_recipients, {id, person}}, %{read_position: position}})
       when is_integer(position) and position >= 0 do
    execute(db, """
      INSERT INTO harklight.read_positions(conversation_id,person_id,position)
      SELECT c.id,$2,$3 FROM harklight.conversations c
      JOIN harklight.messages m ON m.id=c.message_id AND m.kind='shared'
      JOIN harklight.message_recipients recipient ON recipient.message_id=m.id AND recipient.person_id=$2
      WHERE m.id=$1 AND ($3=0 OR EXISTS
        (SELECT 1 FROM harklight.conversation_entries e WHERE e.conversation_id=c.id AND e.position=$3))
      """, [id, person, position])
  end
  defp write(_, {{:row, :shared_recipients, _}, recipient}) when not is_map_key(recipient, :read_position), do: :ok

  defp write(db, {{:row, :shared_answers, id} = address, %{id: id} = source}) do
    with {:ok, record} <- ImportLargeText.project(db, address, source) do
      execute(db, """
        INSERT INTO harklight.answers(id,message_id,person_id,request_revision,body)
        SELECT $1,m.id,$3,$4,$5::text::jsonb FROM harklight.messages m
        JOIN harklight.message_revisions r ON r.message_id=m.id AND r.revision=$4
        JOIN harklight.message_recipients recipient ON recipient.message_id=m.id AND recipient.person_id=$3
        WHERE m.id=$2 AND m.kind='shared'
        """, [id, record.shared_id, record.person, record.request_revision, encode(record)])
    end
  end
  defp write(_, _), do: {:error, :invalid_shared_conversation_source}
  defp encode(value), do: JSON.encode!(Attributes.encode(value))
  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:error, _} = error -> error
      _ -> {:error, :shared_conversation_reference_mismatch}
    end
  end
end

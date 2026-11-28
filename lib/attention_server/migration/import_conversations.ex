defmodule AttentionServer.Migration.ImportConversations do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportMessages, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.JSON

  # Ordinary conversation identity and read positions only. Entry revision
  # semantics and shared conversations are projected by separate phases.
  def run(db, identity, root) do
    with :ok <- ImportMessages.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root),
         {:ok, _} <- project(db, identity, root, manifest, "conversations/ordinary/v1",
           :messages, &containers/2),
         {:ok, _} <- project(db, identity, root, manifest, "conversations/read-positions/v1",
           :chat_read_positions, &positions/2) do
      :ok
    end
  end

  defp project(db, identity, root, manifest, family, field, write) do
    ImportRows.run(db, identity, family, Path.join(root, "records.frames"),
      manifest.data.records, fn
        {{:row, ^field, _}, _} -> true
        _ -> false
      end, write)
  end

  defp containers(db, rows) do
    each(rows, fn {{:row, :messages, key}, record} ->
      id = ImportMessages.id(key, record)
      execute(db,
        "INSERT INTO harklight.conversations(id,message_id,attributes) VALUES($1,$1,$2::text::jsonb)",
        [id, JSON.encode!(Attributes.encode(%{source_key: key}))])
    end)
  end

  defp positions(db, rows) do
    each(rows, fn {{:row, :chat_read_positions, {person, sender, external} = key}, position} ->
      with true <- is_integer(position) and position >= 0,
           {:ok, %{rows: [[id]]}} <- ImportSession.query(db,
             "SELECT id FROM harklight.messages WHERE kind='ordinary' AND person_id=$1 AND sender_id=$2 AND message_id=$3",
             [person, sender, external]),
           :ok <- source_position(db, key, position) do
        execute(db,
          "INSERT INTO harklight.read_positions(conversation_id,person_id,position) VALUES($1,$2,$3)",
          [id, person, position])
      else
        false -> {:error, :invalid_read_position}
        {:ok, _} -> {:error, :conversation_reference_mismatch}
        error -> error
      end
    end)
  end

  defp source_position(_db, _key, 0), do: :ok
  defp source_position(db, {person, _, _} = key, position) do
    address = Attributes.key({:row, :conversation_events, {person, position}})
    with {:ok, %{rows: [[body]]}} <- ImportSession.query(db,
           "SELECT body FROM harklight.compatibility_facts WHERE family='conversation_events' AND address=$1",
           [address]),
         {:ok, %{key: ^key, payload: %{"kind" => "reply", "author" => "origin"}}} <- Attributes.decode(body) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :read_position_reference_mismatch}
    end
  end

  defp each(rows, write) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case write.(row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :conversation_row_count_mismatch}
      error -> error
    end
  end
end

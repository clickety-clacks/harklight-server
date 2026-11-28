defmodule AttentionServer.Migration.ImportAnswers do
  @moduledoc false
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.JSON
  alias AttentionServer.Migration.{Bundle, ImportLargeText, ImportMessages, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "answers/ordinary/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, %{terminal: %{state: "answered"}}}, &1), &write/2) do
      :ok
    end
  end

  # This ID is internal only. Ordinary FORMAT32 answers are the terminal on the
  # closed request revision, not a separately allocated public answer resource.
  def id(message, revision), do: message <> "/answer/" <> Integer.to_string(revision)

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, {person, _, _} = key} = address, source}, :ok ->
      result = with {:ok, record} <- ImportLargeText.project(db, address, source),
                    revision when is_integer(revision) and revision > 0 <- Map.get(record, :revision, 1),
                    message = ImportMessages.id(key, record),
                    {:ok, %{rows: [[true]]}} <- ImportSession.query(db,
                      "SELECT true FROM harklight.messages m JOIN harklight.message_revisions r ON r.message_id=m.id AND r.revision=$3 WHERE m.id=$1 AND m.person_id=$2",
                      [message, person, revision]),
                    body = %{terminal: record.terminal, item_id: Resources.item_id(record)},
                    {:ok, %{num_rows: 1}} <- ImportSession.query(db,
                      "INSERT INTO harklight.answers(id,message_id,person_id,request_revision,body) VALUES($1,$2,$3,$4,$5::text::jsonb)",
                      [id(message, revision), message, person, revision, JSON.encode!(Attributes.encode(body))]) do
        :ok
      else
        {:error, _} = error -> error
        _ -> {:error, :answer_reference_mismatch}
      end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

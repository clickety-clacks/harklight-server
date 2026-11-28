defmodule AttentionServer.Migration.ImportMessages do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.JSON

  # Ordinary messages only. Shared messages and historical actors have distinct
  # mappings; all source facts remain intact and the target stays unpublished.
  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         {:ok, manifest, _} <- Bundle.verify(root),
         {:ok, _} <-
           ImportRows.run(
             db,
             identity,
             "messages/ordinary/v1",
             Path.join(root, "records.frames"),
             manifest.data.records,
             &match?({{:row, :messages, _}, _}, &1),
             &write/2
           ) do
      :ok
    end
  end

  def id({person, _, _}, record), do: "ordinary/" <> person <> "/" <> Resources.item_id(record)

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, key}, record}, :ok ->
      case message(db, key, record) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp message(db, {person, sender, external_id} = key, record) do
    id = id(key, record)
    revision = Map.get(record, :revision, 1)
    revisions = Map.get(record, :revisions, %{1 => record.message})
    attributes =
      Map.drop(record, [
        :message, :origin, :session_handle, :revision,
        :accepted_seq, :free_order, :accepted_at, :terminal
      ])
    terminal = if is_nil(record.terminal), do: nil, else: encode(record.terminal)

    with :ok <- execute(db,
           """
           INSERT INTO harklight.messages
             (id,person_id,origin_key,sender_id,message_id,session_handle,revision,
              accepted_at_ms,accepted_position,free_order,terminal,attributes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::jsonb,$12::text::jsonb)
           """,
           [id, person, record.origin, sender, external_id, Map.get(record, :session_handle),
            revision, record.accepted_at, record.accepted_seq, Map.get(record, :free_order),
            terminal, encode(attributes)]),
         :ok <- revisions(db, id, record, revisions),
         :ok <- execute(db,
           "INSERT INTO harklight.message_recipients(message_id,person_id,item_id) VALUES ($1,$2,$3)",
           [id, person, Resources.item_id(record)]) do
      :ok
    end
  end

  defp revisions(db, id, record, revisions) do
    Enum.reduce_while(Enum.sort(revisions), :ok, fn {revision, body}, :ok ->
      # FORMAT32 retains the first and latest revision timestamps, but not
      # intermediate timestamps. Refuse missing data until the schema supports
      # unknown historical timestamps; do not invent one from another revision.
      at =
        cond do
          revision == 1 -> record.accepted_at
          revision == record.revision -> Map.get(record, :revision_accepted_at)
          true -> nil
        end

      if is_integer(at) do
        case execute(db,
               "INSERT INTO harklight.message_revisions(message_id,revision,accepted_at_ms,body) VALUES ($1,$2,$3,$4::text::jsonb)",
               [id, revision, at, encode(body)]) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      else
        {:halt, {:error, :historical_revision_timestamp_unavailable}}
      end
    end)
  end

  defp encode(value), do: JSON.encode!(Attributes.encode(value))
  defp execute(db, sql, params) do
    case ImportSession.query(db, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :import_row_count_mismatch}
      error -> error
    end
  end
end

defmodule AttentionServer.Migration.ImportMessages do
  @moduledoc false
  alias AttentionServer.Migration.{Bundle, ImportAccounts, ImportLargeText, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.JSON

  # Ordinary and Hosted notice records. Shared messages have a distinct mapping;
  # all source facts remain intact and the target stays unpublished.
  def run(db, identity, root) do
    with :ok <- ImportAccounts.run(db, identity, root),
         {:ok, _} <- ImportLargeText.run(db, identity, root),
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
           ),
         {:ok, _} <- ImportRows.run(db, identity, "messages/large-text/v1",
           Path.join(root, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &project_large_text/2) do
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
    attributes = attributes(record)
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

  # A separate phase also upgrades already-staged loading imports. Existing
  # ordinary-record checkpoints are never reinterpreted or overwritten.
  defp project_large_text(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, key} = address, source}, :ok ->
      with {:ok, projected} <- ImportLargeText.project(db, address, source),
           :ok <- update_large_text(db, key, source, projected) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp update_large_text(_db, _key, source, projected) when source == projected, do: :ok
  defp update_large_text(db, key, _source, projected) do
    id = id(key, projected)
    terminal = if is_nil(projected.terminal), do: nil, else: encode(projected.terminal)
    revisions = Map.get(projected, :revisions, %{1 => projected.message})
    result = Enum.reduce_while(revisions, :ok, fn {revision, body}, :ok ->
      case execute(db,
        "UPDATE harklight.message_revisions SET body=$3::text::jsonb WHERE message_id=$1 AND revision=$2",
        [id, revision, encode(body)]) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
    with :ok <- result do
      execute(db, "UPDATE harklight.messages SET terminal=$2::text::jsonb,attributes=$3::text::jsonb WHERE id=$1",
        [id, terminal, encode(attributes(projected))])
    end
  end

  defp attributes(record) do
    Map.drop(record, [:message, :origin, :session_handle, :revision,
      :accepted_seq, :free_order, :accepted_at, :terminal])
  end

  defp revisions(db, id, record, revisions) do
    Enum.reduce_while(Enum.sort(revisions), :ok, fn {revision, body}, :ok ->
      # FORMAT32 retains the first and latest revision timestamps, but not
      # intermediate timestamps. Schema 002 preserves these as NULL rather than
      # inventing a timestamp from another revision.
      at =
        cond do
          revision == 1 -> record.accepted_at
          revision == record.revision -> Map.get(record, :revision_accepted_at)
          true -> nil
        end

      if is_nil(at) or is_integer(at) do
        case execute(db,
               "INSERT INTO harklight.message_revisions(message_id,revision,accepted_at_ms,body) VALUES ($1,$2,$3,$4::text::jsonb)",
               [id, revision, at, encode(body)]) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      else
        {:halt, {:error, :invalid_revision_timestamp}}
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

defmodule AttentionServer.Migration.ImportRecipients do
  @moduledoc false
  alias AttentionServer.{BounceDecision, JSON}
  alias AttentionServer.Migration.{Bundle, ImportLargeText, ImportMessages, ImportRows, ImportSession}
  alias AttentionServer.Repository.Postgres.Attributes

  def run(db, identity, bundle) do
    with :ok <- ImportMessages.run(db, identity, bundle),
         {:ok, manifest, _} <- Bundle.verify(bundle),
         {:ok, _} <- ImportRows.run(db, identity, "recipients/state/v1",
           Path.join(bundle, "records.frames"), manifest.data.records,
           &match?({{:row, :messages, _}, _}, &1), &write/2) do
      :ok
    end
  end

  # Match Resources.seen? exactly: this is membership, never a high-water mark.
  def substantive(record, state) do
    raw = Map.get(record, :revision, 1)
    value = case state do
      %{raw_to_substantive: mapping} when is_map(mapping) ->
        Map.get(mapping, raw, Map.get(state, :current_substantive_revision, raw))
      _ -> raw
    end
    if is_integer(value) and value > 0, do: {:ok, value}, else: {:error, :invalid_substantive_revision}
  end

  defp write(db, rows) do
    Enum.reduce_while(rows, :ok, fn {{:row, :messages, {person, sender, external} = key} = address, record}, :ok ->
      result = with {:ok, state} <- fact(db, "bounce_revisions", {:row, :bounce_revisions, key}),
        {:ok, revision} <- substantive(record, state),
        {:ok, opened} when opened in [nil, true] <- fact(db, "opened_revisions",
          {:member, :opened_revisions, {person, sender, external, revision}}),
        {:ok, projected} <- ImportLargeText.project(db, address, record),
        id = ImportMessages.id(key, record),
        {:ok, %{rows: [[old]]}} <- ImportSession.query(db,
          "SELECT attributes FROM harklight.message_recipients WHERE message_id=$1 AND person_id=$2", [id, person]),
        {:ok, attributes} <- attributes(old),
        next = Map.merge(attributes, %{visible: BounceDecision.person_visible?(record),
          substantive_revision: revision, origin_result: Map.get(projected, :origin_result)}),
        {:ok, %{num_rows: 1}} <- ImportSession.query(db,
          "UPDATE harklight.message_recipients SET seen_revision=$3,attributes=$4::text::jsonb WHERE message_id=$1 AND person_id=$2",
          [id, person, if(opened == true, do: revision), JSON.encode!(Attributes.encode(next))]) do
          :ok
        else
          {:error, _} = error -> error
          _ -> {:error, :recipient_state_mismatch}
        end
      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp fact(db, family, address) do
    case ImportSession.query(db,
      "SELECT body FROM harklight.compatibility_facts WHERE family=$1 AND address=$2", [family, Attributes.key(address)]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[body]]}} -> Attributes.decode(body)
      {:error, _} = error -> error
      _ -> {:error, :recipient_source_mismatch}
    end
  end
  defp attributes(value) when value == %{}, do: {:ok, %{}}
  defp attributes(value) do
    case Attributes.decode(value) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, :invalid_recipient_attributes}
    end
  end
end

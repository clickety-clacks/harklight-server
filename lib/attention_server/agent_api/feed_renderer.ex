defmodule AttentionServer.AgentAPI.FeedRenderer do
  @moduledoc false

  alias AttentionServer.{Engine, SharedAnswers, SharedMessages, SharedReminders}
  alias AttentionServer.AgentAPI.Advisories
  alias AttentionServer.ClientAPI.{AgentResources, Projection}

  def render_many(%{store: %{engine: engine}} = state, %{person: person, key: key}, rows) do
    Projection.probe(:feed)
    scope = {person, key}

    entries =
      for %{resource: {:entry, position}} <- rows,
          %{key: engine_key} <- [Map.get(engine.conversation_events, {person, position})],
          do: {engine_key, position}

    entries =
      entries
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.flat_map(fn {engine_key, positions} ->
        with {:ok, raw} <- AgentResources.raw_entries(state, scope, engine_key, positions),
             {:ok, wire} <- AgentResources.render_entries(state, scope, raw) do
          Enum.map(wire, &{&1["position"], &1})
        else
          _ -> []
        end
      end)
      |> Map.new()

    Enum.map(rows, fn row ->
      projected_row =
        case row.kind do
          :shared_message -> %{row | kind: :message}
          :shared_entry -> %{row | kind: :entry}
          :shared_seen -> %{row | kind: :seen}
          :shared_answer -> %{row | kind: :answer}
          :shared_advisory -> %{row | kind: :advisory}
          _ -> row
        end
      {projected_row, render_row(state, scope, row, entries)}
    end)
  end

  defp render_row(%{store: %{engine: engine}} = state, {person, origin},
         %{resource: {:shared_advisory, {id, advisory_id}}}, _entries) do
    with %{sender_person: ^person, sender_key: ^origin} = record <- engine.shared_messages[id],
         true <- shared_visible?(state, record),
         true <- Engine.shared_sender_allowed?(engine, person, origin, record.sender_id),
         %{} = advisory <- get_in(record, [:shared_advisories, advisory_id]) do
      {:ok, %{"item_id" => id, "advisory" => SharedReminders.wire(engine, record, advisory)}}
    else
      _ -> :skip
    end
  end

  defp render_row(%{store: %{engine: engine}} = state, {person, origin},
         %{resource: {:shared_answer, id}}, _entries) do
    with %{shared_id: message_id} = answer <- engine.shared_answers[id],
         %{sender_person: ^person, sender_key: ^origin} = record <- engine.shared_messages[message_id],
         true <- shared_visible?(state, record),
         true <- Engine.shared_sender_allowed?(engine, person, origin, record.sender_id) do
      {:ok, SharedAnswers.event(answer)}
    else
      _ -> :skip
    end
  end

  defp render_row(%{store: %{engine: engine}} = state, {person, origin},
         %{resource: {:shared_seen, id}}, _entries) do
    with %{shared_id: message_id} = event <- engine.shared_seen[id],
         %{sender_person: ^person, sender_key: ^origin} = record <- engine.shared_messages[message_id],
         true <- shared_visible?(state, record),
         true <- Engine.shared_sender_allowed?(engine, person, origin, record.sender_id) do
      {:ok, SharedMessages.seen_event(event)}
    else
      _ -> :skip
    end
  end

  defp render_row(%{store: %{engine: engine}} = state, {person, origin},
         %{resource: {:shared_message, id}, rev: rev}, _entries) do
    with %{sender_person: ^person, sender_key: ^origin} = record <- engine.shared_messages[id],
         true <- shared_visible?(state, record),
         true <- Engine.shared_sender_allowed?(engine, person, origin, record.sender_id) do
      {:ok, SharedMessages.sender_message(engine, record, rev)}
    else
      _ -> :skip
    end
  end

  defp render_row(%{store: %{engine: engine}} = state, {person, origin},
         %{resource: {:shared_entry, id}, rev: rev}, _entries) do
    with %{shared_id: shared_id, position: position} = entry <- engine.shared_entries[id],
         %{sender_person: ^person, sender_key: ^origin} = record <- engine.shared_messages[shared_id],
         true <- shared_visible?(state, record),
         true <- Engine.shared_sender_allowed?(engine, person, origin, record.sender_id),
         cutoff = Map.get(record, :sender_frozen_entry_high),
         true <- is_nil(cutoff) or position <= cutoff do
      {:ok, SharedMessages.sender_entry(record, entry, rev)}
    else
      _ -> :skip
    end
  end

  defp render_row(state, scope, %{resource: {:message, key}, rev: rev}, _entries) do
    case AgentResources.message(state, scope, key) do
      {:ok, %{"rev" => ^rev}} = rendered ->
        rendered

      _ ->
        {:error, :storage_unavailable}
    end
  end

  defp render_row(_state, _scope, %{resource: {:entry, position}, rev: rev}, entries) do
    case Map.fetch(entries, position) do
      {:ok, %{"rev" => ^rev} = entry} -> {:ok, entry}
      _ -> {:error, :storage_unavailable}
    end
  end

  defp render_row(
         %{store: %{engine: engine}},
         scope,
         %{resource: {:advisory, {item, id}}},
         _entries
       ) do
    Advisories.item_change(engine, scope, item, id)
  end

  defp render_row(
         %{store: %{engine: engine}},
         {person, key},
         %{resource: {:advisory, id}, session: session},
         _entries
       ) do
    Advisories.change(engine, {person, key, session}, id)
  end

  defp shared_visible?(state, record),
    do: AttentionServer.ClientAPI.SharedCapability.visible?(record, Map.get(state, :shared_projection))

end

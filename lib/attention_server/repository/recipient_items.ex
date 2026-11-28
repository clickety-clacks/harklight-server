defmodule AttentionServer.Repository.RecipientItems do
  @moduledoc false

  alias AttentionServer.ClientAPI.Resources

  def fetch(:postgres, person, item_id),
    do: AttentionServer.Repository.Postgres.Items.recipient_item(person, item_id)

  def fetch(engine, person, item_id) do
    with {:ok, key} <- Resources.person_item_key(engine, person, item_id) do
      {:ok, key, Map.fetch!(engine.messages, key)}
    end
  end

  def revision(:postgres, person, item_id, revision),
    do: AttentionServer.Repository.Postgres.Items.recipient_revision(person, item_id, revision)

  def summary_facts(:postgres, person, item_id),
    do: AttentionServer.Repository.Postgres.ItemSummaryFacts.read(person, item_id)

  def item(:postgres, %{person: person} = principal, item_id) do
    alias AttentionServer.Repository.Postgres.Items

    with {:ok, %{chat: chat, availability: availability} = facts}
         when is_map(chat) and (is_map(availability) or is_nil(availability)) <-
           summary_facts(:postgres, person, item_id),
         {:ok, key, record, files} <- Items.current_recipient(person, item_id, facts.request_revision) do
      item =
        key
        |> Resources.ordinary_item_payload(record, facts.rank, Map.put(facts, :files, files))
        |> Resources.stamp(facts.resource_revision)
        |> then(&Resources.render(:item, &1, principal))

      {:ok, %{"rev" => facts.head, "item" => item}}
    else
      {:ok, _unprojected} -> {:error, :storage_unavailable}
      {:error, _} = error -> error
    end
  end

  def read_through(engine, {person, _, _} = key, entry_id) do
    with {:ok, seq} <- item_entry_seq(engine, person, key, entry_id) do
      current = Map.get(engine.chat_read_positions, key, 0)
      {:ok, newest_origin_reply(engine, person, key, seq, current)}
    end
  end

  defp item_entry_seq(engine, person, message_key, entry_id) do
    with {:ok, seq} <- Resources.entry_seq(entry_id),
         %{key: ^message_key} <- Map.get(engine.conversation_events, {person, seq}) do
      {:ok, seq}
    else
      _ -> {:error, :invalid_field, "entry_id"}
    end
  end

  # Walks only this item's conversation, newest first, from the entry down to
  # the current position (the Route B index keeps each item's seqs in order).
  defp newest_origin_reply(engine, person, key, seq, current) do
    case Map.get(engine.route_b_index.conversations_by_item, key) do
      nil ->
        nil

      tree ->
        first_origin_reply(:gb_trees.iterator_from({-seq, seq}, tree), engine, person, current)
    end
  end

  defp first_origin_reply(iterator, engine, person, current) do
    case :gb_trees.next(iterator) do
      {{_, seq}, _, _next} when seq <= current ->
        nil

      {{_, seq}, _, next} ->
        case Map.get(engine.conversation_events, {person, seq}) do
          %{payload: %{"kind" => "reply", "author" => "origin"}} -> seq
          _ -> first_origin_reply(next, engine, person, current)
        end

      :none ->
        nil
    end
  end
end

defmodule AttentionServer.Repository.AgentItems do
  @moduledoc false

  alias AttentionServer.ClientAPI.Resources

  # A retract is authorized at account scope by its caller. Other operations
  # require the item to belong to the authenticated Agent origin.
  def item_key(engine, {person, _origin}, :retract, item_id),
    do: Resources.item_key(engine, person, item_id)

  def item_key(engine, scope, _operation, item_id) do
    with {:ok, key, _record} <- owned_item(engine, scope, item_id), do: {:ok, key}
  end

  def owned_item(:postgres, scope, item_id),
    do: AttentionServer.Repository.Postgres.Items.agent_item(scope, item_id)

  def owned_item(engine, {person, _origin} = scope, item_id) do
    with {:ok, key} <- Resources.item_key(engine, person, item_id),
         {:ok, record} <- owned_record(engine, scope, key) do
      {:ok, key, record}
    end
  end

  def owned_record(engine, {person, origin}, {person, _, _} = key) do
    case Map.get(engine.messages, key) do
      %{origin: ^origin} = record -> {:ok, record}
      _ -> {:error, :not_found}
    end
  end

  def owned_record(_engine, _scope, _key), do: {:error, :not_found}

  # Turn ids resolve within the identified item's person and may only refer
  # to that item. The event id and request revision are the correlation facts.
  def person_turn(engine, {person, _, _} = key, id) do
    with {:ok, position} <- Resources.entry_seq(id),
         %{
           key: ^key,
           payload: %{"kind" => "discussion", "event_id" => event, "request_revision" => revision}
         } <- Map.get(engine.conversation_events, {person, position}),
         {sequence, ""} when sequence > 0 <- Integer.parse(event) do
      {:ok, sequence, position, revision}
    else
      _ -> {:error, :not_found}
    end
  end
end

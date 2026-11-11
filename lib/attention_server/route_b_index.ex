defmodule AttentionServer.RouteBIndex do
  @moduledoc false

  def new do
    %{
      messages_by_person: %{},
      conversations_by_item: %{},
      conversation_summary_by_item: %{},
      unresolved_count_by_person: %{}
    }
  end

  def rebuild(engine) do
    index =
      Enum.reduce(engine.messages, new(), fn {key, record}, current ->
        put_message(current, key, nil, record)
      end)

    Enum.reduce(engine.conversation_events, index, fn {{person, seq}, event}, current ->
      put_conversation_event(current, person, seq, event.key, event.payload)
    end)
  end

  def put_message(index, {person, sender, id} = key, previous, record) do
    index =
      if is_nil(previous) do
        order = Map.get(index.messages_by_person, person, :gb_trees.empty())
        order_key = {-record.accepted_seq, sender, id}
        order = :gb_trees.enter(order_key, key, order)
        put_in(index, [:messages_by_person, person], order)
      else
        index
      end

    delta = unresolved_delta(previous, record)

    if delta == 0 do
      index
    else
      count = Map.get(index.unresolved_count_by_person, person, 0) + delta
      count = max(count, 0)
      put_in(index, [:unresolved_count_by_person, person], count)
    end
  end

  def put_conversation_event(index, person, seq, {^person, _sender, _id} = key, payload) do
    tree = Map.get(index.conversations_by_item, key, :gb_trees.empty())
    tree = :gb_trees.enter({-seq, seq}, seq, tree)
    index = put_in(index, [:conversations_by_item, key], tree)

    summary =
      Map.get(index.conversation_summary_by_item, key, %{
        latest_seq: 0,
        latest_origin_reply_seq: nil,
        chat_started: false,
        availability: nil
      })

    summary = %{summary | latest_seq: max(summary.latest_seq, seq)}

    summary =
      if payload["kind"] == "reply" and payload["author"] == "origin" do
        %{summary | latest_origin_reply_seq: max(summary.latest_origin_reply_seq || 0, seq)}
      else
        summary
      end

    summary =
      if payload["kind"] in ["discussion", "reply"] do
        %{summary | chat_started: true}
      else
        summary
      end

    summary =
      if payload["kind"] == "availability" do
        case summary.availability do
          %{seq: previous_seq} when previous_seq >= seq -> summary
          _ -> %{summary | availability: %{seq: seq, payload: payload}}
        end
      else
        summary
      end

    put_in(index, [:conversation_summary_by_item, key], summary)
  end

  defp unresolved_delta(previous, record) do
    previous_unresolved? = match?(%{terminal: nil}, previous)
    current_unresolved? = match?(%{terminal: nil}, record)

    cond do
      current_unresolved? and not previous_unresolved? -> 1
      previous_unresolved? and not current_unresolved? -> -1
      true -> 0
    end
  end
end

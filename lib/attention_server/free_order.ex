defmodule AttentionServer.FreeOrder do
  @moduledoc false

  alias AttentionServer.Message

  # Sort units, then flatten: reply rows never split a parent thread.
  def list_items(engine, person, replies, summaries) do
    parents =
      engine.messages
      |> Enum.filter(fn {{p, _, _}, _} -> p == person end)
      |> Enum.group_by(fn {{_, sender, id}, record} ->
        original = Map.get(record, :revisions, %{}) |> Map.get(1, record.message)

        case {record.terminal, Map.get(record, :group_thread, Message.valid_thread(original))} do
          {nil, thread} when is_binary(thread) -> {:thread, sender, thread}
          _ -> {:message, sender, id}
        end
      end)
      |> Enum.map(fn {_, members} ->
        members = Enum.sort_by(members, &parent_order/1)
        [{{_, sender, id}, anchor} | _] = members
        state = if anchor.terminal == nil, do: "active", else: "history"
        common = Map.get(anchor, :free_order)
        group = if common, do: "common", else: "earlier_messages"

        rows =
          Enum.map(members, fn {{_, s, i}, _} ->
            %{"kind" => "message", "sender_id" => s, "message_id" => i}
          end)

        unit(state, group, common || anchor.accepted_seq, 0, sender, id, rows)
      end)

    chats =
      Enum.map(replies, fn {{sender, id} = identity, row} ->
        seq = summaries[identity].newest
        event = Map.get(engine.conversation_events, {person, seq}, %{})
        common = Map.get(event, :free_order)

        verified =
          is_integer(seq) and seq > 0 and
            match?(
              %{
                key: {^person, ^sender, ^id},
                payload: %{"kind" => "reply", "author" => "origin"}
              },
              event
            )

        group =
          cond do
            common -> "common"
            verified -> "earlier_chat_replies"
            true -> "earlier_chat_replies_unknown"
          end

        state = if row["unseen_count"] > 0, do: "active", else: "history"
        unit(state, group, common || if(verified, do: seq, else: 0), 1, sender, id, [row])
      end)

    (parents ++ chats)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(&elem(&1, 1))
  end

  defp parent_order({{_, sender, id}, record}) do
    case Map.get(record, :free_order) do
      nil -> {1, -record.accepted_seq, sender, id}
      order -> {0, -order, sender, id}
    end
  end

  defp unit(state, group, order, kind, sender, id, rows) do
    section = %{"state" => state, "group" => group}

    group_rank =
      Enum.find_index(
        ~w(common earlier_messages earlier_chat_replies earlier_chat_replies_unknown),
        &(&1 == group)
      )

    {{if(state == "active", do: 0, else: 1), group_rank, -order, kind, sender, id},
     Enum.map(rows, &Map.put(&1, "free_section", section))}
  end
end

defmodule AttentionServer.ClientAPI.Search do
  @moduledoc false

  alias AttentionServer.{AgentAccess, BounceDecision, JSON, Message}

  @states ~w(open answered cleared expired superseded retracted)
  @max_token_bytes 4_096
  @max_page_bytes 4 * 1024 * 1024

  def query(params) do
    with :ok <- validate(params, "q", &(is_binary(&1) and byte_size(&1) <= 1_024)),
         :ok <- validate(params, "sender", &Message.sender_id?/1),
         :ok <- validate(params, "message_id", &Message.nonempty?/1),
         :ok <- validate(params, "session", &Message.uuid_v4?/1),
         :ok <- validate(params, "state", &(&1 in @states)),
         {:ok, limit} <- limit(params["limit"]) do
      {:ok, Map.take(params, ~w(q sender message_id state session)), limit}
    end
  end

  defp validate(params, field, valid?) do
    case Map.fetch(params, field) do
      :error -> :ok
      {:ok, value} -> if valid?.(value), do: :ok, else: {:error, :invalid_field, field}
    end
  end

  defp limit(nil), do: {:ok, 20}

  defp limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, min(n, 100)}
      _ -> {:error, :invalid_field, "limit"}
    end
  end

  defp limit(_), do: {:error, :invalid_field, "limit"}

  def position(nil, high, _binding), do: {:ok, high, high + 1}

  def position("s" <> encoded, current_high, binding)
      when byte_size(encoded) < @max_token_bytes do
    with {:ok, <<high::64, before::64, ^binding::binary-size(32)>>} <-
           Base.url_decode64(encoded, padding: false),
         true <- before > 0 and before <= high and high <= current_high do
      {:ok, high, before}
    else
      _ -> {:error, :invalid_field, "page"}
    end
  end

  def position(_, _high, _binding), do: {:error, :invalid_field, "page"}

  def binding(principal, filters) do
    :crypto.hash(
      :sha256,
      JSON.canonical([principal.person, principal.key, Map.get(principal, :id), filters])
    )
  end

  # Content matching may read LargeData. The request does that work between
  # bounded Relay reads of the same person log position.
  def matches(filters, before, limit, load),
    do: collect_matches(filters["q"], before - 1, limit + 1, load, [])

  defp collect_matches(_query, 0, _needed, _load, reversed),
    do: {:ok, Enum.reverse(reversed)}

  defp collect_matches(query, latest, needed, load, reversed) do
    earliest = max(1, latest - 127)

    with {:ok, candidates} <- load.(latest..earliest//-1) do
      Enum.reduce_while(candidates, {needed, reversed}, fn {seq, key, message}, {left, acc} ->
        case {AgentAccess.contains?(message, query), left} do
          {true, 1} -> {:halt, {0, [{seq, key} | acc]}}
          {true, _} -> {:cont, {left - 1, [{seq, key} | acc]}}
          {false, _} -> {:cont, {left, acc}}
        end
      end)
      |> case do
        {0, found} -> {:ok, Enum.reverse(found)}
        {left, found} -> collect_matches(query, earliest - 1, left, load, found)
      end
    end
  end

  def candidates(engine, %{person: person} = principal, sequence, filters) do
    for seq <- sequence,
        %{key: {^person, sender, id} = key} <- [engine.events[{person, seq}]],
        %{accepted_seq: ^seq} = record <- [engine.messages[key]],
        BounceDecision.person_visible?(record),
        AgentAccess.visible_sender?(principal, sender),
        is_nil(filters["sender"]) or sender == filters["sender"],
        is_nil(filters["message_id"]) or id == filters["message_id"],
        is_nil(filters["session"]) or Map.get(record, :session_handle) == filters["session"],
        AgentAccess.state_matches?(record, filters["state"]) do
      {seq, key, if(filters["q"], do: record.message)}
    end
  end

  # Size the wire Items off the Relay, including messages stored as LargeData.
  def response(_position, [], %{"sender" => _, "message_id" => _}, _page),
    do: {:error, :not_found}

  def response(position, rendered, _filters, %{limit: limit, high: high, binding: binding}) do
    {items, last, more?} = take_page(rendered, limit, 0, [], nil)
    body = %{"rev" => position, "items" => items}

    if more? do
      page = "s" <> Base.url_encode64(<<high::64, last::64, binding::binary>>, padding: false)
      {:ok, Map.put(body, "next_page", page)}
    else
      {:ok, body}
    end
  end

  defp take_page([], _left, _size, reversed, last),
    do: {Enum.reverse(reversed), last, false}

  defp take_page(_rows, 0, _size, reversed, last),
    do: {Enum.reverse(reversed), last, true}

  defp take_page([{seq, item} | rest], left, size, reversed, last) do
    next_size = size + JSON.encoded_size(item)

    if reversed != [] and next_size > @max_page_bytes do
      {Enum.reverse(reversed), last, true}
    else
      take_page(rest, left - 1, next_size, [item | reversed], seq)
    end
  end
end

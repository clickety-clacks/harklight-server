defmodule AttentionServer.AgentAccess do
  @moduledoc """
  Authenticated agent read projection. It is called only inside Relay's serialized
  decision process; the caller must release the result before that decision
  boundary ends. Device replay never uses this projection.
  """

  alias AttentionServer.{JSON, Message}

  @terminal_states ~w(answered cleared expired superseded retracted)
  @query_fields ~w(v type sender_id message_id state contains limit cursor)
  @max_page_bytes 4 * 1024 * 1024

  def valid_patterns?(patterns) do
    is_list(patterns) and length(patterns) <= 100 and
      length(Enum.uniq(patterns)) == length(patterns) and
      Enum.all?(patterns, fn pattern ->
        is_binary(pattern) and byte_size(pattern) <= 256 and Message.sender_id?(pattern)
      end)
  end

  def allowed?(patterns, sender) when is_binary(sender) do
    Enum.any?(patterns, fn pattern ->
      pattern == sender or
        (String.ends_with?(pattern, "/") and String.starts_with?(sender, pattern))
    end)
  end

  def allowed?(_, _), do: false

  def get(
        engine,
        key,
        %{"v" => 1, "type" => "get", "sender_id" => sender, "message_id" => id} = request
      ) do
    with true <- Enum.sort(Map.keys(request)) == ~w(message_id sender_id type v),
         {:ok, agent} <- current_agent(engine, key),
         true <- Message.sender_id?(sender) and Message.nonempty?(id),
         true <- allowed?(agent.read_grants, sender),
         record when not is_nil(record) <- Map.get(engine.messages, {agent.person, sender, id}) do
      observed = visible_high_water(engine, agent)

      {:ok,
       %{
         "v" => 1,
         "type" => "get_result",
         "relay_instance_id" => engine.relay_instance_id,
         "person_id" => agent.person,
         "observation_high_water" => observed,
         "item" => project(record, observed)
       }}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, "not_found"}
    end
  end

  def get(_, _, _), do: {:error, "invalid_request"}

  def search(engine, key, %{"v" => 1, "type" => "search"} = request) do
    with {:ok, agent} <- current_agent(engine, key),
         {:ok, filters, limit, cursor} <- validate_query(request),
         {:ok, high, before} <- page_position(engine, key, agent, filters, cursor) do
      observed = visible_high_water(engine, agent)

      matched =
        engine.messages
        |> Enum.filter(fn {{person, sender, id}, record} ->
          person == agent.person and record.accepted_seq <= high and
            (is_nil(before) or record.accepted_seq < before) and
            allowed?(agent.read_grants, sender) and
            (is_nil(filters["sender_id"]) or sender == filters["sender_id"]) and
            (is_nil(filters["message_id"]) or id == filters["message_id"]) and
            state_matches?(record, filters["state"]) and
            contains?(record.message, filters["contains"])
        end)
        |> Enum.sort_by(fn {_, record} -> -record.accepted_seq end)
        |> Enum.take(limit + 1)

      {page, has_more} = page_of(matched, limit, observed)
      items = Enum.map(page, fn {_, record} -> project(record, observed) end)

      next =
        if not has_more do
          nil
        else
          {_, last} = List.last(page)

          seal(engine.agent_cursor_key, %{
            "key" => key,
            "agent_id" => agent.id,
            "person_id" => agent.person,
            "grant_revision" => agent.grant_revision,
            "filters" => filters,
            "high_water" => high,
            "before" => last.accepted_seq
          })
        end

      if items == [] and filters["sender_id"] != nil and filters["message_id"] != nil do
        {:error, "not_found"}
      else
        {:ok,
         %{
           "v" => 1,
           "type" => "search_result",
           "relay_instance_id" => engine.relay_instance_id,
           "person_id" => agent.person,
           "initial_high_water" => high,
           "observation_high_water" => observed,
           "items" => items,
           "next_cursor" => next
         }}
      end
    end
  end

  def search(_, _, _), do: {:error, "invalid_request"}

  defp current_agent(engine, key) do
    case Map.get(engine.agents, key) do
      %{revoked: false, read_grants: grants} = agent when is_struct(grants, MapSet) ->
        {:ok, agent}

      _ ->
        {:error, "forbidden"}
    end
  end

  defp validate_query(request) do
    sender = Map.get(request, "sender_id")
    id = Map.get(request, "message_id")
    state = Map.get(request, "state")
    contains = Map.get(request, "contains")
    limit = Map.get(request, "limit", 20)
    cursor = Map.get(request, "cursor")

    cond do
      Enum.any?(Map.keys(request), &(&1 not in @query_fields)) ->
        {:error, "invalid_request"}

      not is_nil(sender) and not Message.sender_id?(sender) ->
        {:error, "invalid_request"}

      not is_nil(id) and not Message.nonempty?(id) ->
        {:error, "invalid_request"}

      not is_nil(state) and state not in ["open" | @terminal_states] ->
        {:error, "invalid_request"}

      not is_nil(contains) and (not is_binary(contains) or byte_size(contains) > 1_024) ->
        {:error, "invalid_request"}

      not is_integer(limit) or limit < 1 or limit > 100 ->
        {:error, "invalid_request"}

      not is_nil(cursor) and (not is_binary(cursor) or byte_size(cursor) > 4_096) ->
        {:error, "invalid_request"}

      true ->
        {:ok,
         %{"sender_id" => sender, "message_id" => id, "state" => state, "contains" => contains},
         limit, cursor}
    end
  end

  defp page_position(engine, _key, agent, _filters, nil),
    do: {:ok, visible_high_water(engine, agent), nil}

  defp page_position(engine, key, agent, filters, cursor) do
    case open(engine.agent_cursor_key, cursor) do
      {:ok,
       %{
         "key" => ^key,
         "agent_id" => id,
         "person_id" => person,
         "grant_revision" => revision,
         "filters" => ^filters,
         "high_water" => high,
         "before" => before
       }}
      when id == agent.id and person == agent.person and is_integer(high) and
             is_integer(before) and high >= before and before > 0 ->
        if revision == agent.grant_revision,
          do: {:ok, high, before},
          else: {:error, "stale_query"}

      {:ok, %{"key" => ^key, "grant_revision" => revision}}
      when revision != agent.grant_revision ->
        {:error, "stale_query"}

      _ ->
        {:error, "invalid_cursor"}
    end
  end

  defp visible_high_water(engine, agent) do
    Enum.reduce(engine.events, 0, fn
      {{person, seq}, %{key: {event_person, sender, _}}}, high
      when person == agent.person and event_person == person ->
        if allowed?(agent.read_grants, sender), do: max(seq, high), else: high

      _, high ->
        high
    end)
  end

  defp page_of(matched, limit, observed) do
    {reversed, _size, has_more} =
      Enum.reduce_while(matched, {[], 0, false}, fn entry, {selected, size, _} ->
        {_, record} = entry
        bytes = record |> project(observed) |> JSON.encode!() |> byte_size()

        if length(selected) >= limit or (selected != [] and size + bytes > @max_page_bytes) do
          {:halt, {selected, size, true}}
        else
          {:cont, {[entry | selected], size + bytes, false}}
        end
      end)

    {Enum.reverse(reversed), has_more}
  end

  defp state_matches?(_, nil), do: true
  defp state_matches?(%{terminal: nil}, "open"), do: true
  defp state_matches?(%{terminal: %{state: state}}, state), do: true
  defp state_matches?(_, _), do: false

  defp contains?(_value, nil), do: true
  defp contains?(value, needle) when is_binary(value), do: String.contains?(value, needle)
  defp contains?(%JSON.DecimalToken{}, _needle), do: false

  defp contains?(value, needle) when is_map(value),
    do: Enum.any?(value, fn {_, v} -> contains?(v, needle) end)

  defp contains?(value, needle) when is_list(value), do: Enum.any?(value, &contains?(&1, needle))
  defp contains?(_, _), do: false

  defp project(record, observed) do
    message = record.message

    base = %{
      "sender_id" => message["sender_id"],
      "message_id" => message["message_id"],
      "acceptance_seq" => record.accepted_seq,
      "observation_high_water" => observed,
      "message" => message,
      "state" => if(record.terminal, do: record.terminal.state, else: "open")
    }

    base = Map.merge(base, source_status(record))

    base =
      case record.terminal do
        %{state: "retracted", at: at} -> Map.put(base, "retracted_at", at)
        _ -> base
      end

    case record.terminal do
      nil ->
        base

      terminal ->
        base
        |> Map.put("terminal_seq", terminal.seq)
        |> Map.put(
          "terminal_kind",
          Map.get(terminal, :kind) || if(terminal.answer_present, do: "answer", else: "lifecycle")
        )
        |> Map.put("has_answer", terminal.answer_present)
        |> then(fn item ->
          if terminal.answer_present, do: Map.put(item, "answer", terminal.answer), else: item
        end)
    end
  end

  defp source_status(record) do
    case {Map.get(record, :origin_result), Map.get(record, :source_result)} do
      {%{status: origin_status} = origin_result, source_result}
      when not is_nil(source_result) ->
        %{
          "source_status" => "source_reported",
          "source_instance_id" => record.source_binding["source_instance_id"],
          "status" => source_result.status,
          "result_ref" => source_result.result_ref,
          "result_revision" => source_result.revision,
          "origin_result" => %{
            "status" => origin_status,
            "result_ref" => origin_result.result_ref,
            "result_revision" => origin_result.revision
          }
        }

      {%{status: status} = result, _} ->
        %{
          "source_status" => "source_reported",
          "status" => status,
          "result_ref" => result.result_ref,
          "result_revision" => result.revision
        }

      {nil, nil} ->
        %{"source_status" => "unreported"}

      {nil, result} ->
        %{
          "source_status" => "source_reported",
          "source_instance_id" => record.source_binding["source_instance_id"],
          "status" => result.status,
          "result_ref" => result.result_ref,
          "result_revision" => result.revision
        }
    end
  end

  defp seal(key, value) do
    nonce = :crypto.strong_rand_bytes(12)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, JSON.encode!(value), <<>>, 16, true)

    "q1." <> Base.url_encode64(nonce <> tag <> ciphertext, padding: false)
  end

  defp open(key, "q1." <> encoded) do
    with {:ok, <<nonce::binary-size(12), tag::binary-size(16), ciphertext::binary>>} <-
           Base.url_decode64(encoded, padding: false),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, ciphertext, <<>>, tag, false),
         {:ok, decoded} <- JSON.decode_line(plaintext <> "\n") do
      {:ok, decoded}
    else
      _ -> {:error, :invalid_cursor}
    end
  rescue
    _ -> {:error, :invalid_cursor}
  end

  defp open(_, _), do: {:error, :invalid_cursor}
end

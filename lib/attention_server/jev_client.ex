defmodule AttentionServer.JevClient do
  @moduledoc false

  alias AttentionServer.JSON

  @key_path_env "ATTENTION_SERVER_JEV_KEY_PATH"
  @endpoint_timeout 5_000
  @request_timeout 30_000
  @response_limit 4 * 1024 * 1024
  @request_limit 8 * 1024 * 1024
  @request_budget 48_000
  @question_budget 24_000
  @criteria [
    "Low priority: this can wait while more time-sensitive items are handled.",
    "Normal priority: this should be handled soon, but other items can reasonably come first.",
    "High priority: delaying this item has clear near-term consequences; handle it before less urgent items.",
    "Now: this needs the person's attention right away, ahead of everything else."
  ]

  def key_status do
    case read_key() do
      {:ok, _key} -> "readable"
      {:error, :not_set} -> "not set"
      {:error, _} -> "not readable"
    end
  end

  def connection_ready?(endpoint, model) do
    valid_endpoint?(endpoint) and valid_model?(model) and key_status() == "readable"
  end

  def valid_endpoint?(endpoint) when is_binary(endpoint) and byte_size(endpoint) in 1..2_048 do
    uri = URI.parse(String.trim(endpoint))

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
      uri.userinfo == nil and uri.query == nil and uri.fragment == nil and
      is_integer(uri.port) and uri.port in 1..65_535
  rescue
    _ -> false
  end

  def valid_endpoint?(_), do: false

  def valid_model?(model) when is_binary(model) and byte_size(model) in 1..256 do
    String.trim(model) == model and String.valid?(model) and
      not Regex.match?(~r/[\p{Cc}]/u, model)
  end

  def valid_model?(_), do: false

  def compose(items, endpoint, model, now \\ System.system_time(:millisecond))

  def compose(items, endpoint, model, now)
      when is_list(items) and is_binary(endpoint) and is_binary(model) do
    if valid_endpoint?(endpoint) and valid_model?(model) do
      {questions, question_identities} =
        items
        |> Enum.with_index(1)
        |> Enum.reduce({%{}, %{}}, fn {item, index}, {questions, identities} ->
          question_id = "candidate_" <> Integer.to_string(index)

          instructions =
            "Rate candidate #{index} in the supplied pending set for how soon it should be handled. " <>
              "Consider its title, body, stated stake, deadline and the sender's stated urgency relative to the other pending items. " <>
              "The current time is now; due_in_minutes is the time left before the deadline, negative once it has passed."

          question = %{
            "type" => "score",
            "instructions" => instructions,
            "criteria" => @criteria
          }

          {Map.put(questions, question_id, question),
           Map.put(identities, question_id, item.identity)}
        end)

      state = %{
        "now" => timestamp(now),
        "pending_items" =>
          items
          |> Enum.with_index(1)
          |> Enum.map(fn {item, index} ->
            message = item.message

            pending = %{
              "candidate" => index,
              "sender_id" => elem(item.identity, 0),
              "message_id" => elem(item.identity, 1),
              "title" => Map.get(message, "title", ""),
              "body" => Map.get(message, "body", ""),
              "stake" => AttentionServer.Message.effective_stake(message),
              "urgency" => AttentionServer.Message.effective_urgency(message)
            }

            case AttentionServer.Message.effective_by_when(message) do
              by_when when is_integer(by_when) ->
                pending
                |> Map.put("by_when", timestamp(by_when))
                |> Map.put("due_in_minutes", Integer.floor_div(by_when - now, 60_000))

              _ ->
                pending
            end
          end)
      }

      request = %{"model" => model, "state" => state, "questions" => questions}
      body = JSON.encode!(request)
      state_size = byte_size(JSON.encode!(state))

      largest_question =
        questions
        |> Map.values()
        |> Enum.map(&byte_size(JSON.encode!(&1)))
        |> Enum.max(fn -> 0 end)

      if byte_size(body) <= min(@request_limit, @request_budget) and
           state_size + largest_question <= @question_budget do
        with {:ok, uri} <- endpoint_uri(endpoint) do
          {:ok,
           %{
             uri: uri,
             model: model,
             body: body,
             state_bytes: state_size,
             largest_question_bytes: largest_question,
             candidates: Enum.map(items, & &1.identity),
             question_identities: question_identities
           }}
        end
      else
        {:error, :request_too_large}
      end
    else
      {:error, :jev_unconfigured}
    end
  rescue
    _ -> {:error, :invalid_request}
  end

  def compose(_, _, _, _), do: {:error, :jev_unconfigured}

  def item_cost(item, endpoint, model, now) do
    case compose([item], endpoint, model, now) do
      {:ok, request} ->
        empty_state_bytes =
          byte_size(JSON.encode!(%{"now" => timestamp(now), "pending_items" => []}))

        {:ok, request.state_bytes - empty_state_bytes + request.largest_question_bytes + 1}

      error ->
        error
    end
  end

  defp timestamp(milliseconds) do
    milliseconds
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  def evaluate(%{uri: uri, body: body, candidates: candidates, question_identities: identities}) do
    with {:ok, key} <- read_key(),
         {:ok, response} <- post(uri, body, key) do
      rank_response(response, identities, candidates)
    else
      _ -> {:error, :unavailable}
    end
  end

  def evaluate(_), do: {:error, :unavailable}

  def rank_response(body, question_identities, candidates)
      when is_list(candidates) and is_map(question_identities) do
    candidate_questions =
      Map.filter(question_identities, fn {_question_id, identity} -> identity in candidates end)

    with {:ok, scores} <- parse_scores(body, candidate_questions) do
      {:ok, Map.take(scores, candidates)}
    else
      _ -> {:error, :malformed_response}
    end
  end

  def rank_response(_, _, _), do: {:error, :malformed_response}

  def parse_scores(body, question_identities)
      when is_binary(body) and is_map(question_identities) do
    with {:ok, %{"answers" => answers}} <- JSON.decode_line(body <> "\n", @response_limit),
         true <- is_map(answers) do
      Enum.reduce_while(question_identities, {:ok, %{}}, fn {question_id, identity},
                                                            {:ok, scores} ->
        case Map.get(answers, question_id) do
          %{"type" => "score", "score" => score} ->
            case score_value(score) do
              {:ok, value} -> {:cont, {:ok, Map.put(scores, identity, value)}}
              :error -> {:cont, {:ok, scores}}
            end

          _ ->
            {:cont, {:ok, scores}}
        end
      end)
    else
      _ -> {:error, :malformed_response}
    end
  end

  def parse_scores(_, _), do: {:error, :malformed_response}

  defp score_value(value) when is_integer(value) and value in 0..3, do: {:ok, value * 1.0}

  defp score_value(%JSON.DecimalToken{raw: raw}) do
    case Float.parse(raw) do
      {value, ""} when value >= 0 and value <= 3 -> {:ok, value}
      _ -> :error
    end
  end

  defp score_value(value) when is_float(value) and value >= 0 and value <= 3, do: {:ok, value}

  defp score_value(_), do: :error

  defp read_key do
    case System.get_env(@key_path_env) do
      nil ->
        {:error, :not_set}

      "" ->
        {:error, :not_set}

      path ->
        case File.read(path) do
          {:ok, bytes} when byte_size(bytes) > 0 ->
            key = String.trim(bytes)
            if key == "", do: {:error, :not_readable}, else: {:ok, key}

          _ ->
            {:error, :not_readable}
        end
    end
  end

  defp endpoint_uri(endpoint) do
    uri = URI.parse(String.trim(endpoint))

    if valid_endpoint?(endpoint) do
      {:ok, %{uri | path: if(uri.path in [nil, ""], do: "/v1/systemone", else: uri.path)}}
    else
      {:error, :invalid_endpoint}
    end
  end

  defp post(uri, body, key) do
    path = if uri.query, do: uri.path <> "?" <> uri.query, else: uri.path

    with {:ok, connection} <-
           Mint.HTTP2.connect(:https, uri.host, uri.port,
             hostname: uri.host,
             timeout: @endpoint_timeout,
             mode: :passive,
             transport_opts: [verify: :verify_peer, cacerts: :public_key.cacerts_get()]
           ),
         {:ok, connection, request_ref} <-
           Mint.HTTP2.request(
             connection,
             "POST",
             path,
             [
               {"authorization", "Bearer " <> key},
               {"content-type", "application/json"}
             ],
             body
           ),
         {:ok, connection, status, response} <- receive_response(connection, request_ref) do
      _ = Mint.HTTP2.close(connection)
      if status == 200, do: {:ok, response}, else: {:error, :provider_rejected}
    else
      {:error, connection, _reason} ->
        _ = Mint.HTTP2.close(connection)
        {:error, :unavailable}

      {:error, _reason} ->
        {:error, :unavailable}
    end
  rescue
    _ -> {:error, :unavailable}
  end

  defp receive_response(connection, request_ref) do
    deadline = System.monotonic_time(:millisecond) + @request_timeout
    receive_response(connection, request_ref, nil, <<>>, deadline)
  end

  defp receive_response(connection, request_ref, status, body, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    case Mint.HTTP2.recv(connection, 0, timeout) do
      {:ok, next, events} ->
        {status, body, complete?, too_large?} =
          Enum.reduce(events, {status, body, false, false}, fn
            {:status, ^request_ref, value}, {_status, current_body, complete?, too_large?} ->
              {value, current_body, complete?, too_large?}

            {:data, ^request_ref, chunk}, {current_status, current_body, complete?, _too_large?}
            when byte_size(current_body) + byte_size(chunk) <= @response_limit ->
              {current_status, current_body <> chunk, complete?, false}

            {:data, ^request_ref, _chunk},
            {current_status, current_body, complete?, _too_large?} ->
              {current_status, current_body, complete?, true}

            {:done, ^request_ref}, {current_status, current_body, _complete?, too_large?} ->
              {current_status, current_body, true, too_large?}

            _event, acc ->
              acc
          end)

        cond do
          too_large? -> {:error, next, :response_too_large}
          complete? and is_integer(status) -> {:ok, next, status, body}
          byte_size(body) >= @response_limit -> {:error, next, :response_too_large}
          System.monotonic_time(:millisecond) >= deadline -> {:error, next, :timeout}
          true -> receive_response(next, request_ref, status, body, deadline)
        end

      {:error, next, _error, _responses} ->
        {:error, next, :unavailable}
    end
  end
end

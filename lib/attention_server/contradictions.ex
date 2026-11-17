defmodule AttentionServer.Contradictions do
  @moduledoc false

  alias AttentionServer.{JSON, JevClient, LargeData}

  @warning "these possibly contradictory messages are still active, please talk to its poster to reconcile them"
  @criteria [
    "Unrelated: the messages concern independent matters.",
    "Compatible: both requests or claims can be satisfied together.",
    "Uncertain: the messages may compete, but no concrete conflict is apparent.",
    "Possible contradiction: following one request may prevent following the other, or their claims disagree.",
    "Clear contradiction: the messages require mutually exclusive actions or make directly opposing claims."
  ]

  def items(engine, person, posted_key, names \\ %{}) do
    engine.messages
    |> Enum.flat_map(fn
      {{^person, sender, message} = key, %{terminal: nil} = record} when key != posted_key ->
        display =
          AttentionServer.SenderNames.committed(record, sender) ||
            AttentionServer.SenderNames.snapshot(names, person, sender, record.origin)

        name = if(display, do: display["name"], else: record.message["display_name"])

        [
          %{
            identity: {sender, message},
            message: record.message,
            accepted_seq: record.accepted_seq,
            sender_name: name
          }
        ]

      _ ->
        []
    end)
    |> Enum.sort_by(& &1.accepted_seq)
  end

  # No text clipping: a single comparison that cannot fit remains unchecked.
  def compose(posted, items, endpoint, model) do
    cond do
      not JevClient.valid_endpoint?(endpoint) or not JevClient.valid_model?(model) ->
        {:error, :jev_unconfigured}

      LargeData.contains_ref?([posted | Enum.map(items, & &1.message)]) ->
        {:error, :request_too_large}

      true ->
        questions =
          items
          |> Enum.with_index(1)
          |> Map.new(fn {_item, index} ->
            {"candidate_#{index}",
             %{
               "type" => "score",
               "instructions" =>
                 "How likely is open_messages[#{index - 1}] to contradict posted_message? Compare the full title and body of both messages. Judge whether their requests or factual claims conflict, not their urgency or importance. Different topics or compatible requests are not contradictions.",
               "criteria" => @criteria
             }}
          end)

        state = %{
          "posted_message" => Map.take(posted, ~w(title body)),
          "open_messages" => Enum.map(items, &Map.take(&1.message, ~w(title body)))
        }

        body = JSON.encode!(%{"model" => model, "state" => state, "questions" => questions})

        largest =
          questions
          |> Map.values()
          |> Enum.map(&byte_size(JSON.encode!(&1)))
          |> Enum.max(fn -> 0 end)

        uri = URI.parse(endpoint)
        uri = %{uri | path: if(uri.path in [nil, ""], do: "/v1/systemone", else: uri.path)}

        if byte_size(body) <= 48_000 and byte_size(JSON.encode!(state)) + largest <= 24_000 do
          {:ok,
           %{
             uri: uri,
             body: body,
             model: model,
             candidates: Enum.map(items, & &1.identity),
             question_identities:
               items
               |> Enum.with_index(1)
               |> Map.new(fn {item, i} -> {"candidate_#{i}", item.identity} end)
           }}
        else
          {:error, :request_too_large}
        end
    end
  end

  def batches(posted, items, endpoint, model, size) when size > 0 do
    items
    |> Enum.chunk_every(size)
    |> Enum.flat_map(&fit(posted, &1, endpoint, model))
  end

  defp fit(posted, items, endpoint, model) do
    case compose(posted, items, endpoint, model) do
      {:ok, request} ->
        [request]

      _ when length(items) == 1 ->
        []

      _ ->
        {left, right} = Enum.split(items, div(length(items), 2))
        fit(posted, left, endpoint, model) ++ fit(posted, right, endpoint, model)
    end
  end

  # The caller commits delivery before entering this worker. One shared
  # deadline covers composition and all parallel batches, not one per batch.
  def run(posted, items, endpoint, model, size, opts \\ []) do
    deadline = Keyword.get(opts, :deadline, System.monotonic_time(:millisecond) + 5_000)
    evaluator = Keyword.get(opts, :evaluator, &JevClient.evaluate/1)
    requests = batches(posted, items, endpoint, model, size)

    {:ok, supervisor} = Task.Supervisor.start_link()

    try do
      tasks =
        Map.new(requests, fn request ->
          task =
            Task.Supervisor.async_nolink(supervisor, fn ->
              try do
                evaluator.(request)
              rescue
                _ -> {:error, :unavailable}
              catch
                _, _ -> {:error, :unavailable}
              end
            end)

          {task.ref, {task, request.candidates}}
        end)

      {scores, timed_out?} = collect(tasks, %{}, deadline)
      result(items, scores, timed_out?)
    after
      Supervisor.stop(supervisor)
    end
  end

  defp collect(tasks, scores, _deadline) when map_size(tasks) == 0, do: {scores, false}

  defp collect(tasks, scores, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      stop_pending(tasks, scores)
    else
      await(tasks, scores, deadline, remaining)
    end
  end

  defp await(tasks, scores, deadline, remaining) do
    receive do
      {ref, answer} when is_map_key(tasks, ref) ->
        {{_task, identities}, rest} = Map.pop(tasks, ref)
        Process.demonitor(ref, [:flush])

        usable =
          case answer do
            {:ok, values} when is_map(values) -> Map.take(values, identities)
            _ -> %{}
          end

        collect(rest, Map.merge(scores, usable), deadline)

      {:DOWN, ref, :process, _pid, _reason} when is_map_key(tasks, ref) ->
        collect(Map.delete(tasks, ref), scores, deadline)
    after
      remaining ->
        stop_pending(tasks, scores)
    end
  end

  defp stop_pending(tasks, scores) do
    Enum.each(tasks, fn {_ref, {task, _identities}} -> Task.shutdown(task, :brutal_kill) end)
    {scores, true}
  end

  # Answers are normalized scores from JevClient.parse_scores/2. Missing or
  # invalid individual answers never count as negative comparisons.
  def result(items, scores, timed_out?) do
    valid =
      Map.filter(scores, fn {_id, score} -> is_number(score) and score >= 0 and score <= 1 end)

    unchecked = Enum.count(items, &(not Map.has_key?(valid, &1.identity)))

    matches =
      items
      |> Enum.filter(&(Map.get(valid, &1.identity, -1) >= 0.75))
      |> Enum.uniq_by(& &1.identity)
      |> Enum.sort_by(& &1.accepted_seq)
      |> Enum.map(fn item ->
        {sender, message} = item.identity

        entry = %{
          "sender_id" => sender,
          "message_id" => message,
          "title" => item.message["title"]
        }

        if is_binary(Map.get(item, :sender_name)),
          do: Map.put(entry, "sender_name", item.sender_name),
          else: entry
      end)

    check =
      if unchecked == 0 do
        %{"status" => "checked"}
      else
        %{
          "status" => "incomplete",
          "reason" => if(timed_out?, do: "timeout", else: "unavailable"),
          "unchecked" => unchecked
        }
      end

    cond do
      matches != [] -> Map.merge(check, %{"warning" => @warning, "messages" => matches})
      unchecked > 0 -> check
      true -> nil
    end
  end
end

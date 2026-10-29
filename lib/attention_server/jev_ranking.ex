defmodule AttentionServer.JevRanking do
  @moduledoc false

  alias AttentionServer.JevClient
  alias AttentionServer.Message
  alias AttentionServer.Placement.Baseline

  @considered_budget 240_000
  @top_tier_cap 5

  def top_tier_cap, do: @top_tier_cap

  def deadline_class(item, now) do
    case Message.effective_by_when(item.message) do
      due when is_integer(due) ->
        remaining = due - now

        cond do
          remaining > 86_400_000 -> 5
          remaining > 14_400_000 -> 4
          remaining > 3_600_000 -> 3
          remaining > 900_000 -> 2
          remaining > 0 -> 1
          true -> 0
        end

      _ ->
        nil
    end
  end

  def next_deadline(items, now) do
    items
    |> Enum.flat_map(fn item ->
      case Message.effective_by_when(item.message) do
        due when is_integer(due) ->
          [86_400_000, 14_400_000, 3_600_000, 900_000, 0]
          |> Enum.map(&(due - &1))
          |> Enum.filter(&(&1 > now))

        _ ->
          []
      end
    end)
    |> Enum.min(fn -> nil end)
  end

  def run(items, prior, endpoint, model, now, evaluator \\ &JevClient.evaluate/1) do
    ordered = Baseline.ordered_items(items)
    {considered, clipped} = considered(ordered, endpoint, model, now)
    batches = batches(considered, Map.get(prior || %{}, :batches, []), endpoint, model, now)
    pending = Enum.filter(batches, &is_nil(&1.scores))
    started = System.monotonic_time(:millisecond)

    answers =
      pending
      |> Task.async_stream(fn batch -> {batch.id, safe_evaluate(evaluator, batch.request)} end,
        max_concurrency: max(length(pending), 1),
        timeout: 40_000,
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, answer} -> answer
        _ -> {:error, {:error, :unavailable}}
      end)
      |> Map.new()

    {batches, first_failures, first_bytes} =
      Enum.reduce(batches, {[], 0, 0}, fn batch, {acc, failed, bytes} ->
        if is_nil(batch.scores) do
          request_bytes = byte_size(batch.request.body)

          case Map.get(answers, batch.id, {:error, :unavailable}) do
            {:ok, scores} when is_map(scores) and map_size(scores) > 0 ->
              {[%{batch | scores: scores, request: nil} | acc], failed, bytes + request_bytes}

            _ ->
              {[%{batch | scores: nil, request: nil} | acc], failed + 1, bytes + request_bytes}
          end
        else
          {[batch | acc], failed, bytes}
        end
      end)

    batches = Enum.reverse(batches)

    first_scores =
      Enum.reduce(batches, %{}, fn batch, acc -> Map.merge(acc, batch.scores || %{}) end)

    basic_index =
      ordered |> Enum.with_index() |> Map.new(fn {item, index} -> {item.identity, index} end)

    finalists =
      considered
      |> Enum.filter(&(Map.get(first_scores, &1.identity, -1) >= 1.5))
      |> Enum.sort_by(fn item ->
        {-Map.fetch!(first_scores, item.identity), Map.fetch!(basic_index, item.identity)}
      end)

    {final_items, overflow} =
      if length(batches) > 1 and finalists != [] do
        fit_finalists(finalists, endpoint, model, now)
      else
        {[], []}
      end

    signature = Enum.map(final_items, &input_signature(&1, now))
    old_final = Map.get(prior || %{}, :finals)

    reuse_final? =
      is_map(old_final) and old_final.signature == signature and is_map(old_final.scores)

    {finals, final_failure, final_bytes, final_sent?} =
      cond do
        final_items == [] ->
          {nil, 0, 0, false}

        reuse_final? ->
          {old_final, 0, 0, false}

        true ->
          {:ok, request} = JevClient.compose(final_items, endpoint, model, now)
          result = safe_evaluate(evaluator, request)

          scores =
            case result do
              {:ok, values} when is_map(values) and map_size(values) > 0 -> values
              _ -> nil
            end

          {%{signature: signature, scores: scores}, if(is_nil(scores), do: 1, else: 0),
           byte_size(request.body), true}
      end

    sent = length(pending) + if(final_sent?, do: 1, else: 0)
    failures = first_failures + final_failure
    scores = Map.merge(first_scores, if(is_map(finals), do: finals.scores || %{}, else: %{}))

    %{
      cache: %{batches: batches, finals: finals},
      scores: scores,
      overflow: MapSet.new(Enum.map(overflow, & &1.identity)),
      considered: considered,
      clipped: clipped,
      now: now,
      sent: sent,
      failed: failures,
      finals_sent: final_sent?,
      bytes: first_bytes + final_bytes,
      elapsed_ms: if(sent > 0, do: System.monotonic_time(:millisecond) - started, else: 0)
    }
  end

  defp considered(ordered, endpoint, model, now) do
    {chosen, clipped, _used, _full?} =
      Enum.reduce(ordered, {[], [], 0, false}, fn item, {chosen, clipped, used, full?} ->
        case JevClient.item_cost(item, endpoint, model, now) do
          {:ok, cost} when not full? and used + cost <= @considered_budget ->
            {[item | chosen], clipped, used + cost, false}

          {:ok, _cost} ->
            {chosen, [item | clipped], used, true}

          _ ->
            {chosen, [item | clipped], used, full?}
        end
      end)

    {Enum.reverse(chosen), Enum.reverse(clipped)}
  end

  defp batches(considered, prior, endpoint, model, now) do
    by_id = Map.new(considered, &{&1.identity, &1})

    {retained, displaced} =
      Enum.map_reduce(prior, [], fn batch, displaced ->
        members =
          Enum.filter(batch.members, fn id ->
            case Map.get(by_id, id) do
              nil ->
                false

              item ->
                Enum.any?(batch.input, fn {old_id, rev, _class} ->
                  old_id == id and rev == item.revision
                end)
            end
          end)

        {members, removed} = trim_batch(members, by_id, endpoint, model, now)
        {%{batch | members: members}, removed ++ displaced}
      end)

    assigned = retained |> Enum.flat_map(& &1.members) |> MapSet.new()
    displaced = MapSet.new(displaced)

    newcomers =
      Enum.filter(considered, fn item ->
        MapSet.member?(displaced, item.identity) or not MapSet.member?(assigned, item.identity)
      end)

    filled =
      Enum.reduce(newcomers, retained, fn item, batches ->
        origin = Map.get(item, :origin_target)
        preferred = Enum.find(prior, fn batch -> origin in batch.members end)

        choices =
          if preferred,
            do: [preferred.id | Enum.map(batches, & &1.id)],
            else: Enum.map(batches, & &1.id)

        target =
          Enum.find(choices, fn id ->
            case Enum.find(batches, &(&1.id == id)) do
              nil ->
                false

              batch ->
                fits?(
                  Enum.map(batch.members, &Map.fetch!(by_id, &1)) ++ [item],
                  endpoint,
                  model,
                  now
                )
            end
          end)

        if target do
          Enum.map(batches, fn batch ->
            if batch.id == target,
              do: %{batch | members: batch.members ++ [item.identity]},
              else: batch
          end)
        else
          next_id = (Enum.map(batches, & &1.id) |> Enum.max(fn -> 0 end)) + 1

          batches ++
            [%{id: next_id, members: [item.identity], input: [], scores: nil, request: nil}]
        end
      end)

    filled
    |> Enum.reject(&(&1.members == []))
    |> Enum.map(fn batch ->
      members = Enum.map(batch.members, &Map.fetch!(by_id, &1))
      input = Enum.map(members, &input_signature(&1, now))
      scores = if batch.input == input, do: batch.scores, else: nil

      request =
        if is_nil(scores),
          do: elem(JevClient.compose(members, endpoint, model, now), 1),
          else: nil

      %{batch | input: input, scores: scores, request: request}
    end)
  end

  defp input_signature(item, now), do: {item.identity, item.revision, deadline_class(item, now)}

  defp trim_batch([], _by_id, _endpoint, _model, _now), do: {[], []}

  defp trim_batch(members, by_id, endpoint, model, now) do
    items = Enum.map(members, &Map.fetch!(by_id, &1))

    if fits?(items, endpoint, model, now) do
      {members, []}
    else
      {last, kept} = List.pop_at(members, -1)
      {members, displaced} = trim_batch(kept, by_id, endpoint, model, now)
      {members, [last | displaced]}
    end
  end

  defp fits?(items, endpoint, model, now),
    do: match?({:ok, _}, JevClient.compose(items, endpoint, model, now))

  defp fit_finalists(items, endpoint, model, now) do
    Enum.reduce_while(items, {[], []}, fn item, {fitted, _overflow} ->
      candidate = fitted ++ [item]

      if fits?(candidate, endpoint, model, now),
        do: {:cont, {candidate, []}},
        else: {:halt, {fitted, Enum.drop(items, length(fitted))}}
    end)
  end

  defp safe_evaluate(evaluator, request) do
    evaluator.(request)
  rescue
    _ -> {:error, :unavailable}
  catch
    :exit, _ -> {:error, :unavailable}
  end
end

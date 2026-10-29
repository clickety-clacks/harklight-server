defmodule AttentionServer.JevRankingTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{JSON, JevClient, JevRanking, Placement}

  @endpoint "https://example.com/v1/systemone"
  @model "jev-latest"
  @now 1_793_290_000_000

  defp item(n, body, options \\ %{}) do
    %{
      identity: {"sender", Integer.to_string(n)},
      accepted_seq: n,
      revision: Map.get(options, :revision, 1),
      origin_target: Map.get(options, :origin_target),
      message:
        %{
          "title" => "Item #{n}",
          "body" => body,
          "urgency" => Map.get(options, :urgency, "normal")
        }
        |> then(fn message ->
          case Map.get(options, :due) do
            nil -> message
            due -> Map.put(message, "by_when", due)
          end
        end)
    }
  end

  defp scoring(notify \\ nil, score \\ 1.0) do
    fn request ->
      if notify, do: send(notify, {:request, self(), request})
      {:ok, Map.new(request.candidates, &{&1, score})}
    end
  end

  test "time fields use UTC, whole signed minutes, and deadline classes change only at thresholds" do
    due = @now + 10 * 60_000 + 30_000
    assert {:ok, request} = JevClient.compose([item(1, "", %{due: due})], @endpoint, @model, @now)
    assert {:ok, %{"state" => state}} = JSON.decode_line(request.body <> "\n")

    assert state["now"] ==
             DateTime.from_unix!(@now, :millisecond)
             |> DateTime.truncate(:second)
             |> DateTime.to_iso8601()

    assert hd(state["pending_items"])["due_in_minutes"] == 10

    assert hd(state["pending_items"])["by_when"] ==
             DateTime.from_unix!(due, :millisecond)
             |> DateTime.truncate(:second)
             |> DateTime.to_iso8601()

    assert {:ok, past} =
             JevClient.compose([item(1, "", %{due: @now - 1})], @endpoint, @model, @now)

    assert {:ok, %{"state" => %{"pending_items" => [past_item]}}} =
             JSON.decode_line(past.body <> "\n")

    assert past_item["due_in_minutes"] == -1

    deadline = item(2, "", %{due: @now + 3_600_000})
    assert JevRanking.deadline_class(deadline, @now - 1) == 3
    assert JevRanking.deadline_class(deadline, @now) == 2
    assert JevRanking.next_deadline([deadline], @now - 1) == @now
  end

  test "requests stay within both budgets, large items are unsent, and clipped items remain ordered" do
    items = Enum.map(1..210, &item(&1, String.duplicate("x", 1_200)))
    oversized = item(211, String.duplicate("z", 40_000), %{urgency: "now"})
    parent = self()
    result = JevRanking.run([oversized | items], %{}, @endpoint, @model, @now, scoring(parent))

    assert length(result.considered) > 0
    assert length(result.clipped) > 0
    assert Enum.any?(result.clipped, &(&1.identity == oversized.identity))
    assert length(result.considered) + length(result.clipped) == length(items) + 1

    requests = receive_requests([])
    assert length(requests) == result.sent

    assert Enum.all?(requests, fn request ->
             byte_size(request.body) <= 48_000 and
               request.state_bytes + request.largest_question_bytes <= 24_000 and
               oversized.identity not in request.candidates
           end)

    entries =
      Placement.place(
        "jev-set",
        [oversized | items],
        [],
        {:ranked, result.scores, result.overflow}
      )

    assert length(entries) == length(items) + 1
    assert Enum.any?(entries, &(&1["message_id"] == "211"))
  end

  test "unchanged batches are reused, a revision or departure re-scores only its batch" do
    items = Enum.map(1..110, &item(&1, String.duplicate("x", 650)))
    first = JevRanking.run(items, %{}, @endpoint, @model, @now, scoring())
    assert length(first.cache.batches) > 1

    quiet = JevRanking.run(items, first.cache, @endpoint, @model, @now + 1_000, scoring())
    assert quiet.sent == 0

    changed =
      Enum.map(items, fn item ->
        if item.identity == {"sender", "1"}, do: %{item | revision: 2}, else: item
      end)

    revised = JevRanking.run(changed, first.cache, @endpoint, @model, @now, scoring())
    assert revised.sent == 1

    departed = JevRanking.run(tl(items), first.cache, @endpoint, @model, @now, scoring())
    assert departed.sent == 1
  end

  test "deadline class change re-scores one batch and finals cap high and now together" do
    due = @now + 3_600_000

    items =
      Enum.map(1..110, fn n ->
        item(n, String.duplicate("x", 650), if(n == 1, do: %{due: due}, else: %{}))
      end)

    first = JevRanking.run(items, %{}, @endpoint, @model, @now - 1, scoring(nil, 2.0))
    assert first.sent > 1
    assert first.finals_sent
    assert MapSet.size(first.overflow) > 0

    unchanged = JevRanking.run(items, first.cache, @endpoint, @model, @now - 1, scoring(nil, 2.0))
    assert unchanged.sent == 0

    changed = JevRanking.run(items, first.cache, @endpoint, @model, @now, scoring(nil, 2.0))
    assert changed.sent >= 1

    entries = Placement.place("jev-set", items, [], {:ranked, changed.scores, changed.overflow})
    assert Enum.count(entries, &(&1["priority"] in ["high", "now"])) == 5
    assert Enum.at(entries, 5)["priority"] == "normal"

    baseline =
      Placement.place(
        "baseline",
        Enum.map(items, fn item -> put_in(item.message["urgency"], "now") end),
        [],
        nil
      )

    assert Enum.count(baseline, &(&1["priority"] == "now")) == 110
  end

  test "failed requests keep successful batch scores and recover without rescoring them" do
    items = Enum.map(1..110, &item(&1, String.duplicate("x", 650)))
    failed_id = {"sender", "1"}

    failing = fn request ->
      if failed_id in request.candidates,
        do: {:error, :unavailable},
        else: {:ok, Map.new(request.candidates, &{&1, 1.0})}
    end

    first = JevRanking.run(items, %{}, @endpoint, @model, @now, failing)
    assert first.failed == 1
    assert map_size(first.scores) > 0
    assert length(Enum.filter(first.cache.batches, &is_nil(&1.scores))) == 1

    next = JevRanking.run(items, first.cache, @endpoint, @model, @now, scoring())
    assert next.sent == 1
    assert next.failed == 0
    assert map_size(next.scores) == length(items)
  end

  test "a cross-ID successor takes its origin's batch when it fits" do
    items = Enum.map(1..110, &item(&1, String.duplicate("x", 650)))
    first = JevRanking.run(items, %{}, @endpoint, @model, @now, scoring())
    origin = hd(items)
    origin_batch = Enum.find(first.cache.batches, &(origin.identity in &1.members)).id
    successor = item(999, String.duplicate("x", 650), %{origin_target: origin.identity})

    next =
      JevRanking.run([successor | tl(items)], first.cache, @endpoint, @model, @now, scoring())

    assert Enum.find(next.cache.batches, &(successor.identity in &1.members)).id == origin_batch
    assert next.sent == 1
  end

  test "grouping follows the capped engine order without restoring demoted priority" do
    items = Enum.map(1..6, &item(&1, ""))

    items =
      Enum.map(items, fn item ->
        if item.identity in [{"sender", "1"}, {"sender", "6"}],
          do: put_in(item.message["thread"], "same-thread"),
          else: item
      end)

    scores = Map.new(items, &{&1.identity, 2.0})
    ordered = Placement.place("jev-set", items, [], {:ranked, scores, MapSet.new()})
    assert Enum.count(ordered, &(&1["priority"] == "high")) == 5
    assert Enum.at(ordered, 5)["priority"] == "normal"

    shown = Placement.grouped(ordered)
    assert Enum.at(shown, 0)["message_id"] == "6"
    assert Enum.at(shown, 1)["message_id"] == "1"
    assert Enum.at(shown, 1)["priority"] == "normal"
  end

  test "first round requests are in flight together before finals" do
    items = Enum.map(1..110, &item(&1, String.duplicate("x", 650)))

    batch_count =
      length(JevRanking.run(items, %{}, @endpoint, @model, @now, scoring()).cache.batches)

    parent = self()

    evaluator = fn request ->
      send(parent, {:entered, self(), request})

      receive do
        :release -> {:ok, Map.new(request.candidates, &{&1, 2.0})}
      end
    end

    task = Task.async(fn -> JevRanking.run(items, %{}, @endpoint, @model, @now, evaluator) end)

    first_round =
      for _ <- 1..batch_count do
        assert_receive {:entered, pid, request}, 2_000
        {pid, request}
      end

    assert length(Enum.uniq(Enum.map(first_round, &elem(&1, 0)))) == batch_count
    Enum.each(first_round, fn {pid, _request} -> send(pid, :release) end)
    assert_receive {:entered, final_pid, _final_request}, 2_000
    send(final_pid, :release)
    assert Task.await(task, 2_000).finals_sent
  end

  defp receive_requests(acc) do
    receive do
      {:request, _pid, request} -> receive_requests([request | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end

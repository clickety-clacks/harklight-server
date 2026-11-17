# Run only on an authorized test host with approved Jev access and synthetic data.
# Endpoint/model/key path come from the environment; JevClient owns authentication.
alias AttentionServer.{Contradictions, JevClient, JSON}
endpoint = System.fetch_env!("HARKLIGHT_CONTRADICTION_ENDPOINT")
model = System.fetch_env!("HARKLIGHT_CONTRADICTION_MODEL")
_ = System.fetch_env!("ATTENTION_SERVER_JEV_KEY_PATH")

posted = %{
  "title" => "Hold beta release",
  "body" =>
    "Do not publish the beta until the security audit has completed. The audit is still in progress."
}

labels = [
  {true, "Publish beta now",
   "Publish the beta immediately, before the security audit completes."},
  {true, "Skip audit wait",
   "Ship the beta today even if the security audit is still in progress."},
  {true, "Approve immediate beta",
   "Approve immediate public beta release without waiting for audit completion."},
  {true, "Release before audit", "The beta must be public before the security audit finishes."},
  {false, "Wait for audit", "Keep the beta private until the audit is complete."},
  {false, "Prepare release notes",
   "Draft the beta release notes while the audit runs; do not publish the beta yet."},
  {false, "Audit report",
   "Please finish the security audit report so we can consider the beta release."},
  {false, "Private beta testing",
   "Run internal beta tests during the audit without publishing anything."},
  {false, "Schedule audit review",
   "Schedule a meeting to review the completed audit before beta release."},
  {false, "Staging checks", "Check the staging deployment while the security audit runs."},
  {false, "Prepare rollback", "Write the beta rollback procedure before the audit completes."},
  {false, "Keep download private", "Keep the beta download private pending the audit results."},
  {false, "Renew certificate", "Renew the TLS certificate for the documentation site."},
  {false, "Office supplies", "Order printer paper for the office."},
  {false, "Meeting room", "Reserve a meeting room for next week's planning session."},
  {false, "Update contact", "Update the support contact address in the internal directory."}
]

items =
  labels
  |> Enum.with_index(1)
  |> Enum.map(fn {{_, title, body}, n} ->
    %{
      identity: {"app:synthetic", "comparison-#{n}"},
      accepted_seq: n,
      message: %{"title" => title, "body" => body}
    }
  end)

positive =
  labels
  |> Enum.with_index(1)
  |> Enum.filter(fn {{yes, _, _}, _} -> yes end)
  |> MapSet.new(fn {_, n} -> {"app:synthetic", "comparison-#{n}"} end)

results =
  for size <- [1, 8, 16] do
    repeats =
      for _ <- 1..3 do
        requests = Contradictions.batches(posted, items, endpoint, model, size)
        started = System.monotonic_time(:microsecond)

        scores =
          requests
          |> Task.async_stream(&JevClient.evaluate/1,
            max_concurrency: length(requests),
            timeout: :infinity
          )
          |> Enum.reduce(%{}, fn
            {:ok, {:ok, scores}}, acc -> Map.merge(acc, scores)
            _, acc -> acc
          end)

        elapsed = (System.monotonic_time(:microsecond) - started) / 1_000

        found =
          scores
          |> Enum.filter(fn {_, value} -> is_number(value) and value >= 0.75 end)
          |> MapSet.new(fn {id, _} -> id end)

        %{
          calls: length(requests),
          milliseconds: elapsed,
          found: MapSet.size(MapSet.intersection(positive, found)),
          false_warnings: MapSet.size(MapSet.difference(found, positive)),
          usable: map_size(scores)
        }
      end

    times = repeats |> Enum.map(& &1.milliseconds) |> Enum.sort()
    %{size: size, repeats: repeats, median_ms: Enum.at(times, 1), worst_ms: Enum.max(times)}
  end

best_found =
  results |> Enum.map(fn row -> Enum.sum(Enum.map(row.repeats, & &1.found)) end) |> Enum.max()

best_false =
  results
  |> Enum.map(fn row -> Enum.sum(Enum.map(row.repeats, & &1.false_warnings)) end)
  |> Enum.min()

chosen =
  results
  |> Enum.filter(fn row ->
    Enum.sum(Enum.map(row.repeats, & &1.found)) == best_found and
      Enum.sum(Enum.map(row.repeats, & &1.false_warnings)) == best_false and
      Enum.all?(row.repeats, &(&1.usable == length(items))) and row.worst_ms <= 5_000
  end)
  |> Enum.map(& &1.size)
  |> Enum.max(fn -> nil end)

{:ok, example} = Contradictions.compose(posted, [hd(items)], endpoint, model)
{:ok, question} = JSON.decode_line(example.body <> "\n")

IO.puts(
  JSON.encode!(%{
    chosen_size: chosen,
    owner_decision_required: is_nil(chosen),
    posted: posted,
    labels:
      Enum.map(Enum.zip(items, labels), fn {item, {positive, _, _}} ->
        %{identity: Tuple.to_list(item.identity), message: item.message, contradicts: positive}
      end),
    questions: question["questions"],
    threshold_normalized: 0.75,
    answer_schema: %{
      "answers" => %{
        "candidate_1" => %{"type" => "score", "score" => "number 0 through 4 inclusive"}
      }
    },
    measurements: results
  })
)

# Run with release eval, in an isolated resource budget with fresh synthetic data.
alias AttentionServer.{CommitProfile, Relay}
root = System.fetch_env!("HARKLIGHT_FIXTURE_DIR")
:ok = File.mkdir(root)
{:ok, _} = Application.ensure_all_started(:attention_server)

send_band = fn relay, offset ->
  started = System.monotonic_time(:microsecond)

  for n <- (offset + 1)..(offset + 100) do
    key = "00000000-0000-4000-8000-" <> String.pad_leading(to_string(n), 12, "0")

    body = %{
      "sender" => "app:synthetic",
      "session" => "00000000-0000-4000-8000-000000000001",
      "message" => %{"created_at" => n, "title" => "Synthetic profile #{n}"}
    }

    principal = %{kind: :device, person: "synthetic-person", key: "synthetic-origin"}
    digest = :crypto.hash(:sha256, AttentionServer.JSON.encode!(body))
    input = {principal, :send, %{}, key, digest, [], body}

    {:ok, 200, _, false} =
      GenServer.call(
        relay,
        {:client_api, AttentionServer.ClientAPI.AgentWrites, {:execute, input, key, []}},
        :infinity
      )
  end

  System.monotonic_time(:microsecond) - started
end

runs =
  for round <- 1..3, enabled <- if(rem(round, 2) == 1, do: [false, true], else: [true, false]) do
    {:ok, relay} = Relay.start_link(store_path: Path.join(root, "#{round}-#{enabled}.sqlite3"))

    try do
      :ok = Relay.setup_person(relay, "synthetic-person")
      :ok = Relay.setup_device(relay, "synthetic-person", "synthetic-origin")
      :ok = Relay.setup_sender(relay, "synthetic-origin", "app:synthetic")
      send_band.(relay, 0)

      capture =
        Task.async(fn -> CommitProfile.capture(relay, enabled: enabled, duration_ms: 5_000) end)

      # Keep startup outside the timed band. Coverage below rejects an incomplete capture.
      Process.sleep(100)
      elapsed = send_band.(relay, 100)

      report =
        case Task.await(capture, 7_000) do
          {:error, :disabled} when not enabled ->
            nil

          {:ok, report} when enabled ->
            calls =
              Enum.find(
                report.timers,
                &(&1.function == "Elixir.AttentionServer.Store.Persistence.run/1")
              )

            if calls.calls < 100 or elapsed >= 4_800_000,
              do:
                raise(
                  "profile window did not cover the full send band; use a larger isolated budget"
                )

            report
        end

      %{round: round, enabled: enabled, elapsed_us: elapsed, sends: 100, profile: report}
    after
      GenServer.stop(relay)
    end
  end

File.write!(
  Path.join(root, "profile.json"),
  JSON.encode!(%{
    schema: 1,
    runs: runs,
    workload:
      "100 warm-up sends, then 100 measured sends per fresh one-person store; alternating paired runs",
    limitation:
      "Small matched diagnostics overhead smoke; not the 200-organization/stream capacity benchmark."
  })
)

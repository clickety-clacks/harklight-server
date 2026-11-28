# CI-only source generator executed by the built package, never a live Server.
alias AttentionServer.{CompatibilityScenarios, JSON, Relay}
code = System.fetch_env!("HARKLIGHT_FIXTURE_CODE")
Code.require_file(Path.join(code, "tls_fixtures.ex"))
Code.require_file(Path.join(code, "scenarios.exs"))
{:ok, _} = Application.ensure_all_started(:attention_server)
root = System.fetch_env!("HARKLIGHT_FIXTURE_DIR")
count = String.to_integer(System.fetch_env!("HARKLIGHT_REHEARSAL_MESSAGES"))
true = count in 1..1000
fixture = CompatibilityScenarios.fixture(root)
{:ok, relay} = CompatibilityScenarios.start(fixture)

try do
  # A separate synthetic account leaves every existing schema-2 observation
  # exact while adding a measurable retained history to the same stopped store.
  person = "synthetic-profile-person"
  origin = "synthetic-profile-origin"
  :ok = Relay.setup_person(relay, person)
  :ok = Relay.setup_device(relay, person, origin)
  :ok = Relay.setup_sender(relay, origin, "app:synthetic-profile")
  payload = String.duplicate("Synthetic retained text. ", 342)

  for n <- 1..count do
    key = "10000000-0000-4000-8000-" <> String.pad_leading(to_string(n), 12, "0")
    body = %{"sender" => "app:synthetic-profile",
      "session" => "10000000-0000-4000-8000-000000000001",
      "message" => %{"created_at" => n, "title" => "Retained history #{n}", "text" => payload}}
    principal = %{kind: :device, person: person, key: origin}
    # Match AgentWrites.write_digest/3 at the HTTP boundary. Passing raw hash
    # bytes to the internal execute call creates a receipt no HTTP request can
    # produce and which the importer correctly refuses.
    digest = AttentionServer.ClientAPI.Idempotency.digest(
      "POST", ["v2", "agent", "messages"], %{"body" => body, "if_match" => []})
    {:ok, <<_::256>>} = Base.decode16(digest, case: :lower)
    input = {principal, :send, %{}, key, digest, [], body}
    {:ok, 200, _, false} = GenServer.call(relay,
      {:client_api, AttentionServer.ClientAPI.AgentWrites, {:execute, input, key, []}}, :infinity)
  end

  manifest = CompatibilityScenarios.run(relay, fixture)
  :ok = CompatibilityScenarios.verify(relay, fixture, manifest)
  File.write!(Path.join(root, "manifest.json"), JSON.encode!(manifest))
  IO.puts(JSON.encode!(%{"result" => "pass", "manifest_schema" => 2,
    "additional_messages" => count, "text_bytes_per_message" => byte_size(payload)}))
after
  GenServer.stop(relay)
  Agent.stop(fixture.clock)
end

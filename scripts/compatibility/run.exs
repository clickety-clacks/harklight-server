alias AttentionServer.{CompatibilityScenarios, JSON}
code = System.fetch_env!("HARKLIGHT_FIXTURE_CODE")
Code.require_file(Path.join(code, "tls_fixtures.ex"))
Code.require_file(Path.join(code, "scenarios.exs"))
{:ok, _} = Application.ensure_all_started(:attention_server)
root = System.fetch_env!("HARKLIGHT_FIXTURE_DIR")
mode = System.fetch_env!("HARKLIGHT_FIXTURE_MODE")

{fixture, manifest} =
  if mode == "seed" do
    {CompatibilityScenarios.fixture(root), nil}
  else
    {:ok, %{"schema" => 1, "seed" => "compatibility-v1"} = manifest} =
      JSON.decode_line(File.read!(Path.join(root, "manifest.json")) <> "\n")

    {CompatibilityScenarios.open_fixture(root, manifest["clock_ms"]), manifest}
  end

{:ok, relay} = CompatibilityScenarios.start(fixture)

try do
  case mode do
    "seed" ->
      manifest = CompatibilityScenarios.run(relay, fixture)
      :ok = CompatibilityScenarios.verify(relay, fixture, manifest)
      File.write!(Path.join(root, "manifest.json"), JSON.encode!(manifest))

    "verify" ->
      :ok = CompatibilityScenarios.verify(relay, fixture, manifest)

    "expire" ->
      :ok = CompatibilityScenarios.expire(relay, fixture, manifest)
  end

  IO.puts(JSON.encode!(%{"scenario" => "compatibility-v1", "mode" => mode, "result" => "pass"}))
after
  GenServer.stop(relay)
  Agent.stop(fixture.clock)
end

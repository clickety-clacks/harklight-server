Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

defmodule AttentionServer.CompatibilityScenariosTest do
  use ExUnit.Case, async: false
  alias AttentionServer.CompatibilityScenarios, as: Scenarios

  @tag timeout: 60_000
  test "portable API manifest survives restart, retries, and cursor retention" do
    root = Path.join(System.tmp_dir!(), "compatibility-#{System.unique_integer([:positive])}")
    fixture = Scenarios.fixture(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, relay} = Scenarios.start(fixture)

    manifest =
      try do
        manifest = Scenarios.run(relay, fixture)
        assert :ok = Scenarios.verify(relay, fixture, manifest)

        corrupted =
          Map.update!(manifest, "observations", fn rows ->
            Enum.map(rows, fn row ->
              if is_map(row["body"]["item"]),
                do: put_in(row, ["body", "item", "message", "title"], "incorrect content"),
                else: row
            end)
          end)

        assert_raise RuntimeError, ~r/\.item\.message\.title/, fn ->
          Scenarios.verify(relay, fixture, corrupted)
        end

        manifest
      after
        GenServer.stop(relay)
      end

    # Serialize the transport manifest so no tuple/atom identity is assumed.
    encoded = AttentionServer.JSON.encode!(manifest)
    {:ok, manifest} = AttentionServer.JSON.decode_line(encoded <> "\n")
    {:ok, reopened} = Scenarios.start(fixture)

    try do
      assert :ok = Scenarios.verify(reopened, fixture, manifest)
      assert :ok = Scenarios.expire(reopened, fixture, manifest)
    after
      GenServer.stop(reopened)
      Agent.stop(fixture.clock)
    end
  end
end

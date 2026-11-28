Code.require_file("../support/migration_fixtures.ex", __DIR__)
Code.require_file("../scripts/compatibility/scenarios.exs", __DIR__)

defmodule AttentionServer.Migration.HistoryInventoryTest do
  use ExUnit.Case, async: false
  alias AttentionServer.CompatibilityScenarios, as: Scenarios
  alias AttentionServer.Migration.Format32

  @tag timeout: 60_000
  test "inventories M1 history without changing saved cursors, receipts, or resources" do
    root = Path.join(System.tmp_dir!(), "migration-history-#{System.unique_integer([:positive])}")
    fixture = Scenarios.fixture(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, relay} = Scenarios.start(fixture)

    manifest =
      try do
        Scenarios.run(relay, fixture)
      after
        GenServer.stop(relay)
      end

    path = Path.join(root, "relay.sqlite3")

    assert {:ok, inventory} =
             Format32.preflight(path,
               provenance: AttentionServer.MigrationFixtures.provenance(path),
               scratch_root: root
             )

    assert inventory.tables["client_changes"].count > 0
    assert inventory.tables["agent_changes"].count > 0
    assert inventory.tables["client_idempotency"].count > 0
    {:ok, reopened} = Scenarios.start(fixture)

    try do
      assert :ok = Scenarios.verify(reopened, fixture, manifest)
    after
      GenServer.stop(reopened)
      Agent.stop(fixture.clock)
    end
  end
end

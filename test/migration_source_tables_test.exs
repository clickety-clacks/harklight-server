defmodule AttentionServer.Migration.SourceTablesTest do
  use ExUnit.Case, async: true
  alias AttentionServer.Migration.{SourceSchema, SourceTables}

  test "every admitted FORMAT32 table has a deliberate destination or exclusion" do
    for {table, columns} <- SourceSchema.tables() do
      case SourceTables.disposition(table) do
        {:durable, keys} ->
          assert keys != []
          assert Enum.all?(keys, &(&1 in columns))
        other -> assert other in [:derived, :source_only]
      end
    end
    assert {:error, :unsupported_source_table} = SourceTables.disposition("future_table")
  end

  test "SQL identity and typed values survive without ordinal or text conversion" do
    hash = <<0, 255, 127, 0>>
    assert {:ok, {"sqlite", "harklight_browser_sessions", [^hash]}, row} =
      SourceTables.fact("harklight_browser_sessions", [hash, "person", "owner", 7, "fixture"])
    assert row == %{"session_hash" => hash, "person_id" => "person", "owner_key" => "owner",
      "owner_revision" => 7, "issuer" => "fixture"}
    assert {:ok, _, nullable} = SourceTables.fact("agent_changes", ["person", "agent", "resource", 4, nil, nil])
    assert nullable["session"] == nil and nullable["at_ms"] == nil
    assert {:ok, {"sqlite", "relay_inbox_snapshots", ["person", "device", "startup"]}, _} =
      SourceTables.fact("relay_inbox_snapshots",
        ["person", "device", "startup", "snapshot", 1, 9, "ready", 2, 3, 4, hash, 0, 0])
    assert {:error, :missing_source_table_key} =
      SourceTables.fact("harklight_browser_sessions", [nil, "person", "owner", 7, nil])
    assert {:error, :unsupported_source_table_row} = SourceTables.fact("relay_items", [])
    assert {:error, :unsupported_source_table_row} = SourceTables.fact("client_changes", ["short"])
  end
end

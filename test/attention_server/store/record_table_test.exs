defmodule AttentionServer.Store.RecordTableTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{AgentChangeLog, ChangeLog, Engine, Store.RecordTable}
  alias Exqlite.Sqlite3

  @item {"person", "sender", "item"}

  setup do
    directory = Path.join(System.tmp_dir!(), "record-table-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    {:ok, db} = Sqlite3.open(Path.join(directory, "relay.sqlite3"))

    on_exit(fn ->
      Sqlite3.close(db)
      File.rm_rf!(directory)
    end)

    assert :ok = Sqlite3.execute(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL")
    assert :ok = RecordTable.create(db, 9)
    assert :ok = ChangeLog.enable(db, 9)
    assert :ok = AgentChangeLog.enable(db, Engine.new(), 9)
    assert :ok = Sqlite3.execute(db, "CREATE TABLE audit (seq INTEGER PRIMARY KEY)")
    %{db: db}
  end

  test "commits changed records, audit, and real person and agent log writes", %{db: db} do
    old = Engine.new()
    next = put_in(old.messages[@item], %{origin: "agent", session_handle: "session"})

    records = [{:put, {"key", "value", "hash"}}]
    person_heads = %{"person" => 1}
    agent_heads = %{{:agent, "person", "agent"} => 1}

    assert {:ok, {^person_heads, ^agent_heads}} =
             RecordTable.commit(db, 9, records, appends(old, next))

    assert {:ok, [{"key", "value", "hash"}]} = RecordTable.rows(db)
    assert {:ok, [[10]]} = query(db, "SELECT seq FROM audit")
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM client_changes")
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM agent_changes")
    assert_versions(db, 10)

    assert {:ok, {%{}, %{}}} = RecordTable.commit(db, 10, [{:delete, "key"}], appends(next, next))
    assert {:ok, []} = RecordTable.rows(db)
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM client_changes")
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM agent_changes")
    assert_versions(db, 11)
  end

  test "rejects a stale generation without running side writes", %{db: db} do
    assert {:error, :stale_generation} =
             RecordTable.commit(db, 8, [{:put, {"missed", "row", "hash"}}], fn _, _ ->
               flunk("side writes ran on a stale generation")
             end)

    assert_versions(db, 9)
    assert {:ok, []} = RecordTable.rows(db)
  end

  test "a real agent append failure rolls back the person append and other writes", %{db: db} do
    assert :ok =
             Sqlite3.execute(db, """
             CREATE TRIGGER fail_agent BEFORE INSERT ON agent_changes
             BEGIN SELECT RAISE(ABORT, 'write failed'); END
             """)

    old = Engine.new()
    next = put_in(old.messages[@item], %{origin: "agent", session_handle: "session"})

    assert {:error, _} =
             RecordTable.commit(db, 9, [{:put, {"key", "pending", "hash"}}], appends(old, next))

    assert_versions(db, 9)
    assert {:ok, []} = RecordTable.rows(db)
    assert {:ok, []} = query(db, "SELECT rev FROM client_changes")
    assert {:ok, []} = query(db, "SELECT rev FROM agent_changes")
    assert {:ok, []} = query(db, "SELECT seq FROM audit")
  end

  test "bare SQL success produces an error and rolls back", %{db: db} do
    assert {:error, :invalid_append_result} =
             RecordTable.commit(db, 9, [{:put, {"key", "pending", "hash"}}], fn db, _ ->
               Sqlite3.execute(db, "INSERT INTO audit VALUES (10)")
             end)

    assert {:ok, []} = RecordTable.rows(db)
    assert {:ok, []} = query(db, "SELECT seq FROM audit")
    assert_versions(db, 9)
  end

  defp appends(old, next) do
    changed = ChangeLog.changed_resources(old, next)
    person = ChangeLog.changes(old, next, changed)
    agent = AgentChangeLog.changes(old, next, changed)
    created = ChangeLog.new_resources(old, next, changed)

    fn db, version ->
      with :ok <- Sqlite3.execute(db, "INSERT INTO audit VALUES (#{version})"),
           {:ok, person_heads} <- ChangeLog.append(db, person, %{}, version, created),
           {:ok, agent_heads} <- AgentChangeLog.append(db, agent, version) do
        {:ok, {person_heads, agent_heads}}
      end
    end
  end

  defp assert_versions(db, version) do
    assert {:ok, ^version} = RecordTable.generation(db)
    assert {:ok, [[^version]]} = query(db, "SELECT store_version FROM client_change_log_state")
    assert {:ok, [[^version]]} = query(db, "SELECT store_version FROM agent_change_log_state")
  end

  defp query(db, sql) do
    {:ok, stmt} = Sqlite3.prepare(db, sql)

    try do
      Sqlite3.fetch_all(db, stmt)
    after
      Sqlite3.release(db, stmt)
    end
  end
end

defmodule AttentionServer.Store.RecordTableTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Store.RecordTable
  alias Exqlite.Sqlite3

  setup do
    {:ok, db} = Sqlite3.open(":memory:")
    on_exit(fn -> Sqlite3.close(db) end)
    assert :ok = RecordTable.create(db, 9)
    assert :ok = Sqlite3.execute(db, "CREATE TABLE person_log (rev INTEGER PRIMARY KEY)")
    assert :ok = Sqlite3.execute(db, "CREATE TABLE agent_log (rev INTEGER PRIMARY KEY)")
    assert :ok = Sqlite3.execute(db, "CREATE TABLE audit (seq INTEGER PRIMARY KEY)")
    %{db: db}
  end

  test "commits rows, generation and all side writes together", %{db: db} do
    assert {:ok, nil} =
             RecordTable.commit(db, 9, [{:put, {"key", "old", "hash"}}], fn -> {:ok, nil} end)

    assert {:ok, %{person: 1, agent: 1}} =
             RecordTable.commit(db, 10, [{:delete, "key"}, {:put, {"new", "row", "hash"}}], fn ->
               with :ok <-
                      Sqlite3.execute(db, """
                      INSERT INTO person_log VALUES (1);
                      INSERT INTO agent_log VALUES (1);
                      INSERT INTO audit VALUES (1);
                      """) do
                 {:ok, %{person: 1, agent: 1}}
               end
             end)

    assert {:ok, 11} = RecordTable.generation(db)
    assert {:ok, [{"new", "row", "hash"}]} = RecordTable.rows(db)
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM person_log")
    assert {:ok, [[1]]} = query(db, "SELECT rev FROM agent_log")
    assert {:ok, [[1]]} = query(db, "SELECT seq FROM audit")
  end

  test "rejects a stale generation without running side writes", %{db: db} do
    assert {:error, :stale_generation} =
             RecordTable.commit(db, 8, [{:put, {"missed", "row", "hash"}}], fn ->
               flunk("side writes ran on a stale generation")
             end)

    assert {:ok, 9} = RecordTable.generation(db)
    assert {:ok, []} = RecordTable.rows(db)
  end

  test "a failed agent append rolls back earlier person, audit and record writes", %{db: db} do
    assert :ok =
             Sqlite3.execute(db, """
             CREATE TRIGGER fail_agent BEFORE INSERT ON agent_log
             BEGIN SELECT RAISE(ABORT, 'write failed'); END
             """)

    assert {:error, _} =
             RecordTable.commit(db, 9, [{:put, {"key", "pending", "hash"}}], fn ->
               Sqlite3.execute(db, """
               INSERT INTO audit VALUES (1);
               INSERT INTO person_log VALUES (1);
               INSERT INTO agent_log VALUES (1);
               """)
             end)

    assert {:ok, 9} = RecordTable.generation(db)
    assert {:ok, []} = RecordTable.rows(db)
    assert {:ok, []} = query(db, "SELECT rev FROM person_log")
    assert {:ok, []} = query(db, "SELECT seq FROM audit")
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

defmodule AttentionServer.Store.Format31RecordWriteTest do
  use ExUnit.Case, async: true

  alias AttentionServer.Store.{RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    {:ok, db} = Sqlite3.open(Path.join(directory, "rows.sqlite3"))
    on_exit(fn -> Sqlite3.close(db) end)

    old = RecordCodec.encode_row({:field, :pending_clock_high_water}, 7)
    assert {:ok, :seeded} =
             RecordTable.transaction(db, fn ->
               with :ok <- RecordTable.seed(db, 1, [old]), do: {:ok, :seeded}
             end)

    %{db: db, old: old}
  end

  test "an attempted duplicate insert rolls back earlier records and the generation", %{
    db: db,
    old: old
  } do
    unrelated = RecordCodec.encode_row({:field, :browser_sign_in_clock_high_water}, 12)
    duplicate = RecordCodec.encode_row({:field, :pending_clock_high_water}, 8)

    assert {:error, _} =
             RecordTable.commit(db, 1, [{:insert, unrelated}, {:insert, duplicate}], fn _, _ ->
               flunk("must not publish a change log after a duplicate insert")
             end)

    assert {:ok, 1} = RecordTable.generation(db)
    assert {:ok, [^old]} = RecordTable.rows(db)
  end

  test "updating a row missing from the before-image never creates it", %{db: db, old: old} do
    missing = RecordCodec.encode_row({:field, :browser_sign_in_clock_high_water}, 12)

    assert {:error, _} =
             RecordTable.commit(db, 1, [{:update, missing}], fn _, _ ->
               flunk("must not publish a change log for a missing row")
             end)

    assert {:ok, 1} = RecordTable.generation(db)
    assert {:ok, [^old]} = RecordTable.rows(db)
  end

  test "an existing record update and a new record insert advance together", %{db: db} do
    update = RecordCodec.encode_row({:field, :pending_clock_high_water}, 8)
    insert = RecordCodec.encode_row({:field, :browser_sign_in_clock_high_water}, 12)

    assert {:ok, :published} =
             RecordTable.commit(db, 1, [{:update, update}, {:insert, insert}], fn ^db, 2 ->
               {:ok, :published}
             end)

    assert {:ok, 2} = RecordTable.generation(db)
    assert {:ok, rows} = RecordTable.rows(db)
    assert Enum.sort(rows) == Enum.sort([update, insert])
  end
end

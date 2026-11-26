defmodule AttentionServer.Store.AuthSchemaTest do
  use ExUnit.Case, async: true

  alias AttentionServer.ClientAPI.Idempotency
  alias AttentionServer.Store
  alias AttentionServer.Store.{AuthSchema, RecordTable}
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir
  @person "person"
  @key "key"
  @digest "digest"

  setup %{tmp_dir: directory} do
    path = Path.join(directory, "auth.sqlite3")
    {:ok, db} = Sqlite3.open(path)
    on_exit(fn -> Sqlite3.close(db) end)
    %{db: db, path: path}
  end

  test "imports all remaining person receipts once and leaves the agent policy intact",
       %{db: db, path: path} do
    agent = Idempotency.agent_scope(@person, "agent-key")
    store = %Store{db: db, change_log: true}
    assert :ok = Idempotency.save(store, @person, @key, @digest, 201, %{"item" => "one"})
    assert :ok = Idempotency.save(store, agent, @key, @digest, 201, %{"item" => "two"})
    assert :ok = Sqlite3.execute(db, "UPDATE client_idempotency SET at_ms = 1")
    assert {:ok, nil} = AuthSchema.read(db)

    assert {:ok, %{minimum_generation: 1, device_certificates_disabled: false}} =
             AuthSchema.initialize(db)

    assert :ok = Idempotency.save(store, @person, "later-browser", @digest, 200, %{})
    assert {:ok, nil} = Idempotency.lookup_encoded(store, agent, @key)
    assert {:ok, nil} = Idempotency.lookup_retained(db, agent, @key)

    {:ok, reopened} = Sqlite3.open(path)
    on_exit(fn -> Sqlite3.close(reopened) end)

    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(reopened)
    assert {:ok, nil} = Idempotency.lookup_retained(reopened, @person, "later-browser")

    assert {:ok, %{digest: @digest, status: 201, original_response: false, body: body}} =
             Idempotency.lookup_retained(reopened, @person, @key)

    assert AttentionServer.JSON.decode_line(body <> "\n") == {:ok, %{"item" => "one"}}
  end

  test "durable app receipts roll back with their write and preserve the original response",
       %{db: db} do
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(db)

    assert {:error, :write_failed} =
             RecordTable.transaction(db, fn ->
               :ok = Idempotency.save_retained(db, @person, @key, @digest, 201, %{})
               {:error, :write_failed}
             end)

    assert {:ok, nil} = Idempotency.lookup_retained(db, @person, @key)

    assert {:ok, :written} =
             RecordTable.transaction(db, fn ->
               :ok = Idempotency.save_retained(db, @person, @key, @digest, 201, %{"item" => "one"})
               {:ok, :written}
             end)

    assert :ok = Idempotency.save_retained(db, @person, @key, "other", 204, %{})

    assert {:ok, %{digest: @digest, status: 201, original_response: true, body: body}} =
             Idempotency.lookup_retained(db, @person, @key)

    assert AttentionServer.JSON.decode_line(body <> "\n") == {:ok, %{"item" => "one"}}
  end

  test "a failed import does not persist compatibility metadata", %{db: db} do
    assert :ok = Sqlite3.execute(db, "CREATE TABLE harklight_app_receipts (unexpected INTEGER)")
    assert {:error, _} = AuthSchema.initialize(db)
    assert {:ok, nil} = AuthSchema.read(db)
  end

  test "certificate denial survives initialization and an unsupported generation is refused",
       %{db: db} do
    assert {:ok, %{minimum_generation: 1}} = AuthSchema.initialize(db)

    assert :ok =
             Sqlite3.execute(db, "UPDATE harklight_auth_schema SET device_certificates_disabled = 1")

    assert {:ok, %{device_certificates_disabled: true}} = AuthSchema.initialize(db)

    assert :ok =
             Sqlite3.execute(db, "UPDATE harklight_auth_schema SET minimum_generation = 2")

    assert {:error, :auth_generation_unsupported} = AuthSchema.initialize(db)
  end
end

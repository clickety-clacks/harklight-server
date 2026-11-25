defmodule AttentionServer.Store.Format31RelayStartupTest do
  use ExUnit.Case, async: true

  alias AttentionServer.{Engine, JSON, Relay, Store}
  alias AttentionServer.AgentAPI.FeedActivity
  alias Exqlite.Sqlite3

  @moduletag :tmp_dir

  test "Relay starts from the converted row store and commits ordinary mutations", %{
    tmp_dir: directory
  } do
    path = Path.join(directory, "relay.sqlite3")

    assert {:ok, legacy} = Store.open(path, :legacy)
    assert {:ok, engine} = Engine.setup_person(legacy.engine, "person")
    assert {:ok, engine} = Engine.setup_device(engine, "person", "origin")
    audit = %{person: "person", actor: "origin", action: "test_fixture"}
    assert {:ok, legacy} = Store.commit(legacy, engine, audit)
    assert :ok = Store.close(legacy)

    assert {:ok, converted} = Store.open(path, :keyed_cutover)
    assert {:attention_relay_keyed_state, 31, _} = snapshot_anchor(converted)
    assert :ok = Store.close(converted)

    assert {:ok, relay} = Relay.start_link(store_path: path)
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
    assert :ok = GenServer.stop(relay)

    assert {:ok, cold} = Store.open(path, :keyed)
    assert cold.engine.sender_grants[{"person", "origin"}] == MapSet.new(["app:test"])
    assert {:attention_relay_keyed_state, 31, _} = snapshot_anchor(cold)
    assert :ok = Store.close(cold)
  end

  test "cutover moves published availability without changing its rendering", %{tmp_dir: dir} do
    path = Path.join(dir, "relay.sqlite3")
    assert {:ok, store} = Store.open(path, :legacy)
    assert {:ok, engine} = Engine.setup_person(store.engine, "person")
    assert {:ok, engine} = Engine.setup_device(engine, "person", "origin")
    assert {:ok, engine} = Engine.setup_sender(engine, "origin", "app:test")
    session = "00000000-0000-4000-8000-000000000001"

    frame = %{
      "v" => 1,
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000002",
      "session_handle" => session,
      "message" => %{
        "sender_id" => "app:test",
        "message_id" => "item",
        "created_at" => 1,
        "title" => "Fixture"
      }
    }

    {engine, {:ok, reply}} = Engine.request_frame(engine, "origin", frame, 1)
    assert {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(reply)
    key = {"person", "app:test", "item"}
    scope = {"person", "origin", session}

    engine =
      put_in(engine.origin_sessions[scope]["client_availability"], %{1 => {"listening", nil}})

    assert FeedActivity.item_availability(engine, key) == %{"state" => "listening"}

    audit = %{person: "person", actor: "origin", action: "test_fixture"}
    assert {:ok, store} = Store.commit(store, engine, audit)
    assert :ok = Store.close(store)
    assert {:ok, converted} = Store.open(path, :keyed_cutover)

    refute Map.has_key?(converted.engine.origin_sessions[scope], "client_availability")
    assert converted.engine.item_availability[key] == {"listening", nil}
    assert FeedActivity.item_availability(converted.engine, key) == %{"state" => "listening"}
    assert :ok = Store.close(converted)
  end

  defp snapshot_anchor(store) do
    assert {:ok, stmt} = Sqlite3.prepare(store.db, "SELECT payload FROM relay_state WHERE id = 1")

    try do
      assert {:row, [payload]} = Sqlite3.step(store.db, stmt)
      :erlang.binary_to_term(payload, [:safe])
    after
      Sqlite3.release(store.db, stmt)
    end
  end
end

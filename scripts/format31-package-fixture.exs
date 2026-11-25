alias AttentionServer.{Engine, JSON, Store}
alias Exqlite.Sqlite3

{:ok, _} = Application.ensure_all_started(:attention_server)
db_path = System.fetch_env!("HARKLIGHT_REHEARSAL_DB")
action = System.fetch_env!("HARKLIGHT_REHEARSAL_ACTION")
format = System.fetch_env!("HARKLIGHT_REHEARSAL_FORMAT") |> String.to_integer()
person = "fixture-person"
actor = "fixture-origin"
sender = "app:fixture"
item = "synthetic-package-message"
key = {person, sender, item}
title = "Retain this synthetic request"
session = "00000000-0000-4000-8000-000000000001"

if action == "seed" do
  {:ok, store} = Store.open(db_path)
  {:ok, engine} = Engine.setup_person(store.engine, person)
  {:ok, engine} = Engine.setup_device(engine, person, actor)
  {:ok, engine} = Engine.setup_sender(engine, actor, sender)
  frame = %{
    "v" => 1,
    "type" => "forward",
    "op_id" => "00000000-0000-4000-8000-000000000002",
    "session_handle" => session,
    "message" => %{
      "sender_id" => sender,
      "message_id" => item,
      "created_at" => 1,
      "title" => title,
      "body" => "Synthetic checkout and delivery status."
    }
  }

  {engine, {:ok, reply}} = Engine.request_frame(engine, actor, frame, 1)
  {:ok, %{"type" => "ok", "seq" => 1}} = JSON.decode_line(reply)
  audit = %{person: person, actor: actor, action: "release_rehearsal"}
  {:ok, store} = Store.commit(store, engine, audit)
  :ok = Store.close(store)
end

{:ok, db} = Sqlite3.open(db_path, mode: :readonly)
{:ok, statement} = Sqlite3.prepare(db, "SELECT payload, checksum FROM relay_state WHERE id = 1")
{:row, [payload, checksum]} = Sqlite3.step(db, statement)
true = :crypto.hash(:sha256, payload) == checksum
{identity, ^format, _state} = :erlang.binary_to_term(payload)
true = identity == if(format == 31, do: :attention_relay_keyed_state, else: :attention_relay_state)
:ok = Sqlite3.release(db, statement)
:ok = Sqlite3.close(db)

{:ok, store} = Store.open(db_path)
%{person: ^person, revoked: false} = store.engine.devices[actor]
true = MapSet.member?(store.engine.sender_grants[{person, actor}], sender)
%{accepted_seq: 1, session_handle: ^session, message: %{"title" => ^title}} =
  store.engine.messages[key]
:ok = Store.close(store)
IO.puts("format=#{format} synthetic message, device and grant verified")

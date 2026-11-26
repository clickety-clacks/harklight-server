alias AttentionServer.{Engine, Relay, Store}
alias Exqlite.Sqlite3

{:ok, _} = Application.ensure_all_started(:attention_server)
path = System.fetch_env!("FIXTURE_DB")
marker? = System.fetch_env!("FIXTURE_MARKER") == "present"
item = {"fixture-person", "app:fixture", "legacy"}
{person, sender, id} = item
actor = "fixture-origin"
accepted_at = 1_795_564_800_000

case System.fetch_env!("FIXTURE_ACTION") do
  "seed" ->
    {:ok, store} = Store.open(path)
    {:ok, engine} = Engine.setup_person(store.engine, person)
    {:ok, engine} = Engine.setup_device(engine, person, actor)
    {:ok, engine} = Engine.setup_sender(engine, actor, sender)

    frame = %{
      "type" => "forward",
      "op_id" => "00000000-0000-4000-8000-000000000001",
      "message" => %{
        "sender_id" => sender,
        "message_id" => id,
        "created_at" => 1,
        "title" => "Synthetic historical message",
        "answer_ui" => %{
          "schema" => "attention-client.actions/1",
          "actions" => [
            %{"id" => "seen", "label" => "Mark as read", "kind" => "value", "value" => "seen"}
          ]
        }
      }
    }

    {engine, {:ok, %{"seq" => 1}}} = Engine.request_command(engine, actor, frame, accepted_at)
    audit = %{person: person, actor: actor, action: "fixture_seed"}
    {:ok, store} = Store.commit(store, engine, audit)

    if marker? do
      {:ok, store} = Store.enable_change_log(store)
      {:ok, store} = Store.migrate_retained_client_data(store)
      :ok = Store.close(store)
    else
      :ok = Store.close(store)
    end

    {:ok, store} = Store.open(path)
    record = Map.drop(store.engine.messages[item], [
      :session_handle, :vendor_session_id, :revision, :revisions, :revision_accepted_at,
      :files_by_revision
    ])
    engine = %{
      store.engine
      | messages: Map.put(store.engine.messages, item, record),
        bounce_revisions: Map.delete(store.engine.bounce_revisions, item)
    }
    {:ok, store} = Store.commit(store, engine, audit)
    :ok = Store.close(store)

    {:ok, pre} = Store.open(path)
    ^record = pre.engine.messages[item]
    :ok = Store.close(pre)
    IO.puts("Published FORMAT30 reader accepts synthetic sessionless record. Marker present: #{marker?}.")

  "verify" ->
    {:ok, converted} = Store.open(path, :keyed)
    record = converted.engine.messages[item]
    %{"message_type" => "fyi", "title" => "Synthetic historical message"} = record.message
    false = Map.has_key?(record, :session_handle)
    false = Map.has_key?(record, :revision)
    %{} = converted.engine.origin_sessions
    0 = map_size(converted.engine.origin_sessions)

    {:ok, stmt} = Sqlite3.prepare(converted.db, "SELECT session FROM relay_items")
    {:row, [nil]} = Sqlite3.step(converted.db, stmt)
    :done = Sqlite3.step(converted.db, stmt)
    :ok = Sqlite3.release(converted.db, stmt)
    :ok = Store.close(converted)

    {:ok, relay} = Relay.start_link(store_path: path, clock: fn -> accepted_at + 8 * 86_400_000 end)
    reopened = :sys.get_state(relay).store.engine
    false = Map.has_key?(reopened.messages[item], :session_handle)
    0 = map_size(reopened.origin_sessions)
    :ok = GenServer.stop(relay)
    IO.puts("FORMAT31 preserves the sessionless synthetic message and starts the Relay. Marker present: #{marker?}.")
end

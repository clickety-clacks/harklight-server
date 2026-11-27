alias AttentionServer.{JSON, Relay}
alias AttentionServer.ClientAPI.{Feed, Reads, Resources}
alias Exqlite.Sqlite3

{:ok, _} = Application.ensure_all_started(:attention_server)
path = System.fetch_env!("FIXTURE_DB")
stage = System.fetch_env!("STAGE")
reader = "fixture-reader"
origin = "fixture-origin"
sender = "app:fixture"
session = "00000000-0000-4000-8000-000000000001"
{:ok, relay} = Relay.start_link(store_path: path)

request = fn actor, n, frame ->
  op = "00000000-0000-4000-8000-" <> String.pad_leading(Integer.to_string(n), 12, "0")
  raw = JSON.encode!(Map.merge(frame, %{"op_id" => op, "v" => 1})) <> "\n"
  {:ok, line} = Relay.request_line(relay, actor, raw)
  {:ok, %{"type" => "ok"} = result} = JSON.decode_line(line)
  result
end

owner_der = System.fetch_env!("OWNER_DER") |> File.read!()
{:ok, owner_key} = AttentionServer.DeviceIdentity.from_certificate(owner_der)

[person] = Map.keys(:sys.get_state(relay).store.engine.owners)

if System.fetch_env!("HISTORY_SEED") == "yes" do
  :ok = Relay.setup_device(relay, person, reader)
  :ok = Relay.setup_device(relay, person, origin)
  :ok = Relay.setup_sender(relay, origin, sender)
  for {id, n} <- [{"unanswered", 2}, {"answered", 3}, {"discussed", 4}] do
    request.(origin, n, %{
      "type" => "forward",
      "session_handle" => session,
      "message" => %{"sender_id" => sender, "message_id" => id, "created_at" => n,
                    "title" => "Synthetic #{id}", "body" => "History continuity"}
    })
  end
  request.(reader, 5, %{"type" => "discussion", "sender_id" => sender,
    "message_id" => "discussed", "request_revision" => 1, "text" => "Synthetic discussion"})
  request.(reader, 6, %{"type" => "answer", "sender_id" => sender,
    "message_id" => "answered", "answer" => "Synthetic answer"})
end

store = :sys.get_state(relay).store
engine = store.engine
items =
  for {{^person, ^sender, id} = key, record} <- Enum.sort(engine.messages) do
    facts = for {_, %{key: ^key, payload: payload}} <- Enum.sort(engine.conversation_events), do: payload
    %{"message_id" => id, "accepted_seq" => record.accepted_seq, "message" => record.message,
      "terminal" => if(record.terminal, do: Map.new(Map.update!(record.terminal, :actor_kind, &to_string/1), fn {k, v} -> {Atom.to_string(k), v} end)), "facts" => facts}
  end

projections =
  if stage == "0.1.49" do
    %{"legacy_events" => inspect(Relay.events(relay, reader, 0)),
      "legacy_conversation_events" => inspect(Relay.conversation_events(relay, reader, 0))}
  else
    browser = %{kind: :browser, person: person, key: owner_key, owner_revision: 1,
                session_id: "synthetic-browser"}
    {:ok, inbox} = Reads.inbox(relay, reader)
    {:ok, browser_inbox} = Reads.inbox(relay, browser)
    {:error, :unauthorized} = Reads.inbox(relay, %{browser | owner_revision: 2})
    item_reads = fn principal ->
      for {{^person, ^sender, id}, record} <- Enum.sort(engine.messages), into: %{} do
        item_id = Resources.item_id(record)
        {:ok, item} = Reads.item(relay, principal, item_id)
        {:ok, entries} = Reads.entries(relay, principal, item_id)
        {id, %{"id" => item_id, "item" => item, "entries" => entries}}
      end
    end
    feed =
      Enum.find([Feed, AttentionServer.ClientAPI.FeedStep], fn module ->
        Code.ensure_loaded?(module) and function_exported?(module, :read, 4)
      end)

    reader_principal = %{kind: :device, key: reader, person: person}
    %{"inbox" => inbox, "direct_item_reads" => item_reads.(reader),
      "browser_inbox" => browser_inbox, "browser_item_reads" => item_reads.(browser),
      "feed_from_zero" => inspect(feed.read(relay, reader_principal, 0, 20)),
      "feed_from_inbox" => inspect(feed.read(relay, reader_principal, inbox["rev"], 20))}
  end

:ok = GenServer.stop(relay)
{:ok, db} = Sqlite3.open(path, mode: :readonly)
{:ok, stmt} = Sqlite3.prepare(db, "SELECT payload FROM relay_state WHERE id=1")
{:row, [payload]} = Sqlite3.step(db, stmt)
{_, format, _} = :erlang.binary_to_term(payload)
:ok = Sqlite3.release(db, stmt)
:ok = Sqlite3.close(db)
File.write!(System.fetch_env!("INVENTORY"), JSON.encode!(%{
  "stage" => stage, "format" => format, "items" => items, "projections" => projections
}) <> "\n")

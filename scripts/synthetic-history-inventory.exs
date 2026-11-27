alias AttentionServer.{JSON, Relay}
alias AttentionServer.ClientAPI.{Reads, Resources}
alias Exqlite.Sqlite3

{:ok, _} = Application.ensure_all_started(:attention_server)
path = System.fetch_env!("FIXTURE_DB")
stage = System.fetch_env!("STAGE")
person = "fixture-account"
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

if stage == "0.1.49" do
  :ok = Relay.setup_person(relay, person)
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
      "terminal" => inspect(record.terminal), "facts" => facts}
  end

projections =
  if stage == "0.1.49" do
    %{"legacy_events" => inspect(Relay.events(relay, reader, 0)),
      "legacy_conversation_events" => inspect(Relay.conversation_events(relay, reader, 0))}
  else
    {:ok, inbox} = Reads.inbox(relay, reader)
    item_reads =
      for {{^person, ^sender, id}, record} <- Enum.sort(engine.messages), into: %{} do
        item_id = Resources.item_id(record)
        {id, %{"id" => item_id, "item" => inspect(Reads.item(relay, reader, item_id)),
               "entries" => inspect(Reads.entries(relay, reader, item_id))}}
      end
    %{"inbox" => inbox, "direct_item_reads" => item_reads}
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

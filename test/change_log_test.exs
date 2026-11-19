defmodule AttentionServer.ChangeLogTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, Engine, JSON, Relay}
  alias Exqlite.Sqlite3

  @handle "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

  setup do
    dir = Path.join(System.tmp_dir!(), "change-log-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    flags = Path.join(dir, "flags.json")
    File.write!(flags, ~s({"client_api_v2":true}))
    %{path: Path.join(dir, "state.sqlite3"), flags: flags, dir: dir}
  end

  defp op(n), do: "00000000-0000-4000-8000-#{String.pad_leading(Integer.to_string(n), 12, "0")}"

  defp start(ctx, flag \\ true) do
    opts = [store_path: ctx.path, agent_fixture_mode: true, push_enabled: true]
    opts = if flag, do: Keyword.put(opts, :feature_flags_path, ctx.flags), else: opts
    assert {:ok, relay} = Relay.start_link(opts)
    relay
  end

  defp setup_people(relay) do
    assert :ok = Relay.setup_person(relay, "A")
    assert :ok = Relay.setup_device(relay, "A", "origin")
    assert :ok = Relay.setup_device(relay, "A", "reader")
    assert :ok = Relay.setup_sender(relay, "origin", "app:test")
  end

  defp request(relay, key, frame) do
    assert {:ok, line} =
             Relay.request_line(relay, key, JSON.encode!(Map.put(frame, "v", 1)) <> "\n")

    assert {:ok, reply} = JSON.decode_line(line)
    reply
  end

  defp forward(relay, id, n) do
    assert %{"type" => "ok"} =
             request(relay, "origin", %{
               "type" => "forward",
               "op_id" => op(n),
               "session_handle" => @handle,
               "message" => %{
                 "sender_id" => "app:test",
                 "message_id" => id,
                 "created_at" => n,
                 "title" => "Question #{id}"
               }
             })
  end

  defp store(relay), do: :sys.get_state(relay).store

  defp log(relay, after_rev \\ 0, limit \\ 500),
    do: ChangeLog.read(store(relay), "A", after_rev, limit)

  defp head(relay) do
    assert {:ok, head} = ChangeLog.head(store(relay), "A")
    head
  end

  defp item(id), do: ChangeLog.item_resource({"A", "app:test", id})

  defp tables(path) do
    {:ok, db} = Sqlite3.open(path)
    {:ok, stmt} = Sqlite3.prepare(db, "SELECT name FROM sqlite_master WHERE name LIKE 'client_%'")
    names = collect(db, stmt, [])
    Sqlite3.release(db, stmt)
    Sqlite3.close(db)
    names
  end

  defp collect(db, stmt, acc) do
    case Sqlite3.step(db, stmt) do
      {:row, [name]} -> collect(db, stmt, [name | acc])
      :done -> Enum.sort(acc)
    end
  end

  test "with the flag off the store has no change log tables and writes nothing", ctx do
    relay = start(ctx, false)
    setup_people(relay)
    forward(relay, "m1", 2)

    refute store(relay).change_log
    assert {:error, :disabled} = ChangeLog.head(store(relay), "A")
    GenServer.stop(relay)
    assert tables(ctx.path) == []
  end

  test "an account with no data starts at position 0 and its first change is rev 1", ctx do
    relay = start(ctx)
    assert store(relay).change_log
    setup_people(relay)
    assert head(relay) == 0
    assert {:ok, [], false, 0} = log(relay)

    forward(relay, "m1", 2)
    assert {:ok, changes, false, position} = log(relay)
    assert position == head(relay)
    assert [%{rev: 1} | _] = changes
    assert Enum.map(changes, & &1.rev) == Enum.to_list(1..position)
    assert %{kind: :item, resource_id: item_id} = Enum.find(changes, &(&1.kind == :item))
    assert item_id == item("m1")
    GenServer.stop(relay)
  end

  test "a resource keeps only its latest row, so reads deliver it once at its newest rev",
       ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    forward(relay, "m2", 3)
    assert {:ok, first, false, before} = log(relay)
    first_rev = Enum.find(first, &(&1.resource_id == item("m1"))).rev

    assert %{"type" => "ok", "opened" => true} =
             request(relay, "reader", %{
               "type" => "message_open",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "m1",
               "request_revision" => 1
             })

    assert {:ok, changes, false, now} = log(relay)
    assert now > before
    m1 = Enum.filter(changes, &(&1.resource_id == item("m1")))
    assert [%{kind: :item, rev: latest}] = m1
    assert latest > first_rev
    assert {:ok, ^latest} = ChangeLog.rev(store(relay), "A", item("m1"))

    # Strictly increasing and only rows after the cursor.
    revs = Enum.map(changes, & &1.rev)
    assert revs == Enum.sort(Enum.uniq(revs))
    assert {:ok, after_before, false, ^now} = log(relay, before)
    assert Enum.all?(after_before, &(&1.rev > before))
    assert Enum.any?(after_before, &(&1.resource_id == item("m1")))
    GenServer.stop(relay)
  end

  test "batches honor limit, more and the position rule without gaps", ctx do
    relay = start(ctx)
    setup_people(relay)
    for {id, n} <- [{"m1", 2}, {"m2", 3}, {"m3", 4}], do: forward(relay, id, n)
    assert {:ok, all, false, last} = log(relay)

    {collected, final} =
      Stream.unfold(0, fn
        :done ->
          nil

        cursor ->
          assert {:ok, page, more, position} = log(relay, cursor, 2)
          assert length(page) <= 2
          if page != [], do: assert(position == List.last(page).rev)
          {{page, position}, if(more, do: position, else: :done)}
      end)
      |> Enum.reduce({[], 0}, fn {page, position}, {acc, _} -> {acc ++ page, position} end)

    assert collected == all
    assert final == last
    assert {:ok, [], false, ^last} = log(relay, last)
    assert {:error, :cursor_expired} = log(relay, last + 1)
    GenServer.stop(relay)
  end

  test "replies, read positions, levels and push registration each write their resource",
       ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "chat", 2)
    start = head(relay)

    assert %{"type" => "ok", "event_id" => discussion_id} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Could you clarify?"
             })

    assert {:ok, after_reply, _, reply_head} = log(relay, start)
    assert Enum.any?(after_reply, &(&1.kind == :entry))

    assert %{kind: :entry, resource_id: "entry:" <> _} =
             Enum.find(after_reply, &(&1.kind == :entry))

    assert %{"type" => "ok", "seq" => reply_seq} =
             request(relay, "origin", %{
               "type" => "conversation_reply",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "in_reply_to" => String.to_integer(discussion_id),
               "text" => "Here is more detail"
             })

    read_head = head(relay)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "conversation_read",
               "op_id" => op(5),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "read_through_conversation_seq" => reply_seq
             })

    assert {:ok, after_read, _, _} = log(relay, read_head)
    assert Enum.any?(after_read, &(&1.resource_id == item("chat")))

    levels_head = head(relay)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "levels_set",
               "ref" => "levels",
               "op_id" => op(6),
               "levels" => [
                 %{"name" => "Routine", "lower_bound" => 0, "bounce_after_days" => nil}
               ]
             })

    assert {:ok, after_levels, _, push_head} = log(relay, levels_head)
    assert Enum.any?(after_levels, &(&1.kind == :settings and &1.resource_id == "settings"))

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "push_register",
               "op_id" => op(7),
               "revision" => 1,
               "profile_id" => "harklight-sandbox",
               "address_kind" => "apns_token",
               "address" => String.duplicate("ab", 32)
             })

    assert {:ok, after_push, _, _} = log(relay, push_head)
    assert Enum.any?(after_push, &(&1.kind == :settings))
    assert reply_head < head(relay)
    GenServer.stop(relay)
  end

  test "an item that disappears is a tombstone row" do
    {:ok, engine} = Engine.setup_person(Engine.new(), "A")
    key = {"A", "app:test", "gone"}
    with_item = %{engine | messages: Map.put(engine.messages, key, %{revision: 1})}

    assert %{"A" => [{:item, id}]} = ChangeLog.changes(engine, with_item)
    assert %{"A" => [{:item_removed, ^id}]} = ChangeLog.changes(with_item, engine)
    assert ChangeLog.changes(with_item, with_item) == %{}
    assert ChangeLog.decode_resource(id) == {:item, {"app:test", "gone"}}
  end

  test "existing data at cut-over is seeded at rev 1 and after=0 is cursor_expired", ctx do
    relay = start(ctx, false)
    setup_people(relay)
    forward(relay, "old", 2)
    GenServer.stop(relay)

    relay = start(ctx)
    assert head(relay) == 1
    assert {:error, :cursor_expired} = log(relay, 0)
    assert {:ok, [], false, 1} = log(relay, 1)
    assert {:ok, 1} = ChangeLog.rev(store(relay), "A", item("old"))

    forward(relay, "new", 3)
    assert {:ok, changes, false, position} = log(relay, 1)
    assert Enum.all?(changes, &(&1.rev >= 2))
    assert position == head(relay)
    GenServer.stop(relay)
  end

  test "rows survive a restart, and commits made with the log off reset cursors", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    assert {:ok, before_restart, false, logged} = log(relay)
    GenServer.stop(relay)

    relay = start(ctx)
    assert {:ok, ^before_restart, false, ^logged} = log(relay)
    GenServer.stop(relay)

    # A commit while the flag is off is not in the log, so the log restarts
    # past its head and older cursors must re-read a snapshot.
    relay = start(ctx, false)
    forward(relay, "unlogged", 3)
    GenServer.stop(relay)

    relay = start(ctx)
    assert head(relay) == logged + 1
    assert {:error, :cursor_expired} = log(relay, logged)
    assert {:ok, [], false, _} = log(relay, logged + 1)
    assert {:ok, rev} = ChangeLog.rev(store(relay), "A", item("unlogged"))
    assert rev == logged + 1
    GenServer.stop(relay)
  end

  test "subscribers are woken after each committed append", ctx do
    relay = start(ctx)
    setup_people(relay)
    assert :ok = ChangeLog.subscribe("A")
    forward(relay, "m1", 2)
    assert_receive {:change_log, "A", head}, 1_000
    assert head == head(relay)
    assert :ok = ChangeLog.unsubscribe("A")
    GenServer.stop(relay)
  end

  test "pruning raises the floor so older cursors are cursor_expired", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    last = head(relay)

    assert {:ok, deleted} = ChangeLog.prune(store(relay), System.system_time(:millisecond) + 1)
    assert deleted > 0
    assert {:error, :cursor_expired} = log(relay, 0)
    assert {:ok, [], false, ^last} = log(relay, last)

    forward(relay, "m2", 3)
    assert {:ok, [_ | _], false, _} = log(relay, last)
    GenServer.stop(relay)
  end
end

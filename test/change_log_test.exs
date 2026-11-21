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

  test "entries created while the log runs record when they were first written", ctx do
    relay = start(ctx, false)
    setup_people(relay)
    forward(relay, "chat", 2)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "Before the cut-over"
             })

    GenServer.stop(relay)

    relay = start(ctx)
    assert {:ok, [], false, _} = log(relay, 1)
    before = System.system_time(:millisecond)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "discussion",
               "op_id" => op(4),
               "sender_id" => "app:test",
               "message_id" => "chat",
               "request_revision" => 1,
               "text" => "After the cut-over"
             })

    assert {:ok, changes, false, _} = log(relay, 1)
    assert [%{resource_id: new_entry}] = Enum.filter(changes, &(&1.kind == :entry))
    assert {:ok, at_ms} = ChangeLog.created_at(store(relay), "A", new_entry)
    assert at_ms >= before

    # The pre-cut-over entry has no recorded time, and a later row for the
    # same resource keeps the first time.
    assert {:entry, seq} = ChangeLog.decode_resource(new_entry)

    assert {:error, :not_found} =
             ChangeLog.created_at(store(relay), "A", ChangeLog.entry_resource(seq - 1))

    GenServer.stop(relay)
  end

  test "rank keys write one item row per item whose unit keys changed", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    forward(relay, "m2", 3)
    assert {:ok, %{}} = ChangeLog.rank_keys(store(relay), "A")
    assert :ok = ChangeLog.subscribe("A")

    # Unit IDs are opaque: an item row, and an item's chat-reply row.
    items = %{"u1" => item("m1"), "r-u1" => item("m1"), "u2" => item("m2")}
    item_of = &Map.fetch(items, &1)

    first = %{"u1" => "a", "r-u1" => "ab", "u2" => "b"}
    start_head = head(relay)
    assert {:ok, ranked_head} = ChangeLog.append_ranks(store(relay), "A", first, item_of)
    assert_receive {:change_log, "A", ^ranked_head}, 1_000
    assert {:ok, ranked, false, ^ranked_head} = log(relay, start_head)

    assert Enum.map(ranked, & &1.resource_id) |> Enum.sort() ==
             Enum.sort([item("m1"), item("m2")])

    assert Enum.all?(ranked, &(&1.kind == :item))
    assert {:ok, ^first} = ChangeLog.rank_keys(store(relay), "A")

    # Unchanged keys write nothing.
    assert {:ok, ^ranked_head} = ChangeLog.append_ranks(store(relay), "A", first, item_of)

    # A moved reply row writes its item's row; a unit no longer ranked loses its
    # key and also writes its item's row.
    moved = %{"u1" => "a", "r-u1" => "ac"}
    assert {:ok, moved_head} = ChangeLog.append_ranks(store(relay), "A", moved, item_of)
    assert {:ok, rows, false, ^moved_head} = log(relay, ranked_head)
    assert Enum.map(rows, & &1.resource_id) |> Enum.sort() == Enum.sort([item("m1"), item("m2")])
    assert {:ok, ^moved} = ChangeLog.rank_keys(store(relay), "A")

    # A unit whose item no longer exists still loses its key, with no row.
    assert {:ok, ^moved_head} =
             ChangeLog.append_ranks(store(relay), "A", %{"u1" => "a", "r-u1" => "ac"}, item_of)

    assert {:ok, gone_head} =
             ChangeLog.append_ranks(store(relay), "A", %{"u1" => "a"}, fn _ -> :error end)

    assert gone_head == moved_head
    assert {:ok, %{"u1" => "a"}} = ChangeLog.rank_keys(store(relay), "A")
    assert :ok = ChangeLog.unsubscribe("A")
    GenServer.stop(relay)
  end

  test "the relay prunes rows past the 30-day retention window", ctx do
    offset = :atomics.new(1, [])
    clock = fn -> System.system_time(:millisecond) + :atomics.get(offset, 1) end

    assert {:ok, relay} =
             Relay.start_link(
               store_path: ctx.path,
               agent_fixture_mode: true,
               feature_flags_path: ctx.flags,
               clock: clock
             )

    setup_people(relay)
    forward(relay, "m1", 2)
    last = head(relay)

    # Within the window nothing is pruned.
    send(relay, :change_log_prune)
    assert {:ok, [_ | _], false, ^last} = log(relay, 0)

    :atomics.put(offset, 1, 31 * 24 * 60 * 60 * 1_000)
    send(relay, :change_log_prune)
    assert {:error, :cursor_expired} = log(relay, 0)
    assert {:ok, [], false, ^last} = log(relay, last)
    GenServer.stop(relay)
  end

  test "revoking a device signals only that device's subscribers to close", ctx do
    relay = start(ctx)
    setup_people(relay)
    me = self()

    reader =
      spawn_link(fn ->
        :ok = ChangeLog.subscribe_closing(%{kind: :device, key: "reader", person: "A"})
        send(me, :subscribed)

        receive do
          message -> send(me, {:reader, message})
        end
      end)

    origin =
      spawn_link(fn ->
        :ok = ChangeLog.subscribe_closing(%{kind: :device, key: "origin", person: "A"})
        send(me, :subscribed)

        receive do
          message -> send(me, {:origin, message})
        end
      end)

    assert_receive :subscribed
    assert_receive :subscribed
    assert :ok = Relay.revoke_device(relay, "reader")

    assert_receive {:reader, {:client_closing, {:device, "reader"}, :revoked}}, 1_000
    refute_receive {:origin, _}, 200
    Enum.each([reader, origin], &Process.unlink/1)
    GenServer.stop(relay)
  end

  test "ending a browser session signals its feeds to close", ctx do
    relay = start(ctx)
    setup_people(relay)

    :ok =
      ChangeLog.subscribe_closing(%{kind: :browser, session_id: "session-1", person: "A"})

    assert :ok = Relay.end_browser_session(relay, "session-1")
    assert_receive {:client_closing, {:browser_session, "session-1"}, :revoked}, 1_000
    GenServer.stop(relay)
  end

  test "a paused account and a removed device are closings of the same commit" do
    {:ok, engine} = Engine.setup_person(Engine.new(), "A")
    {:ok, engine} = Engine.setup_device(engine, "A", "phone")
    assert ChangeLog.closings(engine, engine) == []

    paused = %{engine | delivery_states: Map.put(engine.delivery_states, "A", "STOPPED")}
    assert ChangeLog.closings(engine, paused) == [{{:person, "A"}, :account_paused}]
    assert ChangeLog.closings(paused, paused) == []

    removed = %{engine | devices: Map.delete(engine.devices, "phone")}
    assert ChangeLog.closings(engine, removed) == [{{:device, "phone"}, :revoked}]
  end
end

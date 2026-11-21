defmodule AttentionServer.ChangeLogTest do
  use ExUnit.Case, async: false

  alias AttentionServer.{ChangeLog, Engine, JSON, Relay}
  alias AttentionServer.ClientAPI.{FeedStep, Reads}
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

  # Tests touch the Relay's store directly, which only the Relay does in
  # production. Pausing it between callbacks keeps its own writes (the rank
  # hook, timers) off the shared SQLite connection meanwhile.
  defp paused(relay, fun) do
    :ok = :sys.suspend(relay)

    try do
      fun.()
    after
      :ok = :sys.resume(relay)
    end
  end

  defp log(relay, after_rev \\ 0, limit \\ 500),
    do: paused(relay, fn -> ChangeLog.read(store(relay), "A", after_rev, limit) end)

  defp head(relay) do
    assert {:ok, head} = paused(relay, fn -> ChangeLog.head(store(relay), "A") end)
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
    assert {:error, :disabled} = paused(relay, fn -> ChangeLog.head(store(relay), "A") end)
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
    # Read from 0, every row is delivered; the Relay's rank rows follow the
    # commit's own, and compaction keeps only each resource's latest.
    revs = Enum.map(changes, & &1.rev)
    assert revs == Enum.sort(Enum.uniq(revs))
    assert hd(revs) >= 1 and List.last(revs) == position
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
    assert {:ok, ^latest} = paused(relay, fn -> ChangeLog.rev(store(relay), "A", item("m1")) end)

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
    assert {:ok, 1} = paused(relay, fn -> ChangeLog.rev(store(relay), "A", item("old")) end)

    forward(relay, "new", 3)
    assert {:ok, changes, false, position} = log(relay, 1)
    assert Enum.all?(changes, &(&1.rev >= 2))
    assert position == head(relay)
    GenServer.stop(relay)
  end

  test "ranks served immediately after cut-over converge with the next feed step", ctx do
    relay = start(ctx, false)
    setup_people(relay)
    for {id, n} <- [{"one", 2}, {"two", 3}, {"three", 4}, {"three-b", 6}],
        do: forward(relay, id, n)
    GenServer.stop(relay)

    relay = start(ctx)
    assert {:ok, %{"items" => initial, "rev" => cursor}} = Reads.inbox(relay, "reader")
    assert length(initial) == 4

    forward(relay, "four", 5)
    assert {:ok, principal} = Reads.client_principal(store(relay).engine, "reader")
    assert {:ok, rows, false, _} = FeedStep.read(relay, principal, cursor, 100)
    changes = for %{"kind" => "item", "data" => data} <- rows, do: data
    held = Map.new(initial ++ changes, &{&1["item_id"], &1["rank"]})

    assert {:ok, %{"items" => current}} = Reads.inbox(relay, "reader")
    assert length(current) == 5
    assert held == Map.new(current, &{&1["item_id"], &1["rank"]})
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

    assert {:ok, rev} =
             paused(relay, fn -> ChangeLog.rev(store(relay), "A", item("unlogged")) end)

    assert rev == logged + 1
    GenServer.stop(relay)
  end

  test "subscribers are woken after each committed append", ctx do
    relay = start(ctx)
    setup_people(relay)
    assert :ok = ChangeLog.subscribe("A")
    forward(relay, "m1", 2)
    current = head(relay)
    assert_receive {:change_log, "A", ^current}, 1_000
    assert :ok = ChangeLog.unsubscribe("A")
    GenServer.stop(relay)
  end

  test "pruning raises the floor so older cursors are cursor_expired", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    last = head(relay)

    assert {:ok, deleted} =
             paused(relay, fn ->
               ChangeLog.prune(store(relay), System.system_time(:millisecond) + 1)
             end)

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

    assert {:ok, at_ms} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", new_entry) end)

    assert at_ms >= before

    # The pre-cut-over entry has no recorded time, and a later row for the
    # same resource keeps the first time.
    assert {:entry, seq} = ChangeLog.decode_resource(new_entry)

    assert {:error, :not_found} =
             paused(relay, fn ->
               ChangeLog.created_at(store(relay), "A", ChangeLog.entry_resource(seq - 1))
             end)

    GenServer.stop(relay)
  end

  test "rank keys write one item row per item whose unit keys changed", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    forward(relay, "m2", 3)
    assert {:ok, %{}} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)
    assert :ok = ChangeLog.subscribe("A")

    # Unit IDs are opaque: an item row, and an item's chat-reply row.
    items = %{"u1" => item("m1"), "r-u1" => item("m1"), "u2" => item("m2")}
    item_of = &Map.fetch(items, &1)

    first = %{"u1" => "a", "r-u1" => "ab", "u2" => "b"}
    start_head = head(relay)

    assert {:ok, ranked_head} =
             paused(relay, fn -> ChangeLog.append_ranks(store(relay), "A", first, item_of) end)

    assert_receive {:change_log, "A", ^ranked_head}, 1_000
    assert {:ok, ranked, false, ^ranked_head} = log(relay, start_head)

    assert Enum.map(ranked, & &1.resource_id) |> Enum.sort() ==
             Enum.sort([item("m1"), item("m2")])

    assert Enum.all?(ranked, &(&1.kind == :item))
    assert {:ok, ^first} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)

    # Unchanged keys write nothing.
    assert {:ok, ^ranked_head} =
             paused(relay, fn -> ChangeLog.append_ranks(store(relay), "A", first, item_of) end)

    # A moved reply row writes its item's row; a unit no longer ranked loses its
    # key and also writes its item's row.
    moved = %{"u1" => "a", "r-u1" => "ac"}

    assert {:ok, moved_head} =
             paused(relay, fn -> ChangeLog.append_ranks(store(relay), "A", moved, item_of) end)

    assert {:ok, rows, false, ^moved_head} = log(relay, ranked_head)
    assert Enum.map(rows, & &1.resource_id) |> Enum.sort() == Enum.sort([item("m1"), item("m2")])
    assert {:ok, ^moved} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)

    # A unit whose item no longer exists still loses its key, with no row.
    assert {:ok, ^moved_head} =
             paused(relay, fn ->
               ChangeLog.append_ranks(store(relay), "A", %{"u1" => "a", "r-u1" => "ac"}, item_of)
             end)

    assert {:ok, gone_head} =
             paused(relay, fn ->
               ChangeLog.append_ranks(store(relay), "A", %{"u1" => "a"}, fn _ -> :error end)
             end)

    assert gone_head == moved_head
    assert {:ok, %{"u1" => "a"}} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)
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
    # Stop the stand-in feeds so their subscriptions don't outlive the test.
    Enum.each([reader, origin], fn pid ->
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end)

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

  test "a person seeded without a log row is moved past their head by a reset", ctx do
    # Data before the cut-over; the person has no log row yet.
    relay = start(ctx, false)
    setup_people(relay)
    forward(relay, "old", 2)
    GenServer.stop(relay)

    relay = start(ctx)
    assert head(relay) == 1
    shown = head(relay)
    GenServer.stop(relay)

    # A commit while the log is off, for that same person.
    relay = start(ctx, false)
    forward(relay, "unlogged", 3)
    GenServer.stop(relay)

    # The cursor the client holds must not read as current.
    relay = start(ctx)
    assert head(relay) > shown
    assert {:error, :cursor_expired} = log(relay, shown)
    GenServer.stop(relay)
  end

  test "a commit that fails inside the change-log write leaves no rows and no state", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    before_head = head(relay)
    before_engine = :sys.get_state(relay).store.engine

    {:ok, db} = Sqlite3.open(ctx.path)

    :ok =
      Sqlite3.execute(
        db,
        "CREATE TRIGGER fail_log BEFORE UPDATE ON client_change_log_state BEGIN SELECT RAISE(ABORT, 'refused'); END"
      )

    reply =
      request(relay, "origin", %{
        "type" => "forward",
        "op_id" => op(3),
        "session_handle" => @handle,
        "message" => %{
          "sender_id" => "app:test",
          "message_id" => "m2",
          "created_at" => 3,
          "title" => "Refused"
        }
      })

    assert reply["type"] == "error"
    assert :sys.get_state(relay).store.engine == before_engine
    assert head(relay) == before_head
    assert {:ok, rows, false, ^before_head} = log(relay)
    refute Enum.any?(rows, &(&1.resource_id == item("m2")))

    # Once the fault is gone the same request commits, gap-free.
    :ok = Sqlite3.execute(db, "DROP TRIGGER fail_log")
    Sqlite3.close(db)
    forward(relay, "m2", 3)
    assert {:ok, after_rows, false, _} = log(relay, before_head)
    assert Enum.any?(after_rows, &(&1.resource_id == item("m2")))
    GenServer.stop(relay)
  end

  test "resources from before the cut-over never get a first-seen time", ctx do
    relay = start(ctx, false)
    setup_people(relay)
    forward(relay, "old", 2)
    GenServer.stop(relay)

    relay = start(ctx)

    # Answering changes the pre-cut-over record itself.
    key = {"A", "app:test", "old"}
    assert %{terminal: nil} = Map.fetch!(store(relay).engine.messages, key)

    assert %{"type" => "ok"} =
             request(relay, "reader", %{
               "type" => "answer",
               "op_id" => op(3),
               "sender_id" => "app:test",
               "message_id" => "old",
               "answer" => "Done"
             })

    assert %{terminal: terminal} = Map.fetch!(store(relay).engine.messages, key)
    refute is_nil(terminal)
    assert {:error, :not_found} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", item("old")) end)

    assert {:ok, _} =
             paused(relay, fn ->
               ChangeLog.append_ranks(store(relay), "A", %{"u-old" => "k"}, fn _ ->
                 {:ok, item("old")}
               end)
             end)

    assert {:ok, [_ | _], false, _} = log(relay, 1)

    assert {:error, :not_found} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", item("old")) end)

    # A new item still gets one.
    forward(relay, "new", 4)

    assert {:ok, _} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", item("new")) end)

    GenServer.stop(relay)
  end

  defp item_id_of(relay, id) do
    engine = :sys.get_state(relay).store.engine
    {:ok, item_id} = AttentionServer.ClientAPI.Resources.item_id(engine, {"A", "app:test", id})
    item_id
  end

  defp block_rank_inserts(ctx) do
    {:ok, db} = Sqlite3.open(ctx.path)

    :ok =
      Sqlite3.execute(db, """
      CREATE TRIGGER fail_rank_insert BEFORE INSERT ON client_ranks
      BEGIN SELECT RAISE(ABORT, 'rank insert failed'); END
      """)

    db
  end

  defp unblock_rank_inserts(db) do
    :ok = Sqlite3.execute(db, "DROP TRIGGER fail_rank_insert")
    Sqlite3.close(db)
  end

  defp wait_for_stored_rank(relay, item_id, deadline) do
    case paused(relay, fn -> ChangeLog.rank(store(relay), "A", item_id) end) do
      {:error, :not_found} = error ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(20)
          wait_for_stored_rank(relay, item_id, deadline)
        else
          error
        end

      result ->
        result
    end
  end

  test "a failed rank write is reported and retried without another commit", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "one", 2)
    cursor = head(relay)
    db = block_rank_inserts(ctx)
    forward(relay, "two", 3)
    item_id = item_id_of(relay, "two")
    assert {:ok, principal} = Reads.client_principal(store(relay).engine, "reader")

    try do
      assert {:error, :storage_unavailable} = Reads.inbox(relay, "reader")
      assert {:error, :storage_unavailable} = Reads.item(relay, "reader", item_id)
      assert {:error, :storage_unavailable} = FeedStep.read(relay, principal, cursor, 100)
    after
      paused(relay, fn -> unblock_rank_inserts(db) end)
    end

    assert {:ok, _} =
             wait_for_stored_rank(relay, item_id, System.monotonic_time(:millisecond) + 2_500)

    assert {:ok, %{"items" => [_, _]}} = Reads.inbox(relay, "reader")
    assert {:ok, %{"item" => %{"item_id" => ^item_id}}} = Reads.item(relay, "reader", item_id)
    assert {:ok, [_ | _], false, _} = FeedStep.read(relay, principal, cursor, 100)
    GenServer.stop(relay)
  end

  test "startup repairs a rank write left incomplete before shutdown", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "one", 2)
    db = block_rank_inserts(ctx)
    forward(relay, "two", 3)
    item_id = item_id_of(relay, "two")
    GenServer.stop(relay)
    unblock_rank_inserts(db)

    relay = start(ctx)
    assert {:ok, _} = paused(relay, fn -> ChangeLog.rank(store(relay), "A", item_id) end)
    assert {:ok, %{"items" => [_, _]}} = Reads.inbox(relay, "reader")
    GenServer.stop(relay)
  end

  test "stored ranks write an item row only when an item's rank changes", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "m1", 2)
    forward(relay, "m2", 3)
    i1 = item_id_of(relay, "m1")
    i2 = item_id_of(relay, "m2")
    store = store(relay)

    # The Relay has already stored a ranking for A; start from a known one.
    ranks = %{
      i1 => %{"key" => "b", "section" => "current"},
      i2 => %{"key" => "c", "section" => "current", "_reply" => %{"key" => "d"}}
    }

    start_head = head(relay)

    assert {:ok, first_head, cache} =
             paused(relay, fn -> ChangeLog.put_ranks(store(relay), "A", ranks, nil) end)

    assert {:ok, page, false} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "A", nil, 10) end)

    assert page == [{i1, ranks[i1]}, {i2, ranks[i2]}]
    assert {:ok, %{"key" => "c"}} = paused(relay, fn -> ChangeLog.rank(store(relay), "A", i2) end)

    reply_id = "r" <> i2

    assert {:ok, %{^i1 => "b", ^i2 => "c", ^reply_id => "d"}} =
             paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)

    # Same ranking: nothing written.
    assert {:ok, ^first_head} =
             paused(relay, fn -> ChangeLog.put_ranks(store(relay), "A", ranks) end)

    # A change in any member writes that item's row only.
    moved = put_in(ranks, [i2, "_reply", "key"], "e")

    assert {:ok, moved_head, cache} =
             paused(relay, fn -> ChangeLog.put_ranks(store(relay), "A", moved, cache) end)

    assert {:ok, %{^reply_id => "e"}} =
             paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)

    assert {:ok, [%{kind: :item, resource_id: resource}], false, ^moved_head} =
             log(relay, first_head)

    assert resource == item("m2")

    # Paging after a key, and an item no longer ranked.
    assert {:ok, [{^i2, _}], false} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "A", "b", 10) end)

    assert {:ok, [{^i1, _}], true} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "A", nil, 1) end)

    assert {:ok, gone_head, cache} =
             paused(relay, fn ->
               ChangeLog.put_ranks(store(relay), "A", Map.take(moved, [i1]), cache)
             end)

    assert {:ok, units} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)
    assert units == cache.units
    assert units == %{i1 => "b"}

    assert gone_head > moved_head
    assert {:error, :not_found} = paused(relay, fn -> ChangeLog.rank(store(relay), "A", i2) end)
    assert start_head < first_head
    _ = store
    GenServer.stop(relay)
  end

  test "rank_page answers :none until a person's ranking is first stored", ctx do
    relay = start(ctx)

    assert {:error, :none} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "Z", nil, 10) end)

    assert {:ok, _} = paused(relay, fn -> ChangeLog.put_ranks(store(relay), "Z", %{}) end)

    assert {:ok, [], false} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "Z", nil, 10) end)

    GenServer.stop(relay)
  end

  test "the relay stores ranks once per change and keeps other items' keys stable", ctx do
    relay = start(ctx)
    setup_people(relay)
    for {id, n} <- [{"m1", 2}, {"m2", 3}, {"m3", 4}, {"m4", 5}], do: forward(relay, id, n)

    assert {:ok, page, false} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "A", nil, 100) end)

    assert length(page) == 4
    keys = Enum.map(page, fn {_id, rank} -> rank["key"] end)
    assert keys == Enum.sort(keys)
    before = Map.new(page)
    before_head = head(relay)

    # A new item is ranked in; items whose rank did not move write no row.
    forward(relay, "m5", 6)

    assert {:ok, after_page, false} =
             paused(relay, fn -> ChangeLog.rank_page(store(relay), "A", nil, 100) end)

    assert length(after_page) == 5
    after_ranks = Map.new(after_page)

    assert {:ok, rows, false, _} = log(relay, before_head)
    engine = :sys.get_state(relay).store.engine

    for {id, rank} <- before do
      assert after_ranks[id] == rank
      {:ok, key} = AttentionServer.ClientAPI.Resources.item_key(engine, "A", id)
      refute Enum.any?(rows, &(&1.resource_id == ChangeLog.item_resource(key)))
    end

    # The stored ranking equals a fresh ranking from the stored keys.
    {:ok, previous} = paused(relay, fn -> ChangeLog.rank_keys(store(relay), "A") end)

    fresh =
      AttentionServer.ClientAPI.Resources.ranks(
        engine,
        "A",
        Map.get(:sys.get_state(relay).priority_views, "A"),
        previous
      )

    assert fresh == after_ranks
    GenServer.stop(relay)
  end

  test "a changed owner key or revision closes that person's browser feeds" do
    {:ok, engine} = Engine.setup_person(Engine.new(), "A")
    owned = %{engine | owners: Map.put(engine.owners, "A", %{key: "owner-key", revision: 1})}
    assert ChangeLog.closings(owned, owned) == []

    bumped = %{owned | owners: Map.put(owned.owners, "A", %{key: "owner-key", revision: 2})}
    assert ChangeLog.closings(owned, bumped) == [{{:owner, "A"}, :revoked}]

    recovered = %{owned | owners: Map.put(owned.owners, "A", %{key: "new-key", revision: 1})}
    assert ChangeLog.closings(owned, recovered) == [{{:owner, "A"}, :revoked}]

    gone = %{owned | owners: Map.delete(owned.owners, "A")}
    assert ChangeLog.closings(owned, gone) == [{{:owner, "A"}, :revoked}]
  end

  test "a browser feed hears its owner key's revocation and its owner change" do
    browser = %{kind: :browser, session_id: "session-hash", person: "A", key: "owner-key"}
    me = self()

    feed =
      spawn_link(fn ->
        :ok = ChangeLog.subscribe_closing(browser)
        send(me, :subscribed)

        receive do
          message -> send(me, {:feed, message})
        end

        receive do
          message -> send(me, {:feed, message})
        end
      end)

    assert_receive :subscribed
    ChangeLog.notify_closing([{{:device, "owner-key"}, :revoked}])
    assert_receive {:feed, {:client_closing, {:device, "owner-key"}, :revoked}}, 1_000
    ChangeLog.notify_closing([{{:owner, "A"}, :revoked}])
    assert_receive {:feed, {:client_closing, {:owner, "A"}, :revoked}}, 1_000
    Process.unlink(feed)
    Process.exit(feed, :kill)

    # Unsubscribing releases every scope it took.
    :ok = ChangeLog.subscribe_closing(browser)
    :ok = ChangeLog.unsubscribe_closing(browser)
    ChangeLog.notify_closing([{{:owner, "A"}, :revoked}, {{:device, "owner-key"}, :revoked}])
    refute_receive {:client_closing, _, _}, 200
  end

  test "switching to stored-rank reads expires cursors from the old rank projection once", ctx do
    relay = start(ctx)
    setup_people(relay)
    forward(relay, "one", 2)
    cursor = head(relay)

    assert {:ok, created_at} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", item("one")) end)

    GenServer.stop(relay)

    # Recreate the change-log state table as it existed before this transition.
    {:ok, db} = Sqlite3.open(ctx.path)

    :ok =
      Sqlite3.execute(db, """
      CREATE TABLE old_log_state (id INTEGER PRIMARY KEY CHECK (id = 1), store_version INTEGER NOT NULL);
      INSERT INTO old_log_state SELECT id, store_version FROM client_change_log_state;
      DROP TABLE client_change_log_state;
      ALTER TABLE old_log_state RENAME TO client_change_log_state;
      """)

    Sqlite3.close(db)

    relay = start(ctx)
    assert {:error, :cursor_expired} = log(relay, cursor)
    position = head(relay)
    assert position == cursor + 1
    assert {:ok, %{"rev" => ^position, "items" => [_]}} = Reads.inbox(relay, "reader")

    assert {:ok, ^created_at} =
             paused(relay, fn -> ChangeLog.created_at(store(relay), "A", item("one")) end)

    GenServer.stop(relay)

    relay = start(ctx)
    assert {:ok, [], false, ^position} = log(relay, position)
    GenServer.stop(relay)
  end

  # Per-change cost of storing a person's ranking (owner's gate: the rank hook
  # must cost no more than today's view rebuild; an unchanged re-rank must be
  # nearly free).
  defp synthetic_ranks(n, move \\ nil) do
    Map.new(1..n, fn i ->
      key = String.pad_leading(Integer.to_string(i), 8, "0")
      key = if i == move, do: key <> "m", else: key
      {"i#{i}", %{"key" => key, "section" => "current", "level" => "normal", "score" => i / n}}
    end)
  end

  defp micros(fun) do
    {us, value} = :timer.tc(fun)
    {div(us, 1000), value}
  end

  test "an unchanged re-rank kept in memory costs almost nothing at 4k and 10k items", ctx do
    relay = start(ctx)
    setup_people(relay)

    results =
      for n <- [4_000, 10_000] do
        person = "P#{n}"
        assert :ok = Relay.setup_person(relay, person)
        ranks = synthetic_ranks(n)

        paused(relay, fn ->
          store = store(relay)

          {first_ms, {:ok, _, cache}} =
            micros(fn -> ChangeLog.put_ranks(store, person, ranks, nil) end)

          # Same ranking, kept in memory: compared in memory, nothing written.
          {same_ms, {:ok, head, cache}} =
            micros(fn -> ChangeLog.put_ranks(store, person, ranks, cache) end)

          # One item moves: one stored rank changes.
          moved = synthetic_ranks(n, 7)

          {move_ms, {:ok, _, _}} =
            micros(fn -> ChangeLog.put_ranks(store, person, moved, cache) end)

          assert {:ok, %{"key" => "00000007m"}} = ChangeLog.rank(store, person, "i7")

          # After a restart (no ranking in memory) the stored one is compared by
          # its JSON: an unchanged ranking writes nothing.
          {restart_ms, {:ok, ^head, _}} =
            micros(fn -> ChangeLog.put_ranks(store, person, moved, nil) end)

          assert same_ms < 25, "unchanged re-rank took #{same_ms} ms at #{n}"

          %{
            n: n,
            first_ms: first_ms,
            unchanged_ms: same_ms,
            one_moved_ms: move_ms,
            restart_ms: restart_ms
          }
        end)
      end

    summary =
      Map.new(results, fn result ->
        {Integer.to_string(result.n),
         result |> Map.delete(:n) |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)}
      end)

    IO.puts("RANK_STORE_COST " <> JSON.encode!(summary))

    GenServer.stop(relay)
  end
end

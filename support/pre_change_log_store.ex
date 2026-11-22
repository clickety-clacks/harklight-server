defmodule AttentionServer.PreChangeLogStore do
  @moduledoc """
  Writes a store the way a Server before 0.2.0 did: plain `Store` commits with
  the client change logs never enabled. Tests use it to seed data from before
  the cut-over, or commits that an older Server made after a newer one had
  run. It needs no switch in the Relay, which always enables the logs.

  Facts that only a protocol 0.1 client or origin could write are seeded as
  the stored state they left (engine fields, and the 0.1 paging tables in
  their row format), since the Server no longer has the code that wrote them.
  """

  alias AttentionServer.{Conversation, Engine, JSON, RouteBIndex, Store}
  alias Exqlite.Sqlite3

  @doc "Opens `path` without the change logs, runs `fun` on the store, and closes it."
  def write(path, fun) do
    {:ok, %Store{change_log: false} = store} = Store.open(path)
    %Store{change_log: false} = store = fun.(store)
    :ok = Store.close(store)
  end

  def operator_bootstrap(store, person, key),
    do:
      commit(
        store,
        Engine.operator_bootstrap(store.engine, person, key),
        %{person: person, actor: "local_operator", action: "owner_bootstrap"}
      )

  def setup_person(store, person),
    do: commit(store, Engine.setup_person(store.engine, person), setup(person, "person_setup"))

  def setup_device(store, person, key),
    do:
      commit(store, Engine.setup_device(store.engine, person, key), setup(person, "device_setup"))

  def setup_sender(store, key, pattern),
    do:
      commit(
        store,
        Engine.setup_sender(store.engine, key, pattern),
        setup(person(store, key), "sender_setup")
      )

  @doc """
  Applies one device request frame through the Engine and commits it, as the
  Relay does when the engine changed. Returns the decoded reply and the store.
  """
  def request(store, key, frame) do
    raw = JSON.encode!(Map.put(frame, "v", 1)) <> "\n"

    {next, {:ok, line}} =
      Engine.request_line(store.engine, key, raw, System.system_time(:millisecond))

    {:ok, reply} = JSON.decode_line(line)

    if next == store.engine do
      {reply, store}
    else
      audit = %{person: person(store, key), actor: key, action: frame["type"]}
      {:ok, store} = Store.commit(store, next, audit)
      {reply, store}
    end
  end

  @doc """
  Runs a device's paged startup through its checkpoint, as a Server before
  0.2.0 did for a 0.1 client: the snapshot of the device's inbox is stored and
  accepted, which moves the device cursor to the snapshot's high water. The
  rows are written in the stored 0.1 paging format; the cursor is a plain
  progress commit, as the checkpoint's was.
  """
  def paging_checkpoint(store, key, startup_id) do
    {snapshot, %{receipts: 0} = counts} = paging_snapshot(store, key, startup_id)
    person = person(store, key)

    :ok =
      run(
        store.db,
        "UPDATE relay_inbox_snapshots SET status = 'pending', item_count = ?, receipt_count = ?, operation_count = ?, receipts_complete = 1 WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status = 'building'",
        [counts.items, counts.receipts, counts.operation_ids, person, key, snapshot.snapshot_id]
      )

    engine = %{
      store.engine
      | cursors: Map.put(store.engine.cursors, {person, key}, snapshot.high_water)
    }

    {:ok, store} =
      Store.commit(store, engine, %{person: person, actor: key, action: "inbox_checkpoint"})

    :ok =
      run(
        store.db,
        "UPDATE relay_inbox_snapshots SET status = 'replaced' WHERE person_id = ? AND device_id = ? AND status IN ('pending', 'accepted') AND snapshot_id != ?",
        [person, key, snapshot.snapshot_id]
      )

    :ok =
      run(
        store.db,
        "UPDATE relay_inbox_snapshots SET status = 'accepted' WHERE person_id = ? AND device_id = ? AND snapshot_id = ? AND status = 'pending'",
        [person, key, snapshot.snapshot_id]
      )

    store
  end

  @doc "Leaves a paged startup building, as a Server stopped mid-snapshot did."
  def paging_building(store, key, startup_id) do
    {_snapshot, _counts} = paging_snapshot(store, key, startup_id)
    store
  end

  @doc """
  Opens a device's conversation feed, as a 0.1 session's `conversation_open`
  did, in its legacy and its bounded (Route B) form alike: it records the
  conversation-opened marker. Route B's `inbox_list` and `conversation_page`
  stored nothing.
  """
  def open_conversation(store, key) do
    {:ok, engine} = Engine.open_conversation(store.engine, key)
    audit = %{person: person(store, key), actor: key, action: "conversation_open"}
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  @doc """
  Acknowledges a device's conversation feed up to `seq`, as a 0.1 session did:
  a stored conversation cursor, committed as progress.
  """
  def acknowledge_conversation(store, key, seq) do
    person = person(store, key)
    true = seq >= Map.get(store.engine.conversation_cursors, {person, key}, 0)

    engine = %{
      store.engine
      | conversation_cursors: Map.put(store.engine.conversation_cursors, {person, key}, seq)
    }

    audit = %{person: person, actor: key, action: "conversation_ack"}
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  @doc """
  Records an origin's protocol 0.1 `support` declaration for its session
  handle, as a 0.1 origin did. Client API v2 never writes one; a stored
  record still decodes, and its `"ack2"` member sets `ack2_expected` on ACK1.
  It stores what the 0.1 Server stored: the declaration with `"origin"` and
  `"reported_at"` on the session, and the operation's receipt. Declaring the
  current support again changes nothing.
  """
  def origin_support(store, key, %{"op_id" => op_id, "session_handle" => handle} = declaration) do
    person = person(store, key)
    engine = store.engine
    {:ok, declared} = Conversation.support_from_frame(declaration)
    true = is_map(declared)
    session = Map.fetch!(engine.origin_sessions, {person, key, handle})

    if Map.drop(Map.get(session, :support) || %{}, ["origin", "reported_at"]) == declared do
      store
    else
      record =
        declared
        |> Map.put("origin", key)
        |> Map.put("reported_at", System.system_time(:millisecond))

      semantic = %{"type" => "support", "session_handle" => handle, "support" => declared}

      receipt = %{
        semantic: JSON.canonical(semantic),
        reply: %{"session_handle" => handle, "listening_support" => record}
      }

      engine = %{
        engine
        | origin_sessions:
            Map.put(
              engine.origin_sessions,
              {person, key, handle},
              Map.put(session, :support, record)
            ),
          receipts: Map.put(engine.receipts, {person, key, op_id}, receipt)
      }

      commit(store, {:ok, engine}, %{person: person, actor: key, action: "origin_support"})
    end
  end

  @doc """
  Records a protocol 0.1 origin `ack` (the legacy delivered receipt) for a
  feed entry, as a 0.1 origin did. The stored receipt still marks the
  entry delivered in v2 reads. It stores what the 0.1 Server stored: the
  origin receipt and a `"receipt"` conversation fact. Only person turns
  without files are seeded; a 0.1 `ack` of a turn with files also carried
  transfer reports.
  """
  def origin_ack(store, key, feed_seq) do
    person = person(store, key)
    engine = store.engine

    %{"kind" => kind, "author" => author} =
      entry = engine.origin_feeds[{person, key}].entries[feed_seq]

    true = kind in ["discussion", "terminal"] and author != "origin"
    [] = Map.get(entry, "files", [])
    nil = Map.get(engine.origin_receipts, {person, key, feed_seq})

    receipt = %{delivered_at: System.system_time(:millisecond), transfers: %{}, uses: %{}}
    message_key = {person, entry["sender_id"], entry["message_id"]}

    payload = %{
      "kind" => "receipt",
      "author" => "origin",
      "event_id" => entry["event_id"],
      "request_revision" => entry["request_revision"],
      "session_handle" => entry["session_handle"],
      "delivered" => true
    }

    seq = Map.get(engine.conversation_highs, person, 0) + 1
    event = %{kind: :conversation, key: message_key, payload: payload}

    engine = %{
      engine
      | origin_receipts: Map.put(engine.origin_receipts, {person, key, feed_seq}, receipt),
        conversation_highs: Map.put(engine.conversation_highs, person, seq),
        conversation_events: Map.put(engine.conversation_events, {person, seq}, event),
        route_b_index:
          RouteBIndex.put_conversation_event(
            engine.route_b_index,
            person,
            seq,
            message_key,
            payload
          )
    }

    commit(store, {:ok, engine}, %{person: person, actor: key, action: "origin_ack"})
  end

  @doc """
  Runs `fun` on a running Relay's store, inside the Relay, for seeding a
  protocol 0.1 fact into a store a test is still serving. `fun` returns the
  next store. Relay-side caches are not refreshed, so seed only facts that
  no cache derives from.
  """
  def in_relay(relay, fun),
    do: :sys.replace_state(relay, fn state -> %{state | store: fun.(state.store)} end)

  # The stored 0.1 paging rows for a device's startup from 0: the snapshot row
  # left building and its index chunks, in the chunk format the 0.1 Server
  # wrote.
  defp paging_snapshot(store, key, startup_id) do
    person = person(store, key)
    {items, receipts, operation_ids, high} = inbox_snapshot(store.engine, person, key)
    snapshot_id = startup_id <> "-snapshot"

    :ok =
      run(
        store.db,
        "INSERT INTO relay_inbox_snapshots (person_id, device_id, startup_id, snapshot_id, since_seq, high_water, status, item_count, receipt_count, operation_count, cursor_secret, receipt_next_offset, receipts_complete) VALUES (?, ?, ?, ?, 0, ?, 'building', 0, 0, 0, ?, 0, 0)",
        [person, key, startup_id, snapshot_id, high, {:blob, :crypto.strong_rand_bytes(32)}]
      )

    for {table, values} <- [
          {"relay_inbox_item_pages", items},
          {"relay_inbox_receipt_sequences", receipts}
        ],
        {chunk, n} <- values |> Enum.chunk_every(20) |> Enum.with_index() do
      payload = :erlang.term_to_binary(chunk, [{:compressed, 6}])

      :ok =
        run(
          store.db,
          "INSERT INTO #{table} (person_id, device_id, snapshot_id, start_offset, payload, checksum) VALUES (?, ?, ?, ?, ?, ?)",
          [
            person,
            key,
            snapshot_id,
            n * 20,
            {:blob, payload},
            {:blob, :crypto.hash(:sha256, payload)}
          ]
        )
    end

    for op_id <- operation_ids do
      :ok =
        run(
          store.db,
          "INSERT INTO relay_inbox_operation_receipts (person_id, device_id, snapshot_id, op_id) VALUES (?, ?, ?, ?)",
          [person, key, snapshot_id, op_id]
        )
    end

    counts = %{
      items: length(items),
      receipts: length(receipts),
      operation_ids: length(operation_ids)
    }

    {%{snapshot_id: snapshot_id, high_water: high}, counts}
  end

  # What a 0.1 startup snapshot from 0 held for the device: every message of
  # the person, waiting before resolved and newest first; the device's own
  # lifecycle receipts; and its operation ids.
  defp inbox_snapshot(engine, person, device) do
    high = engine.persons[person]

    {waiting, resolved} =
      engine.messages
      |> Enum.filter(fn {{message_person, _sender, _id}, record} ->
        message_person == person and record.accepted_seq <= high
      end)
      |> Enum.split_with(fn {_key, record} ->
        not match?(%{seq: seq} when seq <= high, record.terminal)
      end)

    order = fn {{_person, sender, id}, record} -> {-record.accepted_seq, sender, id} end

    items =
      (Enum.sort_by(waiting, order) ++ Enum.sort_by(resolved, order))
      |> Enum.map(fn {{_person, sender, id}, _record} -> {sender, id} end)

    receipts =
      Enum.filter(1..high//1, fn seq ->
        case Map.get(engine.events, {person, seq}) do
          %{kind: :lifecycle, key: message_key} ->
            match?(%{origin: ^device}, Map.get(engine.messages, message_key))

          _ ->
            false
        end
      end)

    operation_ids =
      for {{^person, ^device, op_id}, _receipt} <- engine.receipts, do: op_id

    {items, receipts, Enum.sort(operation_ids), high}
  end

  defp run(db, sql, params) do
    {:ok, statement} = Sqlite3.prepare(db, sql)

    try do
      :ok = Sqlite3.bind(statement, params)
      :done = Sqlite3.step(db, statement)
      :ok
    after
      Sqlite3.release(db, statement)
    end
  end

  defp commit(store, {:ok, engine}, audit) do
    {:ok, store} = Store.commit(store, engine, audit)
    store
  end

  defp setup(person, action), do: %{person: person, actor: "local_setup", action: action}

  defp person(store, key), do: get_in(store.engine.devices, [key, :person])
end

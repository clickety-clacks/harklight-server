defmodule AttentionServer.Store.DerivedIndex do
  @moduledoc false

  alias AttentionServer.{BounceDecision, Conversation}
  alias AttentionServer.AgentAPI.DeliveryBaseline
  alias AttentionServer.ClientAPI.Resources
  alias AttentionServer.Store.{KeyedValidation, RecordCodec, RecordTable}
  alias Exqlite.Sqlite3

  @index_version 1
  @liveness_version 1

  @schema """
  CREATE TABLE relay_items (
    person TEXT NOT NULL,
    seq    INTEGER NOT NULL,
    sender TEXT NOT NULL, message_id TEXT NOT NULL,
    state  TEXT,
    origin TEXT, session TEXT, agent_v2 INTEGER NOT NULL,
    source BLOB,
    inherits_from INTEGER,
    last_entry INTEGER, last_reply INTEGER, read_position INTEGER NOT NULL DEFAULT 0,
    chat_started INTEGER NOT NULL DEFAULT 0, availability_entry INTEGER,
    discussed INTEGER NOT NULL DEFAULT 0,
    origin_revision INTEGER NOT NULL DEFAULT 0,
    bounce_pending INTEGER NOT NULL DEFAULT 0,
    listening INTEGER NOT NULL DEFAULT 0,
    live INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (person, seq)
  ) WITHOUT ROWID;
  CREATE UNIQUE INDEX relay_items_key       ON relay_items (person, sender, message_id);
  CREATE INDEX        relay_items_message   ON relay_items (person, message_id);
  CREATE INDEX        relay_items_session   ON relay_items (person, origin, session, seq);
  CREATE UNIQUE INDEX relay_items_source    ON relay_items (source) WHERE source IS NOT NULL;
  CREATE INDEX        relay_items_live      ON relay_items (person, seq) WHERE live = 1;
  CREATE INDEX        relay_items_listening ON relay_items (person, origin, session, seq) WHERE listening = 1;
  CREATE INDEX        relay_items_inherit   ON relay_items (person, inherits_from) WHERE inherits_from IS NOT NULL;

  CREATE TABLE relay_entries (
    person TEXT NOT NULL, seq INTEGER NOT NULL,
    item INTEGER NOT NULL, kind TEXT NOT NULL, author TEXT, event_id TEXT,
    folds_into INTEGER, fold_event_id TEXT,
    PRIMARY KEY (person, seq)
  ) WITHOUT ROWID;
  CREATE INDEX relay_entries_item  ON relay_entries (person, item, seq);
  CREATE INDEX relay_entries_event ON relay_entries (person, item, event_id) WHERE event_id IS NOT NULL;
  CREATE INDEX relay_entries_fold  ON relay_entries (person, folds_into) WHERE folds_into IS NOT NULL;
  CREATE INDEX relay_entries_fold_target ON relay_entries (person, item, fold_event_id)
    WHERE fold_event_id IS NOT NULL;
  CREATE INDEX relay_entries_item_kind ON relay_entries (person, item, kind, author, seq);

  CREATE TABLE relay_feed (
    person TEXT NOT NULL, origin TEXT NOT NULL, seq INTEGER NOT NULL,
    session TEXT, item INTEGER, kind TEXT, author TEXT,
    actionable INTEGER NOT NULL DEFAULT 0,
    baseline INTEGER NOT NULL DEFAULT 0,
    receipt INTEGER NOT NULL DEFAULT 0,
    acted INTEGER NOT NULL DEFAULT 0,
    baseline_attempt TEXT,
    PRIMARY KEY (person, origin, seq)
  ) WITHOUT ROWID;
  CREATE INDEX relay_feed_session ON relay_feed (person, origin, session, seq);
  CREATE INDEX relay_feed_item    ON relay_feed (person, item, seq);
  CREATE INDEX relay_feed_revision ON relay_feed (person, item, origin, session)
    WHERE kind = 'revision' AND author = 'origin';
  CREATE INDEX relay_feed_unacted ON relay_feed (person, origin, session, seq)
    WHERE actionable = 1 AND (receipt = 0 OR acted = 0);
  CREATE INDEX relay_feed_baseline ON relay_feed (person, origin, baseline_attempt, seq)
    WHERE baseline = 1 AND (author IS NOT 'person' OR (receipt < 2 AND acted = 0));

  CREATE TABLE relay_advisories (
    person TEXT NOT NULL, origin TEXT NOT NULL, session TEXT NOT NULL, seq INTEGER NOT NULL,
    item INTEGER, advisory_id TEXT NOT NULL,
    PRIMARY KEY (person, origin, session, seq)
  ) WITHOUT ROWID;
  CREATE UNIQUE INDEX relay_advisories_id   ON relay_advisories (person, origin, session, advisory_id);
  CREATE INDEX        relay_advisories_item ON relay_advisories (person, item, seq);

  CREATE TABLE relay_sessions (
    person TEXT NOT NULL, origin TEXT NOT NULL, session TEXT NOT NULL,
    gone INTEGER NOT NULL, origin_listener INTEGER NOT NULL,
    PRIMARY KEY (person, origin, session)
  ) WITHOUT ROWID;

  CREATE TABLE relay_authority   (owner BLOB NOT NULL, key BLOB NOT NULL, PRIMARY KEY (owner, key)) WITHOUT ROWID;
  CREATE TABLE relay_upload_refs (upload_id TEXT NOT NULL, key BLOB NOT NULL, PRIMARY KEY (upload_id, key)) WITHOUT ROWID;
  CREATE INDEX relay_authority_key ON relay_authority (key);
  CREATE INDEX relay_upload_refs_key ON relay_upload_refs (key);

  CREATE TABLE relay_index_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    index_version INTEGER NOT NULL, liveness_version INTEGER NOT NULL, generation INTEGER NOT NULL
  );
  """

  @tables ~w(relay_items relay_entries relay_feed relay_advisories relay_sessions
             relay_authority relay_upload_refs relay_index_state)
  @item_facts [:messages, :chat_read_positions, :bounce_revisions, :item_availability]
  @entry_kinds ~w(discussion reply revision)

  @fold_target """
  (SELECT target.seq FROM relay_entries AS target
   WHERE target.person = fact.person AND target.item = fact.item
     AND target.event_id = fact.fold_event_id AND target.kind IN ('discussion', 'reply', 'revision')
   ORDER BY target.seq DESC LIMIT 1)
  """

  @item_summary """
  last_entry = (SELECT max(seq) FROM relay_entries WHERE person = item.person AND item = item.seq),
  last_reply = (SELECT max(seq) FROM relay_entries WHERE person = item.person AND item = item.seq
                AND kind = 'reply' AND author = 'origin'),
  chat_started = EXISTS (SELECT 1 FROM relay_entries WHERE person = item.person AND item = item.seq
                         AND kind IN ('discussion', 'reply')),
  availability_entry = (SELECT max(seq) FROM relay_entries WHERE person = item.person AND item = item.seq
                        AND kind = 'availability'),
  discussed = EXISTS (SELECT 1 FROM relay_entries WHERE person = item.person AND item = item.seq
                      AND kind = 'discussion' AND author = 'person'),
  origin_revision = EXISTS (SELECT 1 FROM relay_feed WHERE person = item.person AND item = item.seq
                             AND origin = item.origin AND session = item.session
                             AND kind = 'revision' AND author = 'origin')
  """

  @base_live "(state IS NULL OR coalesce(last_reply, 0) > read_position OR bounce_pending = 1)"

  def create(db, generation) do
    with :ok <- Sqlite3.execute(db, @schema),
         :ok <- timed(:seed_items, fn -> seed_items(db) end),
         :ok <- timed(:seed_records, fn -> seed_records(db) end),
         :ok <-
           traced_sql(
             db,
             :folds,
             "UPDATE relay_entries AS fact SET folds_into = #{@fold_target} WHERE fold_event_id IS NOT NULL"
           ),
         :ok <- traced_sql(db, :summaries, "UPDATE relay_items AS item SET #{@item_summary}"),
         :ok <- seed_liveness(db) do
      RecordTable.run(db, "INSERT INTO relay_index_state VALUES (1, ?, ?, ?)", [
        @index_version,
        @liveness_version,
        generation
      ])
    end
  end

  defp timed(phase, run) do
    IO.puts("derive_begin #{phase}")
    {micros, result} = :timer.tc(run)
    IO.puts("derive_end #{phase} #{micros}")
    result
  end

  defp traced_sql(db, phase, sql) do
    {:ok, plan} = RecordTable.select(db, "EXPLAIN QUERY PLAN " <> sql)
    IO.puts("derive_plan #{phase} #{inspect(plan, width: :infinity)}")
    timed(phase, fn -> Sqlite3.execute(db, sql) end)
  end

  def ensure(db, generation) do
    case RecordTable.select(db, "SELECT index_version, liveness_version, generation FROM relay_index_state WHERE id = 1") do
      {:ok, [[@index_version, @liveness_version, ^generation]]} ->
        :ok

      {:ok, [[index, live, _]]} when index > @index_version or live > @liveness_version ->
        {:error, {:unsupported_derived_indexes, index, live}}

      {:ok, _} ->
        rebuild(db, generation)

      error ->
        error
    end
  end

  defp rebuild(db, generation) do
    RecordTable.transaction(db, fn ->
      with {:ok, ^generation} <- RecordTable.generation(db),
           :ok <- each(@tables, &Sqlite3.execute(db, "DROP TABLE #{&1}")),
           :ok <- create(db, generation) do
        {:ok, :rebuilt}
      else
        {:ok, _} -> {:error, :stale_generation}
        error -> error
      end
    end)
    |> case do
      {:ok, :rebuilt} -> :ok
      error -> error
    end
  end

  # Only the rows affected by the transition enter the persistence worker.
  def changes(old, next, touched) do
    addresses = Enum.map(touched, &derived_address/1) |> MapSet.new()

    Enum.reduce(addresses, %{remove: [], add: [], items: MapSet.new(), folds: MapSet.new()}, fn address, acc ->
      before = row_changes(old, address)
      after_rows = row_changes(next, address)

      if before == after_rows do
        acc
      else
        %{
          remove: before.rows ++ acc.remove,
          add: after_rows.rows ++ acc.add,
          items: acc.items |> MapSet.union(before.items) |> MapSet.union(after_rows.items),
          folds: acc.folds |> MapSet.union(before.folds) |> MapSet.union(after_rows.folds)
        }
      end
    end)
  end

  def write(db, changes) do
    with {:ok, seeds} <- old_ancestors(db, changes.items),
         :ok <- each(changes.remove, &delete(db, &1)),
         :ok <- each(changes.add, &insert(db, &1)),
         :ok <- each(changes.folds, &refresh_fold(db, &1)),
         :ok <- each(changes.items, &refresh_item(db, &1)) do
      each(seeds, &refresh_liveness(db, &1))
    end
  end

  def advance(db, generation) do
    with :ok <- RecordTable.run(db, """
                UPDATE relay_index_state SET generation = ?
                WHERE id = 1 AND generation = ? AND index_version = ? AND liveness_version = ?
                """, [generation, generation - 1, @index_version, @liveness_version]),
         {:ok, 1} <- Sqlite3.changes(db) do
      :ok
    else
      {:ok, _} -> {:error, :stale_derived_indexes}
      error -> error
    end
  end

  # Item identities must exist before resolving records that refer to them.
  # Later passes stream the stored values and hold one decoded record at a time.
  defp seed_items(db) do
    RecordTable.scan_keys(db, fn keys ->
      each(keys, fn
        key when is_binary(key) ->
          with {:ok, address} <- RecordCodec.decode_key(key), do: seed_item(db, address)

        error ->
          error
      end)
    end)
  catch
    {:derived_seed_error, error} -> error
  end

  defp seed_item(db, {:row, :messages, {person, _, _} = key} = address) do
    record = stored_value(db, address)
    values = item_fields(key, record) ++ [nil, 0, 0, 0]
    insert(db, {"relay_items", [person, record.accepted_seq], values})
  end

  defp seed_item(_db, _address), do: :ok

  defp seed_records(db) do
    lookup = &stored_value(db, &1)
    item_position = &stored_item_position(db, &1)

    RecordTable.scan(db, fn rows ->
      each(Stream.with_index(rows, 1), fn {row, number} ->
        if rem(number, 100_000) == 0, do: IO.puts("derive_rows #{number}")
        with {:ok, address, value} <- RecordCodec.decode_row(row),
             :ok <- seed_primary(db, address, value, lookup, item_position) do
          each(links(address, value), &insert(db, &1))
        end
      end)
    end)
  catch
    {:derived_seed_error, error} -> error
  end

  defp seed_primary(db, {:row, :messages, {person, _, _} = key}, value, lookup, item_position) do
    RecordTable.run(db, """
    UPDATE relay_items SET inherits_from = ?, read_position = ?, bounce_pending = ?, listening = ?
    WHERE person = ? AND seq = ?
    """, item_facts(key, value, lookup, item_position) ++ [person, value.accepted_seq])
  end

  defp seed_primary(db, address, value, lookup, item_position) do
    rows = primary_rows(address, value, lookup, item_position)
    each(rows.rows, &insert(db, &1))
  end

  defp stored_value(db, address) do
    key = RecordCodec.encode_key(address)

    case RecordTable.select(db, "SELECT payload, checksum FROM relay_records WHERE key = ?", [{:blob, key}]) do
      {:ok, []} ->
        nil

      {:ok, [[payload, checksum]]} ->
        case RecordCodec.decode_row({key, payload, checksum}) do
          {:ok, ^address, value} -> value
          error -> throw({:derived_seed_error, error})
        end

      error ->
        throw({:derived_seed_error, error})
    end
  end

  defp stored_item_position(db, {person, sender, message_id}) do
    case RecordTable.select(db, "SELECT seq FROM relay_items WHERE person = ? AND sender = ? AND message_id = ?", [
           person, sender, message_id
         ]) do
      {:ok, [[seq]]} -> seq
      {:ok, []} -> nil
      error -> throw({:derived_seed_error, error})
    end
  end

  defp stored_item_position(_db, nil), do: nil

  defp derived_address({:row, field, key}) when field in @item_facts, do: {:row, :messages, key}

  defp derived_address({:row, field, {person, origin, seq}})
       when field in [:origin_actions, :origin_receipts],
       do: {:origin_feed, {person, origin}, {:entry, seq}}

  defp derived_address(address), do: address

  defp row_changes(engine, address) do
    lookup = fn address ->
      case RecordCodec.lookup(engine, address) do
        {:ok, value} -> value
        :error -> nil
      end
    end

    case RecordCodec.lookup(engine, address) do
      {:ok, value} ->
        item_position = &get_in(engine.messages, [&1, :accepted_seq])
        rows = primary_rows(address, value, lookup, item_position)
        %{rows | rows: rows.rows ++ links(address, value)}

      :error ->
        empty()
    end
  end

  defp primary_rows({:row, :messages, {person, _, _} = key}, record, lookup, item_position) do
    seq = record.accepted_seq
    values = item_fields(key, record) ++ item_facts(key, record, lookup, item_position)
    row("relay_items", [person, seq], values, {person, seq})
  end

  defp primary_rows({:row, :conversation_events, {person, seq}}, %{key: key, payload: payload}, _, item_position) do
    case item_position.(key) do
      item when is_integer(item) ->
        kind = payload["kind"]
        event = payload["event_id"]
        target = Resources.delivery_target(payload)
        fold = if kind in @entry_kinds, do: event, else: target

        row("relay_entries", [person, seq], [item, kind, payload["author"], event, nil, target],
          {person, item}, if(is_binary(fold), do: {person, item, fold}))

      _ ->
        empty()
    end
  end

  defp primary_rows({:origin_feed, {person, origin}, {:entry, seq}}, entry, lookup, item_position) do
    item = item_position.({person, entry["sender_id"], entry["message_id"]})
    facts = {person, origin, seq}

    row("relay_feed", [person, origin, seq], [
      entry["session_handle"], item, entry["kind"], entry["author"],
      flag(Conversation.actionable?(entry)), flag(DeliveryBaseline.baseline_entry?(entry)),
      receipt_level(lookup.({:row, :origin_receipts, facts})),
      flag(not is_nil(lookup.({:row, :origin_actions, facts}))), Conversation.answer_baseline_attempt(entry)
    ], if(item, do: {person, item}))
  end

  defp primary_rows({:advisory, {person, origin, session}, {:entry, seq}}, entry, _, item_position) do
    item = item_position.({person, entry["sender_id"], entry["message_id"]})
    row("relay_advisories", [person, origin, session, seq], [item, entry["advisory_id"]])
  end

  defp primary_rows({:row, :origin_sessions, {person, origin, session}}, data, _, _) do
    row("relay_sessions", [person, origin, session], [
      flag(data.gone), flag(get_in(data, [:support, "origin_listener"]) == true)
    ])
  end

  defp primary_rows(_, _, _, _), do: empty()

  defp item_fields({_person, sender, id}, record) do
    source = KeyedValidation.source_key(record)

    [sender, id, get_in(record, [:terminal, :state]), record.origin, record.session_handle,
     flag(Map.get(record, :agent_api_v2) == true), if(source, do: blob(source))]
  end

  defp item_facts(key, record, lookup, item_position) do
    parent = item_position.(Map.get(record, :placement_origin_target))
    listening = match?({"listening", _}, lookup.({:row, :item_availability, key}))

    [parent, lookup.({:row, :chat_read_positions, key}) || 0,
     flag(BounceDecision.pending_selection?(lookup.({:row, :bounce_revisions, key}))), flag(listening)]
  end

  defp links(address, value) do
    key = {:blob, RecordCodec.encode_key(address)}
    owners = KeyedValidation.record_authority(address, value) |> Enum.uniq()
    uploads = KeyedValidation.record_uploads(address, value) |> Enum.uniq()

    Enum.map(owners, &{"relay_authority", [blob(&1), key], []}) ++
      Enum.map(uploads, &{"relay_upload_refs", [&1, key], []})
  end

  defp receipt_level(nil), do: 0
  defp receipt_level(%{implicit_ack: true}), do: 1
  defp receipt_level(_), do: 2

  defp flag(true), do: 1
  defp flag(false), do: 0

  defp blob(term), do: {:blob, :erlang.term_to_binary(term, [:deterministic, {:minor_version, 2}])}

  defp empty, do: %{rows: [], items: MapSet.new(), folds: MapSet.new()}

  defp row(table, key, values, item \\ nil, fold \\ nil) do
    %{
      rows: [{table, key, values}],
      items: if(item, do: MapSet.new([item]), else: MapSet.new()),
      folds: if(fold, do: MapSet.new([fold]), else: MapSet.new())
    }
  end

  @columns %{
    "relay_items" => {~w(person seq), ~w(sender message_id state origin session agent_v2 source inherits_from read_position bounce_pending listening)},
    "relay_entries" => {~w(person seq), ~w(item kind author event_id folds_into fold_event_id)},
    "relay_feed" => {~w(person origin seq), ~w(session item kind author actionable baseline receipt acted baseline_attempt)},
    "relay_advisories" => {~w(person origin session seq), ~w(item advisory_id)},
    "relay_sessions" => {~w(person origin session), ~w(gone origin_listener)},
    "relay_authority" => {~w(owner key), []},
    "relay_upload_refs" => {~w(upload_id key), []}
  }

  defp insert(db, {table, key, values}) do
    {keys, fields} = Map.fetch!(@columns, table)
    columns = keys ++ fields
    names = Enum.join(columns, ", ")
    params = Enum.map_join(columns, ", ", fn _ -> "?" end)
    RecordTable.run(db, "INSERT INTO #{table} (#{names}) VALUES (#{params})", key ++ values)
  end

  defp delete(db, {table, key, _values}) do
    {keys, _fields} = Map.fetch!(@columns, table)
    where = Enum.map_join(keys, " AND ", &"#{&1} = ?")
    RecordTable.run(db, "DELETE FROM #{table} WHERE #{where}", key)
  end

  defp refresh_fold(db, {person, item, event}) do
    RecordTable.run(db, """
    UPDATE relay_entries AS fact SET folds_into = #{@fold_target}
    WHERE person = ? AND item = ? AND fold_event_id = ?
    """, [person, item, event])
  end

  defp refresh_item(db, {person, item}) do
    RecordTable.run(db, "UPDATE relay_items AS item SET #{@item_summary} WHERE person = ? AND seq = ?", [person, item])
  end

  defp seed_liveness(db) do
    traced_sql(db, :liveness, """
    WITH RECURSIVE live(person, seq) AS (
      SELECT person, seq FROM relay_items WHERE #{@base_live}
      UNION
      SELECT item.person, item.inherits_from FROM relay_items AS item
      JOIN live ON live.person = item.person AND live.seq = item.seq
      WHERE item.inherits_from IS NOT NULL
    )
    UPDATE relay_items SET live = 1 WHERE (person, seq) IN live
    """)
  end

  @ancestors """
  ancestors(seq) AS (
    VALUES (?)
    UNION
    SELECT inherits_from FROM relay_items JOIN ancestors ON relay_items.seq = ancestors.seq
    WHERE person = ? AND inherits_from IS NOT NULL
  )
  """

  defp old_ancestors(db, items) do
    Enum.reduce_while(items, {:ok, items}, fn {person, item}, {:ok, affected} ->
      case RecordTable.select(db, "WITH RECURSIVE #{@ancestors} SELECT seq FROM ancestors", [item, person]) do
        {:ok, seqs} -> {:cont, {:ok, Enum.reduce(seqs, affected, fn [seq], set -> MapSet.put(set, {person, seq}) end)}}
        error -> {:halt, error}
      end
    end)
  end

  defp refresh_liveness(db, {person, item}) do
    RecordTable.run(db, """
    WITH RECURSIVE #{@ancestors},
    descendants(root, seq) AS (
      SELECT seq, seq FROM ancestors
      UNION
      SELECT descendants.root, child.seq FROM relay_items AS child
      JOIN descendants ON child.inherits_from = descendants.seq WHERE child.person = ?
    )
    UPDATE relay_items AS item SET live = EXISTS (
      SELECT 1 FROM descendants JOIN relay_items AS child USING (seq)
      WHERE root = item.seq AND child.person = item.person AND #{@base_live}
    )
    WHERE person = ? AND seq IN ancestors
    """, [item, person, person, person])
  end

  defp each(values, fun) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case fun.(value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

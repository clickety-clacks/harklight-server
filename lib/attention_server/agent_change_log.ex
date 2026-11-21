defmodule AttentionServer.AgentChangeLog do
  @moduledoc """
  The per-agent change log behind the agent API. Its scope is the enrolled
  key within the person's account. Session filters select rows while keeping
  the same cursor.
  """

  import AttentionServer.ChangeLog.SQLite, only: [all: 3, one: 3, run: 3]

  alias AttentionServer.{ChangeLog, Engine, Store}
  alias Exqlite.Sqlite3

  @schema """
  CREATE TABLE IF NOT EXISTS agent_change_log_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    store_version INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS agent_change_log_meta (
    person_id TEXT NOT NULL,
    agent_key TEXT NOT NULL,
    head INTEGER NOT NULL,
    floor INTEGER NOT NULL,
    seed_rev INTEGER NOT NULL,
    PRIMARY KEY (person_id, agent_key)
  ) WITHOUT ROWID;
  CREATE TABLE IF NOT EXISTS agent_changes (
    person_id TEXT NOT NULL,
    agent_key TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    session TEXT,
    at_ms INTEGER,
    PRIMARY KEY (person_id, agent_key, resource_id)
  ) WITHOUT ROWID;
  CREATE INDEX IF NOT EXISTS agent_changes_by_rev
    ON agent_changes (person_id, agent_key, rev) WHERE at_ms IS NOT NULL;
  CREATE INDEX IF NOT EXISTS agent_changes_by_session
    ON agent_changes (person_id, agent_key, session, rev) WHERE at_ms IS NOT NULL;
  CREATE INDEX IF NOT EXISTS agent_changes_by_age
    ON agent_changes (at_ms) WHERE at_ms IS NOT NULL;
  """

  @doc """
  Finds the agent resources changed by one engine transition. Store appends
  these rows in the same transaction as the engine and the person's log.
  """
  def changes(%Engine{} = old, %Engine{} = next, changed) do
    entries =
      for {_, seq} = event <- changed.entries do
        %{key: key, payload: payload} = Map.fetch!(next.conversation_events, event)
        {seq, key, payload}
      end

    messages =
      [
        changed.messages,
        changed.bounce,
        changed.opened,
        for(
          {_seq, key, %{"kind" => kind}} <- entries,
          kind in ~w(receipt delivery_ack acted),
          do: key
        )
      ]
      |> Enum.concat()
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.flat_map(fn key ->
        item_row(next, key, :message, fn -> ChangeLog.item_resource(key) end)
      end)

    entries =
      Enum.flat_map(entries, fn {seq, key, _payload} ->
        item_row(next, key, :entry, fn -> ChangeLog.entry_resource(seq) end)
      end)

    (messages ++ entries ++ advisory_changes(old, next))
    |> Enum.group_by(fn {agent, _row} -> agent end, fn {_agent, row} -> row end)
  end

  defp item_row(engine, {person, _, _} = key, kind, resource) do
    case Map.get(engine.messages, key) do
      %{origin: origin} = record when is_binary(origin) ->
        [{{person, origin}, {kind, resource.(), Map.get(record, :session_handle)}}]

      _ ->
        []
    end
  end

  defp advisory_changes(old, next) do
    old.advisory_feeds
    |> changed_keys(next.advisory_feeds)
    |> Enum.sort()
    |> Enum.flat_map(fn {person, agent, session} = key ->
      case Map.get(next.advisory_feeds, key) do
        %{high: high, entries: entries} ->
          previous_high = get_in(old.advisory_feeds, [key, :high]) || 0

          for seq <- (previous_high + 1)..high//1,
              %{"advisory_id" => id} <- [Map.get(entries, seq)],
              do: {{person, agent}, {:advisory, "advisory:" <> id, session}}

        nil ->
          []
      end
    end)
  end

  defp changed_keys(old, next) when old === next, do: []

  defp changed_keys(old, next) do
    for {key, value} <- next, Map.fetch(old, key) !== {:ok, value}, do: key
  end

  @doc "Enables the log at startup, seeding only agents with retained messages."
  def enable(db, engine, version) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- Sqlite3.execute(db, @schema),
         :ok <- reconcile(db, engine, version),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp reconcile(db, engine, version) do
    case one(db, "SELECT store_version FROM agent_change_log_state WHERE id = 1", []) do
      {:ok, [^version]} ->
        :ok

      {:ok, nil} ->
        with :ok <- seed(db, engine) do
          run(db, "INSERT INTO agent_change_log_state VALUES (1, ?)", [version])
        end

      {:ok, [_stale]} ->
        # Commits made with the flag off did not append rows. Invalidate
        # existing cursors and give agents first seen while off a baseline.
        with :ok <-
               run(
                 db,
                 "UPDATE agent_change_log_meta SET head = head + 1, floor = head + 1, seed_rev = head + 1",
                 []
               ),
             :ok <- run(db, "DELETE FROM agent_changes", []),
             :ok <- seed(db, engine) do
          run(db, "UPDATE agent_change_log_state SET store_version = ? WHERE id = 1", [version])
        end

      error ->
        error
    end
  end

  defp seed(db, engine) do
    engine.messages
    |> Enum.reduce(MapSet.new(), fn
      {{person, _, _}, %{origin: agent}}, agents when is_binary(agent) ->
        MapSet.put(agents, {person, agent})

      _, agents ->
        agents
    end)
    |> Enum.reduce_while(:ok, fn {person, agent}, :ok ->
      case run(db, "INSERT OR IGNORE INTO agent_change_log_meta VALUES (?, ?, 1, 1, 1)", [
             person,
             agent
           ]) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc "Appends the pending changes inside the store's snapshot transaction."
  def append(db, pending, version) do
    now = System.system_time(:millisecond)

    result =
      Enum.reduce_while(pending, {:ok, %{}}, fn {{person, agent} = scope, rows}, {:ok, heads} ->
        case append_agent(db, scope, rows, now) do
          {:ok, head} -> {:cont, {:ok, Map.put(heads, {:agent, person, agent}, head)}}
          error -> {:halt, error}
        end
      end)

    with {:ok, heads} <- result,
         :ok <-
           run(db, "UPDATE agent_change_log_state SET store_version = ? WHERE id = 1", [version]),
         {:ok, 1} <- Sqlite3.changes(db) do
      {:ok, heads}
    else
      {:ok, 0} -> {:error, :change_log_unavailable}
      error -> error
    end
  end

  defp append_agent(db, {person, agent} = scope, rows, now) do
    with {:ok, {head, _, _}} <- meta(db, scope),
         {:ok, head} <- write_rows(db, scope, rows, head, now),
         :ok <-
           run(
             db,
             """
             INSERT INTO agent_change_log_meta VALUES (?, ?, ?, 0, 0)
             ON CONFLICT (person_id, agent_key) DO UPDATE SET head = excluded.head
             """,
             [person, agent, head]
           ) do
      {:ok, head}
    end
  end

  defp write_rows(db, {person, agent}, rows, head, now) do
    Enum.reduce_while(rows, {:ok, head}, fn {_kind, resource_id, session}, {:ok, previous} ->
      rev = previous + 1

      case run(
             db,
             """
             INSERT INTO agent_changes VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (person_id, agent_key, resource_id) DO UPDATE SET
               rev = excluded.rev, session = excluded.session, at_ms = excluded.at_ms
             """,
             [person, agent, resource_id, rev, session, now]
           ) do
        :ok -> {:cont, {:ok, rev}}
        error -> {:halt, error}
      end
    end)
  end

  defp meta(db, {person, agent}) do
    case one(
           db,
           "SELECT head, floor, seed_rev FROM agent_change_log_meta WHERE person_id = ? AND agent_key = ?",
           [person, agent]
         ) do
      {:ok, [head, floor, seed_rev]} -> {:ok, {head, floor, seed_rev}}
      {:ok, nil} -> {:ok, {0, 0, 0}}
      error -> error
    end
  end

  @doc "The agent-wide position, independent of the person's position."
  def head(%Store{change_log: true, db: db}, scope) do
    with {:ok, {head, _, _}} <- meta(db, scope), do: {:ok, head}
  end

  def head(%Store{}, _scope), do: {:error, :disabled}

  @doc "The current rev of a resource already authorized by the caller."
  def rev(%Store{change_log: true, db: db}, {person, agent} = scope, resource) do
    case one(
           db,
           "SELECT rev FROM agent_changes WHERE person_id = ? AND agent_key = ? AND resource_id = ?",
           [person, agent, resource_id(resource)]
         ) do
      {:ok, [rev]} ->
        {:ok, rev}

      {:ok, nil} ->
        case meta(db, scope) do
          {:ok, {_, _, seed_rev}} when seed_rev > 0 -> {:ok, seed_rev}
          {:ok, _} -> {:error, :not_found}
          error -> error
        end

      error ->
        error
    end
  end

  def rev(%Store{}, _scope, _resource), do: {:error, :disabled}

  @doc "Reads one bounded page, advancing across sessions even when no row matches."
  def read(%Store{change_log: true, db: db}, {person, agent} = scope, after_rev, limit, session) do
    with {:ok, {head, floor, _}} <- meta(db, scope),
         true <- after_rev >= floor and after_rev <= head,
         {:ok, rows} <- page(db, person, agent, after_rev, limit + 1, session) do
      {page, rest} = Enum.split(rows, limit)

      changes =
        Enum.map(page, fn [rev, id, row_session] ->
          {kind, _} = resource = decode_resource(person, id)
          %{rev: rev, kind: kind, resource: resource, session: row_session}
        end)

      more = rest != []
      position = if more, do: List.last(changes).rev, else: head
      {:ok, changes, more, position}
    else
      false -> {:error, :cursor_expired}
      error -> error
    end
  end

  def read(%Store{}, _scope, _after, _limit, _session), do: {:error, :disabled}

  defp page(db, person, agent, after_rev, limit, nil) do
    all(
      db,
      """
      SELECT rev, resource_id, session FROM agent_changes
      WHERE person_id = ? AND agent_key = ? AND rev > ? AND at_ms IS NOT NULL
      ORDER BY rev LIMIT ?
      """,
      [person, agent, after_rev, limit]
    )
  end

  defp page(db, person, agent, after_rev, limit, session) do
    all(
      db,
      """
      SELECT rev, resource_id, session FROM agent_changes
      WHERE person_id = ? AND agent_key = ? AND session = ? AND rev > ? AND at_ms IS NOT NULL
      ORDER BY rev LIMIT ?
      """,
      [person, agent, session, after_rev, limit]
    )
  end

  defp resource_id({:message, key}), do: ChangeLog.item_resource(key)
  defp resource_id({:entry, seq}), do: ChangeLog.entry_resource(seq)
  defp resource_id({:advisory, id}), do: "advisory:" <> id

  defp decode_resource(_person, "advisory:" <> id), do: {:advisory, id}

  defp decode_resource(person, resource_id) do
    case ChangeLog.decode_resource(resource_id) do
      {:item, {sender, id}} -> {:message, {person, sender, id}}
      {:entry, seq} -> {:entry, seq}
    end
  end

  @doc """
  Advances each affected agent's floor inside the change log's prune
  transaction. Retired rows retain just their resource revision, which reads
  still need in order to stamp snapshots.
  """
  def prune(db, before_ms) do
    with {:ok, floors} <-
           all(
             db,
             """
             SELECT person_id, agent_key, MAX(rev) FROM agent_changes
             WHERE at_ms < ? GROUP BY person_id, agent_key
             """,
             [before_ms]
           ),
         :ok <- raise_floors(db, floors) do
      run(db, "UPDATE agent_changes SET at_ms = NULL, session = NULL WHERE at_ms < ?", [before_ms])
    end
  end

  defp raise_floors(db, floors) do
    Enum.reduce_while(floors, :ok, fn [person, agent, rev], :ok ->
      case run(
             db,
             "UPDATE agent_change_log_meta SET floor = MAX(floor, ?) WHERE person_id = ? AND agent_key = ?",
             [rev, person, agent]
           ) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

defmodule AttentionServer.ChangeLog do
  @moduledoc """
  The per-person change log behind client API v2 (see
  `docs/change-log-interface.md` and `docs/client-api-v2.md`).

  Every client-visible change is found by one diff, `changes/2`, which runs
  inside `Store.commit/3` against the previous and next engine. Its rows are
  written by `append/4` in the same SQLite transaction as the snapshot, so a
  change and its rows commit or fail together.

  A row is `{rev, kind, resource_id}`. It carries no payload: a resource keeps
  only its latest row, older rows are deleted when a newer one is written, and
  a reader renders each delivered row from current state. A client therefore
  receives each changed resource once, at its latest `rev`, in increasing
  `rev` order with no gap.

  Rows are written only while `Store.enable_change_log/1` has been called,
  which the Relay does at startup when the `client_api_v2` flag is on.
  """

  alias AttentionServer.{Engine, JSON}
  alias Exqlite.Sqlite3

  @registry AttentionServer.ChangeLog.Registry

  @type person :: String.t()
  @type position :: non_neg_integer()
  @type rev :: pos_integer()
  @type kind :: :item | :item_removed | :entry | :counts | :settings
  @type resource_id :: String.t()
  @type change :: %{rev: rev, kind: kind, resource_id: resource_id}
  @type pending :: %{person => [{kind, resource_id}]}

  @kinds %{
    item: "item",
    item_removed: "item_removed",
    entry: "entry",
    counts: "counts",
    settings: "settings"
  }
  @kind_atoms Map.new(@kinds, fn {atom, text} -> {text, atom} end)

  @schema [
    """
    CREATE TABLE IF NOT EXISTS client_changes (
      person_id TEXT NOT NULL,
      rev INTEGER NOT NULL,
      kind TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      at_ms INTEGER NOT NULL,
      PRIMARY KEY (person_id, rev)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_resource_revs (
      person_id TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      rev INTEGER NOT NULL,
      PRIMARY KEY (person_id, resource_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_change_log_meta (
      person_id TEXT PRIMARY KEY,
      head INTEGER NOT NULL,
      floor INTEGER NOT NULL,
      seed_rev INTEGER NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_resource_created (
      person_id TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (person_id, resource_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_rank_keys (
      person_id TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      rank_key TEXT NOT NULL,
      PRIMARY KEY (person_id, resource_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_change_log_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      store_version INTEGER NOT NULL
    )
    """
  ]

  # -- resource identifiers ---------------------------------------------------

  @doc "The log's resource ID for an item, from its engine key."
  def item_resource({_person, sender, message_id}),
    do: "item:" <> Base.url_encode64(JSON.encode!([sender, message_id]), padding: false)

  @doc "The log's resource ID for a conversation entry, from its conversation sequence."
  def entry_resource(seq) when is_integer(seq), do: "entry:" <> Integer.to_string(seq)

  @doc "Decodes a resource ID into `{:item, {sender, message_id}}`, `{:entry, seq}`, `:counts` or `:settings`."
  def decode_resource("item:" <> encoded) do
    with {:ok, json} <- Base.url_decode64(encoded, padding: false),
         {:ok, [sender, id]} when is_binary(sender) and is_binary(id) <-
           JSON.decode_line(json <> "\n") do
      {:item, {sender, id}}
    else
      _ -> :error
    end
  end

  def decode_resource("entry:" <> seq) do
    case Integer.parse(seq) do
      {value, ""} when value > 0 -> {:entry, value}
      _ -> :error
    end
  end

  def decode_resource("counts"), do: :counts
  def decode_resource("settings"), do: :settings
  def decode_resource(_), do: :error

  # -- the one diff -----------------------------------------------------------

  @doc """
  The rows `old -> next` produces, per person, in apply order. Pure.

  These are the engine inputs of the client API resources. A projection input
  added to `ClientAPI.Resources` must be added here too; the replay property
  test fails otherwise.
  """
  @spec changes(struct(), struct()) :: pending
  def changes(%Engine{} = old, %Engine{} = next) when old === next, do: %{}

  def changes(%Engine{} = old, %Engine{} = next) do
    item_keys =
      [
        changed_keys(old.messages, next.messages),
        changed_keys(old.chat_read_positions, next.chat_read_positions),
        changed_keys(
          old.route_b_index.conversation_summary_by_item,
          next.route_b_index.conversation_summary_by_item
        ),
        changed_keys(old.bounce_revisions, next.bounce_revisions),
        opened_keys(old.opened_revisions, next.opened_revisions),
        subscription_keys(old, next)
      ]
      |> Enum.concat()
      |> Enum.uniq()

    items =
      Enum.map(item_keys, fn {person, _, _} = key ->
        kind = if Map.has_key?(next.messages, key), do: :item, else: :item_removed
        {person, {kind, item_resource(key)}}
      end)

    entries =
      old.conversation_events
      |> changed_keys(next.conversation_events)
      |> Enum.filter(&Map.has_key?(next.conversation_events, &1))
      |> Enum.sort()
      |> Enum.map(fn {person, seq} -> {person, {:entry, entry_resource(seq)}} end)

    counts =
      old.route_b_index.unresolved_count_by_person
      |> changed_keys(next.route_b_index.unresolved_count_by_person)
      |> Enum.map(&{&1, {:counts, "counts"}})

    settings =
      [
        changed_keys(old.bounce_levels, next.bounce_levels),
        old.push_bindings
        |> changed_keys(next.push_bindings)
        |> Enum.map(fn {person, _device} -> person end)
      ]
      |> Enum.concat()
      |> Enum.uniq()
      |> Enum.map(&{&1, {:settings, "settings"}})

    (Enum.sort(items) ++ entries ++ counts ++ settings)
    |> Enum.group_by(fn {person, _row} -> person end, fn {_person, row} -> row end)
  end

  defp changed_keys(old, next) when old === next, do: []

  defp changed_keys(old, next) when is_map(old) and is_map(next) do
    changed =
      Enum.flat_map(next, fn {key, value} ->
        case Map.fetch(old, key) do
          {:ok, ^value} -> []
          _ -> [key]
        end
      end)

    removed = for {key, _} <- old, not Map.has_key?(next, key), do: key
    changed ++ removed
  end

  defp opened_keys(old, next) when old === next, do: []

  defp opened_keys(old, next) do
    old
    |> MapSet.symmetric_difference(next)
    |> Enum.map(fn {person, sender, id, _revision} -> {person, sender, id} end)
  end

  # `as` (message or knock) follows the device's subscription, so a changed
  # subscription touches every item of that person from the affected senders.
  defp subscription_keys(%{subscriptions: old}, %{subscriptions: next}) when old === next,
    do: []

  defp subscription_keys(old, next) do
    old.subscriptions
    |> changed_keys(next.subscriptions)
    |> Enum.map(fn {person, _device} -> person end)
    |> Enum.uniq()
    |> Enum.flat_map(fn person ->
      for {{^person, _, _} = key, _} <- next.messages, do: key
    end)
  end

  # -- closing signal ---------------------------------------------------------

  @doc """
  The client API connections `old -> next` must end, from the same commit:
  `{{:device, key}, :revoked}` for a device revoked or removed, and
  `{{:person, person}, :account_paused}` for an account whose delivery stopped.
  Pure.
  """
  def closings(%Engine{} = old, %Engine{} = next) when old === next, do: []

  def closings(%Engine{} = old, %Engine{} = next) do
    devices =
      if old.devices === next.devices do
        []
      else
        for {key, device} <- old.devices,
            not Map.get(device, :revoked, false),
            match?(nil, Map.get(next.devices, key)) or
              Map.get(Map.get(next.devices, key, %{}), :revoked, false),
            do: {{:device, key}, :revoked}
      end

    paused =
      if old.delivery_states === next.delivery_states do
        []
      else
        for {person, "STOPPED"} <- next.delivery_states,
            Map.get(old.delivery_states, person) != "STOPPED",
            do: {{:person, person}, :account_paused}
      end

    devices ++ paused
  end

  @doc """
  Tells every subscriber of each scope that its connections must end:
  `{:client_closing, scope, reason}`. Scopes are `{:device, key}`,
  `{:browser_session, id}` and `{:person, person}`.
  """
  def notify_closing(closings) when is_list(closings) do
    if Process.whereis(@registry) do
      Enum.each(closings, fn {scope, reason} ->
        Registry.dispatch(@registry, {:closing, scope}, fn entries ->
          for {pid, _} <- entries, do: send(pid, {:client_closing, scope, reason})
        end)
      end)
    end

    :ok
  end

  @doc """
  The calling process (a feed) is told `{:client_closing, scope, reason}` when
  its principal is revoked, its browser session ends or its account is paused.
  """
  def subscribe_closing(%{kind: :device, key: key, person: person}),
    do: register_closing([{:device, key}, {:person, person}])

  def subscribe_closing(%{kind: :browser, session_id: session_id, person: person}),
    do: register_closing([{:browser_session, session_id}, {:person, person}])

  defp register_closing(scopes) do
    Enum.each(scopes, fn scope -> Registry.register(@registry, {:closing, scope}, nil) end)
    :ok
  end

  @doc "Whether a person already had client-visible data, which seeds their log at rev 1."
  def seeded?(%Engine{} = engine, person) do
    Enum.any?(engine.messages, fn {{p, _, _}, _} -> p == person end) or
      Enum.any?(engine.conversation_events, fn {{p, _}, _} -> p == person end) or
      Map.has_key?(engine.bounce_levels, person)
  end

  # -- schema and reconciliation ----------------------------------------------

  @doc """
  Creates the tables and reconciles the log with the store snapshot. If the
  store was committed while the log was off, every person's log restarts past
  their head: clients holding an older cursor get `cursor_expired` and
  re-read a snapshot, so nothing changed meanwhile is missed.
  """
  def enable(db, store_version) do
    with :ok <- Enum.reduce_while(@schema, :ok, &create(db, &1, &2)),
         :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- reconcile(db, store_version),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      :ok
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp create(db, sql, :ok) do
    case Sqlite3.execute(db, sql) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end

  defp reconcile(db, store_version) do
    case one(db, "SELECT store_version FROM client_change_log_state WHERE id = 1", []) do
      {:ok, nil} ->
        run(db, "INSERT INTO client_change_log_state (id, store_version) VALUES (1, ?)", [
          store_version
        ])

      {:ok, [^store_version]} ->
        :ok

      {:ok, [_stale]} ->
        with :ok <-
               run(
                 db,
                 "UPDATE client_change_log_meta SET head = head + 1, floor = head + 1, seed_rev = head + 1",
                 []
               ),
             :ok <- run(db, "DELETE FROM client_changes", []),
             :ok <- run(db, "DELETE FROM client_resource_revs", []),
             :ok <- run(db, "DELETE FROM client_rank_keys", []) do
          run(db, "UPDATE client_change_log_state SET store_version = ? WHERE id = 1", [
            store_version
          ])
        end

      error ->
        error
    end
  end

  # -- write side -------------------------------------------------------------

  @doc """
  Inside the caller's open transaction: numbers and writes `pending`, keeps
  only each resource's latest row, and records `store_version` as the
  snapshot this log reflects. `seeded` says, per person in `pending`, whether
  they had data before this commit. Returns each touched person's new head.
  """
  @spec append(term(), pending, %{person => boolean()}, non_neg_integer()) ::
          {:ok, %{person => position}} | {:error, term()}
  def append(db, pending, seeded, store_version) do
    now = System.system_time(:millisecond)

    result =
      Enum.reduce_while(pending, {:ok, %{}}, fn {person, rows}, {:ok, heads} ->
        case append_person(db, person, rows, Map.get(seeded, person, false), now) do
          {:ok, head} -> {:cont, {:ok, Map.put(heads, person, head)}}
          error -> {:halt, error}
        end
      end)

    with {:ok, heads} <- result,
         :ok <-
           run(db, "UPDATE client_change_log_state SET store_version = ? WHERE id = 1", [
             store_version
           ]),
         {:ok, 1} <- Sqlite3.changes(db) do
      {:ok, heads}
    else
      {:ok, 0} -> {:error, :change_log_unavailable}
      error -> error
    end
  end

  defp append_person(db, person, rows, seeded?, now) do
    with {:ok, {head, floor, seed_rev}} <- load_meta(db, person, seeded?),
         {:ok, head} <- write_rows(db, person, rows, head, now),
         :ok <-
           run(
             db,
             "INSERT INTO client_change_log_meta (person_id, head, floor, seed_rev) VALUES (?, ?, ?, ?) " <>
               "ON CONFLICT (person_id) DO UPDATE SET head = excluded.head",
             [person, head, floor, seed_rev]
           ) do
      {:ok, head}
    end
  end

  defp write_rows(db, person, rows, head, now) do
    Enum.reduce_while(rows, {:ok, head}, fn {kind, resource_id}, {:ok, previous_head} ->
      rev = previous_head + 1

      with :ok <-
             run(
               db,
               "DELETE FROM client_changes WHERE person_id = ? AND rev = (SELECT rev FROM client_resource_revs WHERE person_id = ? AND resource_id = ?)",
               [person, person, resource_id]
             ),
           :ok <-
             run(
               db,
               "INSERT INTO client_changes (person_id, rev, kind, resource_id, at_ms) VALUES (?, ?, ?, ?, ?)",
               [person, rev, Map.fetch!(@kinds, kind), resource_id, now]
             ),
           :ok <-
             run(
               db,
               "INSERT INTO client_resource_revs (person_id, resource_id, rev) VALUES (?, ?, ?) " <>
                 "ON CONFLICT (person_id, resource_id) DO UPDATE SET rev = excluded.rev",
               [person, resource_id, rev]
             ),
           :ok <-
             run(
               db,
               "INSERT OR IGNORE INTO client_resource_created (person_id, resource_id, created_at_ms) VALUES (?, ?, ?)",
               [person, resource_id, now]
             ) do
        {:cont, {:ok, rev}}
      else
        error -> {:halt, error}
      end
    end)
  end

  # A person's log starts on first use: at rev 1 with every existing resource
  # stamped rev 1 when they already had data, else empty at 0.
  # `seeded` is a boolean or a zero-arity function, evaluated only when the
  # person has no log yet.
  defp load_meta(db, person, seeded) do
    case one(db, "SELECT head, floor, seed_rev FROM client_change_log_meta WHERE person_id = ?", [
           person
         ]) do
      {:ok, [head, floor, seed_rev]} ->
        {:ok, {head, floor, seed_rev}}

      {:ok, nil} ->
        if (is_function(seeded, 0) and seeded.()) or seeded == true,
          do: {:ok, {1, 1, 1}},
          else: {:ok, {0, 0, 0}}

      error ->
        error
    end
  end

  @doc "Wakes every subscriber of each person after a committed append."
  def notify(heads) when heads == %{}, do: :ok

  def notify(heads) when is_map(heads) do
    # A wake-up only: without the registry (a Store used outside the
    # application) nobody can be subscribed, and the commit stands.
    if Process.whereis(@registry), do: dispatch(heads), else: :ok
  end

  defp dispatch(heads) do
    Enum.each(heads, fn {person, head} ->
      Registry.dispatch(@registry, person, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:change_log, person, head})
      end)
    end)
  end

  # -- read side --------------------------------------------------------------

  # The interface the client API components use: a Store whose change log is
  # enabled. A store without it answers {:error, :disabled}.

  def head(%AttentionServer.Store{change_log: true, db: db, engine: engine}, person),
    do: head(db, engine, person)

  def head(%AttentionServer.Store{}, _person), do: {:error, :disabled}

  def rev(%AttentionServer.Store{change_log: true, db: db, engine: engine}, person, resource_id),
    do: rev(db, engine, person, resource_id)

  def rev(%AttentionServer.Store{}, _person, _resource_id), do: {:error, :disabled}

  def read(
        %AttentionServer.Store{change_log: true, db: db, engine: engine},
        person,
        after_rev,
        limit
      ),
      do: read(db, engine, person, after_rev, limit)

  def read(%AttentionServer.Store{}, _person, _after, _limit), do: {:error, :disabled}

  def prune(%AttentionServer.Store{change_log: true, db: db}, before_ms),
    do: prune_rows(db, before_ms)

  def prune(%AttentionServer.Store{}, _before_ms), do: {:error, :disabled}

  @doc """
  When the log first wrote a row for the resource: an entry's creation time
  for entries created while the log runs. Resources from before the cut-over
  have none (`{:error, :not_found}`).
  """
  def created_at(%AttentionServer.Store{change_log: true, db: db}, person, resource_id) do
    case one(
           db,
           "SELECT created_at_ms FROM client_resource_created WHERE person_id = ? AND resource_id = ?",
           [person, resource_id]
         ) do
      {:ok, [at_ms]} -> {:ok, at_ms}
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  def created_at(%AttentionServer.Store{}, _person, _resource_id), do: {:error, :disabled}

  @doc "The rank key last emitted for each of the person's items, by item resource ID."
  def rank_keys(%AttentionServer.Store{change_log: true, db: db}, person) do
    with {:ok, rows} <-
           all(db, "SELECT resource_id, rank_key FROM client_rank_keys WHERE person_id = ?", [
             person
           ]) do
      {:ok, Map.new(rows, fn [resource_id, key] -> {resource_id, key} end)}
    end
  end

  def rank_keys(%AttentionServer.Store{}, _person), do: {:error, :disabled}

  @doc """
  Records the person's current rank keys (`%{item_resource => key}`, from the
  reads component's ranking) in one transaction. Every item whose key changed
  gets an `:item` row, and items no longer ranked lose their stored key.
  Unchanged keys write nothing. Returns the person's head.
  """
  def append_ranks(%AttentionServer.Store{change_log: true, db: db, engine: engine}, person, keys)
      when is_map(keys) do
    with {:ok, previous} <- rank_keys(%AttentionServer.Store{change_log: true, db: db}, person) do
      changed = for {id, key} <- keys, Map.get(previous, id) != key, do: id
      dropped = for {id, _} <- previous, not Map.has_key?(keys, id), do: id

      if changed == [] and dropped == [] do
        head(db, engine, person)
      else
        write_ranks(db, engine, person, keys, Enum.sort(changed), dropped)
      end
    end
  end

  def append_ranks(%AttentionServer.Store{}, _person, _keys), do: {:error, :disabled}

  defp write_ranks(db, engine, person, keys, changed, dropped) do
    now = System.system_time(:millisecond)

    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           {:ok, {head, floor, seed_rev}} <-
             load_meta(db, person, fn -> seeded?(engine, person) end),
           {:ok, head} <- write_rows(db, person, Enum.map(changed, &{:item, &1}), head, now),
           :ok <-
             run(
               db,
               "INSERT INTO client_change_log_meta (person_id, head, floor, seed_rev) VALUES (?, ?, ?, ?) " <>
                 "ON CONFLICT (person_id) DO UPDATE SET head = excluded.head",
               [person, head, floor, seed_rev]
             ),
           :ok <- store_rank_keys(db, person, Map.take(keys, changed), dropped),
           :ok <- Sqlite3.execute(db, "COMMIT") do
        {:ok, head}
      end

    case result do
      {:ok, head} ->
        notify(%{person => head})
        {:ok, head}

      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp store_rank_keys(db, person, keys, dropped) do
    upserts =
      Enum.map(keys, fn {id, key} ->
        {"INSERT INTO client_rank_keys (person_id, resource_id, rank_key) VALUES (?, ?, ?) " <>
           "ON CONFLICT (person_id, resource_id) DO UPDATE SET rank_key = excluded.rank_key",
         [person, id, key]}
      end)

    deletes =
      Enum.map(dropped, fn id ->
        {"DELETE FROM client_rank_keys WHERE person_id = ? AND resource_id = ?", [person, id]}
      end)

    Enum.reduce_while(upserts ++ deletes, :ok, fn {sql, params}, :ok ->
      case run(db, sql, params) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc "The person's position: every change up to and including it has been written."
  @spec head(term(), struct(), person) :: {:ok, position} | {:error, term()}
  def head(db, %Engine{} = engine, person) do
    with {:ok, {head, _floor, _seed}} <- load_meta(db, person, fn -> seeded?(engine, person) end),
         do: {:ok, head}
  end

  @doc "The resource's current rev; a seeded resource with no row since reads as the seed rev."
  @spec rev(term(), struct(), person, resource_id) :: {:ok, rev} | {:error, :not_found | term()}
  def rev(db, %Engine{} = engine, person, resource_id) do
    case one(
           db,
           "SELECT rev FROM client_resource_revs WHERE person_id = ? AND resource_id = ?",
           [person, resource_id]
         ) do
      {:ok, [rev]} ->
        {:ok, rev}

      {:ok, nil} ->
        case load_meta(db, person, fn -> seeded?(engine, person) end) do
          {:ok, {_head, _floor, seed_rev}} when seed_rev > 0 -> {:ok, seed_rev}
          {:ok, _} -> {:error, :not_found}
          error -> error
        end

      error ->
        error
    end
  end

  @doc """
  Rows with `rev > after`, ascending, at most `limit`. The returned position
  is the last row's rev, or `after` when there are none. `after` below the
  retained floor, or above the head, is `:cursor_expired`.
  """
  @spec read(term(), struct(), person, position, pos_integer()) ::
          {:ok, [change], boolean(), position} | {:error, :cursor_expired | term()}
  def read(db, %Engine{} = engine, person, after_rev, limit)
      when is_integer(after_rev) and after_rev >= 0 and is_integer(limit) and limit > 0 do
    with {:ok, {head, floor, _seed}} <- load_meta(db, person, fn -> seeded?(engine, person) end),
         :ok <- servable(after_rev, floor, head),
         {:ok, rows} <-
           all(
             db,
             "SELECT rev, kind, resource_id FROM client_changes WHERE person_id = ? AND rev > ? ORDER BY rev LIMIT ?",
             [person, after_rev, limit + 1]
           ) do
      {page, rest} = Enum.split(rows, limit)

      changes =
        Enum.map(page, fn [rev, kind, resource_id] ->
          %{rev: rev, kind: Map.fetch!(@kind_atoms, kind), resource_id: resource_id}
        end)

      position = if changes == [], do: after_rev, else: List.last(changes).rev
      {:ok, changes, rest != [], position}
    end
  end

  defp servable(after_rev, floor, head) when after_rev >= floor and after_rev <= head, do: :ok
  defp servable(_after, _floor, _head), do: {:error, :cursor_expired}

  @doc "The caller receives `{:change_log, person, head}` after each committed append for the person."
  def subscribe(person), do: Registry.register(@registry, person, nil) |> subscribed()
  def unsubscribe(person), do: Registry.unregister(@registry, person)

  defp subscribed({:ok, _}), do: :ok
  defp subscribed({:error, {:already_registered, _}}), do: :ok

  @doc """
  Deletes rows written before `before_ms` and raises each affected person's
  floor to the newest deleted rev, so older cursors get `cursor_expired`.
  """
  @spec prune_rows(term(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune_rows(db, before_ms) when is_integer(before_ms) do
    with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         {:ok, floors} <-
           all(
             db,
             "SELECT person_id, MAX(rev), COUNT(*) FROM client_changes WHERE at_ms < ? GROUP BY person_id",
             [before_ms]
           ),
         :ok <- raise_floors(db, floors),
         :ok <- run(db, "DELETE FROM client_changes WHERE at_ms < ?", [before_ms]),
         :ok <- Sqlite3.execute(db, "COMMIT") do
      {:ok, Enum.reduce(floors, 0, fn [_, _, count], acc -> acc + count end)}
    else
      error ->
        _ = Sqlite3.execute(db, "ROLLBACK")
        error
    end
  end

  defp raise_floors(db, floors) do
    Enum.reduce_while(floors, :ok, fn [person, max_rev, _count], :ok ->
      case run(
             db,
             "UPDATE client_change_log_meta SET floor = MAX(floor, ?) WHERE person_id = ?",
             [max_rev, person]
           ) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # -- SQLite helpers ---------------------------------------------------------

  defp one(db, sql, params) do
    statement(db, sql, params, fn stmt ->
      case Sqlite3.step(db, stmt) do
        {:row, row} -> {:ok, row}
        :done -> {:ok, nil}
        error -> error
      end
    end)
  end

  defp all(db, sql, params), do: statement(db, sql, params, &collect(db, &1, []))

  defp collect(db, stmt, acc) do
    case Sqlite3.step(db, stmt) do
      {:row, row} -> collect(db, stmt, [row | acc])
      :done -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp run(db, sql, params) do
    case statement(db, sql, params, &Sqlite3.step(db, &1)) do
      :done -> :ok
      error -> error
    end
  end

  defp statement(db, sql, params, fun) do
    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      result =
        case Sqlite3.bind(stmt, params) do
          :ok -> fun.(stmt)
          error -> error
        end

      _ = Sqlite3.release(db, stmt)
      result
    end
  end
end

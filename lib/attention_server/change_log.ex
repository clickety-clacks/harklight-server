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
  import AttentionServer.ChangeLog.SQLite, only: [all: 3, one: 3, run: 3]

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

  @rank_projection_version 1

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
    CREATE TABLE IF NOT EXISTS client_ranks (
      person_id TEXT NOT NULL,
      item_id TEXT NOT NULL,
      rank_key TEXT NOT NULL,
      rank_json TEXT NOT NULL,
      PRIMARY KEY (person_id, item_id)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS client_ranks_order ON client_ranks (person_id, rank_key)",
    """
    CREATE TABLE IF NOT EXISTS client_rank_state (
      person_id TEXT PRIMARY KEY
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE IF NOT EXISTS client_change_log_epoch (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      epoch INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS client_change_log_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      store_version INTEGER NOT NULL,
      rank_projection_version INTEGER NOT NULL DEFAULT 0
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

  def changes(%Engine{} = old, %Engine{} = next),
    do: changes(old, next, changed_resources(old, next))

  @doc false
  def changed_resources(%Engine{} = old, %Engine{} = next) do
    %{
      messages: changed_keys(old.messages, next.messages),
      bounce: changed_keys(old.bounce_revisions, next.bounce_revisions),
      opened: opened_keys(old.opened_revisions, next.opened_revisions),
      entries:
        old.conversation_events
        |> changed_keys(next.conversation_events)
        |> Enum.filter(&Map.has_key?(next.conversation_events, &1))
        |> Enum.sort()
    }
  end

  @doc false
  def changed_resources(%Engine{} = old, %Engine{} = next, touched) do
    changed =
      Enum.reduce(touched, %{messages: [], bounce: [], opened: [], entries: []}, fn
        {:row, :messages, key}, acc ->
          collect_change(acc, :messages, key, old.messages, next.messages)

        {:row, :bounce_revisions, key}, acc ->
          collect_change(acc, :bounce, key, old.bounce_revisions, next.bounce_revisions)

        {:row, :conversation_events, key}, acc ->
          if Map.has_key?(next.conversation_events, key) do
            collect_change(acc, :entries, key, old.conversation_events, next.conversation_events)
          else
            acc
          end

        {:member, :opened_revisions, {person, sender, id, _} = key}, acc ->
          if MapSet.member?(old.opened_revisions, key) ==
               MapSet.member?(next.opened_revisions, key) do
            acc
          else
            %{acc | opened: [{person, sender, id} | acc.opened]}
          end

        _, acc ->
          acc
      end)

    %{changed | entries: Enum.sort(changed.entries)}
  end

  defp collect_change(changed, kind, key, old, next) do
    if Map.fetch(old, key) === Map.fetch(next, key),
      do: changed,
      else: Map.update!(changed, kind, &[key | &1])
  end

  @doc false
  def changes(%Engine{} = old, %Engine{} = next, changed) do
    item_keys =
      [
        changed.messages,
        changed_keys(old.chat_read_positions, next.chat_read_positions),
        changed_keys(
          old.route_b_index.conversation_summary_by_item,
          next.route_b_index.conversation_summary_by_item
        ),
        changed.bounce,
        changed.opened,
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
      Enum.map(changed.entries, fn {person, seq} -> {person, {:entry, entry_resource(seq)}} end)

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

  @doc """
  The items and entries `old -> next` brings into being, per person, as
  resource IDs: the only resources whose first-seen time this commit stamps.
  Pure.
  """
  def new_resources(%Engine{} = old, %Engine{} = next) when old === next, do: %{}

  def new_resources(%Engine{} = old, %Engine{} = next),
    do: new_resources(old, next, changed_resources(old, next))

  @doc false
  def new_resources(%Engine{} = old, %Engine{} = next, changed) do
    items =
      for {person, _, _} = key <- changed.messages,
          Map.has_key?(next.messages, key) and not Map.has_key?(old.messages, key),
          do: {person, item_resource(key)}

    entries =
      for {person, seq} = key <- changed.entries,
          not Map.has_key?(old.conversation_events, key),
          do: {person, entry_resource(seq)}

    (items ++ entries)
    |> Enum.group_by(fn {person, _} -> person end, fn {_, id} -> id end)
    |> Map.new(fn {person, ids} -> {person, MapSet.new(ids)} end)
  end

  # -- closing signal ---------------------------------------------------------

  @doc """
  The client API connections `old -> next` must end, from the same commit:
  `{{:device, key}, :revoked}` for a device revoked or removed,
  `{{:owner, person}, :revoked}` for a changed owner key or revision (which
  ends that person's browser sessions), and
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

    # A browser session is pinned to its person's owner key and revision, so
    # any change to the owner entry (a revoked or recovered owner key, a new
    # revision) ends every browser session of that person.
    owners =
      if old.owners === next.owners do
        []
      else
        for {person, owner} <- old.owners,
            Map.get(next.owners, person) != owner,
            do: {{:owner, person}, :revoked}
      end

    devices ++ paused ++ owners
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

  def subscribe_closing(%{kind: :browser, session_id: session_id, person: person} = principal) do
    owner_key = Map.get(principal, :key)
    device = if is_binary(owner_key), do: [{:device, owner_key}], else: []

    register_closing(
      [{:browser_session, session_id}, {:owner, person}, {:person, person}] ++ device
    )
  end

  defp register_closing(scopes) do
    Enum.each(scopes, fn scope -> Registry.register(@registry, {:closing, scope}, nil) end)
    :ok
  end

  @doc """
  Ends `subscribe_closing/1` for the calling process, which may serve later
  requests on the same connection.
  """
  def unsubscribe_closing(%{kind: :device, key: key, person: person}),
    do: unregister_closing([{:device, key}, {:person, person}])

  def unsubscribe_closing(%{kind: :browser, session_id: session_id, person: person} = principal) do
    owner_key = Map.get(principal, :key)
    device = if is_binary(owner_key), do: [{:device, owner_key}], else: []

    unregister_closing(
      [{:browser_session, session_id}, {:owner, person}, {:person, person}] ++ device
    )
  end

  defp unregister_closing(scopes) do
    Enum.each(scopes, fn scope -> Registry.unregister(@registry, {:closing, scope}) end)
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
  store was committed while the log was off, or its previous rank projection
  used different keys, every person's log restarts past their head. Clients
  holding an older cursor get `cursor_expired` and re-read a snapshot.
  """
  def enable(db, store_version) do
    with :ok <- Enum.reduce_while(@schema, :ok, &create(db, &1, &2)),
         :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
         :ok <- add_rank_projection_version(db),
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

  defp add_rank_projection_version(db) do
    with {:ok, columns} <- all(db, "PRAGMA table_info(client_change_log_state)", []) do
      if Enum.any?(columns, &match?([_, "rank_projection_version" | _], &1)) do
        :ok
      else
        Sqlite3.execute(
          db,
          "ALTER TABLE client_change_log_state ADD COLUMN rank_projection_version " <>
            "INTEGER NOT NULL DEFAULT 0"
        )
      end
    end
  end

  defp reconcile(db, store_version) do
    case one(
           db,
           "SELECT store_version, rank_projection_version FROM client_change_log_state WHERE id = 1",
           []
         ) do
      {:ok, nil} ->
        run(
          db,
          "INSERT INTO client_change_log_state (id, store_version, rank_projection_version) VALUES (1, ?, ?)",
          [store_version, @rank_projection_version]
        )

      {:ok, [^store_version, @rank_projection_version]} ->
        :ok

      {:ok, [_, _]} ->
        with :ok <-
               run(
                 db,
                 "UPDATE client_change_log_meta SET head = head + 1, floor = head + 1, seed_rev = head + 1",
                 []
               ),
             :ok <- run(db, "DELETE FROM client_changes", []),
             :ok <- run(db, "DELETE FROM client_resource_revs", []),
             :ok <- run(db, "DELETE FROM client_rank_keys", []),
             :ok <- run(db, "DELETE FROM client_ranks", []),
             :ok <- run(db, "DELETE FROM client_rank_state", []),
             # A person with no log yet reads as seeded at the epoch; raising
             # it moves them past any head they were shown before the reset.
             {:ok, epoch} <- epoch(db),
             :ok <-
               run(
                 db,
                 "INSERT INTO client_change_log_epoch (id, epoch) VALUES (1, ?) " <>
                   "ON CONFLICT (id) DO UPDATE SET epoch = excluded.epoch",
                 [epoch + 1]
               ) do
          run(
            db,
            "UPDATE client_change_log_state SET store_version = ?, rank_projection_version = ? WHERE id = 1",
            [store_version, @rank_projection_version]
          )
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
  @spec append(term(), pending, %{person => boolean()}, non_neg_integer(), map()) ::
          {:ok, %{person => position}} | {:error, term()}
  def append(db, pending, seeded, store_version, created \\ %{}) do
    now = System.system_time(:millisecond)

    result =
      Enum.reduce_while(pending, {:ok, %{}}, fn {person, rows}, {:ok, heads} ->
        new = Map.get(created, person, MapSet.new())

        case append_person(db, person, rows, Map.get(seeded, person, false), now, new) do
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

  defp append_person(db, person, rows, seeded?, now, created) do
    with {:ok, {head, floor, seed_rev}} <- load_meta(db, person, seeded?),
         {:ok, head} <- write_rows(db, person, rows, head, now, created),
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

  # `created` holds the resources this commit brings into being; only those
  # get a first-seen time, so a resource from before the cut-over never does.
  defp write_rows(db, person, rows, head, now, created) do
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
           :ok <- stamp_created(db, person, resource_id, now, created) do
        {:cont, {:ok, rev}}
      else
        error -> {:halt, error}
      end
    end)
  end

  # A person's log starts on first use: at rev 1 with every existing resource
  # stamped rev 1 when they already had data, else empty at 0.
  defp stamp_created(db, person, resource_id, now, created) do
    if MapSet.member?(created, resource_id),
      do:
        run(
          db,
          "INSERT OR IGNORE INTO client_resource_created (person_id, resource_id, created_at_ms) VALUES (?, ?, ?)",
          [person, resource_id, now]
        ),
      else: :ok
  end

  # `seeded` is a boolean or a zero-arity function, evaluated only when the
  # person has no log yet. A person with data and no log starts at the
  # epoch: 1 until the log is first reset, then higher.
  defp load_meta(db, person, seeded) do
    case one(db, "SELECT head, floor, seed_rev FROM client_change_log_meta WHERE person_id = ?", [
           person
         ]) do
      {:ok, [head, floor, seed_rev]} ->
        {:ok, {head, floor, seed_rev}}

      {:ok, nil} ->
        if (is_function(seeded, 0) and seeded.()) or seeded == true do
          with {:ok, epoch} <- epoch(db), do: {:ok, {epoch, epoch, epoch}}
        else
          {:ok, {0, 0, 0}}
        end

      error ->
        error
    end
  end

  defp epoch(db) do
    case one(db, "SELECT epoch FROM client_change_log_epoch WHERE id = 1", []) do
      {:ok, [epoch]} -> {:ok, epoch}
      {:ok, nil} -> {:ok, 1}
      error -> error
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

  @doc """
  The rank key last emitted for each of the person's rank units, by the reads
  component's opaque unit ID (an item row or an item's chat-reply row).
  """
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
  Records the person's current rank keys (`%{unit_id => key}`, from the reads
  component's ranking) in one transaction. `item_of` maps a unit ID to its
  item's resource ID (`{:ok, resource}`), or `:error` for an item that no
  longer exists. Each item with a unit whose key changed, appeared or
  disappeared gets one `:item` row; units no longer ranked lose their stored
  key. Unchanged keys write nothing. Returns the person's head.
  """
  def append_ranks(
        %AttentionServer.Store{change_log: true, db: db, engine: engine},
        person,
        keys,
        item_of
      )
      when is_map(keys) and is_function(item_of, 1) do
    with {:ok, previous} <- rank_keys(%AttentionServer.Store{change_log: true, db: db}, person) do
      changed = for {id, key} <- keys, Map.get(previous, id) != key, do: id
      dropped = for {id, _} <- previous, not Map.has_key?(keys, id), do: id

      items =
        (changed ++ dropped)
        |> Enum.flat_map(fn unit ->
          case item_of.(unit) do
            {:ok, resource} -> [resource]
            _ -> []
          end
        end)
        |> Enum.uniq()
        |> Enum.sort()

      if changed == [] and dropped == [] do
        head(db, engine, person)
      else
        write_ranks(db, engine, person, Map.take(keys, changed), items, dropped)
      end
    end
  end

  def append_ranks(%AttentionServer.Store{}, _person, _keys, _item_of), do: {:error, :disabled}

  defp write_ranks(db, engine, person, keys, items, dropped) do
    now = System.system_time(:millisecond)

    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           {:ok, {head, floor, seed_rev}} <-
             load_meta(db, person, fn -> seeded?(engine, person) end),
           {:ok, head} <-
             write_rows(db, person, Enum.map(items, &{:item, &1}), head, now, MapSet.new()),
           :ok <-
             run(
               db,
               "INSERT INTO client_change_log_meta (person_id, head, floor, seed_rev) VALUES (?, ?, ?, ?) " <>
                 "ON CONFLICT (person_id) DO UPDATE SET head = excluded.head",
               [person, head, floor, seed_rev]
             ),
           :ok <- store_rank_keys(db, person, keys, dropped),
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

  @doc """
  Stores the person's whole ranking from the reads component
  (`%{item_id => rank}`, from `ClientAPI.Resources.ranks/4`) in one
  transaction. Every item whose rank changed in any member (key, section,
  group, score, due date, reply row), and every item no longer ranked, gets
  one `:item` row. The rank-unit keys are kept for the next ranking's
  `previous`. Unchanged ranks write nothing. Returns the person's head.
  Reads then page from the stored order (`rank_page/4`, `rank/3`) instead
  of ranking per read.
  """
  def put_ranks(%AttentionServer.Store{} = store, person, ranks) when is_map(ranks) do
    with {:ok, head, _cache} <- put_ranks(store, person, ranks, nil), do: {:ok, head}
  end

  @doc """
  `put_ranks/3` against the person's last ranking kept in memory (`cache`,
  as returned by the previous call, or `nil` after a restart). Rankings are
  compared in memory, so an unchanged re-rank reads and writes nothing but
  the head. With `nil`, the stored ranking is read once and compared by its
  JSON. Returns `{:ok, head, cache}` for the next call.
  """
  def put_ranks(
        %AttentionServer.Store{change_log: true, db: db, engine: engine},
        person,
        ranks,
        cache
      )
      when is_map(ranks) do
    with {:ok, prior} <- prior_ranks(db, person, cache) do
      changed = for {id, rank} <- ranks, not same_rank?(Map.get(prior.ranks, id), rank), do: id
      dropped = for {id, _} <- prior.ranks, not Map.has_key?(ranks, id), do: id
      changed_ranks = Map.take(ranks, changed)

      {unit_keys, changed_units, dropped_units} =
        rank_unit_changes(prior, ranks, changed_ranks, changed ++ dropped, cache)

      next_cache = %{ranks: ranks, units: unit_keys, ranked?: true}

      if changed == [] and dropped == [] and map_size(changed_units) == 0 and
           dropped_units == [] and prior.ranked? do
        with {:ok, head} <- head(db, engine, person), do: {:ok, head, next_cache}
      else
        # A person's first stored ranking is what reads computed until now, so
        # it changes no item and writes no row.
        items =
          if prior.ranked?, do: rank_changed_items(engine, person, changed ++ dropped), else: []

        with {:ok, head} <-
               write_stored_ranks(db, engine, person, %{
                 items: items,
                 ranks: changed_ranks,
                 encoded: Map.new(changed_ranks, fn {id, rank} -> {id, JSON.encode!(rank)} end),
                 dropped: dropped,
                 units: changed_units,
                 dropped_units: dropped_units
               }),
             do: {:ok, head, next_cache}
      end
    end
  end

  def put_ranks(%AttentionServer.Store{}, _person, _ranks, _cache), do: {:error, :disabled}

  # The cache is the result of the last successful write, so only changed
  # items can add, remove or move its rank units. On a cold read, compare
  # all units to reconcile the rows recovered separately from SQLite.
  defp rank_unit_changes(prior, ranks, _changed_ranks, _changed_or_dropped, nil) do
    units = AttentionServer.ClientAPI.Resources.rank_unit_keys(ranks)
    {upserts, deletes} = compare_rank_units(prior.units, units)
    {units, upserts, deletes}
  end

  defp rank_unit_changes(prior, _ranks, changed_ranks, changed_or_dropped, _cache) do
    old_units =
      prior.ranks
      |> Map.take(changed_or_dropped)
      |> AttentionServer.ClientAPI.Resources.rank_unit_keys()

    new_units = AttentionServer.ClientAPI.Resources.rank_unit_keys(changed_ranks)
    {upserts, deletes} = compare_rank_units(old_units, new_units)
    units = prior.units |> Map.drop(deletes) |> Map.merge(upserts)
    {units, upserts, deletes}
  end

  defp compare_rank_units(old_units, new_units) do
    upserts = for {id, key} <- new_units, Map.get(old_units, id) != key, into: %{}, do: {id, key}
    deletes = for {id, _} <- old_units, not Map.has_key?(new_units, id), do: id
    {upserts, deletes}
  end

  # A ranking kept in memory compares by value; one read back from SQLite
  # (after a restart) compares by its stored JSON, which encodes the same
  # way.
  defp prior_ranks(_db, _person, %{ranks: _, units: _, ranked?: _} = cache), do: {:ok, cache}

  defp prior_ranks(db, person, nil) do
    with {:ok, stored} <- stored_ranks(db, person),
         {:ok, units} <- rank_keys(%AttentionServer.Store{change_log: true, db: db}, person),
         {:ok, ranked?} <- ranked?(db, person) do
      {:ok,
       %{
         ranks: Map.new(stored, fn {id, json} -> {id, {:json, json}} end),
         units: units,
         ranked?: ranked?
       }}
    end
  end

  defp same_rank?(nil, _rank), do: false
  defp same_rank?({:json, json}, rank), do: json == JSON.encode!(rank)
  defp same_rank?(previous, rank), do: previous == rank

  defp rank_changed_items(engine, person, item_ids) do
    item_ids
    |> Enum.flat_map(fn item_id ->
      case AttentionServer.ClientAPI.Resources.item_key(engine, person, item_id) do
        {:ok, key} -> [item_resource(key)]
        _ -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc false
  def ranks_available(%AttentionServer.Store{change_log_touched: touched}, person) do
    if MapSet.member?(touched, person), do: {:error, :storage_unavailable}, else: :ok
  end

  @doc """
  Up to `limit` stored ranks after `after_key` (or from the start), in
  ascending rank-key order: `{:ok, [{item_id, rank}], more}`. `{:error, :none}`
  until the person's ranking has first been stored.
  """
  def rank_page(%AttentionServer.Store{change_log: true, db: db}, person, after_key, limit)
      when (is_nil(after_key) or is_binary(after_key)) and is_integer(limit) and limit > 0 do
    with {:ok, true} <- ranked?(db, person),
         {:ok, rows} <-
           all(
             db,
             "SELECT item_id, rank_json FROM client_ranks WHERE person_id = ? AND rank_key > ? " <>
               "ORDER BY rank_key LIMIT ?",
             [person, after_key || "", limit + 1]
           ) do
      {page, rest} = Enum.split(rows, limit)
      {:ok, Enum.map(page, fn [item_id, json] -> {item_id, decode_rank(json)} end), rest != []}
    else
      {:ok, false} -> {:error, :none}
      error -> error
    end
  end

  def rank_page(%AttentionServer.Store{}, _person, _after_key, _limit), do: {:error, :disabled}

  @doc "One item's stored rank."
  def rank(%AttentionServer.Store{change_log: true, db: db}, person, item_id) do
    case one(db, "SELECT rank_json FROM client_ranks WHERE person_id = ? AND item_id = ?", [
           person,
           item_id
         ]) do
      {:ok, [json]} -> {:ok, decode_rank(json)}
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  def rank(%AttentionServer.Store{}, _person, _item_id), do: {:error, :disabled}

  defp decode_rank(json) do
    {:ok, rank} = JSON.decode_line(json <> "\n")
    rank
  end

  defp stored_ranks(db, person) do
    with {:ok, rows} <-
           all(db, "SELECT item_id, rank_json FROM client_ranks WHERE person_id = ?", [person]) do
      {:ok, Map.new(rows, fn [item_id, json] -> {item_id, json} end)}
    end
  end

  defp ranked?(db, person) do
    with {:ok, row} <- one(db, "SELECT 1 FROM client_rank_state WHERE person_id = ?", [person]),
         do: {:ok, row != nil}
  end

  defp write_stored_ranks(db, engine, person, change) do
    now = System.system_time(:millisecond)

    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE"),
           {:ok, {head, floor, seed_rev}} <-
             load_meta(db, person, fn -> seeded?(engine, person) end),
           {:ok, head} <-
             write_rows(db, person, Enum.map(change.items, &{:item, &1}), head, now, MapSet.new()),
           :ok <-
             run(
               db,
               "INSERT INTO client_change_log_meta (person_id, head, floor, seed_rev) VALUES (?, ?, ?, ?) " <>
                 "ON CONFLICT (person_id) DO UPDATE SET head = excluded.head",
               [person, head, floor, seed_rev]
             ),
           :ok <- upsert_ranks(db, person, change.ranks, change.encoded),
           :ok <- delete_ranks(db, person, change.dropped),
           :ok <- store_rank_keys(db, person, change.units, change.dropped_units),
           :ok <-
             run(db, "INSERT OR IGNORE INTO client_rank_state (person_id) VALUES (?)", [person]),
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

  defp upsert_ranks(db, person, ranks, encoded) do
    Enum.reduce_while(ranks, :ok, fn {item_id, rank}, :ok ->
      case run(
             db,
             "INSERT INTO client_ranks (person_id, item_id, rank_key, rank_json) VALUES (?, ?, ?, ?) " <>
               "ON CONFLICT (person_id, item_id) DO UPDATE SET rank_key = excluded.rank_key, rank_json = excluded.rank_json",
             [person, item_id, Map.fetch!(rank, "key"), Map.fetch!(encoded, item_id)]
           ) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp delete_ranks(db, person, item_ids) do
    Enum.reduce_while(item_ids, :ok, fn item_id, :ok ->
      case run(db, "DELETE FROM client_ranks WHERE person_id = ? AND item_id = ?", [
             person,
             item_id
           ]) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
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
         :ok <- AttentionServer.AgentChangeLog.prune(db, before_ms),
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
end

# Change log: module interface for the client API v2 components

`AttentionServer.ChangeLog` is the per-person change log behind
[client-api-v2.md](client-api-v2.md). The other client API v2 components
code against the functions below. It runs only while the `client_api_v2`
feature flag is on at startup; with the flag off, the store is unchanged and
nothing is written.

## Model

- **One append-only log per person.** A row is `{rev, kind, resource_id}`,
  and `rev` increases per person. The person's **head** is the rev of the
  newest row: their current position. It is 0 for a person with no data.
- **One write path.** `ChangeLog.changes/2` diffs the client-visible inputs
  of the old and next engine inside `Store.commit/3`. `ChangeLog.append/4`
  writes the rows in the same SQLite transaction as the snapshot. No other
  module writes rows.
- **Rows carry no payload.** Each resource keeps only its latest row, and
  older rows are deleted when a newer one is written. A reader renders each
  row from current state (`ClientAPI.Resources`, the reads component). A
  client therefore receives each changed resource once, at its newest rev, in
  increasing rev order with no gap. Per-viewer members such as `as` are
  applied at render time, so one row serves every device and browser of the
  person.
- **Resource IDs.** Items are `item:<base64url of [sender, message_id]>`,
  from `item_resource/1`. Entries are `entry:<conversation_seq>`, from
  `entry_resource/1`. The others are `counts` and `settings`.
  `decode_resource/1` reverses them. The public `item_id` and `entry_id`
  encodings belong to the reads component.
- **Cut-over seed.** A person who has data and no log starts at head 1, with
  every existing resource at rev 1. No row is written for the seed, and
  `after = 0` is then `:cursor_expired`.
- **Missed commits.** If the store was committed while the log was off, every
  person's log restarts past their head at startup. Older cursors get
  `:cursor_expired`, so nothing changed meanwhile is lost.

## Functions

```elixir
@type position :: non_neg_integer()   # a head, or a client's cursor
@type rev :: pos_integer()
@type kind :: :item | :item_removed | :entry | :counts | :settings
@type change :: %{rev: rev, kind: kind, resource_id: String.t()}

# Read side. Each takes the Relay's committed %Store{}; a store without the log
# answers {:error, :disabled}. Call them from the Relay process, so they are
# serialized with commits.
ChangeLog.head(store, person) :: {:ok, position}
ChangeLog.rev(store, person, resource_id) :: {:ok, rev} | {:error, :not_found}
ChangeLog.read(store, person, after, limit) ::
  {:ok, [change], more :: boolean, position} | {:error, :cursor_expired}
  # rows with rev > after, ascending; position = last row's rev, or after when
  # there are none; after below the retained floor or above the head is
  # :cursor_expired.
ChangeLog.subscribe(person) :: :ok     # caller gets {:change_log, person, head}
ChangeLog.unsubscribe(person) :: :ok   # after each committed append; a wake-up only
ChangeLog.prune(store, before_ms) :: {:ok, deleted}   # raises the floor

# Write side. Store only.
ChangeLog.changes(old_engine, next_engine) :: %{person => [{kind, resource_id}]}
ChangeLog.append(db, pending, seeded, store_version) :: {:ok, %{person => position}}
Store.enable_change_log(store) :: {:ok, store}   # Relay, at startup, with the flag on
```

## What counts as a change

`changes/2` diffs these engine inputs. Anything new that a client resource
reads must be added here; the replay test catches an omission.

- **Item:** `messages`, `chat_read_positions`, the Route B conversation
  summary, `bounce_revisions`, `opened_revisions`, and a device's
  `subscriptions` (which drive `as`). A missing message yields
  `:item_removed`.
- **Entry:** `conversation_events`.
- **Counts:** the unresolved count.
- **Settings:** `bounce_levels` and the person's `push_bindings`.
- **Rank** is derived in the Relay, not the engine. Its rows come in a later
  slice.

## Not yet in this slice

- Rank rows.
- Retention scheduling (`prune/2` exists but nothing calls it yet).
- The replay-equals-inbox property test, which needs the reads component's
  renderer.

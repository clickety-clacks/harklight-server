# Change log: module interface for the client API v2 components

Status: **proposal.** Nothing here is implemented yet. This is the minimal
interface that the other client API v2 components code against. The behavior
it serves is in [client-api-v2.md](client-api-v2.md): positions, resource
`rev`s, the feed and cursor expiry. Everything here is behind the
`client_api_v2` feature flag, off by default. With the flag off, nothing is
written to the log.

## Model

- One append-only log per person. Each row is `{rev, kind, resource_id,
  payload}`, with `rev` increasing per person.
- The person's **head** is the `rev` of the last row: their current position.
  It is 0 for a person with no data.
- **Every client-visible change gets its rows in the same SQLite transaction
  as the state change.** There is exactly one write path:
  `ChangeLog.changes/3` runs inside `Store.commit/3` against the old and new
  engine. Changes that live outside the engine (rank, which is derived from
  the priority view) go through `ChangeLog.append_derived/3`. No other module
  writes log rows.
- `payload` is **principal-neutral**: the person-level resource. Members that
  differ by viewer, such as `as` or answer values seen only by the origin, are
  applied when a change is rendered for a principal
  (`ClientAPI.Resources.render/3`, owned by the reads component). So one row
  serves every device and browser of the person.
- Each resource's current `rev` is kept beside the log, so reads can stamp
  `rev` without scanning the log.
- **Cut-over seed.** The first time a person is served with the flag on, if
  they have data and no log, their head becomes 1 and every existing resource
  reads as `rev` 1. No row is written for the seed.

## Types

```elixir
@type person :: String.t()
@type position :: non_neg_integer()          # head, or a client's cursor
@type rev :: pos_integer()                   # one row / one resource version
@type kind :: :item | :item_removed | :entry | :counts | :settings
@type resource_id :: String.t()              # item_id, entry_id, "counts", "settings"
@type change :: %{rev: rev, kind: kind, resource_id: resource_id, payload: map()}
@type pending :: %{person => [{kind, resource_id, map()}]}   # rows not yet numbered
```

## Write side (Store and Relay only)

```elixir
# Pure. Diffs the client-visible projection of old vs next engine and returns the rows
# to append, per person, in apply order. Unchanged resources produce no rows. An item
# that becomes invisible yields :item_removed. `hint` may name the touched persons to
# bound the diff; nil means diff every person.
@spec changes(old :: Engine.t(), next :: Engine.t(), hint :: [person] | nil) :: pending

# Inside Store's open transaction: numbers the rows, writes them and the resource revs,
# and returns each touched person's new head. Any error aborts the whole commit.
@spec append(db :: term(), pending) :: {:ok, %{person => position}} | {:error, :storage_unavailable}

# For changes outside the engine (rank moves, a full re-rank, time-driven moves).
# Opens its own transaction and notifies subscribers.
@spec append_derived(Store.t(), person, [{kind, resource_id, map()}]) ::
        {:ok, position} | {:error, :storage_unavailable}
```

## Read side (reads, writes and feed components)

```elixir
# The person's position, taken in the caller's serialized step. Seeds the person on
# first use if needed (see the cut-over seed above).
@spec head(Store.t(), person) :: {:ok, position}

# Current rev of one resource (>= 1); seeded resources read as 1.
@spec rev(Store.t(), person, kind, resource_id) :: {:ok, rev} | {:error, :not_found}

# Rows with rev > after, ascending, at most `limit`. `more` is true when rows remain.
# The returned position is the last row's rev, or `after` when the batch is empty.
# after = 0 is served only if the log is retained from rev 1.
@spec read(Store.t(), person, after :: position, limit :: pos_integer()) ::
        {:ok, [change], more :: boolean(), position} | {:error, :cursor_expired}

# The calling process receives {:change_log, person, head} after every append for
# that person. It is a wake-up only: the receiver calls read/4 from its own cursor,
# so a missed or merged message loses nothing.
@spec subscribe(person) :: :ok
@spec unsubscribe(person) :: :ok

# Retention: deletes rows older than the window and records the lowest retained rev
# so read/4 can answer :cursor_expired. Run on the existing periodic cleanup.
@spec prune(Store.t(), older_than_ms :: non_neg_integer()) :: {:ok, deleted :: non_neg_integer()}
```

## Files this component touches

- New: `lib/attention_server/change_log.ex` and `test/change_log_test.exs`.
- `lib/attention_server/store.ex`:
  - adds the schema tables `client_changes`, `client_resource_revs` and
    `client_change_log_meta`, created on first use with the flag on, so a
    store that never runs with the flag is unchanged;
  - calls `changes/3` and `append/2` inside the existing commit transaction,
    only while the flag is on.
- `lib/attention_server/relay.ex`: one call to `append_derived/3` where the
  priority view's order changes, and the flag read.

No other component edits these files for the change log.

## Tests (red first)

- Replaying the log from 0 to the head through `render/3` reproduces what
  `GET /v2/inbox` and the entries reads return, for a scripted mix of origin
  sends, revisions, replies, answers, retractions, reads, levels and rank
  changes.
- Each commit writes its rows in the same transaction. A failed commit leaves
  no rows, and a committed one cannot lose them, across a restart.
- `read/4` is gap-free, honors `limit`, `more` and the position rule, and
  returns `:cursor_expired` after pruning or for a seeded person at
  `after = 0`.
- With the flag off, no rows and no tables are touched.

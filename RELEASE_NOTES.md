# Harklight Server v0.1.40

## Changes

- Every registered, non-revoked agent can read all messages on the account it
  is registered to, regardless of sender. Sender filters select search results
  only; they do not limit access. New registrations need no per-sender read
  grant for reads. Answering still requires both a matching read grant and a
  matching respond grant; response behavior is unchanged.
- Continues to write FORMAT29; no store-format or migration code changes are
  included in this candidate.

## Upgrade and recovery

The existing FORMAT27-to-FORMAT29 startup migration is unchanged. Before the
first v0.1.40 start, stop the server and take a complete private-state backup.
After any v0.1.40 write, returning to a FORMAT27 release such as v0.1.39 requires
stopping the server, restoring that pre-start backup and selecting the prior
release. This loses every write made since the backup and cannot undo external
notifications, bounces or agent deliveries. Restore success is unproved and has
not been tested; no recovery success or time is guaranteed. See SELF_HOST.md
for the exact installer behavior and limits.

---

# Harklight Server v0.1.37

## Changes

- Per-device focus permissions default off when added during migration; existing
  values are preserved.
- Hosted delivery state uses `STOPPED` and `RESTORED` values. Migrated state
  starts with an empty delivery-state map, retaining restored behavior until a
  person is explicitly stopped. Stopped people do not receive new origin
  forwards, revisions or chat replies, and their push claims are suppressed.
- Relay reconnect cleanup closes the stale transport outside the serialized
  request path. Hosted sign-out completes its configured redirect when the
  Server session cookie is missing or expired; valid sessions are revoked first.

## Upgrade and recovery

The server writes FORMAT29. From supported FORMAT27 state such as the one
written by v0.1.36, startup applies two ordered steps: first it adds
`see_focus: false` and `push_view: false` only where device records lack those
values, preserving existing values; then it initializes the hosted
`delivery_states` map to empty. The migrated store is FORMAT29; no separately
persisted intermediate FORMAT28 state is established. The v0.1.35 and v0.1.36
packages write FORMAT27 and cannot read FORMAT29. A code-only downgrade is not
established as safe.

Before the first v0.1.37 start, stop the server and take a complete private
state backup. After any v0.1.37 write, returning to v0.1.36 requires stopping
the server, restoring that pre-start backup and selecting v0.1.36. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. The installer restore path and prior-release
readiness are untested; no recovery success or time is guaranteed.
See `SELF_HOST.md` for detailed installer behavior and limits.

---

## Harklight Server v0.1.32 (historical)

### Changes

- The origin API supports reading a message by sender ID and message ID. The
  matching `harklight-agent message-get` interface was source-only when the
  Server v0.1.32 release was published; Agent 0.1.9 followed later, on
  2026-11-11. The command returns the
  latest committed revision by default or a specific retained revision with
  `--revision N`. Reading does not mark Seen; an unavailable requested revision
  returns `revision_unavailable`.
- Stable v0.1.31 was published on 2026-11-11 before v0.1.32 and already included
  the device-revocation, access-level synchronization, bounded Relay-timeout,
  and cached-token crash/status changes described below.

### Upgrade and recovery

The server writes FORMAT27. On first start it durably migrates supported FORMAT25
state, used by v0.1.28, to FORMAT27. The v0.1.28 package cannot read the resulting
state.

Before the first v0.1.32 start, stop the server and take a complete private
state backup. After any v0.1.32 write, returning to v0.1.28 requires stopping
the server, restoring that pre-start backup and then running v0.1.28. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. Restore success is unproved and has not been
tested; no recovery success is guaranteed. See `SELF_HOST.md` for the detailed
installer behavior and recovery limits.

---

## Harklight Server v0.1.31 (published 2026-11-11)

Stable v0.1.31 was published before v0.1.32. Configurable priority levels,
unseen-message advisories, closed-chat behavior, and Bandit 1.14.0 were already
present in v0.1.30 and remain available.

## Changes

- Device revocation ends every origin session from that device with the
  existing gone availability.
- Enrolled devices can read and update their access levels. Updates emit
  connection-local synchronization hints, and operation receipts are durable.
- Relay timeout and outcome-unknown snapshots are bounded and recorded outside
  the request path.
- Push-dispatcher crash/status output omits cached APNs tokens.
- Release publication does not trigger installation. Installation is available
  only through an explicitly authorized manual workflow dispatch.
- Configurable score-based priority levels, unseen-message advisories, and
  closed-chat behavior remain available.
- The server writes FORMAT27. On first start it durably migrates supported
  FORMAT25 state, used by v0.1.28, to FORMAT27. The v0.1.28 package cannot read
  the resulting state.

## Upgrade and recovery

Before the first v0.1.31 start, stop the server and take a complete private
state backup. After any v0.1.31 write, returning to v0.1.28 requires stopping
the server, restoring that pre-start backup and then running v0.1.28. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. Restore success is unproved and has not been
tested; no recovery success is guaranteed. See `SELF_HOST.md` for the detailed
installer behavior and recovery limits.

# Harklight Server v0.1.36

## Changes

- Bundles the reviewed hosted Agent customer guide v7 in the SaaS skill file.
  The package assembly preserves the guide bytes without appending the shared
  core. The existing dashboard file route serves the packaged file unchanged.
- This guide-only change adds no Server runtime, protocol, authorization,
  schema or store-format change. It retains FORMAT27 and the v0.1.35 Relay
  close repair.

## Upgrade and recovery

The package writes FORMAT27 and adds no schema or store-format migration over
v0.1.35. Returning to v0.1.35 needs no data migration rollback. Before an
upgrade, stop the server and take a complete private-state backup. The
installer restore path is untested and can fail; restoring loses later writes
and cannot undo external effects. No installation or recovery result is
claimed by these package notes.

---

# Harklight Server v0.1.35

## Changes

- Fixes a shared Relay stall when a dropped TLS session is closed. Session
  removal and fencing remain serialized; physical socket closure no longer
  blocks other devices' Relay work.
- Retains the FORMAT27 store format from v0.1.33. This repair adds no database
  schema/store-format migration, wire change, authorization change or grant
  change.

## Upgrade and recovery

Before upgrading, stop the server and take a complete private-state backup.
The installer stops the service, creates a full mutable-root backup, switches
the release selector and starts the candidate. The installer restore path has
not been exercised; stop, restore, selector switching or prior-release startup
can fail and leave the service stopped. Restoring loses writes made after the
backup and cannot undo external effects. Published v0.1.33 uses the same
FORMAT27 store format, so reverting this repair to .33 requires no data
migration rollback. Returning to .1.28 or earlier still requires restoring a
compatible pre-upgrade backup; .28 cannot read FORMAT27. No installation or
recovery result is claimed by these package notes.

---

# Harklight Server v0.1.33

## Changes

- Authorized agents can now reply to or mark Seen a revised message by sending
  its current `request_revision` alongside the stable `acceptance_seq` returned
  by get or search. The bundled `bin/harklight agent respond` accepts
  `--request-revision`. Omitting it retains legacy revision-1 behavior; stale
  target, stale revision, authorization and operation-receipt checks remain.
- This release continues to write FORMAT27 and introduces no database schema
  or store-format migration.

## Upgrade and recovery

Before the first v0.1.33 start, stop the server and take a complete private
state backup. After any v0.1.33 write, returning to v0.1.28 requires stopping
the server, restoring that pre-start backup and then running v0.1.28. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. Restore success is unproved and has not been
tested; no recovery success is guaranteed. See `SELF_HOST.md` for the detailed
installer behavior and recovery limits.

---

# Harklight Server v0.1.32

## Changes

- The origin API supports reading a message by sender ID and message ID. Agent
  0.1.9 or newer provides `harklight-agent message-get`, which returns the latest
  committed revision by default or a specific retained revision with
  `--revision N`. Reading does not mark Seen; an unavailable requested revision
  returns `revision_unavailable`.
- This is the first published release of the remaining device-revocation,
  access-level synchronization, bounded Relay-timeout, and cached-token
  crash/status changes prepared for the unpublished v0.1.31 cut.

## Upgrade and recovery

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

# Proposed Harklight Server v0.1.31 release notes (not published)

No v0.1.31 tag or release was published. The changes in this draft that were
not already present in v0.1.30 are first released in v0.1.32. Configurable
priority levels, unseen-message advisories, closed-chat behavior, and Bandit
1.14.0 were already included in v0.1.30.

## Changes

- Device revocation ends every origin session from that device with the
  existing gone availability.
- Enrolled devices can read and update their access levels. Updates emit
  connection-local synchronization hints, and operation receipts are durable.
- Relay timeout and outcome-unknown snapshots are bounded and recorded outside
  the request path.
- Push-dispatcher crash/status output omits cached APNs tokens.
- The Bandit dependency is updated to 1.14.0.
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

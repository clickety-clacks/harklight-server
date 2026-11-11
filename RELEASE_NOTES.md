# Harklight Server v0.1.32

## Changes

- The origin API supports reading a message by sender ID and message ID. The
  matching `harklight-agent message-get` interface is defined in Agent 0.1.9
  source, but Agent 0.1.9 has not yet been published. The command returns the
  latest committed revision by default or a specific retained revision with
  `--revision N`. Reading does not mark Seen; an unavailable requested revision
  returns `revision_unavailable`.
- Stable v0.1.31 was published on 2026-11-11 before v0.1.32 and already included
  the device-revocation, access-level synchronization, bounded Relay-timeout,
  and cached-token crash/status changes described below.

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

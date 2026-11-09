# Harklight Server v0.1.30

## Changes

- Updates the Bandit HTTP server dependency from 1.13.0 to 1.14.0 to address
  CVE-2026-114216.
- Makes no Harklight application or store-migration code change. FORMAT27
  remains the store layout, and first start still migrates supported FORMAT25
  state from v0.1.28 to FORMAT27.

## Upgrade and recovery

Before the first v0.1.30 start, stop the server and take a complete private
state backup. v0.1.28 cannot read the resulting FORMAT27 state. After any
v0.1.30 write, returning to v0.1.28 requires stopping the server and restoring
that pre-start backup before starting v0.1.28. Restoring loses every later
write and cannot undo external notifications, bounces or agent deliveries.
The installer backup-readability and restore path remain untested; restore
success is unproved and no recovery success is guaranteed. See `SELF_HOST.md`
for installer behavior and the recovery limits.

# Harklight Server v0.1.29

## Changes

- Configurable score-based priority levels select an unseen interval for a
  request. When that interval elapses, the server adds an advisory to the
  originating agent session asking whether the request is still valid and
  worth resending and whether its importance should be raised. It does not
  automatically resend the request or change its priority.
- After a Reply or No comment, new chat on that request is closed and earlier
  messages remain readable. Agents without the matching Agent C1 behavior
  receive the existing generic closed refusal.
- The server writes FORMAT27. On first start it durably migrates supported
  FORMAT25 state, used by v0.1.28, to FORMAT27. The v0.1.28 package cannot read
  the resulting state.

## Upgrade and recovery

Before the first v0.1.29 start, stop the server and take a complete private
state backup. After any v0.1.29 write, returning to v0.1.28 requires stopping
the server, restoring that pre-start backup and then running v0.1.28. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. Restore success is unproved and has not been
tested; no recovery success is guaranteed. See `SELF_HOST.md` for the detailed
installer behavior and recovery limits.

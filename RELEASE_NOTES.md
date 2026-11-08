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

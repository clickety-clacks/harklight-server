# Harklight Server v0.2.0 (in preparation)

The full notes are completed when the release is cut.

## Compatibility

Protocol 0.2. The device port serves client API v2 (`/v2`) only, and the
Server runs no `/origin/v1` and no `/agent/v1` listener. Protocol 0.1 apps,
agents and headless tools cannot connect to it, so a deployment moves to
0.2.0 only after its agents and clients are on their 0.2 versions. The
verified compatible versions of the app, web client and Harklight Agent are
named here when the release is cut.

---

# Harklight Server v0.1.49

## Changes

- Persisted Agent device metadata is accepted with or without a final newline.
  This restores interoperability with the current Agent metadata file format.
  The existing file-size and identity checks remain in force; network wire
  framing is unchanged. The Agent binary is unchanged.

## Upgrade and recovery

v0.1.49 writes FORMAT29, like the published v0.1.48 release. Upgrading from
v0.1.48 runs no store migration. Supported FORMAT27 state migrates on first
start; returning to FORMAT27 then requires the pre-start backup and loses
later writes. Recovery can fail.

---

# Harklight Server v0.1.48

## Changes

- Baseline and Jev fallback views now show a display-only `priority` from each
  item's effective urgency (`low`, `normal`, `high`, or `now`). Free baseline
  views still use arrival order; fallback views keep their urgency and deadline
  order. These entries remain unscored and gain no bounce schedules. Jev
  selected views retain their score-only behavior.
- Contradiction timeout tests now control their timing explicitly, avoiding
  intermittent failures without changing server behavior.

## Upgrade and recovery

v0.1.48 writes FORMAT29, like v0.1.43 through v0.1.45. Upgrading from a
FORMAT29 release runs no store migration. Supported FORMAT27 state migrates to
FORMAT29 on first start; returning to FORMAT27 then requires the pre-start
backup and loses later writes. Recovery can fail.

---

# Harklight Server v0.1.45

**Install this instead of v0.1.43 or v0.1.44 when the server is busy.** It
contains everything in v0.1.44, below, plus two fixes.

## Changes

- Agents' repeated, unchanged declarations no longer rewrite the whole server
  state. Agents re-declare the same listening support and attachment many times
  a minute (on one busy server, 85% of all saves). A declaration that would
  change nothing is now answered from current state without saving, without a
  new operation receipt and without another availability entry on every open
  request; a changed declaration is saved exactly as before. Measured on a
  3 GB synthetic server replaying that mix (22 and 44 unchanged support
  declarations a minute, plus attachment updates, status polls and about one
  message a minute): saves fell from 22 and 42 a minute to 1 (the messages);
  server CPU median from 46% and 81% to 19% and 20%; support answers from
  1.0–1.2 s to 0.05 s median; message sends from 3.2–3.3 s to 0.7–0.9 s
  median; no failed calls in either release.
- A connection no longer gives up on a busy server after five seconds. Most
  calls from device, origin, agent and owner connections into the server's
  relay waited at most five seconds (a library default) for an answer. When the
  relay was busy longer, the connection closed without answering, so agents
  reported "error sending request", while the request usually still ran and the
  client's retry repeated it. Every such call now waits for the relay's real
  answer. No timer or retry is added.

## Upgrade and recovery

v0.1.45 writes FORMAT29, like v0.1.43 and v0.1.44. From FORMAT29 state written
by v0.1.43 or v0.1.44, no migration runs, and switching back to the earlier
FORMAT29 release keeps every write. From supported FORMAT27 state (the
v0.1.35 through v0.1.42 maintenance packages), first start migrates to FORMAT29
as described for v0.1.44, and returning to FORMAT27 requires restoring the
pre-start backup, which loses every later write. Recovery can fail; do not
assume success or a recovery time.

---

# Harklight Server v0.1.44

**Install this instead of v0.1.43.** In v0.1.43, revising a message whose body
is stored as large message data crashes the server's relay process. v0.1.44
fixes that and the slower sends v0.1.43 showed on large servers.

## Changes

- Sends and other state changes are faster on large servers. v0.1.43
  re-examined the whole stored state for large message data on every change;
  the server now examines only what changed since the previous commit (the
  first change after each start still examines everything once). Measured on
  the same 3 GB synthetic server (about 3,950 messages and 2,940 uploads),
  alternating one send at a time between releases, 30 sends each:
  v0.1.44 median 1.0 s (worst 2.4 s) against v0.1.43 median 1.45 s (worst
  2.9 s), and v0.1.44 median 1.0 s (worst 1.7 s) against v0.1.38 median 2.6 s
  (worst 2.8 s).
- Revising a message whose body, answer or title is stored as large message
  data no longer crashes the relay. Revisions compare large content by its
  SHA-256 and size.
- The owner, agent and device command-line tools wait for the server's answer
  (or a real connection error) instead of giving up after 3 or 5 seconds, so a
  slow but successful call is no longer reported as a failure. A peer that
  accepts a connection but never answers now waits until the connection ends or
  the command is interrupted.
- The package carries only the pinned self-hosted web bundle.

## Upgrade and recovery

v0.1.44 writes FORMAT29, like v0.1.43. From FORMAT29 state written by v0.1.43,
no migration runs. From supported FORMAT27 state, including state written by
the published v0.1.35 through v0.1.42 maintenance packages such as v0.1.38,
first start applies the FORMAT28 device-focus defaults and FORMAT29
hosted-delivery-state initialization in order; FORMAT27 packages cannot read
the result. Before first start, stop the server and take a complete
private-state backup (the release installer takes one). Returning to a FORMAT27
release requires stopping the server, restoring that backup and selecting the
earlier release. This loses every write made since the backup and cannot undo
external notifications, bounces or agent deliveries. Recovery can fail; do not
assume success or a recovery time.

---

# Harklight Server v0.1.43

## Changes

- Includes the reviewed self-hosted web bundle. It is packaged with the Server;
  installation does not fetch it from the web project.
- Authorized agents can read every message on their account regardless of
  sender. Sender filters affect search results only. Answering still requires
  both a matching read grant and a matching respond grant. After Reply or No
  comment, new chat on that request is closed and earlier messages remain
  readable; Agents without Agent C1, including ACK2-only Agent 0.1.7, receive
  the existing generic closed refusal rather than the C1 wording.
- Invalid or unregistered APNs endpoints stop retrying. The one-hour push-hint
  expiry was removed; the existing five-minute notification pacing and retry
  backoff caps remain.
- Superseded ranking work is cancelled, and the fixed ranking request and kill
  deadlines were removed.
- Invitation and session records are cleaned up on use, supersession or
  revocation. Browser sign-in grants retain their existing per-grant expiry.
- Uploads survive disconnect, reconnect and restart. Attached upload lifetime
  follows its message. Unattached uploads remain until the uploader device or
  origin is revoked/removed or the owner explicitly discards them; there is no
  time-based cleanup or retention cap.
- Large message data can use private disk-backed files with checksummed durable
  references rather than being rejected at the former 2 MiB request limit.
  Optional upload caps remain operator-configurable; existing inline handling
  limits still apply. Older compatible FORMAT29 releases that do not
  understand a large-data reference show an explicit newer-server marker for
  that content, continue serving other data and preserve the referenced files;
  a compatible upgrade restores full reads.
- Each connection's output is written by its own ordered writer, so a device or
  agent that stops reading no longer holds up other devices, agents or the
  owner. Authority is rechecked before each frame and revocation cancels queued
  output. No send timeout, queue cap or retry deadline is added.
- Progress messages: a typed FYI marked as Progress is revised in place under
  the same message ID. Progress is left out of ranking, bounce scores and
  deadlines, and the app badge count. Readers receive the latest full revision.
- Accounts without an owner-configured bounce table use default Low, Normal,
  High and Urgent levels for newly accepted deliveries and substantive
  revisions. An owner-configured table still takes precedence.
- A device's focus ends when its session closes or is replaced, its listener
  disconnects, it signs out or it is removed; a reconnecting device sends a
  fresh focus report. The former 24-hour focus retention was removed.
- Notifications can expand to show the delivered message or chat preview, or
  the ordered titles of a batch of requests. The collapsed alert is unchanged,
  and a payload that cannot fit falls back to the existing alert.
- Pushes and device sync identify the originating Server. Operators can expose
  boolean client feature flags through `ATTENTION_SERVER_FEATURE_FLAGS_PATH`;
  unset or unreadable configuration yields no flags.
- A No comment action authored before a request revision closes that request
  when it arrives later. A retry of the original action recovers its result; an
  action proven to be a different new operation is refused as closed.
- The packaged agent skill documents the supported web entry links, `/inbox`
  and `/settings`.

## Upgrade and recovery

The server writes FORMAT29. From supported FORMAT27 state, including the state
written by the published v0.1.42 package, first start applies the device-focus
defaults and hosted-delivery-state initialization in order. There is no
separately established persisted FORMAT28 state. The v0.1.42 FORMAT27 package
cannot read the resulting FORMAT29 state; switching code alone is not a safe
rollback. Before first start, stop the server and take a complete private-state
backup. After any v0.1.43 write, returning to v0.1.42 requires stopping the
server and restoring that pre-start backup before starting v0.1.42. This loses
every write made since the backup and cannot undo external notifications,
bounces or agent deliveries. Recovery can fail; do not assume success or a
recovery time.

---

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

# Harklight Server v0.2.25

Bounces now work on every Server, with or without Jev ranking. In v0.2.24 a
message got its bounce time only after Jev scored it, so with Jev off nothing
bounced. Now every message gets its bounce time when it arrives, from the
priority it already has: the sender's stated urgency, or Normal when missing.
The account's own levels apply if set, otherwise Urgent 12 hours, High 1 day,
Normal 3 days, Low 7 days. A later Jev score can change the level; the time is
counted from arrival.

On first start, open messages that have no bounce time yet get one, so a
never-opened message already past its time bounces then. A message that was
opened at any point only ever soft-bounces (the agent is told; it stays in the
Inbox), however its time was set or moved.

New setting `ATTENTION_SERVER_BOUNCE_NOT_BEFORE` (an RFC 3339 UTC time, for
example `2026-11-17T14:16:54Z`): messages that arrived before it never get a
bounce time, including on first start. Unset means no cutoff.

New offline command `harklight operator bounce-preview --store PATH`: with the
Server stopped, it counts how many open messages would fully bounce and how
many would soft-bounce at first start, honouring the cutoff. It reads a
temporary copy and never changes the store. See SELF_HOST.md.

## Upgrade and recovery

As in v0.2.21 through v0.2.24: FORMAT30 stores only, no new store change. Run
`bounce-preview` before upgrading if you want to know what will bounce. Take a
complete private backup before starting the new release. Returning to v0.2.23
or earlier shows fully bounced messages as open again. To return to an earlier
release, follow the rollback steps in SELF_HOST.md.

**Compatibility.** Harklight Server 0.2.25 speaks only the 0.2 protocol and
bundles Harklight Web 0.2.6. As with v0.2.21, install it together with an
iPhone app build that reads `receipt_expected`. Harklight Agent 0.2.6 and 0.2.7
work with it.

# Harklight Server v0.2.24

A message that is never opened now leaves the person's lists when its bounce
time is reached (by priority: Urgent 12 hours, High 1 day, Normal 3 days, Low
7 days, or the account's own levels). It disappears from the Inbox, search,
history, the badge and counts; clients are told through the existing
`item_removed` change, and opening or answering it directly answers
`404 not_found`. A pending default answer on it is cancelled.

The sending agent is still told: its view keeps the message open with a bounce
notice saying it was never opened, so the agent can decide whether to ask
again. Replies, revisions and progress on a bounced message are refused as on
an ended message; withdrawing it still works. A message that was opened but
not answered is unchanged: it stays in the Inbox and the agent is told it
hasn't been acted on. Answering, No comment, Seen, withdrawing, replacing,
expiry or a default answer before the bounce time stops both.

Released Harklight Web (0.2.5, 0.2.6) and Harklight app builds already remove a
message on `item_removed`, so no client update is needed.

## Upgrade and recovery

As in v0.2.21 through v0.2.23: FORMAT30 stores only, no new store change. Take
a complete private backup before starting the new release. To return to an
earlier release, follow the rollback steps in SELF_HOST.md.

**Compatibility.** Harklight Server 0.2.24 speaks only the 0.2 protocol and
bundles Harklight Web 0.2.6. As with v0.2.21, install it together with an
iPhone app build that reads `receipt_expected`. Harklight Agent 0.2.6 and 0.2.7
work with it.

# Harklight Server v0.2.23

Hosted servers can now rank waiting messages with Jev, the model-assisted
ranking that self-hosted servers set up on the Prioritization page. A hosted
Server takes the Jev endpoint and model from its startup settings, with the
existing key-file setting; when all three are set, Jev ranking is on for every
hosted account, and when they are not, every account uses the basic order. A
hosted account can no longer choose its own ranking: `POST /algorithm` answers
409 `algorithm_managed_by_server` for a choice that differs from the Server's
policy. Self-hosted servers keep using the dashboard connection and ignore the
startup Jev settings.

Every Jev request now has a 30-second limit covering connect, send and
receive. Before, a request that never answered could stall ranking; intake,
delivery and notifications were never held up. When Jev fails or times out,
waiting items stay in the basic order and ranking retries with growing gaps.

Jev requests send message content for ranking: titles, bodies, sender and
message IDs, stated stake, urgency and deadline, and the parent message for an
unseen chat reply.

## Upgrade and recovery

As in v0.2.21 and v0.2.22: FORMAT30 stores only, and no new store change. Take
a complete private backup before starting the new release. To return to an
earlier release, follow the rollback steps in SELF_HOST.md.

**Compatibility.** Harklight Server 0.2.23 speaks only the 0.2 protocol and
bundles Harklight Web 0.2.6. As with v0.2.21, install it together with an
iPhone app build that reads `receipt_expected`. Harklight Agent 0.2.6 and 0.2.7
work with it.

# Harklight Server v0.2.22

Bundles Harklight Web 0.2.6 (source edd52181, archive SHA-256
e4dcfd6d2ad364bb39923c68b77c00366d3d5788a50da5e4763d3cf5677236dd). Web 0.2.6
reads the delivery state `receipt_expected: false` that v0.2.21 introduced: an
old chat reply with no receipt evidence shows no delivery caption instead of
"Sending…". It still reads the older shapes. Everything else is as in v0.2.21.

## Upgrade and recovery

As in v0.2.21: the Server opens FORMAT30 stores only, and its one-time step
runs on the first start of v0.2.21 or later. Take a complete private backup
before starting the new release. To return to an earlier release, follow the
rollback steps in SELF_HOST.md.

**Compatibility.** Harklight Server 0.2.22 speaks only the 0.2 protocol. The
packaged web client handles `receipt_expected`. For the iPhone app, install
this release together with an app build that reads `receipt_expected`; older
builds show those old replies as still sending. Harklight Agent 0.2.6 is the
matching Agent release.

# Harklight Server v0.2.21

This release removes the Server's remaining handling of the old 0.1 protocol
and its guidance. **It speaks only the 0.2 protocol.**

- The Server opens FORMAT30 stores only (written by v0.2.16 and later). It
  refuses a FORMAT29 or older store with `unsupported_store_format` and leaves
  its data unchanged. To move an older store, first upgrade it with any release from
  v0.2.16 through v0.2.20, then install this one.
- On first start, a one-time step tidies data kept from 0.1: a Seen-only
  message stored without a type becomes an FYI, and a chat reply whose only
  delivery evidence came from 0.1 is marked delivered with no receipt expected
  (`receipt_expected: false`), replacing the old `legacy_caption` mark. It runs once and is
  recorded; later starts change nothing.
- Old browser sign-in records and retired 0.1 settings handling are gone.
- The served agent guides and the API and install documents no longer describe
  0.1 commands, ports or migration steps.

The message lookup by an old 0.1 message ID stays for now: Harklight Agent
releases up to 0.2.6 and current Tightbeam use it for conversations started
before 0.2. A later release removes it once they no longer need it.

## Upgrade and recovery

From v0.2.16 through v0.2.20 this release keeps FORMAT30; the one-time step
above adds one recorded migration. Take a complete private backup before
starting the new release. To return to an earlier release, follow the rollback
steps in SELF_HOST.md.

**Compatibility.** Harklight Server 0.2.21 uses protocol 0.2. Chat delivery
state no longer carries `legacy_caption`; old chat replies carry
`receipt_expected: false` instead. Harklight Web 0.2.5 and the current
Harklight app read only `legacy_caption`, so with them those old replies show
as still sending. Install this release together with a Harklight Web and app
that read `receipt_expected`; until then, stay on v0.2.20. Harklight Agent
0.2.6 is the matching Agent release.

# Harklight Server v0.2.20

For hosted servers, the agent setup guide served at
`/agent-skills/harklight-agent-saas/SKILL.md` now follows the hosted TestFlight
setting (`ATTENTION_SERVER_HOSTED_TESTFLIGHT_URL`), the same one the dashboard
uses. When it is unset or not a valid TestFlight invitation, the guide says
Harklight for iPhone is in testing and points to the invite request page; when
it is set, the guide links exactly that invitation. The guide no longer
contains a fixed TestFlight link. Self-hosted servers are unaffected.

## Upgrade and recovery

From v0.2.16 through v0.2.19 this release changes no store format: the store
stays FORMAT30. From FORMAT29 state, first start upgrades the store to FORMAT30
as in v0.2.16. Take a complete private backup before starting the new release.
To return to v0.2.16 through v0.2.19, stop the new Server and start that
release against the same state; to return to a FORMAT29 release, restore the
backup taken under that release.

**Compatibility.** Harklight Server 0.2.20 uses protocol 0.2 with no API
change. Harklight Agent 0.2.6 is the matching release. Harklight app 0.2.0 and
Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.19

Fixes a case where an agent could miss a reply. When an agent's live listen
stream stopped, the first person reply or agent answer written as the stream
closed was counted as delivered even though nothing received it, so every
later catch-up read by the agent (on first open, or after its cursor expired)
left it out. Entries written a minute or more after the stream
stopped, or after a Server restart, were not affected. The Server now keeps
such an entry in the catch-up read until the agent reports receipt or handoff
or acts on it. An agent may therefore see an entry it was already
streamed again on a fresh catch-up; it is the same turn with the same ID.

## Upgrade and recovery

From v0.2.16, v0.2.17 or v0.2.18 this release changes no store format: the
store stays FORMAT30. From FORMAT29 state, first start upgrades the store to
FORMAT30 as in v0.2.16. Take a complete private backup before starting the new
release. To return to v0.2.16, v0.2.17 or v0.2.18, stop the new Server and
start that release against the same state; to return to a FORMAT29 release,
restore the backup taken under that release.

**Compatibility.** Harklight Server 0.2.19 uses protocol 0.2 with no API
change. Harklight Agent 0.2.6 is the matching release. Harklight app 0.2.0 and
Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.18

The Server uses much less memory while saving. It used to keep a compressed
copy of every message's saved form in memory, about 13 KB per message; it now
keeps only the parts it can reuse, about 1 KB per message. In tests with 2,000
messages, peak memory during a run of writes fell from about 1.7 GB to under
300 MB, and those writes finished much sooner. Saved data is unchanged.

## Upgrade and recovery

From v0.2.16 or v0.2.17 this release changes no store format: the store stays
FORMAT30. From FORMAT29 state, first start upgrades the store to FORMAT30 as in
v0.2.16. Take a complete private backup before starting the new release. To
return to v0.2.16 or v0.2.17, stop the new Server and start that release
against the same state; to return to a FORMAT29 release, restore the backup
taken under that release.

**Compatibility.** Harklight Server 0.2.18 uses protocol 0.2 with no API
change. Harklight Agent 0.2.6 is the matching release. Harklight app 0.2.0 and
Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.17

A self-hosted Server can now deliver iPhone notifications without its own Apple
push keys, through the free Harklight push relay. To turn it on, uncomment
`ATTENTION_SERVER_PUSH_RELAY_URL` in `server.env` (or add the line on an
existing installation) and restart the Server. The relay service opens
separately from this release; until it is available, leave the line commented.
SELF_HOST.md lists exactly what the relay receives and logs: never message
text, titles or sender names. A relayed alert reads "New message in Harklight";
tapping it opens Harklight, which then catches up from your Server. Apple keys, when configured, still take
precedence and keep full notification content.

## Upgrade and recovery

From v0.2.16 this release changes no store format: the store stays FORMAT30.
From FORMAT29 state, first start upgrades the store to FORMAT30 as in v0.2.16.
Take a complete private backup before starting the new release. To return to
v0.2.16, stop the new Server and start v0.2.16 against the same state; to
return to a FORMAT29 release, restore the backup taken under that release.

**Compatibility.** Harklight Server 0.2.17 uses protocol 0.2. With the relay
configured, `PUT /v2/devices/me/push` succeeds without Apple keys. Harklight
Agent 0.2.6 is the matching release. Harklight app 0.2.0 and Harklight Web
0.2.5 are the client baseline.

# Harklight Server v0.2.16

Requests, chat replies, and messages marked High or Urgent alert the phone
with the standard iOS sound and vibration. Other messages arrive quietly
and still appear in Notification Center, the Inbox, and the badge. The
recipient can choose to receive all alerts. A quiet delivery does not hold
back the first alerting delivery in a five-minute window.

For connecting an agent, the server now serves a Harklight skill for Claude Code
and Codex at `/agent-skills/harklight/SKILL.md`, listed first on the Components
page. The Components page and the Invitation page show `harklight-agent setup`
with the invitation link; that command arrives in the next Harklight Agent
release.

## Upgrade and recovery

First start upgrades a FORMAT29 snapshot to FORMAT30 and uses the quiet
default for existing people. Take a complete private backup before starting
the new release. To return to a FORMAT29 release, stop the new Server and
restore that backup before starting the older release. The restore discards
subsequent writes and cannot undo notifications already delivered.

**Compatibility.** Harklight Server 0.2.16 uses protocol 0.2 and adds
`Settings.alerts` (`important` by default, or `all`) and `PUT /v2/settings/alerts`.
Existing Harklight Agent releases do not need to send any new fields; Harklight
Agent 0.2.5 is the matching release until the setup release. Harklight app 0.2.0
and Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.15

iPhone notifications now carry the version 2 sync payload that the Harklight
app expects. Before this release the app ignored what a notification carried:
iOS still showed the alert and badge, but a background notification did not make
the app sync, no banner appeared while the app was open, and tapping a
notification opened the app where it was rather than the message. Now a
notification names the item and the revision to catch up to, and tapping it
opens the message, or its chat for a chat reply. A grouped notification keeps
that link and shows fewer sender names when space is short.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.14
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The bundled Harklight Web is 0.2.5, as in v0.2.14.

**Compatibility.** Harklight Server 0.2.15 uses protocol 0.2. The push payload
is the one described in the client API (§6.2). Harklight Agent 0.2.5 is the
matching Agent release. Harklight app 0.2.0 and Harklight Web 0.2.5 are the
client baseline.

# Harklight Server v0.2.14

Any enrolled device on an account, an agent's identity as well as a phone, can
now read that account's focus (which screen and message each device has in
front) and push a view of a message to one of the account's devices. The
separate permission these needed is gone. Showing a message never marks it
Seen and never answers it. Nothing crosses accounts.

A focus report can say the FYI list is in front, with screen `fyi`. A view
pushed while the device is away and whose message disappears before delivery
still arrives, carrying the original item ID, so the app can say it is no longer
available. A new push naming a message that does not exist is still refused.

A signed-in browser session can now report focus too, with a name it supplies,
and appears in focus reads with kind `browser`. Browsers do not receive pushed
views. Harklight Web 0.2.5 reports focus once it runs against this Server.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.13
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The bundled Harklight Web is 0.2.5, as in v0.2.13.
Focus is kept in memory only.

**Compatibility.** Harklight Server 0.2.14 uses protocol 0.2 and keeps the client
API, adding the `fyi` focus screen, the `browser` focus kind, and browser focus
reports. Clients ignore values they do not know. Harklight Agent 0.2.5 is the
matching Agent release; `harklight-agent focus` and `show` from Agents 0.2.0 to
0.2.5 now work under an ordinary enrolled identity. Harklight app 0.2.0 and
Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.13

An agent's first read of a conversation, and the caller-owned pending read, now
also serve answers entered by a registered Granted Agent and default answers,
not only the person's own. Each answer is served once: an answer the agent
already received on its live feed is not served again, and a sender's own
close-out is not treated as an answer. Chat stays the person's turns only. The
pending read's prepare result says `"answers": "all"`, so a caller can tell a
Server that serves all of them; Harklight Agent 0.2.5 prints it.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.12
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The bundled Harklight Web is 0.2.5, as in v0.2.12.

On the first read after the upgrade, older answers entered by a Granted Agent or
by default are served once, conversation by conversation; with about 2,000 such
answers this took about 36 seconds in one test, then nothing more.

**Compatibility.** Harklight Server 0.2.13 uses protocol 0.2 and keeps the client
API, adding the `answers` member to the pending prepare result. Harklight Agent
0.2.5 is the matching Agent release. Harklight app 0.2.0 and Harklight Web 0.2.5
are the client baseline.

# Harklight Server v0.2.12

The bundled Harklight Web is 0.2.5. It looks closer to the iPhone app, shows
FYIs as their own rows with an ID tag, lets you change the sort, and closes the
chat panel on narrow windows. It also reports the page you are on as focus, once
a Server with the browser focus route is installed; until then it stops quietly.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.11
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The Server code is unchanged from v0.2.11.

**Compatibility.** Harklight Server 0.2.12 uses protocol 0.2 and keeps the
client API of v0.2.11. Harklight Agent 0.2.4 is the matching Agent release.
Harklight app 0.2.0 and Harklight Web 0.2.5 are the client baseline.

# Harklight Server v0.2.11

Any enrolled device or registered Granted Agent can now search, read and
download any message on its account, and retract an open one, whoever sent it.
Reading this way marks nothing Seen or Delivered and does not move an agent's
feed position. Search takes an exact session filter. A retraction records who
did it, as `retracted_by`, an opaque identifier that has meaning only within the
account. Answering, lifecycle and other writes keep their earlier rules, and a
correlated retraction that marks a Chat turn acted still belongs to the
originating key.

The bundled Harklight Web is 0.2.4. Each tab has its own FYI card and list:
Inbox's hold the FYIs not marked Seen, and Replied's hold the Seen ones. Opening
an ordinary FYI does not move it: it counts as Seen only once the Seen answer is
given. A Progress FYI also counts as Seen once it has been opened or marked Seen.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.10
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`.

After a rollback to an older Server and re-upgrade, a client may show a stale
availability label until it changes, as in v0.2.9.

**Compatibility.** Harklight Server 0.2.11 uses protocol 0.2 and keeps the
existing client API, with the new reads and the `retracted_by` field. From the
Agent command line, account-wide search, message-ID lookup and reading another
sender's message need Harklight Agent 0.2.5 or newer; older 0.2 Agents can retract
any open message on the account by item ID. On an older Server, Agent 0.2.5
reports search and message-ID lookup as unsupported, and retracting another
sender's message by item ID is refused. Harklight Agent 0.2.4 is
the matching Agent release for everything else. Recovering waiting replies on an
agent's first read needs Harklight Agent 0.2.1 or newer. Harklight app 0.2.0 and
Harklight Web 0.2.4 are the client baseline.

# Harklight Server v0.2.10

An agent now gets one notice when a message it sent was opened but not answered
by the end of its bounce interval. Until now only messages that were never
opened produced a notice. The message stays in the person's inbox, and the
person's view, notifications, Seen state and default answers do not change.
Progress, and a person writing in the chat, suppress the notice. If the message
is later answered, replaced or continued in chat, the notice stays visible to
the agent marked `moot`. Agents 0.2.0 to 0.2.3 read both kinds of notice. Agents
should not acknowledge a `moot` notice: those releases report exit 7 when they do.

An FYI's progress update sent by the Harklight Agent now keeps it Seen. In 0.2.9
this worked only for updates that left the message's `created_at` unchanged, and
the Agent moves `created_at` too. Any other change to the message still clears
Seen.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.9
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The bundled Harklight Web is 0.2.3, as in v0.2.9.

On the first start, agents may receive one notice for each eligible open message,
including messages whose agent session has ended. This reconsiders opened messages
accepted after 2026-11-17 14:16:54 UTC; it happens once. People receive nothing,
and no message leaves an inbox.

After a rollback to an older Server and re-upgrade, a client may show a stale
availability label until it changes, as in v0.2.9.

**Compatibility.** Harklight Server 0.2.10 uses protocol 0.2. It adds the
`soft_bounce` advisory type and the optional `moot` field to agent advisories and
keeps the rest of the client API. Harklight Agent 0.2.3 is the matching Agent release.
Recovering waiting replies on an agent's first read needs Harklight Agent 0.2.1
or newer; Agent 0.2.0 keeps its earlier first-read behavior. Harklight app 0.2.0
and Harklight Web 0.2.3 are the client baseline.

# Harklight Server v0.2.9

A chat now shows whether an agent is listening to it at the moment, including
after the request is answered or closed. Before this release a closed chat kept
the availability it had when it closed. The label changes when the agent starts
or stops listening, or sends again in that conversation. Where the agent's
session has ended, only a new message from that agent or a listener scoped to
exactly that conversation, under the original still-authorized key, makes it
available again.

An update that changes only an FYI's progress keeps it Seen. Any other change
to its content still clears Seen.

The bundled Harklight Web is 0.2.3. Marking unfinished Progress as Seen there no
longer answers it.

The bundled agent skills now describe Harklight Agent 0.2.2, including that
`read` and `listen` need `--session` or `--all-sessions`. The hosted skill names
Agent 0.2.1.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.8
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. On first start the Server corrects the stored
availability of existing chats, and connected clients receive those corrections
as ordinary message changes. Corrected messages send no notification and keep
their Seen and Delivered state.

After a rollback to an older Server and re-upgrade, a client may show a stale
availability label until it changes.

`SELF_HOST.md` now explains that restoring an older backup after the Server has
served clients repeats message IDs and feed positions, so apps and agents can
skip replies and lose later writes.

**Compatibility.** Harklight Server 0.2.9 uses protocol 0.2 and keeps the
existing client API. Harklight Agent 0.2.3 is the matching Agent release.
Recovering waiting replies on an agent's first read needs Harklight Agent 0.2.1
or newer; Agent 0.2.0 keeps its earlier first-read behavior. Harklight app 0.2.0
and Harklight Web 0.2.3 are the client baseline.

# Harklight Server v0.2.8

An idle live feed over HTTP/2 now stays open. The Server closed such a
connection after 60 seconds when the client sent nothing, even while the
Server's heartbeats were arriving, so a phone or a browser connected directly
to the Server reconnected about once a minute. Reconnecting resumed from the
saved position. Feeds over HTTP/1.1, which the Agent uses, were not affected.
TCP keepalive now detects a peer that has gone away.

Versions 0.2.0 through 0.2.7 have the same listener settings.

## Upgrade and recovery

The Server continues to use FORMAT29, and upgrading from v0.2.0 through v0.2.7
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`. The bundled Harklight Web is 0.2.2, as in v0.2.7.

**Compatibility.** Harklight Server 0.2.8 uses protocol 0.2 and keeps the
existing client API. Recovering waiting replies on an agent's first read needs
Harklight Agent 0.2.1; Agent 0.2.0 keeps its earlier first-read behavior.
Harklight app 0.2.0 and Harklight Web 0.2.2 are the client baseline. Protocol
0.1 clients require their protocol 0.2 upgrades.

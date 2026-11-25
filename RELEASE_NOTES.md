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
Inbox's hold the FYIs not marked Seen, and Replied's hold the Seen ones. An FYI
that offers Seen counts as Seen only with that answer, so opening it elsewhere
does not move it.

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
Harklight Web 0.2.4 are the client baseline. Protocol 0.1 clients require their
protocol 0.2 upgrades.

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
and Harklight Web 0.2.3 are the client baseline. Protocol 0.1 clients require
their protocol 0.2 upgrades.

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
and Harklight Web 0.2.3 are the client baseline. Protocol 0.1 clients require
their protocol 0.2 upgrades.

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

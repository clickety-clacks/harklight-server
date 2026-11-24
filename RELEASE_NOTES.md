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

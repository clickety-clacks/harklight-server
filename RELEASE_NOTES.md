# Harklight Server v0.2.7

An agent's first read on this Server, and recovery after its saved position
expires, now hands over every person reply and answer not yet recorded as
delivered, including replies on answered items and turns retained from
earlier versions. Each is recorded as delivered once, when the Server serves
it, and an interrupted read can be resumed. Using it from an agent needs
Harklight Agent 0.2.1.

Chat entries carry delivery evidence for people: whether an agent handed a
turn off, and, when it did not, the reason. Clients that support it show that
from the recorded evidence; turns already served by earlier Servers keep their
previous caption. `GET /v2/me` advertises `handoff_captions`.

The self-hosted dashboard bundles Harklight Web 0.2.2 (commit `d12cff3`), which
shows these captions. It is paired with this Server or newer; the Web bundle in
0.2.6 stays with earlier Servers.

## Upgrade and recovery

The Server continues to use FORMAT29. Upgrading from v0.2.0 through v0.2.6
requires no store format migration. A published 0.2.5 Server opens a store
written by this version without changing it. Follow the backup, upgrade, and
recovery steps in `SELF_HOST.md`.

**Compatibility.** Harklight Server 0.2.7 uses protocol 0.2 and keeps the
existing client API. Harklight Agent 0.2.0 keeps working with its earlier
first-read behavior; the new delivery behavior needs Harklight Agent 0.2.1.
Harklight app 0.2.0 and Harklight Web 0.2.2 are the client baseline. Protocol
0.1 clients require their protocol 0.2 upgrades.

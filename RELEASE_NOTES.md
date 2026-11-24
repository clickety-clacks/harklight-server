# Harklight Server v0.2.5

When an owner revokes an enrolled device, affected open messages from its
protocol 0.2 sessions now show that the session is gone. The update appears in
the change feed and message history. Existing conversations remain readable
and open to replies, and revoked credentials stay blocked.

## Upgrade and recovery

The Server continues to use FORMAT29. Upgrading from v0.2.0 through v0.2.4
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`.

**Compatibility.** Harklight Server 0.2.5 uses protocol 0.2 and retains the
existing client API and bundled Harklight Web at commit `3434a38`. The
protocol compatibility baseline is Harklight app 0.2.0 and Harklight Agent
0.2.0. Protocol 0.1 clients require their protocol 0.2 upgrades.

# Harklight Server v0.2.4

The native Inbox and change feed can render resolved older messages whose
stored records use an implicit initial revision. The existing message,
history, and file references are preserved.

## Upgrade and recovery

The Server continues to use FORMAT29. Upgrading from v0.2.0 through v0.2.3
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`.

**Compatibility.** Harklight Server 0.2.4 uses protocol 0.2 and retains the
existing client API and bundled Harklight Web at commit `3434a38`. The
protocol compatibility baseline is Harklight app 0.2.0 and Harklight Agent
0.2.0. Protocol 0.1 clients require their protocol 0.2 upgrades.

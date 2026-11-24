# Harklight Server v0.2.6

The self-hosted dashboard bundles Harklight Web 0.2.1 (commit `f7b1f91`),
with updated branding and a dedicated FYI inbox, which the Web client turns on
by default.

A catch-up page on the person feed no longer repeats an entry whose state is
unchanged. An entry that changes while a client is reading catch-up can still
appear once more at its newer revision, as section 6.1 of `client-api-v2.md`
describes.

## Upgrade and recovery

The Server continues to use FORMAT29. Upgrading from v0.2.0 through v0.2.5
requires no store format migration. Follow the backup, upgrade, and recovery
steps in `SELF_HOST.md`.

**Compatibility.** Harklight Server 0.2.6 uses protocol 0.2 and keeps the
existing client API. The protocol compatibility baseline is Harklight app
0.2.0 and Harklight Agent 0.2.0. Protocol 0.1 clients require their protocol
0.2 upgrades.

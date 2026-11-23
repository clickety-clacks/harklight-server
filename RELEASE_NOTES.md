# Harklight Server v0.2.2

Local `bin/harklight operator bootstrap` and `operator recover` failures now
tell you whether the store could not open or opened but could not finish
initialization. Check the `--store` path and permissions for an open failure,
or the startup diagnostics for an initialization failure. Both failures
return exit status 1. Bad arguments still return 2, and successful commands
return 0.

## Upgrade and recovery

The packaged Server still uses the FORMAT29 snapshot store. Upgrading from
v0.2.1 requires no format migration. The existing startup migration from
supported FORMAT27 stores to FORMAT29 remains available. Stop the old Server,
back up the complete private state, and keep the existing configuration and
identities. See `SELF_HOST.md` for upgrade and backup restoration.

**Compatibility.** Harklight Server 0.2.2 uses protocol 0.2, with the same
client API and bundled web revision as Server 0.2.1. The client compatibility
baseline is Harklight app 0.2.0, bundled Harklight Web at commit `3434a38`, and
Harklight Agent 0.2.0, exercised with Server 0.2.0. That client matrix was
run on 0.2.0. Protocol 0.1 clients require upgrading to their protocol 0.2 versions.

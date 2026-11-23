# Harklight Server v0.2.1

- **Packaged CLI help.** `bin/harklight --help` and `-h` show the same usage
  as `bin/harklight help`. Subcommand usage errors and obsolete agent options
  point to the bundled `SELF_HOST.md`.
- **Keyed storage preparation.** Engine transitions report the records they
  touch. Keyed commits validate changed records, including cross-record
  constraints, bounce state and open-default entries. Full validation still
  runs on keyed open and on irregular keyed transitions. Client and agent
  change-log rows for keyed commits are derived from the touched records.
  The detailed check for omitted touched records stays enabled. Tests compare
  incremental validation and change logs against their full-state equivalents.
  The packaged Server continues to open and write FORMAT29 through its existing
  snapshot store. These changes do not activate the FORMAT30 keyed store.

## Upgrade and recovery

Upgrading from v0.2.0 keeps FORMAT29 and requires no format migration. The
existing migration from supported FORMAT27 state to FORMAT29 remains available.
Stop the old server, back up the complete private state, and start the new
package with the existing configuration and identity. See `SELF_HOST.md` for
the upgrade and backup-restoration procedure.

**Compatibility.** Harklight Server 0.2.1 uses protocol 0.2 and keeps the same
client API and bundled web revision as Server 0.2.0. The compatibility baseline
is Harklight app 0.2.0, bundled Harklight Web at commit `3434a38`, and Harklight
Agent 0.2.0, exercised with Server 0.2.0. That client matrix was run on 0.2.0.
Protocol 0.1 clients require upgrading to their protocol 0.2 versions.

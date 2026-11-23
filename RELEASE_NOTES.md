# Harklight Server v0.2.3

The Server dashboard uses the updated Harklight wordmark and tab icon on
the owner dashboard, Components and install page, sign-in page, and join
page in hosted and self-hosted mode.

## Upgrade and recovery

The packaged Server continues to use the FORMAT29 snapshot store. Upgrading
from v0.2.2 requires no format migration. The existing startup migration for
supported FORMAT27 stores remains available. Follow the backup and upgrade
steps in `SELF_HOST.md`.

**Compatibility.** Harklight Server 0.2.3 uses protocol 0.2 and retains the
Server 0.2.2 client API and bundled Web revision. The client compatibility
baseline is Harklight app 0.2.0, bundled Harklight Web at commit `3434a38`,
and Harklight Agent 0.2.0, exercised with Server 0.2.0. Protocol 0.1 clients
require their protocol 0.2 upgrades.

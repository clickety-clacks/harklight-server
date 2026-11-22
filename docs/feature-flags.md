# Feature flags

Temporary flags hide unfinished features on `main`. Follow the rules in
[AGENTS.md](../AGENTS.md). Add a row in the PR introducing a flag, update it when
the default changes, and delete it in the PR removing the flag. Use a public
team or role for the owner and a public issue or work-item reference; keep private
machine names and user identifiers out of this register.

| Flag | Purpose | Owner | Work item | Default | Remove when |
| --- | --- | --- | --- | --- | --- |
| `client_api_v2` | Client API v2: the per-person change log and the `/v2` device and browser API ([contract](client-api-v2.md)). The Server no longer reads it: it always serves `/v2` only, never starts the `/relay` device WebSocket, always bounds each APNs provider attempt, and starts its Relay with both change logs on, ignoring any `client_api_v2` entry in the `ATTENTION_SERVER_FEATURE_FLAGS_PATH` file. Only a Relay started directly without the Server's value (tests) still reads the file, where an absent key is off. The headless `device` and `enroll status` tools always speak `/v2`. | Harklight Server delivery owner | wi_e41bc552-9279-41d0-b015-07174b692dec | On (no off path under the Server) | Removed inside Server 0.2.0 by owner exception: this row goes with the Relay's own reading of the flag |

The whole-server `ATTENTION_SERVER_ENABLE` startup switch and permanent operator
configuration are not temporary feature flags. This register does not declare
flags for unmerged work; authors add actual flags with their implementation.

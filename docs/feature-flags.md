# Feature flags

Temporary flags hide unfinished features on `main`. Follow the rules in
[AGENTS.md](../AGENTS.md). Add a row in the PR introducing a flag, update it when
the default changes, and delete it in the PR removing the flag. Use a public
team or role for the owner and a public issue or work-item reference; keep private
machine names and user identifiers out of this register.

| Flag | Purpose | Owner | Work item | Default | Remove when |
| --- | --- | --- | --- | --- | --- |
| `client_api_v2` | Client API v2: the per-person change log now, then the `/v2` device and browser API ([contract](client-api-v2.md)). With it on, the device port serves `/v2` only and the `/relay` device WebSocket does not start, so 0.1.x apps cannot connect. The APNs dispatcher also bounds each provider attempt on this path. Read once at startup from the `ATTENTION_SERVER_FEATURE_FLAGS_PATH` file. The headless `device` and `enroll status` tools read the same key from the same variable on their own machine, and speak `/v2` when it is on. | Harklight Server delivery owner | wi_e41bc552-9279-41d0-b015-07174b692dec | Off | Server 0.2.0 turns it on and removes the old client paths; the flag is deleted after that release |

The whole-server `ATTENTION_SERVER_ENABLE` startup switch and permanent operator
configuration are not temporary feature flags. This register does not declare
flags for unmerged work; authors add actual flags with their implementation.

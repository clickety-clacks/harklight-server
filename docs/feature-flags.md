# Feature flags

Temporary flags hide unfinished features on `main`. Follow the rules in
[AGENTS.md](../AGENTS.md). Add a row in the PR introducing a flag, update it when
the default changes, and delete it in the PR removing the flag. Use a public
team or role for the owner and a public issue or work-item reference; keep private
machine names and user identifiers out of this register.

| Flag | Purpose | Owner | Work item | Default | Remove when |
| --- | --- | --- | --- | --- | --- |
| `client_api` | Serves `POST /client/v1`, sessionless device reads and mutations on the device listener ([protocol](route-b-protocol.md#client-api-requests)) | Harklight Server delivery owner | wi_e41bc552-9279-41d0-b015-07174b692dec | Off; set `"client_api": true` in the `ATTENTION_SERVER_FEATURE_FLAGS_PATH` file | The flag has been on in a published release and the client API is accepted |

The whole-server `ATTENTION_SERVER_ENABLE` startup switch and permanent operator
configuration are not temporary feature flags. This register does not declare
flags for unmerged work; authors add actual flags with their implementation.

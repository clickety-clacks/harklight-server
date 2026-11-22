# Feature flags

Temporary flags hide unfinished features on `main`. Follow the rules in
[AGENTS.md](../AGENTS.md). Add a row in the PR introducing a flag, update it when
the default changes, and delete it in the PR removing the flag. Use a public
team or role for the owner and a public issue or work-item reference; keep private
machine names and user identifiers out of this register.

| Flag | Purpose | Owner | Work item | Default | Remove when |
| --- | --- | --- | --- | --- | --- |
| `config :attention_server, :keyed_validation` | Choose the keyed commit validator: `:full` whole-state check, `:incremental` per-address check, or `:both` (run both, raise on disagreement). Keyed commits always derive change-log rows from their touched addresses; `:both` also computes the whole-map log and raises naming any differing part | Harklight Server maintainers | W3 incremental keyed validation (W3a, stacked on #336; change logs W3d) | `:full`; `:both` in the test environment | Per-address validation is the production default (W3e) in a published release and accepted; then remove the whole-state keyed path |

The whole-server `ATTENTION_SERVER_ENABLE` startup switch and permanent operator
configuration are not temporary feature flags. This register does not declare
flags for unmerged work; authors add actual flags with their implementation.

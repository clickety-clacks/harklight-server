# Feature flags

Temporary flags hide unfinished features on `main`. Follow the rules in
[AGENTS.md](../AGENTS.md). Add a row in the PR introducing a flag, update it when
the default changes, and delete it in the PR removing the flag. Use a public
team or role for the owner and a public issue or work-item reference; keep private
machine names and user identifiers out of this register.

| Flag | Purpose | Owner | Work item | Default | Remove when |
| --- | --- | --- | --- | --- | --- |
| `config :attention_server, :keyed_validation` | Choose the keyed commit validator: `:full` whole-state check, `:incremental` per-address check, or `:both` (run both, raise on disagreement). Keyed commits always derive change-log rows from their touched addresses; `:both` also computes the whole-map log and raises naming any differing part | Harklight Server maintainers | W3 incremental keyed validation (W3a, stacked on #336; change logs W3d) | `:incremental`; `:both` in the test environment | Per-address validation is the production default (W3e) in a published release and accepted; then remove the whole-state keyed path |

| `ATTENTION_SERVER_SIGN_IN_S1=1` | Activate durable sign-in before the Server accepts traffic. A store already activated retains its auth generation on restart. | Harklight Server maintainers | wi_a080b661-df8a-414c-89f6-7fd006746149 | Off | After the S1 release is accepted and the gate is removed |

| `ATTENTION_SERVER_NATIVE_PASSKEYS=1` | Offer passkey sign-in in app sheets. Use only in an isolated acceptance fixture until the supported real-phone flow is verified. | Harklight Server maintainers | wi_a080b661-df8a-414c-89f6-7fd006746149 | Off | After the supported App passkey paths are accepted |

The whole-server `ATTENTION_SERVER_ENABLE` startup switch and permanent operator
configuration are not temporary feature flags. This register does not declare
flags for unmerged work; authors add actual flags with their implementation.

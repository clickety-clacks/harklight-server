# Harklight Server

## CI and releases

Keep changes small enough to review and merge while `main` remains usable. Do
not delay a focused fix for unrelated unfinished work unless there is a real
dependency. The Server CI workflow builds and tests pull requests to `main` and
pushes to `main`; fix a red `main` promptly or revert the breaking change.

CI uses Elixir 1.19.5 and Erlang/OTP 27.3.3 on Ubuntu 24.04, matching the
supported Linux build family. Its source checks are `mix deps.get`,
`MIX_ENV=prod mix compile`, and `mix test`. They do not create a release
package, install one, or establish runtime or deployment behavior.

Release installation stays separate in
`.github/workflows/harklight-release-install.yml`. That manually dispatched
workflow accepts an exact, published stable `vX.Y.Z` release tag, verifies the
release assets and source identity, then invokes the package installer. Keep
ordinary CI away from dispatching that workflow or installing a release. A
passing build can ship independently of unfinished feature behavior; enabling
that behavior remains a separate deployment decision after its checks pass.

`ATTENTION_SERVER_ENABLE=1` is an operator-controlled server startup switch;
this repository does not provide a general per-feature flag framework. When a
real change needs a temporary feature flag, default unfinished behavior off and
gate its server endpoint or background work as well as any client surface. Test
the production settings with the flag off and the intended next settings with
it on. A flag must not hide compile or startup failures, incompatible
migrations, or shared-dependency failures. Remove temporary flags after
rollout.

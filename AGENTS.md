# Harklight Server

## CI and releases

Work directly on `main`, the integration line; do not make feature branches.
Keep increments small, reviewable and releasable. Long features also land
incrementally with incomplete behavior disabled. Preserve independent review
and CI; if a required repository route prevents direct-main integration,
report the exact conflict to the delivery owner rather than bypassing or
changing protections. Do not delay a focused fix for unrelated unfinished work
unless there is a real dependency. CI builds and tests pull requests to `main`
and pushes to `main`; read the integrated result and promptly fix or revert a
red `main`.

CI uses Elixir 1.19.5 and Erlang/OTP 27.3.3 on Ubuntu 24.04, matching the
supported Linux build family. Its source checks are `mix deps.get`,
`MIX_ENV=prod mix compile`, and `mix test`. They do not create a release
package, install one, or establish runtime or deployment behavior.

Release installation stays separate in
`.github/workflows/harklight-release-install.yml`. That manually dispatched
workflow accepts an exact, published stable `vX.Y.Z` release tag, verifies the
release assets and source identity, then invokes the package installer. Keep
ordinary CI away from dispatching that workflow or installing a release.

Run the relevant tests and the smoke check or other validation you judge
appropriate. After they pass and independent review and CI are satisfied, ship
through the normal live release route without another user approval. Treat
later defects as bug reports to fix. Deploy passing builds independently of
feature enablement; keep unfinished behavior off until ready.

For the hosted web dashboard and sign-in surface, publishing a Server release
or running this repository's separate user-service installer does not promote
the hosted Server. Use the hosted repository's
[release-lock promotion route](https://github.com/clickety-clacks/harklight-hosted/blob/main/deploy/server-promotion.md),
with its readiness, compatibility, recovery and exact running-version checks.
Read back the affected live surface before claiming delivery; a commit on
`main`, a passing CI run or a published package alone is not live adoption.

`ATTENTION_SERVER_ENABLE=1` is an operator-controlled server startup switch;
this repository does not provide a general per-feature flag framework. When a
real change needs a temporary feature flag, use supported configuration,
default unfinished behavior off and gate its server endpoint or background
work as well as any client surface. Test
the production settings with the flag off and the intended next settings with
it on. A flag must not hide compile or startup failures, incompatible
migrations, or shared-dependency failures. Remove temporary flags after
rollout, including their obsolete conditional code. Do not invent a flag service
or repurpose the whole-server startup switch as a feature flag.

Background: [trunk-based development](https://trunkbaseddevelopment.com/) and
[feature toggles](https://martinfowler.com/articles/feature-toggles.html).

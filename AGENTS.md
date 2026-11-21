# Working in Harklight Server

This file holds the repository conventions for agents and contributors. Read it
explicitly when your harness does not load it. Automatic loading by Claude has
not been verified in this repository; no harness configuration is changed here.

## Main-based development

`main` is the long-lived integration line. Releases are tags on reviewed `main`
commits, with the temporary maintenance exception below.

- Work on a short-lived branch from current `main` and merge through a pull
  request within a day or two. Split larger work into reviewed slices behind a
  default-off feature flag instead of keeping a long-running branch.
- Keep `main` releasable. Merge a PR to `main`, or to maintenance for an emergency
  fix, once checks show no failure the base does not already have. Independent
  review runs alongside; fix findings in follow-up PRs. Do not hold a green PR
  for review. Compare failures with the base rather than waiving checks, and
  respect repository protections. Keep the branch current with `main` and delete
  it after its work has merged.
- Do not create release, candidate or notes branches. Release notes and package
  metadata change through ordinary PRs to `main`.
- If `main` breaks, restoring it takes priority; revert when that is the quickest
  repair. Do not delay a focused fix for unrelated unfinished work unless there
  is a real dependency.

Preserve in-flight commits, drafts and PRs while moving their work to `main`.
Their authors reconcile and retarget them without resetting, stashing or
discarding someone else's work. Do not delete an existing branch until its
remaining work has an accepted home.

The existing `release/0.1.35.x` branch is temporarily retained only for emergency
repairs to the currently installed release. An urgent repair may land there
first and be ported promptly to `main`; the transition must not delay it. Retire
that branch once `main` contains its fixes and the operator can move to a release
from `main`. This exception does not authorize new feature work on maintenance.

## Large features behind a feature flag

A feature too large for one short-lived PR lands in small reviewed slices with
unfinished behavior off by default. Prefer one flag per feature.

- Keep the old behavior working while the flag is off. Test both states,
  including production settings with it off and intended next settings with it
  on. Gate server endpoints and background work as well as any client surface.
- Record each temporary flag in [docs/feature-flags.md](docs/feature-flags.md):
  name, purpose, owner, work item, current default and removal condition.
- Change the default to on in its own small reviewed PR. Users receive the
  enabled default when they install a release containing that commit, or when
  the hosted deployment promotes it.
- Once the flag has been on in a published release and the feature is accepted,
  remove the flag, obsolete off path and its tests in a follow-up PR, and remove
  the register entry. Flag removal is part of completing the feature.
- For a replacement, branch by abstraction is an alternative: keep the old
  implementation behind an interface, introduce the replacement alongside it,
  switch over and remove the old implementation.

Use supported configuration for flags; this repository has no general feature
flag framework. `ATTENTION_SERVER_ENABLE=1` is the operator-controlled whole-server
startup switch, not a feature flag. Temporary flags are not permanent settings.
They cannot hide compile or startup failures, incompatible migrations or broken
shared dependencies. Do not invent a flag service for this process.

## Verification and packaging

[Server CI](.github/workflows/server-ci.yml) runs on PRs to `main` and pushes to
`main`, using Elixir 1.19.5 and Erlang/OTP 27.3.3 on Ubuntu 24.04. It runs
`mix deps.get`, `MIX_ENV=prod mix compile` and `mix test`. Read the integrated
result. These checks do not build or install a release package or establish
deployment behavior.

Run the relevant tests and smoke checks for the change. For release acceptance,
build from the clean intended commit with `scripts/build-package.sh` on the
Linux x86_64 target described in [SELF_HOST.md](SELF_HOST.md). Keep the version in
`mix.exs`, the package script, guide and release tag consistent. Verify the
tarball, `SHA256SUMS`, `BUILD_INFO` source identity and packaged guide; use that
same artifact for clean-install evidence and publication. The package bundles
ERTS, native dependencies, CLIs, guides and assembled skills; source CI alone
does not prove these work together.

Before publishing a store-upgrading release, complete isolated FORMAT migration
and upgrade rehearsals as release acceptance evidence. Exercise the packaged release with isolated representative state from supported
prior formats, checking retained data and authority, migration, restart and
recovery behavior. Follow the compatibility and backup limits in `SELF_HOST.md`:
one writer per database, no repeated bootstrap, and no assumption that switching
code or a symlink downgrades a store. Restoring a pre-upgrade backup loses later
writes and cannot undo external deliveries. Report unexercised recovery honestly;
a shared FORMAT number alone does not establish downgrade compatibility. Keep
test state isolated from live state and external provider sends.

## Releases and installation

Before tagging a release, every change it includes must have a clean independent
review by a reviewer who did not author it. Tag the resulting `main` commit as
`vMAJOR.MINOR.PATCH` and publish the verified package, checksum and guide through
the available release route. Tagging and
publishing need no additional human approval. Fix a bad release on `main` and
ship a new release; do not patch a deployment by hand.

End every release's notes with a **Compatibility** paragraph. Name the protocol
version (0.1 or 0.2) and list the verified compatible versions of Harklight
Server, the app, the web client, and Harklight Agent. For releases using
protocol 0.1, state that protocol 0.2 supersedes it.

The intended process is CI publishing releases, but this repository currently
has no automatic release-building or publishing workflow. Implementing that
automation is separate work. Source CI does not publish, and publishing does
not install.

Installation is separately authorized and manually dispatched through
[harklight-release-install.yml](.github/workflows/harklight-release-install.yml)
at an exact published stable tag. It verifies the release assets and source
identity before invoking the packaged installer. Follow
[HARKLIGHT_RELEASE_CI.md](docs/HARKLIGHT_RELEASE_CI.md) for this route; ordinary CI
must not dispatch it. A production installation that requires the operator's
explicit approval still requires that approval for the exact proposed install.
Feature enablement remains separate from release delivery.

For the hosted web dashboard and sign-in surface, a Server release or this
repository's user-service installer does not promote the hosted Server. Use the
hosted repository's
[release-lock promotion route](https://github.com/clickety-clacks/harklight-hosted/blob/main/deploy/server-promotion.md),
including its readiness, compatibility, recovery and exact running-version
checks. Read back the affected live surface before claiming delivery; a commit,
passing CI or published package alone is not live adoption.

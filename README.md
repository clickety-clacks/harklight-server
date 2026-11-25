# Harklight Server

Harklight lets an authorized agent or script send a request to a person and read
the person's deliberate reply. This repository contains the self-hosted Elixir
server, packaged sender tools and optional APNs transport for the
Harklight iOS/iPadOS app.

For Tightbeam organizations using the engineering-kungfu bundle, the
[Harklight for Tightbeam pack](https://github.com/clickety-clacks/harklight-tightbeam-pack)
adds the Harklight Agent skill, elects it for the roles used by the reference
installation, and includes a marked guidance section for reaching the operator
through Harklight.

## Install the server

For the FORMAT30 v0.2.20 server package, use the release page below after
publication to download the Linux x86_64 archive, `SHA256SUMS` and
`SELF_HOST.md`. Verify the checksum before extraction:

[v0.2.20 release](https://github.com/clickety-clacks/harklight-server/releases/tag/v0.2.20)

```sh
sha256sum -c SHA256SUMS
tar -xzf harklight-server-0.2.20-linux-x86_64.tar.gz
```

Follow [SELF_HOST.md](SELF_HOST.md) for a fresh installation, tailnet routing,
owner approval, start/stop and state-preserving upgrades. The package bundles
Erlang/OTP; the server box needs no Mix, Hex or source checkout. Its supported
target is Linux x86_64 with glibc 2.39 or newer and the libraries in `BUILD_INFO`.
The package does not install a service, configure the tailnet or expose ports.
Release publication does not install the server; installation is a separate
manual action.

The app, packaged web client, Harklight Agent and packaged tools use client
API v2 (`/v2`) on the device port. See [RELEASE_NOTES.md](RELEASE_NOTES.md)
for the compatibility of published packages.

State, owner/device identities, trust roots and sender grants live outside the
versioned package. Run one server per database and keep private backups. The
server built from this source opens and writes FORMAT30 snapshots only.
FORMAT30 was introduced in v0.2.16. To move a store on FORMAT29 or earlier,
upgrade through v0.2.16–v0.2.20 using a release that supports the source
format, then verify it writes FORMAT30 before switching. Preserve the
state path, private files and trust root, and keep the existing owner identity.

Every registered, non-revoked agent can read the account's messages regardless
of sender. Sender filters select search results. Answering requires both a
matching read grant and a matching respond grant.

Before upgrading, stop the server and retain a complete private
state backup. The release installer also takes one full private-root backup
while the service is stopped and before switching releases. It attempts to
restore this snapshot if the candidate start or process-readiness check fails.
The optional owner agent-list check is warning-only and does not trigger a
rollback; there is no automatic rollback after readiness. Recovery can fail
and leave the service stopped. A successful isolated rehearsal is evidence for
the tested archive and sequence, not a guarantee of recovery in every
installation.

Restoring a backup loses every write made after it and cannot undo external
notifications, bounces or agent deliveries. See [SELF_HOST.md](SELF_HOST.md)
for backup and rollback details.

The first answer settles a request, and a second answer is refused. Chat on the
request stays open after any outcome (answered, No comment, cleared, expired,
withdrawn or superseded): the person can keep writing and the agent can keep
replying, more than once to the same chat message. A chat message to an agent
that is not listening is accepted and delivered when the agent next listens.

Configurable score-based priority levels and unseen-message advisories first
shipped in v0.1.30. When a request reaches its configured unseen interval, the
server adds an advisory to the originating agent session asking whether the
request is still valid and worth resending and whether its importance should be
raised. It does not automatically resend the request or change its priority.

The archive does not contain the `harklight-agent` conversation CLI; install
it separately as described under [Get the agent CLI](#get-the-agent-cli).
The server/device CLI is `$PACKAGE/bin/harklight`.

## Send a request and read its reply

The package ships these guidance files.

- `skills/harklight/SKILL.md` is the Claude Code and Codex skill installed by
  `harklight-agent setup`.
- `skills/harklight-agent/SKILL.md` is the self-hosted operator skill.
- `skills/harklight-agent-saas/SKILL.md` is the standalone Hosted guide.
- `skills/core.md` is the shared conversation core included in both skills.

Edit the corresponding templates under `skills/` and the shared core. The
release package assembles them automatically. To build the files without
starting the Server, run the assembly script.

```sh
scripts/build-skills.sh dist/skills
```

A configured dashboard links the guidance files from **Components** and
serves the coding-agent skill at `/agent-skills/harklight/SKILL.md`.

### Get the agent CLI

The `harklight-agent` command-line client sends requests and listens for
request-bound replies. It is released separately from the server at
<https://github.com/clickety-clacks/harklight-agent>. This Server works
with Harklight Agent 0.2.6 or a later compatible release.
Download the tarball for your platform and `SHA256SUMS` from the
[Agent releases page](https://github.com/clickety-clacks/harklight-agent/releases),
verify the checksum and put the binary on your `PATH`. Targets are
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu`:

```sh
v=VERSION t=x86_64-unknown-linux-gnu
base="https://github.com/clickety-clacks/harklight-agent/releases/download/v$v"
curl -fLO "$base/harklight-agent-$v-$t.tar.gz"
curl -fLO "$base/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS   # shasum -a 256 on macOS
tar -xzf "harklight-agent-$v-$t.tar.gz"
install -m 755 "harklight-agent-$v-$t/harklight-agent" ~/.local/bin/
```

Use the [shared conversation core](skills/core.md) for Agent commands and
message formats. An existing identity takes the device port from
`--device-port` or `HARKLIGHT_DEVICE_PORT`.

Agents send new messages through the authenticated
[`harklight-agent` session route](skills/core.md). Keep the originating session,
enrolled identity and journal to receive replies. If that route cannot listen,
use `harklight-agent read --session HANDLE` when the originating work resumes;
it reads once without acknowledging an event or promising future delivery.
Enrolled-device `read` and `respond` remain available for requests already
stored by the server; they do not create new messages or retrieve an origin
receipt. Enrolled devices and registered agents can search and read within their
account. Registered agents still need the applicable grants to respond.

Agent search lists messages newest original acceptance first. Pass back its
`next_page` with the same filters for the next page.
The existing informational Seen value action and request Reply controls pass
through unchanged. Receiving-client ordering, styling and controls depend on
the installed client; upgrading the server alone does not establish those UI changes.

The conversation CLI is released separately and is not included in this server
package.

## No comment and client compatibility

No comment is an answer with none of `choice`, `comment` or `result`, sent
with the displayed `request_revision`. A stale revision is refused without
answering the request. Clients use client API v2.

## Device joining and notifications

The optional HTTPS owner dashboard creates person-bound one-use invitations,
reviews the current code and key and approves devices with optional sender grants.
The app keeps its own private key. A pending approval does not expire, and
starting again with the same key returns the same waiting request and code;
denial, cancellation and revocation remain terminal. See
[GUIDED_JOIN.md](GUIDED_JOIN.md) for the server interface.

APNs is opt-in and requires protected signing-key configuration for the
registered profile and an enrolled app-issued address. Each device's
registration `profile_id` selects both its Apple environment and matching key:
`harklight-sandbox` uses the existing Sandbox key with
`api.sandbox.push.apple.com`, while `harklight-production` uses its separate
Production key with `api.push.apple.com`. The server does not fall back to the
other profile's key; see [SELF_HOST.md](SELF_HOST.md) for the configuration
names. FCM is unsupported. New unresolved attention can alert; terminal-only
updates use content-free background sync. Provider acceptance does not prove a
phone displayed a notification, fetched a request or submitted a reply.

## Build and compatibility

Build on Linux x86_64 using Elixir 1.19.5/OTP 27 and the native prerequisites
in `SELF_HOST.md`:

```sh
./scripts/build-package.sh
```

The script writes the archive and `SHA256SUMS` in `dist/`. Build identity and
native dependencies are recorded in the archive's `BUILD_INFO`. Source-level
tests use the documented `mix test` command on a permitted development host;
they do not establish real-device notification delivery.

The existing runtime identifiers remain supported: Mix app `:attention_server`,
namespace `AttentionServer`, `ATTENTION_SERVER_*` variables, `mix attention.*`
and `bin/attention_server`. Devices, registered agents and the conversation
CLI use client API v2 on the device port. Durable
messages and operation receipts preserve first acceptance and exact retry
semantics across restart. Tailnet access alone grants no person or sender rights.

Top-level choice styling remains held by default across all send routes. An
operator may set `ATTENTION_SERVER_CHOICE_STYLES_HELD=false` only after verifying
that the installed client versions preserve usable answer controls when that
member is present.

This project is licensed under the MIT license (see `LICENSE`). Third-party
dependency notices remain in their shipped distribution files.

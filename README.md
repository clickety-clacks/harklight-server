# Harklight Server

Harklight lets an authorized agent or script send a request to a person and read
the person's deliberate reply. This repository contains the self-hosted Elixir
server, packaged sender tools and optional APNs transport for the
Harklight iOS/iPadOS app.

## Install the server

For the FORMAT24 v0.1.21 server package, use the release page below after it
is published to download the Linux x86_64 archive, `SHA256SUMS` and
`SELF_HOST.md`. Verify the checksum before extraction:

[v0.1.21 release](https://github.com/clickety-clacks/harklight-server/releases/tag/v0.1.21)
(available only after publication).

```sh
sha256sum -c SHA256SUMS
tar -xzf harklight-server-0.1.21-linux-x86_64.tar.gz
```

Follow [SELF_HOST.md](SELF_HOST.md) for a fresh installation, tailnet routing,
owner approval, start/stop and state-preserving upgrades. The package bundles
Erlang/OTP; the server box needs no Mix, Hex or source checkout. Its supported
target is Linux x86_64 with glibc 2.39 or newer and the libraries in `BUILD_INFO`.
The package does not install a service, configure the tailnet or expose ports.

State, owner/device identities, trust roots and sender grants live outside the
versioned package. Run one server per database. Keep private keys and backups
private; do not repeat bootstrap over an existing installation.
This source, v0.1.20, v0.1.19, v0.1.18, v0.1.17, v0.1.16, v0.1.15, v0.1.14 and v0.1.13 use FORMAT24 and migrate supported older state on startup.
The v0.1.12 package uses FORMAT23, v0.1.11 uses FORMAT22, and v0.1.10 accepts
formats 1–6 and 8–21. Those pre-FORMAT24 packages cannot start against FORMAT24
state. The release installer creates one full private backup before switching
releases. If
the candidate process does not become ready, the installer restores that
backup, selects and starts the prior release, and reports that writes since the
backup may have been lost. Returning to v0.1.12 or earlier after a FORMAT24
upgrade requires restoring the complete pre-upgrade backup.

The archive does not contain the `harklight-agent` conversation CLI; install
it separately as described under [Get the agent CLI](#get-the-agent-cli).
The server/device CLI is `$PACKAGE/bin/harklight`.

## Send a request and read its reply

The package ships three guidance files: `skills/harklight-agent/SKILL.md` for
self-hosted agents, `skills/harklight-agent-saas/SKILL.md` for hosted agents, and
`skills/core.md`. Both skills contain the shared core and can be copied alone
into a harness's configured skill directory. Installation does not copy them
automatically or grant credentials.

The [self-hosted source](skills/harklight-agent/SKILL.md.in) and
[hosted source](skills/harklight-agent-saas/SKILL.md.in) contain only their
mode-specific instructions. [The core](skills/core.md) is authored once. Build
the same three files without building or starting the server:

```sh
scripts/build-skills.sh dist/skills
```

The release package invokes this assembly automatically. Edit the source
files, not generated copies. The generated SaaS skill ships in the server
archive and can be served at `/agent-skills/harklight-agent-saas/SKILL.md` by a
configured dashboard; repository availability does not establish publication
at a hosted service's address.

### Get the agent CLI

The `harklight-agent` command-line client sends requests and listens for
request-bound replies. It is released separately from the server at
<https://github.com/clickety-clacks/harklight-agent>; the current release is
v0.1.5. Install it with Homebrew on macOS or Linux:

```sh
brew install clickety-clacks/tap/harklight-agent
```

Or download the tarball for your platform and `SHA256SUMS` from the
[release page](https://github.com/clickety-clacks/harklight-agent/releases/tag/v0.1.5),
verify the checksum and put the binary on your `PATH`. Targets are
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu`:

```sh
v=0.1.5 t=x86_64-unknown-linux-gnu
base="https://github.com/clickety-clacks/harklight-agent/releases/download/v$v"
curl -fLO "$base/harklight-agent-$v-$t.tar.gz"
curl -fLO "$base/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS   # shasum -a 256 on macOS
tar -xzf "harklight-agent-$v-$t.tar.gz"
install -m 755 "harklight-agent-$v-$t/harklight-agent" ~/.local/bin/
```

`harklight-agent --version` prints `harklight-agent 0.1.5`. The
[shared conversation core](skills/core.md) describes
its commands and message format.

Agents send new messages and listen for replies through the authenticated
[`harklight-agent` session route](skills/core.md).
Enrolled-device `send` and correlated `receipt` commands are available with the
earlier device interface. `read` and `respond` remain available for requests
already stored by the server. Registered agent identities have separately
granted search/get/respond access.

Agent inbox search lists unresolved requests before resolved history, newest
original server acceptance first within each group. Search pagination preserves
its initial ordering snapshot; relay replay and contiguous ACK stay chronological.
The existing informational Seen value action and request Reply controls pass
through unchanged. Receiving-client ordering, styling and controls depend on
the installed client; upgrading the server alone does not establish those UI changes.

The origin API also supports a receipt-backed attachment update for an existing
session handle. It stores or clears the provider session identifier on that same
origin session; it does not transfer a session to another device or server, or
prove that the provider session remains active. The conversation CLI is released
separately and is not included in this server package.

## No comment and client compatibility

This release includes the payload-free No comment terminal outcome. The app must
send the displayed `request_revision` with `answer_no_comment`; ordinary requests
use revision `1`. Missing or stale revisions are refused without answering the
request. Installed app build11 sends the older two-field frame and cannot use
No comment until a client update supplies the revision. Upgrading this server
does not update the app. Ordinary choice and typed replies retain their existing
protocol. See [SELF_HOST.md](SELF_HOST.md) for the compatibility evidence limits.

## Device joining and notifications

The optional HTTPS owner dashboard creates person-bound one-use invitations,
reviews the current code/key/deadline and approves devices without sender rights.
The app keeps its own private key. Default pending approval lasts 24 hours;
ordinary expiry can renew the same retained device request without a new scan or
key, while denial, cancellation and revocation remain terminal. See
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
namespace `AttentionServer`, `ATTENTION_SERVER_*` variables, `mix attention.*`,
`bin/attention_server`, `/relay`, `attention-relay/1` and `/agent/v1`. Durable
messages and operation receipts preserve first acceptance and exact retry
semantics across restart. Tailnet access alone grants no person or sender rights.

Top-level choice styling remains held by default across all send routes. An
operator may set `ATTENTION_SERVER_CHOICE_STYLES_HELD=false` only after verifying
that the installed client versions preserve usable answer controls when that
member is present.

This project is licensed under the MIT license (see `LICENSE`). Third-party
dependency notices remain in their shipped distribution files.

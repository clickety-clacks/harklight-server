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

For the FORMAT29 v0.1.40 server package, use the release page below after
publication to download the Linux x86_64 archive, `SHA256SUMS` and
`SELF_HOST.md`. Verify the checksum before extraction:

[v0.1.40 release](https://github.com/clickety-clacks/harklight-server/releases/tag/v0.1.40)

```sh
sha256sum -c SHA256SUMS
tar -xzf harklight-server-0.1.40-linux-x86_64.tar.gz
```

Follow [SELF_HOST.md](SELF_HOST.md) for a fresh installation, tailnet routing,
owner approval, start/stop and state-preserving upgrades. The package bundles
Erlang/OTP; the server box needs no Mix, Hex or source checkout. Its supported
target is Linux x86_64 with glibc 2.39 or newer and the libraries in `BUILD_INFO`.
The package does not install a service, configure the tailnet or expose ports.

State, owner/device identities, trust roots and sender grants live outside the
versioned package. Run one server per database. Keep private keys and backups
private; do not repeat bootstrap over an existing installation.
This source, v0.1.40, writes FORMAT29. From supported FORMAT27 state such as
the one written by v0.1.36, startup applies two ordered steps: first it adds
`see_focus: false` and `push_view: false` only where device records lack those
values, preserving existing values; then it initializes the hosted
`delivery_states` map to empty. The final migrated store is FORMAT29; the source
does not establish a separately persisted intermediate FORMAT28 state.
Every registered, non-revoked agent can read all messages on the account it
is registered to, regardless of sender. Sender filters select search results
only; they do not limit access. Existing respond-grant values and response
checks are unchanged. New registrations need no per-sender read grant for
reads; answering still requires both a matching read grant and a matching
respond grant. The v0.1.35, v0.1.36, v0.1.38, and planned v0.1.39 packages
write FORMAT27 and
cannot read resulting FORMAT29 state. Changing only the selected code or
release link is not a safe downgrade. Earlier v0.1.28 writes FORMAT25, while
v0.1.27 and v0.1.26 write
FORMAT24; see `SELF_HOST.md` for earlier package-format details.

The release installer takes one full private-root backup while the service is
stopped and before switching releases. It attempts to restore this snapshot if
the candidate does not become ready, but the restore path has not been
exercised in preparation of this package. Restore or prior-release startup can
fail and leave the service stopped. Restoring the snapshot discards every write
made after capture and cannot undo external notifications, bounces or agent
deliveries. No recovery success or time is guaranteed.

Before the first v0.1.40 start, stop the server and take a complete private-state
backup. After any v0.1.40 write, returning to v0.1.39 requires stopping the
server, restoring that pre-start backup and selecting v0.1.39; changing only
the code or release link cannot make FORMAT29 readable. This restore loses every
write made since the backup and cannot undo external notifications, bounces or
agent deliveries. Restore success is unproved and has not been tested; no
recovery success is guaranteed.

v0.1.32 adds a side-effect-free origin-message read by sender ID and message ID.
The matching `harklight-agent message-get` interface shipped in Agent 0.1.9.
That Agent release was published on 2026-11-11. The command reads the latest
committed revision by default or a specific retained revision with
`--revision N`. The read does not mark Seen; an unavailable requested revision
returns `revision_unavailable`.

Configurable score-based priority levels and unseen-message advisories first
shipped in v0.1.30. When a request reaches its configured unseen interval, the
server adds an advisory to the originating agent session asking whether the
request is still valid and worth resending and whether its importance should be
raised. It does not automatically resend the request or change its priority.

Stable v0.1.31 was published on 2026-11-11 before v0.1.32. It includes the
device-revocation origin-session gone behavior, durable device-level access reads
and updates with connection-local sync hints and durable operation receipts,
bounded Relay timeout outcomes recorded outside the request path, and omission
of cached APNs tokens from push-dispatcher crash/status output. Configurable
priority levels, unseen-message advisories, closed-chat behavior and Bandit
1.14.0 were already present in v0.1.30. Release publication does not install the
server; installation is a separate manual action.

Version 0.1.25 and later persist delivery acknowledgements (ACK1 and ACK2).
No safe code-only downgrade to 0.1.24 or earlier has been established once
0.1.25 or later writes new data. Agree on recovery before cutover and retain the
complete private pre-upgrade backup; restoring it discards all writes since
capture.

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
<https://github.com/clickety-clacks/harklight-agent>; the latest published
release as of 2026-11-13 is v0.1.11. Install it with Homebrew on macOS or Linux:

```sh
brew install clickety-clacks/tap/harklight-agent
```

Or download the tarball for your platform and `SHA256SUMS` from the
[release page](https://github.com/clickety-clacks/harklight-agent/releases/tag/v0.1.11),
verify the checksum and put the binary on your `PATH`. Targets are
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu`:

```sh
v=0.1.11 t=x86_64-unknown-linux-gnu
base="https://github.com/clickety-clacks/harklight-agent/releases/download/v$v"
curl -fLO "$base/harklight-agent-$v-$t.tar.gz"
curl -fLO "$base/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS   # shasum -a 256 on macOS
tar -xzf "harklight-agent-$v-$t.tar.gz"
install -m 755 "harklight-agent-$v-$t/harklight-agent" ~/.local/bin/
```

`harklight-agent --version` prints `harklight-agent 0.1.11`. The
[shared conversation core](skills/core.md) describes
its commands and message format.

Agents send new messages through the authenticated
[`harklight-agent` session route](skills/core.md). Keep the originating session,
enrolled identity and journal to receive replies. If that route cannot listen,
use `harklight-agent read --session HANDLE` when the originating work resumes;
it reads once without acknowledging an event or promising future delivery.
Enrolled-device `read` and `respond` remain available for requests already
stored by the server; they do not create new messages or retrieve an origin
receipt. Registered agent identities have separately granted
search/get/respond access.

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

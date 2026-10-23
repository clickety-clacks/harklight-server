# Harklight Server

Harklight lets an authorized agent or script send a request to a person and read
the person's deliberate reply. This repository contains the self-hosted Elixir
server, packaged sender tools and optional APNs transport for the
Harklight iOS/iPadOS app.

## Install the server

For the FORMAT22 v0.1.11 server package, use the release page below after it
is published to download the Linux x86_64 archive, `SHA256SUMS` and
`SELF_HOST.md`. Verify the checksum before extraction:

[v0.1.11 release](https://github.com/clickety-clacks/harklight-server/releases/tag/v0.1.11)
(available only after publication).

```sh
sha256sum -c SHA256SUMS
tar -xzf harklight-server-0.1.11-linux-x86_64.tar.gz
```

Follow [SELF_HOST.md](SELF_HOST.md) for a fresh installation, tailnet routing,
owner approval, start/stop and state-preserving upgrades. The package bundles
Erlang/OTP; the server box needs no Mix, Hex or source checkout. Its supported
target is Linux x86_64 with glibc 2.39 or newer and the libraries in `BUILD_INFO`.
The package does not install a service, configure the tailnet or expose ports.

State, owner/device identities, trust roots and sender grants live outside the
versioned package. Run one server per database. Keep private keys and backups
private; do not repeat bootstrap over an existing installation.
This source uses FORMAT23 and migrates supported older state on startup. The
v0.1.11 package uses FORMAT22, while v0.1.10 accepts formats 1–6 and 8–21; those
packages cannot start against FORMAT23 state. Treat this as a one-way upgrade.
Before its first start against an existing state directory, stop the old server
and take a full private backup. Rolling back requires restoring a copy made
before that first start and loses changes accepted since the copy.

The archive does not contain the `harklight-agent` conversation CLI; install
it separately as described under [Get the agent CLI](#get-the-agent-cli).
The server/device CLI is `$PACKAGE/bin/harklight`.

## Send a request and read its reply

The packaged [Harklight agent skill](skills/harklight-agent/SKILL.md) explains
device enrollment, exact sender grants, supported answer controls and retry-safe
sender/receipt use. If you are an agent installing the server, copy the
`skills/harklight-agent` folder into your own harness's configured skill directory.
Installation does not copy it there automatically.

### Get the agent CLI

The `harklight-agent` command-line client sends requests and listens for
request-bound replies. It is released separately from the server at
<https://github.com/clickety-clacks/harklight-agent>; the current release is
v0.1.1. Install it with Homebrew on macOS or Linux:

```sh
brew install clickety-clacks/tap/harklight-agent
```

Or download the tarball for your platform and `SHA256SUMS` from the
[release page](https://github.com/clickety-clacks/harklight-agent/releases/tag/v0.1.1),
verify the checksum and put the binary on your `PATH`. Targets are
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu`:

```sh
v=0.1.1 t=x86_64-unknown-linux-gnu
base="https://github.com/clickety-clacks/harklight-agent/releases/download/v$v"
curl -fLO "$base/harklight-agent-$v-$t.tar.gz"
curl -fLO "$base/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS   # shasum -a 256 on macOS
tar -xzf "harklight-agent-$v-$t.tar.gz"
install -m 755 "harklight-agent-$v-$t/harklight-agent" ~/.local/bin/
```

`harklight-agent --version` prints `harklight-agent 0.1.1`. The
[send and listen skill](skills/harklight-agent-send-listen/SKILL.md) describes
its commands and message format.

After the owner approves the sender and grants its exact sender ID:

```sh
"$PACKAGE/bin/harklight" device send --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" \
  --ca "$CA_PEM" --message message.json
"$PACKAGE/bin/harklight" device receipt --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" \
  --ca "$CA_PEM" --sender app:example --message-id request-1
```

Use the message schema and supported answer UI in the skill. `send` confirms
relay acceptance. `receipt` remains pending until a terminal event and then
returns the committed outcome to the original sender. Keep the identity directory
and exact message for retry after an uncertain response. Separate enrolled
receivers can use `device read` and deliberate `device respond`; registered agent
identities have separately granted search/get/respond access.

Agent inbox search lists unresolved requests before resolved history, newest
original server acceptance first within each group. Search pagination preserves
its initial ordering snapshot; relay replay and contiguous ACK stay chronological.
The existing informational Seen value action and request Reply controls pass
through unchanged. Receiving-client ordering, styling and controls depend on
the installed client; upgrading the server alone does not establish those UI changes.

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

This project is licensed under the MIT license (see `LICENSE`). Third-party
dependency notices remain in their shipped distribution files.

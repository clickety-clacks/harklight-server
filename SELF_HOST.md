# Harklight Server self-hosted installation

Version: **0.2.34**. Package target: **Linux x86_64, glibc 2.39 or newer**, with OpenSSL 3 shared libraries and libstdc++6 (built on Ubuntu 24.04). The package bundles
Erlang/OTP and Elixir bytecode; users need no Mix, Hex, source checkout or
Erlang installation. It needs Bash, OpenSSL CLI, tar and the platform libraries
listed in BUILD_INFO. This is a single-process self-hosted server cut.

This package includes the scripted sender/reply path, owner-approved guided
device joining, the optional HTTPS owner dashboard and APNs transport.
It includes the server/device CLI `bin/harklight`. The `harklight-agent`
conversation CLI is released separately; see "Agent conversation CLI" below.
Install the Harklight iOS/iPadOS app separately. No macOS or Windows executable
is shipped.
Provider acceptance does not prove notification display or a person's reply.

## Protocol 0.2

The device port serves client API v2 (`/v2`) over mutual TLS. For agents,
use Harklight Agent 0.2.6 or a later compatible release.

No comment is an answer with none of `choice`, `comment` or `result`, sent with
the displayed `request_revision`. A stale revision is refused without answering
the request; a valid current-revision answer commits a payload-free
`no_comment` result.

## Download and verify

After publication, download the v0.2.34 release from
clickety-clacks/harklight-server, tag v0.2.34. Get
harklight-server-0.2.34-linux-x86_64.tar.gz, SHA256SUMS and SELF_HOST.md from
that release. Run sha256sum -c SHA256SUMS and extract into an operator-owned
versioned directory. BUILD_INFO identifies the source commit, build host,
toolchain, source status and native dependencies.

This project is licensed under the MIT license (see `LICENSE`). Third-party
runtime/dependency notices remain in their shipped distribution files.

## First owner bootstrap

Choose the DNS hostname clients will validate. For a same-machine preview,
localhost works. Install into a directory you own and run:

    PACKAGE="$PWD/harklight-server-0.2.34-linux-x86_64"
    STATE="$HOME/.local/share/harklight-server"
    "$PACKAGE/bin/harklight-bootstrap" "$STATE" localhost
    cat "$STATE/person-id"

Bootstrap refuses any existing state directory. It creates private owner and CA
keys, a public trust root, hostname-bound server certificate, first person and
server.env. Directory mode is 0700 and files are private. It does not silently
rotate an owner or reuse a database. An interrupted bootstrap leaves partial
private state to inspect; do not treat that as success or rerun over it.

Keep owner/identity.key and tls/ca.key private. Copy only tls/ca.pem to clients
over a trusted channel and verify it with the operator. That explicit trust root
is part of setup, not a page that decides which server to trust automatically.
Certificate renewal requires explicit owner/trust continuity. Generated server
and owner certificates expire after one year.

## Start, stop and persistent state

    source "$STATE/server.env"
    "$PACKAGE/bin/attention_server" start

Stop the foreground process with Ctrl-C and confirm exit before local operator
commands or upgrade. Historical binary name attention_server, Mix application
:attention_server and ATTENTION_SERVER_* variables retain their existing
meaning.

Three listeners default to loopback: device HTTPS 18443, enrollment HTTPS 18444
and owner TLS 18445. Devices and registered agents share the device port. For
another machine to connect, set each required ATTENTION_SERVER_*_BIND explicitly
in server.env and arrange routing and firewall access. Use the DNS hostname
embedded in the certificate.
No service, boot autostart, DNS or firewall change is performed by the package.

State is external to the package; SQLite, certificates, owner identity and person
persist across restart. Run one active server per database. Enrollment and owner
commands use authenticated endpoints. Local operator bootstrap/recover must run
with the server stopped. bin/harklight disables automatic listeners in its CLI
process even if server.env was sourced, avoiding a second listener/writer.

## Enroll an actual sender and receiver

On each device machine keep a separate private identity directory:

    "$PACKAGE/bin/harklight" enroll start       --directory "$HOME/.local/share/harklight-sender"       --connect 127.0.0.1 --name localhost --port 18444       --ca "$STATE/tls/ca.pem" --label "Reference sender"

Give the code to the authenticated owner. On the owner machine inspect the
shown account, label and key fingerprint before typing APPROVE:

    PERSON="$(cat "$STATE/person-id")"
    "$PACKAGE/bin/harklight" owner approve       --connect 127.0.0.1 --name localhost --port 18445       --ca "$STATE/tls/ca.pem" --cert "$STATE/owner/identity.pem"       --key "$STATE/owner/identity.key" --person "$PERSON"       --code CODE --senders app:reference

Sender scope is explicit and independent of enrollment. For a receiver use its
own directory and label and omit --senders unless it also needs origin rights.

    "$PACKAGE/bin/harklight" enroll status       --directory "$HOME/.local/share/harklight-sender"       --connect 127.0.0.1 --name localhost --port 18444 --device-port 18443       --ca "$STATE/tls/ca.pem"

After a lost submission reply keep the existing identity and follow status/retry
guidance. Enrollment success is not proof of receiver application of a message.

## Tailnet box and client connection

Use the server box's full Tailscale DNS hostname at bootstrap, for example
box.example-tailnet.ts.net. In server.env set the required bind variables to
that box's Tailscale IP; keep the same ports unless you explicitly configure
all peers. A Tailscale peer must be permitted by your tailnet access policy.
The package does not configure Tailscale, expose a public Internet endpoint
or turn tailnet membership into enrollment or response authority.

Copy the public tls/ca.pem to headless senders over your chosen trusted
channel. Before S1 is enabled, the legacy native app uses an owner-created
invitation from the dashboard below: it carries the configured destination and pinned public CA. The app keeps
its own private device key; never transfer an owner or CA private key to it.
For headless commands use the tailnet IP in --connect and the exact
certificate hostname in --name. Enrollment uses 18444, owner management 18445,
and the device client API https://HOSTNAME:18443/v2. Headless senders and
registered agents use mutual TLS on that port. The app uses it before S1 and
on designated devices during the S1 overlap. After S1 sign-in, the app uses
a bearer session on the trusted HTTPS origin. Legacy invitations carry the
device port's address as `device_url`.

## Agent conversation CLI

The `harklight-agent` conversation CLI is not part of this package. It is
released from <https://github.com/clickety-clacks/harklight-agent>. Use a compatible release from that repository's release tarballs. Installing it does not start a listener, install a vendor-session integration or create an identity. The
assembled `skills/harklight-agent` directory may be copied manually
into an agent harness's configured skill directory; installation does not copy
it there.

The CLI uses an existing separately enrolled device identity and its sender grant,
not the server owner's private key. Set `HARKLIGHT_IDENTITY_DIR` and
`HARKLIGHT_CA_FILE` to that device's existing private identity directory and
trusted public CA. Its local journal is separate from server state; retain it
for uncertain actions and exact retries. The package does not migrate or reset
an agent's journal during server installation. The CLI's README covers
`send`, `listen`, `retry`, `retract` and file commands.

The CLI sends and listens over client API v2 on the device port, with the same
server certificate and enrolled-device authentication. For an existing identity,
set the device port with `--device-port` or `HARKLIGHT_DEVICE_PORT` when needed.
New enrollments save the port.

`ATTENTION_SERVER_DEPLOYMENT_MODE` defaults to `self_hosted`.
Shared messaging is off by default. In the private `server.env`, add
`export ATTENTION_SERVER_SHARED_MESSAGES=1` to enable it and
`export ATTENTION_SERVER_GROUPS=1` to enable groups as well. On first
enablement, existing local owners form the default organization. Later
owner-tool changes to affiliation persist.
Hosted groups remain off until the Hosted component enables them after
business membership and consent are ready.
`ATTENTION_SERVER_UPLOAD_CAP_BYTES` defaults to `none` for self-hosting or may be
a positive integer. The cap is optional in `saas` mode too: without it, the
server starts uncapped and logs one startup notice; with it, uploads over the
configured value are refused individually. The package elects no production
SaaS cap. These settings need deliberate configuration; installation leaves the
protected environment unchanged.

### Hosted loopback interface

In `saas` mode, `ATTENTION_SERVER_HOSTED_PORT` enables the private HTTP
listener on `127.0.0.1` used by the Hosted sign-in component. The listener
trusts local access, as for `POST /accounts` and `POST /sign-in-links`.

`POST /persons/{person_id}/notices` delivers an FYI from **Harklight**
(`app:harklight/notices`) to an existing person on this Server. It accepts a
JSON object with a nonblank `title`, a string `body`, a nonblank `key`, and an
optional absolute HTTPS `link`. When supplied, the URL appears on its own
line after the message body. The request uses the listener's 4 KiB body limit.

A successful response is `200 {"item_id":"i1","created":true}`. Repeating the
same key and content for the same person returns the original item ID with
`"created":false`. Keys are independent across people. Changed content for an
existing key returns `409`. Invalid input returns `400`, oversized requests
return `413`, and an unknown person returns `404`. Delivery uses the normal
attention-based delivery and push behavior for an FYI. An unopened notice can
full-bounce without producing a sender advisory. The system sender is reserved
and its notices are visible to the recipient's clients. Agents cannot read
these notices or receive grants for the system sender.

**FORMAT31 rollback.** Once this Server writes a hosted notice, earlier
FORMAT31 builds cannot open the store. Rolling back to one of those builds
requires restoring the complete pre-upgrade backup and loses all writes made
after that backup.

### Hosted attention list prioritization

For a Server running in `saas` mode, set these variables in its private
startup environment:

| Variable | Value |
| --- | --- |
| `ATTENTION_SERVER_JEV_ENDPOINT` | HTTPS System One endpoint, such as `https://api.typesafe.ai/v1/systemone`. A URL without a path uses `/v1/systemone`. |
| `ATTENTION_SERVER_JEV_MODEL` | Model name or alias available to the provider account. |
| `ATTENTION_SERVER_JEV_KEY_PATH` | Absolute path to a protected file containing the Hosted deployment's provider key. |

In `saas` mode the Server uses the startup endpoint and model and ignores
any dashboard connection or algorithm choices saved in the store. In
`self_hosted` mode it uses the connection saved on the Prioritization
dashboard and ignores the startup endpoint and model. Both modes use
`ATTENTION_SERVER_JEV_KEY_PATH` for the provider key.

Restart the Server to change the Hosted endpoint or model. With these settings,
Jev ranks each existing and new Hosted account separately. Missing or invalid
connection settings select baseline order for every Hosted account. The
authenticated Hosted component's `POST /algorithm` accepts the configured
algorithm and returns `409 algorithm_managed_by_server` for a different one.
An unreadable key or unavailable provider yields baseline order when no
usable scores are available. Provider requests have a 30-second total
timeout per batch. Delivery and push continue independently of ranking.

## S1 sign-in for the app and browser

S1 is off by default. Before the first activation on a self-hosted Server:

1. Collect the exact IDs of existing phones and iPads that need certificate
   overlap. Each device's authenticated `GET /v2/me` reports its ID in
   `principal.device_id`.
2. Stop the Server and make a complete private backup of the stopped,
   checkpointed database, uploads, configuration and keys. Keep the backup
   outside the writable state directory. The managed installer takes a
   stopped backup before switching a package. If you installed 0.2.34 earlier
   with S1 off, take a fresh stopped backup before enabling S1. That captures
   the writes made since installation.
3. Edit the private `server.env`. Set the exact trusted HTTPS origin, including
   a non-default port when used. Configure the dashboard listener below with
   a certificate and private key for that origin. The app and browser use
   their normal platform trust store. A Tailscale HTTPS certificate for the
   configured name is one option.

   ```sh
   export ATTENTION_SERVER_PUBLIC_BASE_URL=https://sign-in.example.net:18447
   export ATTENTION_SERVER_AUTH_MIGRATION_DEVICES='<first-device-id>,<second-device-id>'
   export ATTENTION_SERVER_SIGN_IN_S1=1
   ```

   Replace the placeholders with the collected IDs, or leave the roster empty
   if no device needs overlap. Leave `ATTENTION_SERVER_NATIVE_PASSKEYS` unset
   outside isolated App passkey acceptance.
4. Start through the managed launcher. Use the packaged installer if switching
   releases. The first S1 start records the roster and auth generation 1 in
   the existing database. Later starts use the recorded roster.
   Removing the activation setting after that start leaves S1 active.

Use the existing owner TLS connection to run `harklight owner sign-in-code`
with the owner connection options and the target person's ID. Give the
person only the displayed code. It works once within ten minutes. An owner
can also issue a code from the signed-in **Devices** page. The person enters
it on the selected Server or in the app. A signed-in browser can set up a
passkey from **Devices**. App passkey availability follows real-device
acceptance.

An S1 sign-in session survives Server restart. The browser's own cookie
retention determines how long it survives browser close and reopen.
Browser sign-out ends that browser session. Removing a phone from
**Devices** ends that phone's sessions and delivery binding.

To add an Agent while S1 is active, use **Create Agent invitation** in
**Devices**, then run `harklight-agent setup` with the full invitation.
Approve the exact waiting key and give it at least one sender ID. Self-hosted
owners can also use `harklight owner invite` and `harklight owner approve`
with `--senders`. A Hosted account owner uses the signed-in dashboard.
A pending request has no device access before approval. New phones and iPads
use the shared sign-in form.

For the overlap, the existing people-device certificate transport is
available only to the designated self-hosted installations. The rest use
the S1 HTTPS sign-in door. Owner and registered agent certificate
connections continue on their existing endpoints.

The managed installer and launcher check the package's relay format and auth
generation before startup. A direct launch of v0.2.31 can open an upgraded
FORMAT31 store and ignores S1 session state and certificate denial. The
managed checks refuse v0.2.31 against that store. For recovery, follow the
[S1 upgrade and rollback steps](#upgrading-and-rolling-back-s1).

## Optional self-hosted device dashboard

The dashboard shows device status and agent skill guidance. A signed-in person
owner can create a one-use invitation, review a pending request, and approve or
decline its exact device key. These actions use that person's
current owner authority; account-wide metadata visibility grants no
cross-person approval or sender rights. It is off until the operator sets the
required dashboard values below in the private `server.env` and restarts the
server. Use the same certificate hostname chosen at bootstrap; the browser must
trust that server certificate's CA.
The dashboard has its own HTTPS port and defaults to loopback binding.

- `ATTENTION_SERVER_DASHBOARD_PORT`: a free HTTPS port, for example `18447`.
- `ATTENTION_SERVER_DASHBOARD_OWNER_PERSON`: the trusted person ID in
  `$STATE/person-id`. Only that person's current owner key can view device
  metadata across the self-hosted server; other owners stay person-scoped.
- `ATTENTION_SERVER_DASHBOARD_SKILL_PATH`: the installed package's absolute
  `skills/harklight-agent/SKILL.md` path.
- `ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SAAS` (optional): the installed
  package's absolute `skills/harklight-agent-saas/SKILL.md` path. When
  omitted, the dashboard derives this path from the configured
  `skills/harklight-agent/SKILL.md` path, so existing `server.env` files remain
  valid after an upgrade.

The dashboard serves the two skills, Hosted guide and shared core without
requiring sign-in. The signed-in **Components** page links to all four.

- `/agent-skills/harklight/SKILL.md` serves the Claude Code and Codex skill.
- `/agent-skills/SKILL.md` serves the self-hosted operator skill.
- `/agent-skills/harklight-agent-saas/SKILL.md` serves the Hosted guide.
- `/agent-skills/core.md` serves the shared conversation core.

The coding-agent skill and shared core are located relative to the configured
operator skill path.

For access from another machine, set `ATTENTION_SERVER_DASHBOARD_BIND` to the
server's permitted interface and arrange routing. Optional
`ATTENTION_SERVER_DASHBOARD_CERTFILE` and `ATTENTION_SERVER_DASHBOARD_KEYFILE`
select a separate browser-trusted TLS certificate; otherwise the server's
existing certificate and key are used. Opening the page does not grant sender
or device authority.

Before S1 is enabled, the current owner can issue a one-use sign-in link
over its authenticated owner TLS channel. The command saves the bearer
link in a new private file; share it only with the intended owner and remove
the file when no longer needed. A used link needs a fresh one.

    "$PACKAGE/bin/harklight" owner browser-sign-in --connect 127.0.0.1 --name localhost --port 18445 --ca "$STATE/tls/ca.pem" --cert "$STATE/owner/identity.pem" --key "$STATE/owner/identity.key" --person "$(cat "$STATE/person-id")" --output "$STATE/owner/sign-in-link.txt"

The link carries its one-use secret after `#`, which the browser sends only to
the pinned HTTPS host page for durable consumption. A successful exchange
sets a secure browser cookie. Owner recovery invalidates that session. Device
rows show approved or revoked state and **Unknown** connection until the
server has a real presence signal; enrollment does not imply online status.
The signed-in owner opens **Add a device** to create the full invitation link,
then **Waiting for approval** to compare the current code and exact key with
the joining device. The waiting request does not expire; the app keeps its
key and can start again with it to get the same request and code. Denial,
cancellation and revocation remain terminal. The owner may name sender grants
when approving.

The self-hosted dashboard also has a **Prioritization** page. It sets the
dashboard owner's waiting-item order to the deterministic baseline or a
model-assisted whole-set ranking. The endpoint and model are server-wide; the
algorithm choice is per person. SaaS deployments have no such page. To enable
model-assisted ranking, set
`ATTENTION_SERVER_JEV_KEY_PATH` in private `server.env` to the absolute path of
a protected key file. The page reports whether the file is readable; it never
accepts or displays the path or key. The owner can save the model-assisted
choice before its HTTPS endpoint, model or key file is ready. Waiting items
remain in basic order until setup is complete and an ordering response arrives;
the page shows whether setup is incomplete or the server is waiting for an
answer. Each provider request has a 30-second total timeout. Intake, delivery
and push continue normally.

Optional private applied-score record: set `ATTENTION_SERVER_JEV_SCORE_RECORD_PATH`
in private `server.env` to a file in an existing private directory writable by
the server account. It is off when unset; hosted mode ignores it. Each applied
Jev score appends one JSON line containing `time` (Unix milliseconds), `person`,
`sender_id`, `message_id`, `title`, `score` (0–1), and `model`. The file is mode
0600; it contains no endpoint or key. Writes run separately from delivery and
ranking; a write failure is ignored and logged once per server process. There
is no rotation or retention job. It records existing applied scores only and
never triggers scoring. Reused scores with unchanged values and model are not
appended again while they remain in the applied set during the same server run.

## Connect Claude Code or Codex

Set `ATTENTION_SERVER_JOIN_NAME` to the Server DNS name clients validate. Use
an Agent CLI release that includes `setup`. On the computer running Claude Code
or Codex, install the CLI with `brew install clickety-clacks/tap/harklight-agent`.

1. In the dashboard, open **Devices** and choose **Create invitation**. Copy
   the **On a computer** command and run it on the agent’s computer.
2. In **Devices**, review the pending request, match the approval code printed
   by `setup`, and enter `app:agent/` in **Sender IDs** before approving.
3. Open Harklight and answer the test question. Then ask Claude Code or Codex
   to send you a question through Harklight.

With the owner CLI, use `harklight owner invite --output FILE` with your usual
owner connection options and transfer the private invitation file to the
agent’s computer. Run `harklight-agent setup FILE` there. Approve the displayed
code with `harklight owner approve --code CODE --senders app:agent/`, using the
same owner connection options.

## iPhone notifications via the Harklight push relay

The relay service at `app.harklight.net` opens separately from this release;
until it is available, leave the line commented. To enable notifications in
the Harklight iPhone app, uncomment this line in
`server.env` and restart Harklight Server. On an existing installation, add the
line if it is missing.

```sh
export ATTENTION_SERVER_PUSH_RELAY_URL=https://app.harklight.net
```

For each notification, the relay receives your iPhone's push token, the push
kind (alert or background), the badge count when supplied, the push environment
(production or sandbox), whether an alert plays a sound or arrives quietly when
specified, your Server's random identifier, and the address it connects from.
The relay never receives message text, titles or sender names. It stores none
of this as push or registration records and uses the data only to ask Apple to
deliver the notification. An alert reads “New message in Harklight,” and a
background notification shows no alert. The relay writes one operational log
line per push with the Server identifier, kind, environment, Apple response
status, a recognized error reason when available, and duration. It also logs
the Server identifier at registration. These lines never include the token.
The system journal keeps these lines until it removes older entries to make
space, with no fixed retention period. The web front end's access logs include
the connecting address and are kept for 14 days.

The relay is free and offered on fair use, with no guarantee of availability.
We may stop serving a Server that abuses it.

## Optional direct APNs notifications

Configured Apple keys take precedence over the relay URL. The Sandbox profile
uses its authorized P-256 signing key. Set its path and key identifier as
follows:

- `ATTENTION_SERVER_APNS_KEY_PATH`: absolute path to the protected Sandbox key.
- `ATTENTION_SERVER_APNS_KEY_ID`: the Sandbox key's ten-character Apple key ID.

Both profiles share `ATTENTION_SERVER_APNS_TEAM_ID`, the ten-character Apple
team ID. The Production profile uses its own related topic-specific key through
these settings:

- `ATTENTION_SERVER_APNS_PRODUCTION_KEY_PATH`: absolute path to the protected
  Production key.
- `ATTENTION_SERVER_APNS_PRODUCTION_KEY_ID`: the Production key's ten-character
  Apple key ID, distinct from the Sandbox key ID.

Each key must be a private regular file readable only by the server account.
Store both keys using the existing protected secret handling and set their
references in private `server.env`; do not put key bytes or actual private path
values in source, logs, chat or release assets. The Sandbox settings and shared
team ID must be complete to enable APNs. The Production path and ID may both be
absent while only Sandbox is configured; supplying only one of them refuses
startup. Bootstrap does not generate Apple keys, copy credentials or enable
provider sends.

Each device's registration `profile_id` selects both its endpoint and signing
key: `harklight-sandbox` uses the Sandbox key with
`api.sandbox.push.apple.com`, and `harklight-production` uses the Production
key with `api.push.apple.com`. Both profiles use the fixed app topic
`co.clicketyclacks.Harklight`. The server never falls back to the other
profile's key. If a Production binding is registered before Production key
settings are present, its send is recorded as `blocked_configuration` without
an APNs request; after adding the Production settings, the device must register
a higher revision to clear that binding. FCM is unsupported.

An enrolled app registers its own current address over the authenticated relay.
The full provider attempt has a provisional 20,000 ms deadline, including
connection setup and any provider-token refresh.
Set `ATTENTION_SERVER_APNS_ATTEMPT_TIMEOUT_MS` in `server.env` to override it.
Use a positive integer in milliseconds within the runtime timer range.
An invalid value uses the provisional default. The Server logs each provider
attempt's elapsed milliseconds and outcome as `harklight_apns_attempt` to inform
future tuning.

When the deadline expires, the Server stops the attempt, keeps the notification
on the existing retry schedule, and moves on to other eligible devices.
APNs may have accepted the interrupted request, so a retry can produce a
duplicate alert. The message stays in the durable store.

For an APNs alert about a single pending request, the alert title is the supplied
`display_name`; when absent, it is the message's exact `sender_id`. An explicitly
supplied empty string stays empty. The name is carried whole when it fits with
the required metadata. If it cannot fit, the notification is still sent with a
neutral sender label. An alert grouping several pending requests uses a count
title and carries as many whole deduplicated names as fit, in their existing
order, followed by a count of the remaining names. No name in a payload is
altered, truncated or ellipsized; the fitter may shorten other text. The phone
may clip its display. There is no name-input cap or validation, name rejection,
or silent notification drop because a name does not fit. The full original
name remains unchanged in the message, history and agent feed.
The independent `sender_id` and message ID remain the identities used for
authentication, grants and navigation.
This requires the server release implementing the display-name field and
whole-name payload fitting; sender CLI support alone does not establish
notification or app presentation. The APNs body
may include a whitespace-normalized preview of up to
160 graphemes from that message's body; when the body is absent or blank, its
title supplies the preview. The payload also carries a content-free sync hint.
Terminal-only updates use background sync without an alert, sound or badge,
including updates to the answering device. A notification can carry a message
excerpt, while full conversation history and answers remain in the durable relay.
Provider acceptance does not prove notification display, application fetch or
reply. Device settings, network state and app lifecycle still affect those
outcomes. Invalid-address responses clear the matching address; provider-error
text is restricted to fixed safe reasons in durable status. Retry and status
survive restart.

For a manual upgrade of an existing installation, stop the old server and back
up the full private state before upgrading. The release installer makes one
full mutable-root backup after stopping the service and before switching
releases. Follow the [FORMAT31 upgrade steps](#releases-that-write-format31)
before starting this build on a FORMAT30 store. For an earlier store, follow
the bridge upgrade described there. Retained Seen-only messages gain an
explicit FYI type during migration. For old Chat
turns with no recorded handoff expectation or outcome, updated clients omit
the delivery caption until the turn receives qualifying evidence.
Preserve the database, referenced files, trust root, owner and person.
Messages, accounts, grants, enrolled devices and registered agents carry over.
Every registered, non-revoked agent can read its account's messages.
Answering requires both a matching read grant and a matching respond grant.

Starting a server with APNs configured can send pending notifications. The
release installer does not inspect, clear, or mark provider work delivered. It
stops the sole writer, retains a private archive of the mutable deployment root,
and starts the verified package against the same external state and
configuration. The archive includes `state/` and any root-level APNs,
dashboard-TLS, sender, or other private identity/configuration material. It
excludes immutable versioned releases, prior backups, and the installer lock;
those release packages remain in place and the current symlink is recorded in
the archive. The archive is stored under the private `backups/` directory with
owner-only permissions.
Pending and in-flight work remains durable and follows the server's existing
claim, retry, and expiry behavior after startup. A provider may accept a push
before the server commits its local completion; a restart cannot establish
exactly-once delivery. Installation does not create a test notification.
Keep existing provider configuration only when its operation is authorized; a
setup check with no provider sends must use an isolated state copy and leave
the APNs variables unset.

## Read and answer a stored message

Agents send new messages through the authenticated
[`harklight-agent` session route](skills/core.md). Replies use that same origin
identity, session and journal. A sender whose route cannot listen can use
`harklight-agent read --session HANDLE` when its work resumes. The feed fetch is
origin-wide; the CLI selects pending output for that handle locally. Read does
not acknowledge an event, enable listening or promise future delivery. Follow
the shared core for matching the request and reporting an outcome.
The enrolled-device `read` and `respond` commands remain available for messages
already stored by the server. They do not create new messages or retrieve a
handoff receipt. Existing messages and their replay events remain in server
history.

Use an enrolled device with read/respond access to see its person's stored
messages:

    "$PACKAGE/bin/harklight" device read --directory "$HOME/.local/share/harklight-receiver" --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$DEVICE_PORT" --ca "$CA_PEM"

`DEVICE_PORT` is the device port (18443 by default), which serves client API
v2. To deliberately answer a request already delivered to that device, save the
answer body in `answer.json`: a JSON object with at most one of `choice` (an
action `id`), `comment` or `result`, or `{}` to answer with no comment. Then
provide the exact sender and message ID:

    printf '%s\n' '{"choice":"continue"}' > answer.json
    "$PACKAGE/bin/harklight" device respond --directory "$HOME/.local/share/harklight-receiver" --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$DEVICE_PORT" --ca "$CA_PEM" --sender app:example --message-id request-1 --answer answer.json

The read command stores the attention list snapshot and subsequent change
batches before advancing its cursor. It acknowledges nothing. A response is deliberate and
terminal; it does not execute a source action or authorize external operations.
Use a supported answer UI when reading through the native app. Unknown UI
remains unsupported.

## Authorized-agent tools remain available

bin/harklight agent exposes keygen, search, get and answer (also spelled
respond) on the device port over client API v2: `GET /v2/search`,
`GET /v2/items/{item_id}` with its entries, and
`POST /v2/items/{item_id}/answer`. Owner agent-register, agent-grants-replace,
agent-list and agent-revoke remain available. Register the exact public key and
grant read/respond separately; reading needs no read grant, and answering needs
both grants for the item's sender. An answer names its item (`--item-id`, or an
exact `--sender` and `--message`) and `--request-revision`, and its answer file
is an AnswerBody object. Its exact body and Idempotency-Key are saved in the
agent directory before it is sent, it needs a deliberate ANSWER confirmation,
and rerunning the same command replays a committed answer instead of writing a
second one. It carries no source ruling authority.

## Upgrade or rollback

### Releases that write FORMAT32

For an upgrade from FORMAT31, stop the Server and keep a checkpointed backup
of its private state, including the database and matching uploads directory.
The installer takes and retains that backup before it runs the stopped-store
converter. The converter verifies the row-store marker, codec and generation, then
changes the format marker and checksum in one transaction. It preserves existing rows and
feed positions. A retry after the transaction commits recognizes FORMAT32
and skips the conversion. Supported FORMAT30 databases still go through
the snapshot conversion described below, including its capacity checks.

A FORMAT31 binary cannot open FORMAT32. To roll back, stop the Server,
restore the pre-upgrade state and uploads together, and restart the previous
release. A code or symlink change alone cannot restore the old format.
Restoring the backup loses writes made since the backup. External
deliveries that already occurred cannot be undone.

### Upgrading and rolling back S1

Take a stopped, checkpointed backup before first S1 activation, including when
enabling it after an earlier package install. Keep the full private state and
configuration together.

If the managed installer fails during conversion or compatibility checking
before it switches releases, it attempts to restore the pre-install backup
and restart the prior release. If recovery fails, keep the backup and follow
the reported service state.

If startup or readiness fails after the installer switches to an auth-capable
candidate, it tries to stop the candidate. Once stopped, the selected package,
latest state and original backup are retained. Repair forward with a
compatible newer release against the preserved state. After S1 has written
the store, returning to a pre-S1 release requires stopping the Server and
restoring the complete pre-activation backup with its corresponding package.
Restoration discards every later write and leaves external deliveries in place.

### Releases that write FORMAT31

Check the release notes before upgrading. A FORMAT31 release writes individual
SQLite records. Its normal startup creates a row store for a fresh install and
requires a stopped-store conversion for existing FORMAT30 databases. For an
older store, first upgrade to a published Harklight Server v0.2.16 through
v0.2.20 release. Start that release to write FORMAT30, then stop it and follow
the FORMAT31 upgrade steps.

The packaged release installer stops the service and takes its full
mutable-root backup before running the converter. It switches the release and
starts the service only after conversion succeeds. On conversion failure it
uses the same restore and prior-release startup as a failed candidate start.

For a manual upgrade, stop the Server and take a complete private backup before
running the new package's converter. The backup must include the SQLite file,
any associated SQLite journal files, the uploads directory, configuration and
keys. Keep it outside the writable state directory. Use the verified package:

```sh
ATTENTION_SERVER_ENABLE=0 /path/to/new-release/bin/harklight store convert-stopped \
  /path/to/existing/state/relay.sqlite3
```

The converter checks host capacity before decoding the snapshot. If it reports
`fit not established`, keep the Server stopped and preserve the backup. For
insufficient memory or free disk, copy the stopped state directory to a larger
isolated Linux system and run the same verified package against that copy.
Copy the converted database and uploads back together before starting the
new release with the existing configuration. A store above a calibrated size
or row limit needs a verified conversion path for that shape. Report the
measured value and the limit shown by the converter. A refusal for an
unsupported or unreadable header requires resolving the store version or
recovering a readable backup before proceeding.

Conversion is transactional. An interruption before commit leaves the old
snapshot for a retry from the beginning. A retry that finds FORMAT31 skips
conversion. Retry an interrupted install with the same verified release. The
installer keeps the original snapshot backup through the retry and retains it
after candidate readiness. Do not rerun bootstrap against converted or restored state. To
return to the previous snapshot release, stop the Server, restore the complete
pre-upgrade backup and run the release that wrote it. Restoring that backup
discards every later write and leaves external deliveries in place.

### Earlier releases that write snapshots

For a manual upgrade, stop the old process and back up the complete private
state directory, including the database and keys. Verify the package checksum
and extract it into a separate versioned directory. Keep the state path, trust
root, owner/person and configuration. Start with the existing `server.env`.
Bootstrap is for a fresh installation. Only one server may own the database.

The release installer stops the service and confirms the process has stopped,
then creates one full mutable-root backup before switching `current`. If backup
creation fails, it attempts to resume the prior service and does not switch the
release. If that restart cannot be confirmed, it reports the failure. After a
successful backup it switches `current` and starts the selected package. It waits
for the selected process to become active under that release. If the candidate start or process readiness fails, it attempts to
stop the candidate, restore the pre-install mutable root and prior `current`
link from that backup, then start and check the prior release. This description
does not itself establish tested recovery; consult the exact archive's recorded
rehearsal result. The restore or prior-release startup/readiness can fail and
leave the service stopped. The failed package remains under its
versioned release directory but is not selected. The archive includes the
database and private configuration, and excludes immutable versioned releases,
prior backups and the installer lock; prior release files are preserved. The
backup is retained under the private `backups/` directory.

If the backup cannot be restored, or the prior release cannot start and become
active, the installer reports that recovery did not complete and may leave the
service stopped. Preserve the backup and state for recovery. The optional
authenticated owner agent-list readiness probe reports a warning when that
route is unavailable; that route does not block other server functionality or
trigger a whole-release rollback.

Restoring the pre-install backup discards every write made after its capture,
including any writes made by the candidate during its failed start. The backup's
readability is not validated by the installer. Although it captures a full
mutable-root backup while the service is stopped, restoration or prior-release
startup can fail; do not assume recovery or a recovery time.

The installer attempts backup restoration only when candidate start or
process-readiness fails. After readiness, it does not automatically roll back;
the optional owner agent-list probe is warning-only.

For a manual rollback, stop the server, restore the complete backup taken
with the prior release stopped, and run that release against the restored
state. A shared format number alone does not establish downgrade compatibility.
Restoration loses every write made after the backup and cannot undo external
notifications, bounces or agent deliveries.

### Preview automatic bounces before an upgrade

Set `ATTENTION_SERVER_BOUNCE_NOT_BEFORE` to an RFC 3339 UTC instant, for example
`2026-11-17T14:16:54Z`, to exclude messages accepted strictly before that
instant. An unset value applies no cutoff. The setting takes effect when
the server starts. If the cutoff moves earlier or is cleared, eligible open
messages without a selection are scheduled from their original acceptance
time and may bounce immediately.

After a clean stop of the current server, use the new package's CLI before
installing it. Pass the same cutoff planned for startup, or omit the setting
for an unrestricted preview.

```sh
ATTENTION_SERVER_BOUNCE_NOT_BEFORE=2026-11-17T14:16:54Z \
  /path/to/new-package/bin/harklight operator bounce-preview --store /path/to/relay.sqlite3
```

The command prints JSON with separate `full` and `soft` counts for all
accounts in the stopped store, using their configured levels or defaults.
A full bounce removes an unopened item from person views. A soft bounce
notifies the sender while the item stays in place. The preview reads the
stopped store through a private temporary copy and does not start the server,
write to the original database, or deliver advisories. The command refuses
to count a store with a nonempty SQLite WAL or rollback journal. The actual
counts can change if additional deadlines pass before startup.

Restoring an older backup after the server has served clients also repeats
message IDs and feed positions. Apps and agents that already read past those
positions can skip new replies. Treat such a
restore as a last resort that the owner decides on, not as a routine way to
return to an earlier release. The installer's own restore after a failed start,
before any new write, is not affected.

Preserve the backup and current state if restoration does not
succeed. Do not assume recovery success or a recovery time. Keep backups
and keys private.

## Build

On the declared Linux target use Elixir 1.19.5/OTP 27, a C compiler, make,
OpenSSL CLI and the pinned Mix lock:

    ./scripts/build-package.sh

This production package includes the Mix release/ERTS, server/device CLI,
bootstrap, guides, both assembled agent skills and their single-source core.
It preserves dependency notices, records BUILD_INFO and writes tarball/SHA256SUMS in dist/. Archive ordering,
timestamps, owners and gzip headers are normalized; full toolchain bit-for-bit
reproducibility is not claimed. Build from the clean intended commit and use
that same artifact for clean-install evidence and publication.

### Client feature flags

Optionally set `ATTENTION_SERVER_FEATURE_FLAGS_PATH` to a small nonsecret JSON
object file containing names and boolean values. `GET /v2/me` reports the
client features the Server still controls (`server_matched_push_routing` and
`priority_level_editing`) in `features`, each `true` only when the file sets
it to `true`, alongside the Server's persisted identity in `server.id`. The
Server reads the file on each `GET /v2/me`, so file changes reach the next
read without restarting. Other names and nonboolean entries are ignored. An
unset path turns every feature off. A missing, unreadable or malformed
configured file also turns every feature off and logs one warning per Relay
lifetime, at the first read that finds it so, without preventing startup or
sync. APNs `attention.server` carries the same persisted identity.

The Server no longer reads a `client_api_v2` entry; one left in the file from
an earlier release is ignored.

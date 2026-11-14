# Harklight Server self-hosted installation

Version: **0.1.37**. Package target: **Linux x86_64, glibc 2.39 or newer**, with OpenSSL 3 shared libraries and libstdc++6 (built on Ubuntu 24.04). The package bundles
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

## No comment compatibility

This package includes the revision-bound No comment action. The client must send
exactly `type`, `request_id` and a positive integer `request_revision`; ordinary
requests use revision `1`, while conversation requests use the displayed current
revision. Missing revisions return `invalid_field`; stale revisions return
`stale_revision` and leave an open request unanswered. A valid current-revision
action commits a payload-free `no_comment` result.

Installed app build11 sends the older two-field frame, so its No comment action
cannot work with this release until the app is updated to send the revision.
The server package can be prepared and released independently of that client
update. Choice and typed-reply frames remain unchanged. No captured build11
two-field frame and response exists from an older FORMAT17 server. The
refusal above is established by source review and protocol-level tests, not
a live phone test.

## Download and verify

After publication, download the v0.1.37 release from
clickety-clacks/harklight-server, tag v0.1.37. Get
harklight-server-0.1.37-linux-x86_64.tar.gz, SHA256SUMS and SELF_HOST.md from
that release. Run sha256sum -c SHA256SUMS and extract into an operator-owned
versioned directory. BUILD_INFO identifies the source commit, build host,
toolchain, source status and native dependencies.

This project is licensed under the MIT license (see `LICENSE`). Third-party
runtime/dependency notices remain in their shipped distribution files.

## First owner bootstrap

Choose the DNS hostname clients will validate. For a same-machine preview,
localhost works. Install into a directory you own and run:

    PACKAGE="$PWD/harklight-server-0.1.37-linux-x86_64"
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
:attention_server, ATTENTION_SERVER_* variables, /relay, attention-relay/1 and
/agent/v1 retain their existing meaning.

Four listeners default to loopback: WebSocket 18443, enrollment HTTPS 18444,
owner TLS 18445 and agent HTTPS 18446. For another machine to connect, set each
required ATTENTION_SERVER_*_BIND explicitly in server.env and arrange routing
and firewall access. Use the DNS hostname embedded in the certificate.
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

    "$PACKAGE/bin/harklight" enroll status       --directory "$HOME/.local/share/harklight-sender"       --connect 127.0.0.1 --name localhost --port 18444 --relay-port 18443       --ca "$STATE/tls/ca.pem"

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
channel. For the native app, use an owner-created invitation from the dashboard
below: it carries the configured destination and pinned public CA. The app keeps
its own private device key; never transfer an owner or CA private key to it.
For headless commands use the tailnet IP in --connect and the exact
certificate hostname in --name. Enrollment uses 18444, owner management 18445,
device relay wss://HOSTNAME:18443/relay and authorized-agent HTTPS 18446.
An app/server peer must negotiate attention-relay/1 and enroll its exact key.

## Agent conversation CLI

The `harklight-agent` conversation CLI is not part of this package. It is
released from <https://github.com/clickety-clacks/harklight-agent>; install it
with `brew install clickety-clacks/tap/harklight-agent` or from that repository's
release tarballs, as its README describes. Installing it does not start a
listener, install a vendor-session integration or create an identity. The
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

Origin HTTPS is opt-in: set `ATTENTION_SERVER_ORIGIN_PORT` to a distinct free
port and `ATTENTION_SERVER_ORIGIN_BIND` to its permitted bind address in private
`server.env`, then use the normal authorized server restart. The origin listener
uses the existing server TLS certificate/key and enrolled-device authentication.
No new trust root or sender grant is created. The CLI needs this exact port in
its enrolled `server.json` as `origin_port`, or through `--origin-port` or
`HARKLIGHT_ORIGIN_PORT`; it does not infer it from the relay port. Routing and
certificate-name validation still apply. An upgrade alone does not enable the
origin endpoint or establish actual agent listening.

`ATTENTION_SERVER_DEPLOYMENT_MODE` defaults to `self_hosted`.
`ATTENTION_SERVER_UPLOAD_CAP_BYTES` defaults to `none` for self-hosting or may be
a positive integer. The cap is optional in `saas` mode too: without it, the
server starts uncapped and logs one startup notice; with it, uploads over the
configured value are refused individually. The package elects no production
SaaS cap. These settings and an origin endpoint change need deliberate
configuration; installation leaves the protected environment unchanged.

## Optional self-hosted device dashboard

The dashboard shows device status and agent skill guidance. A signed-in person
owner can create a one-use invitation, review a pending request, and approve or
decline its exact device key before the deadline. These actions use that person's
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

Both self-contained skills are served without signing in, at
`/agent-skills/SKILL.md` (self-hosted) and
`/agent-skills/harklight-agent-saas/SKILL.md` (hosted). The shared core is served
at `/agent-skills/core.md`, derived from the same package's `skills/core.md`.
The signed-in Components page links all three and shows each skill's version.
The old send-and-listen file is replaced by the core copied into both skills at
build time. The former `ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SEND_LISTEN`
setting is no longer used; the optional SaaS path defaults from the primary
skill path without requiring an environment change.

For access from another machine, set `ATTENTION_SERVER_DASHBOARD_BIND` to the
server's permitted interface and arrange routing. Optional
`ATTENTION_SERVER_DASHBOARD_CERTFILE` and `ATTENTION_SERVER_DASHBOARD_KEYFILE`
select a separate browser-trusted TLS certificate; otherwise the server's
existing certificate and key are used. Opening the page does not grant sender
or device authority.

After the server is running, the current owner can issue a ten-minute sign-in
link over its authenticated owner TLS channel. The command saves the bearer
link in a new private file; share it only with the intended owner and remove
the file when no longer needed. A used or expired link needs a fresh one.

    "$PACKAGE/bin/harklight" owner browser-sign-in --connect 127.0.0.1 --name localhost --port 18445 --ca "$STATE/tls/ca.pem" --cert "$STATE/owner/identity.pem" --key "$STATE/owner/identity.key" --person "$(cat "$STATE/person-id")" --output "$STATE/owner/sign-in-link.txt"

The link carries its one-use secret after `#`, which the browser sends only to
the pinned HTTPS host page for durable consumption. A successful exchange
sets a secure browser cookie. Owner recovery invalidates that session. Device
rows show approved or revoked state and **Unknown** connection until the
server has a real presence signal; enrollment does not imply online status.
The signed-in owner opens **Add a device** to create the full invitation link,
then **Waiting for approval** to compare the current code and exact key with
the joining device. The approval window lasts 24 hours by default. The app retains its
waiting request and can renew the same device after ordinary expiry without a
new invitation or key. The owner must review the renewed current request;
an old code or deadline cannot approve it. Denial, cancellation and revocation
remain terminal and never grant sender rights.

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
answer. Intake, delivery and push continue normally.

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

## Optional APNs notifications

APNs is disabled when all five APNs settings below are absent. The Sandbox
profile keeps using its existing authorized P-256 signing key. Its path and key
identifier stay in the existing settings:

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
name remains unchanged in the message, history and origin feed.
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
releases. This source, v0.1.37, writes store FORMAT29. From a supported FORMAT27
store such as the one written by v0.1.36, startup applies two ordered steps:
first it adds `see_focus: false` and `push_view: false` only where device
records lack those values, preserving existing values; then it initializes the
hosted `delivery_states` map to empty. The final migrated store is FORMAT29;
the source does not establish a separately persisted intermediate FORMAT28
state. The published v0.1.35 and v0.1.36 packages write FORMAT27 and cannot read
FORMAT29; changing only the code or release link is not a safe downgrade. The
FORMAT25 state
introduced by v0.1.28 includes hosted-account records, the separate
conversation feed, conversation push high water and upload activity accounting
while retaining existing requests, answers and authority, and persisted chat
read positions. v0.1.28 uses FORMAT25; v0.1.27, v0.1.26, v0.1.25, the v0.1.24 repair line, v0.1.23, v0.1.22,
v0.1.21, v0.1.20, v0.1.19, v0.1.18, v0.1.17, v0.1.16, v0.1.15, v0.1.14
and v0.1.13 use FORMAT24; v0.1.12 uses FORMAT23; v0.1.11 uses
FORMAT22; and v0.1.10 accepts formats 1–6 and 8–21 only. Those earlier packages cannot
start against FORMAT24. Preserve the trust root, person and sender grants; do
not repeat bootstrap. The v0.1.27 and v0.1.26 packages use FORMAT24 and cannot
read FORMAT25 state written by v0.1.28.

v0.1.32 includes configurable score-based priority levels and unseen-message
advisories. When a request reaches its configured unseen interval, the server
adds an advisory to the originating agent session asking whether the request is
still valid and worth resending and whether its importance should be raised. It
does not automatically resend the request or change its priority. Device
revocation ends all origin sessions for that device with the existing gone
availability. Enrolled devices can read and update their access levels, with
connection-local sync hints. Relay timeout outcomes are recorded in bounded
snapshots outside the request path. Crash/status output omits cached APNs
tokens, and the Bandit dependency is updated to 1.14.0.

Version 0.1.25 and later persist delivery acknowledgements (ACK1 and ACK2). Sharing
FORMAT24 with an older release does not establish downgrade compatibility.
Once 0.1.25 or later writes new data, no safe code-only downgrade to 0.1.24 or earlier
has been established. Before cutover, retain the complete private pre-upgrade
backup and agree on recovery with the release owner. Restoring that backup
returns to its captured state and discards all later writes.

Starting a server with APNs configured can dispatch existing queued work. The
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
the shared core for matching the request, explicit ACK and reporting an outcome.
The enrolled-device `read` and `respond` commands remain available for messages
already stored by the server. They do not create new messages or retrieve an
origin receipt. Existing messages and their replay events remain in server
history.

Use an enrolled device with read/respond access to see its person's stored
messages:

    "$PACKAGE/bin/harklight" device read --directory "$HOME/.local/share/harklight-receiver" --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" --ca "$CA_PEM"

To deliberately answer a request already delivered to that device, save one JSON
value in `answer.json` and provide the exact sender and message ID:

    printf '%s\n' '{"choice":"continue","note":null}' > answer.json
    "$PACKAGE/bin/harklight" device respond --directory "$HOME/.local/share/harklight-receiver" --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" --ca "$CA_PEM" --sender app:example --message-id request-1 --answer answer.json

The read command stores replayed events before ACK. A response is deliberate and
terminal; it does not execute a source action or authorize external operations.
Use a supported answer UI when reading through the native app. Unknown UI
remains unsupported.

## Authorized-agent tools remain available

bin/harklight agent exposes existing keygen/search/get/respond tools.
Owner agent-register, agent-grants-replace, agent-list and agent-revoke remain
available. Register the exact public key and grant read/respond separately.
Responses retain deliberate ANSWER confirmation, caller-held operation UUID,
exact acceptance sequence and intent reference, without source ruling authority.

## Upgrade or rollback

For a manual upgrade, stop the old process and back up the complete private
state directory, including the database and keys, before the first start of a
v0.1.37 package. Verify the package checksum and extract into a different
versioned directory. Keep the state path, trust root, owner/person and
configuration. Start with the existing `server.env`; do not rerun bootstrap.
From supported FORMAT27 state, first start applies the FORMAT28 focus-default
step and then the FORMAT29 hosted-delivery-state initialization in order. The
resulting store is FORMAT29. There is no hot upgrade; only one server may own a
database.

The release installer stops the service and confirms the process has stopped,
creates one full mutable-root backup, switches `current`, and starts the
selected package. It waits for the selected process to become active under that
release. If the candidate start or process readiness fails, it attempts to
stop the candidate, restore the pre-install mutable root and prior `current`
link from that backup, then start and check the prior release. The restore path
has not been exercised in preparation of this package; this description is not
evidence of tested recovery. The restore or prior-release startup/readiness can
fail and leave the service stopped. The failed package remains under its
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
including any writes made by the candidate during its failed start. The
installer's restore path has not been exercised in preparation of this package.
Although it captures a full mutable-root backup while the service is stopped,
restoration or prior-release startup can fail; do not assume recovery or a
recovery time.

The installer attempts backup restoration only when candidate start or
process-readiness fails. After readiness, it does not automatically roll back;
the optional owner agent-list probe is warning-only.

Before the first v0.1.37 start, stop the server and take a complete private
state backup, including the database and keys. After any v0.1.37 write,
returning to v0.1.36 requires stopping the server, restoring that pre-start
backup and selecting v0.1.36. The v0.1.36 package cannot read the resulting
FORMAT29 state, and changing only the code or release link does not restore
compatibility. Restoring the backup loses every write since it was taken and
cannot undo external notifications, bounces or agent deliveries. Restore
success is unproved and has not been tested; it may fail, so no recovery success
or time is guaranteed. Preserve the backup and current state if restoration
does not succeed. The same format number does not make a downgrade from
v0.1.25 or later safe: after ACK2 data has been written, do not switch to
v0.1.24 or earlier against the current state. Keep backups and keys private.

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

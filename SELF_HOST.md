# Harklight Server self-hosted installation

Version: **0.1.12**. Package target: **Linux x86_64, glibc 2.39 or newer**, with OpenSSL 3 shared libraries and libstdc++6 (built on Ubuntu 24.04). The package bundles
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

After publication, download the v0.1.12 release from
clickety-clacks/harklight-server, tag v0.1.12. Get
harklight-server-0.1.12-linux-x86_64.tar.gz, SHA256SUMS and SELF_HOST.md from
that release. Run sha256sum -c SHA256SUMS and extract into an operator-owned
versioned directory. BUILD_INFO identifies the source commit, build host,
toolchain, source status and native dependencies.

This project is licensed under the MIT license (see `LICENSE`). Third-party
runtime/dependency notices remain in their shipped distribution files.

## First owner bootstrap

Choose the DNS hostname clients will validate. For a same-machine preview,
localhost works. Install into a directory you own and run:

    PACKAGE="$PWD/harklight-server-0.1.12-linux-x86_64"
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
separate `skills/harklight-agent-send-listen` directory may be copied manually
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
a positive integer. `saas` requires a finite configured upload cap and refuses
startup without it; the package elects no production SaaS cap. These settings
and an origin endpoint change need deliberate configuration; installation leaves
the protected environment unchanged.

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
- `ATTENTION_SERVER_DASHBOARD_SKILL_PATH_SEND_LISTEN` (optional): the installed
  package's absolute `skills/harklight-agent-send-listen/SKILL.md` path. When
  omitted, the dashboard derives this path from the configured
  `skills/harklight-agent/SKILL.md` path, so existing `server.env` files remain
  valid after an upgrade.

Both skill files are served without signing in, at `/agent-skills/SKILL.md`
and `/agent-skills/harklight-agent-send-listen/SKILL.md`. The signed-in
Components page links both and shows each one's packaged version.

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
accepts or displays the path or key. Model-assisted ranking can be selected only
after its HTTPS endpoint, model and key file are ready. If the ranking is
unavailable, the server uses the baseline order while intake, delivery and push
continue normally.

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
New unresolved attention uses the triggering message's exact `sender_id` as its
alert title. Its APNs body may include a whitespace-normalized preview of up to
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

For an existing installation, stop the old server and back up the full private
state before upgrading. This source uses store FORMAT23; supported older state,
including FORMAT17, is normalized and durably committed on startup. The new
state includes hosted-account records, the separate conversation feed,
conversation push high water and upload activity accounting while retaining
existing requests, answers and authority. The v0.1.12 package uses FORMAT23;
v0.1.11 uses FORMAT22 and v0.1.10 accepts formats 1–6 and 8–21 only. Those
earlier packages cannot start against FORMAT23. Preserve the trust root, person
and sender grants; do not repeat bootstrap. Do not treat an earlier package as a rollback path; use a compatible
forward repair or a separately reviewed, state-aware recovery plan.

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

## Send and retrieve a real reply from scripts

The packaged `skills/harklight-agent/SKILL.md` gives an agent the supported enrollment, sender-grant, message, receipt and search/response workflow. An agent installing this package can copy its `skills/harklight-agent` folder into its own harness's configured skill directory; the package does not install it there automatically.

Enroll two same-person identities using the commands above: one sender with
app:reference rights and one receiver. On the receiver machine, use its private
directory in place of the sender directory in these commands. No Mix is needed.
Create a real sender document file, for example:

    printf '%s\n' '{"sender_id":"app:reference","message_id":"request-1","created_at":1,"title":"Choose the next action","body":"Reply with the deliberate answer you want this sender to receive."}' > message.json

Send from the enrolled origin:

    "$PACKAGE/bin/harklight" device send --directory "$HOME/.local/share/harklight-sender" --connect 127.0.0.1 --name localhost --port 18443 --ca "$STATE/tls/ca.pem" --message message.json

Read at the receiver with its separate enrolled directory:

    "$PACKAGE/bin/harklight" device read --directory "$HOME/.local/share/harklight-receiver" --connect 127.0.0.1 --name localhost --port 18443 --ca "$STATE/tls/ca.pem"

Supply the deliberate answer as a JSON value, then respond at the receiver:

    printf '%s\n' '{"choice":"continue","note":null}' > answer.json
    "$PACKAGE/bin/harklight" device respond --directory "$HOME/.local/share/harklight-receiver" --connect 127.0.0.1 --name localhost --port 18443 --ca "$STATE/tls/ca.pem" --sender app:reference --message-id request-1 --answer answer.json

Retrieve the exact terminal receipt at the origin:

    "$PACKAGE/bin/harklight" device receipt --directory "$HOME/.local/share/harklight-sender" --connect 127.0.0.1 --name localhost --port 18443 --ca "$STATE/tls/ca.pem" --sender app:reference --message-id request-1

Receipt remains pending until a terminal event is committed. Send/respond keep
their exact operation IDs in the identity directory for retry after a lost reply.
Read stores replay events before ACK. Keep the private identity/state directory
intact; do not create fresh operations to conceal uncertainty. These commands
also remain available to source developers as mix attention.device.

For a tailnet box use its tailnet address and exact certificate hostname in
--connect/--name and copy only the public trust root to each peer. Enrollment,
sender rights and receiver authority remain independent of tailnet membership.

Pending, relay acceptance, device application and the sender's terminal receipt
remain distinct. A response does not execute a source action or authorize
external operations. Use a supported answer UI when sending to the native app;
the packaged agent skill includes an example. Unknown UI remains unsupported.

## Authorized-agent tools remain available

bin/harklight agent exposes existing keygen/search/get/respond tools.
Owner agent-register, agent-grants-replace, agent-list and agent-revoke remain
available. Register the exact public key and grant read/respond separately.
Responses retain deliberate ANSWER confirmation, caller-held operation UUID,
exact acceptance sequence and intent reference, without source ruling authority.

## Upgrade or rollback

Stop the old process. Before the first start of a FORMAT23 build, back up the
complete state directory including SQLite and keys into private operator
storage. Verify the new checksum and extract into a different versioned
directory. Keep the state path, trust root, owner/person and configuration.
Start with existing server.env; do not rerun bootstrap. Startup migrates
supported earlier store formats immediately. There is no hot upgrade; only one
server may own a database.

If the new process fails, stop it and retain the current database and backup.
Earlier packages refuse FORMAT23 state. Rollback requires restoring the copy
made before the first FORMAT23 start and loses every change accepted after that
copy. Do not restore a stale backup over newer accepted writes. A failed
candidate may already have migrated the store. The existing installer retains
the selected candidate, prior packages, stopped backup and latest state; it
does not automatically downgrade or restore a database after switching. Use a
compatible forward repair or a separately reviewed state-aware recovery plan.
The full-root archive is a recovery source for the deployment's private state
and configuration; it does not justify replacing newer accepted data. Keep
backups and keys private.

## Build

On the declared Linux target use Elixir 1.19.5/OTP 27, a C compiler, make,
OpenSSL CLI and the pinned Mix lock:

    ./scripts/build-package.sh

This production package includes the Mix release/ERTS, server/device CLI,
bootstrap, guides and both existing agent skills. It preserves dependency
notices, records BUILD_INFO and writes tarball/SHA256SUMS in dist/. Archive ordering,
timestamps, owners and gzip headers are normalized; full toolchain bit-for-bit
reproducibility is not claimed. Build from the clean intended commit and use
that same artifact for clean-install evidence and publication.

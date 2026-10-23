# Guided join server foundation

The v0.1.5 server package includes these owner invitation, status and recovery
commands. See SELF_HOST.md for installation and dashboard setup.

The authenticated owner creates a person-bound invitation. The app keeps its
fresh device private key and submits a public certificate. Redemption creates a
pending request; the current owner separately approves its exact code, SPKI and
deadline with empty sender grants. Existing enrollment status and current relay
TLS proof complete the server side of the flow.

## Operator configuration and owner commands

Set `ATTENTION_SERVER_JOIN_NAME` to the DNS name validated by clients. Invitations
use that name, the configured enrollment/relay ports and the public certificate
in `ATTENTION_SERVER_CACERTFILE`. New bootstrap output includes this setting.
Existing installations can add it to their server configuration for a future
authorized restart, preserving the store, CA and owner. With no join name,
invitation creation is unavailable and the existing manual CLI remains usable.
No deployment or live restart is performed by this source change.

The existing owner TLS interface uses the current owner certificate. With the
usual connection, CA, certificate, key and person options, run:

```sh
bin/harklight owner invite ... --output /private/directory/invitation.json
bin/harklight owner approve ... --code CODE
bin/harklight owner decline ... --code CODE
```

Use a private destination directory. The invitation command creates a new
private file and displays its path and expiry, keeping its bearer token out of
terminal output. Approval and decline display the stored label, exact public-key
fingerprint and deadline and require the deliberate `APPROVE` or `DECLINE` input.
Guided approval refuses nonempty `--senders`. These commands use the existing
owner certificate authority boundary. The optional dashboard provides the
same current-owner review and approval operations; see SELF_HOST.md.

## Invitation and redemption wire

An owner TLS v1 request with `type: "invite_create"` and its authenticated
`person_id` returns `ok: true` and an `invitation` object. The invitation fields
are `v: 1`, `person_id`, `server_name`, `enrollment_url`, `relay_url`, `ca_pem`,
`ca_sha256`, `invite_token` and `expires_at`. The pin is lowercase SHA-256 of the
single public CA certificate's DER bytes. `expires_at` is Unix milliseconds,
30 minutes after creation. Endpoint and trust values come from server
configuration; a caller cannot supply their own in the owner request.

The invitation object also includes `join_url`: `harklight://join#` followed by
unpadded base64url of the UTF-8 JSON invitation v1 (without the `join_url` field
itself). The CLI's private JSON file includes this link. The full native link has
no HTTP query or path. Optional `person_label` and
`inviter_label` inside a link are display-only; `person_id`, configured pinned
trust and the redeemed invitation define the binding. The app validates the
entire invitation, not display labels.

When the opt-in dashboard HTTPS origin is configured, the owner page also shows
a short tappable `https://<configured-host>:<dashboard-port>/join#<invite_token>`
link. Share or copy that link to the joining person; it fits ordinary short-link
transport. The owner TLS response includes the same `tappable_url` outside the
unchanged full invitation object, and the authorized agent handoff exposes it as
`share_url` with the original native link in `full_url`. The recipient page needs
no owner sign-in. Its script removes the fragment from browser history, submits
the bearer in a bounded same-origin POST body, and offers an explicit **Open in
Harklight** action carrying the unchanged full native invitation. GET or preview
does not redeem it. An invalid, expired, used or owner-retired link cannot return
the full invitation. HTTPS trust of the self-hosted page and the app's separate
pinned invitation verification both matter; source tests do not prove a live
Pushover-to-browser-to-app handoff.

Deliver this private payload through the approved invitation UI/transport. The
app must use the owner-delivered trust pin and verify the configured TLS name
before sending secrets. Do not fetch trust from an arbitrary URL or put bearer
secrets into HTTP query strings, logs or generic status metadata.

Before its first POST, the app generates and persists its device private key
and a random 32-byte status secret. It sends `POST /enroll` over the pinned TLS
connection with exactly `invite_token`, `public_certificate` (PEM),
`status_secret` (64 lowercase hex characters), and optional `label`. The default
label is `New device`. No person or sender authority field is accepted from the
app. Invitation and status secrets are stored only as hashes.

Successful redemption atomically commits invitation consumption and its
person-bound pending request before a reply. The HTTP 200 response contains
`state: "pending"`, `approval_code`, `expires_at` and `status_expires_at`. The
app retains its own status secret; the server does not generate a replacement.
Pending approval expires 24 hours after redemption by default. The status
response includes person/device identity and both deadlines. A compact durable
binding retains same-device recovery authority beyond ordinary status expiry.

The same invitation, device SPKI and status secret return the same live pending
code and deadlines after a lost reply, concurrent retry or restart. A retry does
not replace the certificate/label or extend expiry. Once redeemed, its fixed
24-hour pending window applies even when the original thirty-minute initial
redemption window ends. A different binding gets HTTP 409 `used`; an unredeemed
expired invitation or expired pending request gets HTTP 410 `expired`. Unknown,
wrong-destination or retired-owner invitations get HTTP 404 `refused`.

Matching retries of approved, declined or cancelled requests return their truthful terminal
state. `POST /enroll/status` with only `status_secret` preserves existing status
behavior, including `declined`, `cancelled` and `revoked` terminal states.

For ordinary expiry, send `POST /enroll/resume` with the same saved
`status_secret`, original PEM `public_certificate`, and optional `label`. No
invitation token or private key is sent. The server verifies the retained
person/SPKI/secret binding and atomically creates one current pending generation
with a new 24-hour deadline. Repeating the same request after a lost response or
restart returns the same live code and deadlines without extending them. The
response includes `person_id` and `device_id`; the client must check the exact
saved binding before adopting it. Superseded codes cannot approve the new
request. Decline, cancellation, revocation and retired owner authority cannot
renew. An approved saved request returns its retained approved binding, subject
to current revocation. Native proof of current relay access is still required;
a historical approval does not override revocation.

Owner decline uses `type: "pending_decline"`, `approval_code`,
`expected_device_id` (SPKI) and `expected_expires_at` alongside `v` and
`person_id`. Existing `pending_approve` has the same exact binding and must use
`sender_grants: []` for guided requests.

## Requester cancellation and own-device removal

For E5, the app sends `POST /enroll/cancel` over the same pinned enrollment TLS
endpoint with exactly `status_secret`, `approval_code`, `expected_device_id`
(SPKI), and `expected_expires_at`. These identify its exact pending request;
knowledge of the code alone is insufficient. The server atomically persists
`state: "cancelled"` before HTTP 200, preserving the existing status deadline.
The same exact retry or `/enroll/status` returns `cancelled` after restart, and
neither approval nor invitation retry creates a device or grants sender rights.
Unknown or mismatched bindings return HTTP 404 `not_found` without mutation.
A stale cancellation tuple gets HTTP 409 `stale_request` with the current tuple;
it neither renews the request nor cancels a newer generation.

Approval and cancellation are serialized against the same durable snapshot. If
approval committed first, cancellation returns HTTP 200 `state: "approved"`
with `person_id` and `device_id`; it does **not** remove that device. If decline
or expiry won, cancellation reports `declined` or `expired`. The client must
follow the returned state and must not assume cancellation while an outcome is
unknown. A requester can then deliberately use the removal operation for an
already approved device.

For E9, send `POST /device/remove` to the enrollment service's host/port using
pinned server TLS **and the current device's client certificate/private key**.
The JSON body contains only `expected_device_id`. TLS CertificateVerify proves
possession; the expected SPKI must match that peer. No person, other device,
account-deletion or sender-grant field is accepted. No-cert requests, an owner
certificate, another device's certificate or an unknown key cannot remove the
target. The server commits revocation, cancels its push binding and fences its
relay sessions before HTTP 200 `state: "removed"`, `person_id`, `device_id`, and
`enrollment_revision`.

Removal is idempotent. A retained revoked device certificate is accepted only
for cleanup on this enrollment listener, so a lost response can be retried with
the same key and yields the same server-confirmed removal after restart. It
grants no relay, owner or sender access. Other TLS certificate errors (validity,
key usage, invalid chain) remain fatal. The client must preserve the identity
until it receives `removed` for that exact SPKI before replacing it. HTTP 404
`not_found`, network failure or HTTP 503 `unavailable` is not a removal proof.
Standalone `EnrollmentHTTPS` callers must supply `cacertfile` to enable this
optional client-certificate interface; `Server` supplies its existing CA.

## Durable device metadata

First successful owner approval copies the original 1–256-byte device `label`
into the device record and records `enrolled_at` as approval-time Unix
milliseconds, using the enrollment clock fence. Approval retries do not change
either value. Receipt cleanup, restart and revocation preserve both. These are
metadata, not evidence of presence or a current connection.

Older device records initialize missing `label` and `enrolled_at` to `nil`
(unknown) without inventing a name or time; existing values, owner/person
bindings and grants are preserved. Trusted diagnostic setup without an approval
also has unknown metadata. The authenticated dashboard uses these records;
its connection indicator stays Unknown without a real presence signal.

## Durable upgrade boundary

This release uses FORMAT22. Supported earlier snapshots, including FORMAT17,
migrate on startup, preserving persons, owners, devices, sender grants and
pending requests while initializing the hosted-account registry.
Invitations, consumed bindings and resumable request generations share the
atomic SQLite snapshot transaction. Keep a protected stopped backup before an
upgrade. The v0.1.10 package accepts formats 1–6 and 8–21 only, so it will not
start against FORMAT22. Retain current state and use a compatible forward repair
or separately reviewed recovery path rather than overwriting newer accepted work
with a stale backup. See SELF_HOST.md for upgrade and APNs configuration limits.

## Separate browser sign-in foundation

The configured owner can issue a short-lived one-use browser sign-in link
over authenticated owner TLS, then consume it on the HTTPS host page. See
SELF_HOST.md for the complete command and session behavior. Device invitation
and status tokens are not browser login credentials. Metadata visibility does
not grant cross-person approval, sender authority or private message access.

## Authorized agent invitation result

The owner-agent product action is `bin/harklight owner invite` with its existing
owner TLS connection/certificate/key/person options, plus `--handoff` and a
stable `--request-id UUID`. This is a dedicated sensitive JSON result channel:
it returns `state: ready`, destination, account and expiry directly to its
authorized caller. With the dashboard bridge configured, `share_url` is the
short tappable HTTPS link and `full_url` is the exact pinned native invitation;
otherwise both are the full native link. It does not require browsing, retrieving a
private invitation file or developer clipboard injection. Do not route this
explicit result channel into generic logs or chat. The receiving product adapter
can deliver that result to the intended joining person through its authorized
handoff; no provider notification is sent by this action.

The in-process equivalent is `InvitationHandoff.create(endpoint, owner_cert_path,
owner_key_path, person_id, request_id)` and explicit `encode_result/1` for its
sensitive result. Ordinary inspection redacts the share URL. Enrollment, send,
read and respond credentials do not authorize this owner operation. No delegated
agent role is inferred from those rights.

The shared server call is `Relay.owner_invite_create(relay, person_id,
verified_owner_key, request_id)`; owner TLS `invite_create` accepts that optional
`request_id`, and `HeadlessOwner.invite/5` carries it. A same-person/current-owner
retry with the same UUID returns the identical original unused invitation and
deadline across lost replies and restart, without another active invitation or
an extended deadline. A consumed action returns `used`, an expired action
returns `expired`, a changed destination returns `request_conflict`, and failed
persistence never returns success. No raw bearer is added to the store: it is
recovered through purpose-separated HMAC under the existing durable random
server key, bound to person, owner revision, request UUID and original deadline.
The request UUID is nonsecret and creates no authority. Old snapshots initialize
missing request IDs without changing existing invitation links or grants.

An unconfirmed transport/result failure returns `state: unconfirmed` with the
same request UUID and an instruction to retry that action; it is not an
invitation proof. Explicit refused, expired, used and capacity results name the
available recovery. A deliberate new invitation uses a fresh request UUID.
Expired records retain the existing bounded history; after history reclamation,
a new authorized action can issue a fresh token, and the old shared URL is never
reactivated because its original deadline is part of bearer derivation. The
legacy private-file command remains available for diagnostic compatibility.
Creating a handoff result does not send a notification or approve enrollment.

# Person-bound owner browser sign-in seam

This source supplies owner issuance and durable one-use authentication for the
approved W1 host-page flow. HTTP/UI, link construction, browser cookies and
session storage remain the dashboard owner's responsibility. The published
v0.1.2 binary predates this source. No live listener, deployment, bootstrap link
or provider traffic is added here.

## Issuance

Call `Relay.owner_browser_sign_in_create(relay, person_id, verified_owner_key)`.
The trusted management adapter supplies the current owner key proved by TLS;
a browser body or cookie cannot select it. The core verifies ownership of that
person and returns `{:ok, credential}` with JSON string keys:

- `v`: 1
- `person_id`: the stored person
- `sign_in_token`: `hbs1_` followed by 64 lowercase hex characters encoding 32
  random bytes
- `expires_at`: Unix milliseconds, exactly ten minutes after issuance
- `sign_in_url`: present only when the opt-in dashboard host and port are
  configured; `https://HOST:PORT/sign-in#hbs1_...` keeps the bearer out of the
  HTTP request path and query

The existing authenticated owner TLS endpoint accepts only
`{"v":1,"type":"browser_sign_in_create","person_id":"PERSON_UUID"}` and
returns `ok: true` plus `browser_sign_in: credential`. Extra identity, lifetime
or permission fields are rejected. `HeadlessOwner.browser_sign_in(endpoint,
certificate_path, key_path, person_id)` wraps this owner operation. Credentials
are not printed by these functions. The owner CLI `browser-sign-in --output`
saves a configured sign-in URL in a new private file. Keep that file and link
out of logs and generic status metadata; share it only with the intended
owner. No public token-issuance endpoint or resolver is constructed here.

Only a purpose-separated SHA-256 hash is persisted, alongside person, issuer
key/revision, issuance/expiry, bounded retention and consumption time. At most
1024 token records are retained. At the next issuance, spent or expired records
and records from a retired owner key/revision are reclaimed before considering
capacity, so unusable historical credentials cannot prevent a fresh login.
Reclaimed tokens return `invalid_sign_in` and cannot authenticate again; a
retained spent token returns `used`. The capacity refusal applies to the bounded
set of still usable, unconsumed links. Issuance refuses stale/currently invalid
owner authority and cannot change device, invitation, sender or person grants.

## Authentication and consume

The trusted server-side HTTP/session backend calls
`Relay.consume_browser_sign_in(relay, sign_in_token)`. It supplies only the
bearer token, without a person, device or owner selector. Success is
`{:ok, %{person_id: person, owner_key: key, owner_revision: revision,
scope: :person_owner}}`, selected from durable state. The current issuer
key/revision is revalidated at consumption. SQLite commits the consumed state
before releasing this authenticated principal. Concurrent/restarted consumers
cannot receive it twice; a stale database writer returns storage failure
instead of an authenticated principal.

Refusals are `{:error, :invalid_sign_in}` for malformed, unknown or retired-issuer
tokens, `{:error, :expired}` for a live token past expiry, and
`{:error, :used}` for an already consumed token whose issuer remains current.
No refusal discloses person/device metadata. Database failure returns
`{:error, :storage_unavailable}`. An observed crossed expiry is durably fenced
against clock rollback. Issuance during a rollback returns `clock_unavailable`
until the clock catches up. This clock is independent of enrollment deadlines.

The `hbs1_` format and hash purpose are distinct from invitation and enrollment
status credentials; those credentials cannot authenticate a browser. There is
no public consume route or person/device enumeration added by this seam.

## Session/backend handoff

Only a successful consume result authorizes creating the browser session. Store
that result in the backend's trusted session record; never accept corresponding
principal fields from the browser. The backend must recheck current issuer
key/revision on later protected requests, so owner recovery invalidates a
previously issued session as well as unused sign-in tokens. Existing narrow
owner operations recheck person ownership, but a session resolver must also
check the recorded revision. This person-owner result does not grant sender
rights, other-person mutation, delegated owner-device trust or account-wide
visibility. The approved self-hosted account-owner metadata view needs its
separate authority model. Hosted own-device-only access remains the HTTP/session
owner's scope; this self-hosted person-owner token is not a hosted login grant.

A token stays consumed even if the HTTP/session response is lost or session
creation fails afterward. Recovery uses a fresh owner-issued sign-in token;
retrying a used token does not mint another session. The token snapshot and a
separate session database are not an atomic transaction. The HTTP/session owner
must present the actual failure and recovery state to the person. Adding the
first bootstrap-issued host-page link is later work, not implicit issuance.

## Durable upgrade

Older snapshots initialize an empty sign-in map and clock floor, preserving
existing persons, owners, devices, invitations and sender grants. Before any
future authorized live upgrade, keep the stopped backup: older binaries cannot
read the newer format. This source work does not mutate the live store or APNs.

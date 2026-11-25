# Person-bound owner browser sign-in seam

The owner issues a one-use link to sign in to the self-hosted dashboard for
the person they manage. The dashboard creates a browser session after
redeeming the link. See [SELF_HOST.md](SELF_HOST.md) for setup.

## Issuance

Call `Relay.owner_browser_sign_in_create(relay, person_id, verified_owner_key)`.
The trusted management adapter supplies the current owner key proved by TLS;
a browser body or cookie cannot select it. The core verifies ownership of that
person and returns `{:ok, credential}` with JSON string keys:

- `v`: 1
- `person_id`: the stored person
- `sign_in_token`: `hbs1_` followed by 64 lowercase hex characters encoding 32
  random bytes
- `expires_at`: `null`; a sign-in link does not expire, it works once
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

The store contains a purpose-separated SHA-256 hash, the person and issuer
key and revision, and the issuance time. Consuming a token deletes its
record. Issuance removes records for superseded owners and requires current
owner authority.

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

Malformed, unknown, already consumed and retired-issuer tokens return
`{:error, :invalid_sign_in}`. Database failure returns
`{:error, :storage_unavailable}`.

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
creation fails afterward. Recovery uses a fresh owner-issued sign-in token. The token deletion and
browser-session creation are separate operations.

## Durable upgrade

Upgrading a FORMAT30 store preserves unused sign-in links and keeps consumed
links unusable. Follow [SELF_HOST.md](SELF_HOST.md#upgrade-or-rollback) for
backup and recovery.

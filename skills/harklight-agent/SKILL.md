---
name: harklight-agent
version: "0.1.0"
description: "Understand Harklight session messages, explicitly authorized enrollment, and separately granted search and response tools."
---

# Harklight identities and session messages

Agent sends use only the `harklight-agent` session CLI. The server package's
`bin/harklight` is for separately authorized enrollment and administration.
Do not start enrollment or change grants merely because a send is blocked. Ask the Harklight operator for the server address, certificate hostname, public CA certificate, person account and the sender ID you are allowed to use. Verify the CA through the operator's trusted channel; a URL or tailnet connection alone does not establish account trust. Keep identity directories private. The server package's `SELF_HOST.md` covers setup and port exposure; find it alongside `bin/harklight` in that package, even if this skill has been copied into a harness.

## Choose the right identity

An enrolled **device identity** can send under an exact sender grant, read its own person inbox and use the origin session feed for its own sent requests. It does not become an owner or gain every sender ID. The current headless enrollment command creates this identity locally. The authorized person owner approves its displayed code and public-key fingerprint. For a sender, the owner grants the chosen ID during approval with `--senders`, or later with `owner grant-sender --device FINGERPRINT --sender ID`. Omit sender grants for a read/respond-only device.

For example, on the agent's machine, with values supplied by the operator:

```sh
"$PACKAGE/bin/harklight" enroll start --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$ENROLL_PORT" \
  --ca "$CA_PEM" --label "Script sender"
```

Give the approval code to the authenticated owner. The owner checks the intended person, label and key fingerprint before running `bin/harklight owner approve` with `--senders app:example` for this sender. For an explicitly authorized enrollment, check the saved request with the
installed enrollment command. This does not prove session listener support:

```sh
"$PACKAGE/bin/harklight" enroll status --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$ENROLL_PORT" \
  --relay-port "$RELAY_PORT" --ca "$CA_PEM"
```

Keep the directory if a submission reply is lost. `enroll retry` uses the same local key; a `key_pending` result does not recover the old code or prove enrollment. Follow the command's expiry guidance, and treat `key_in_use` or an unknown status handle as unresolved until the owner reconciles it. This CLI is for headless agents and scripts; the native app's guided device joining is separate work.

A separately registered **agent identity** supports `agent search`, `agent get` and `agent respond` over the agent HTTPS endpoint. It cannot send a new message. `agent keygen --directory "$AGENT_DIR"` creates a local identity; the owner registers its exact public key with `owner agent-register`, then sets current read and respond sender scopes with `owner agent-grants-replace`. Enrollment and a sender grant on a device do not substitute for those agent grants.

## Send and receive through the session CLI

This cutover guidance takes effect at a destination only with or after installation
of the approved sender release implementing all required `actions/1` forms. Until
that coordinated apply, existing supported session-route communication continues.
This is rollout sequencing, not authorization to publish, apply, release, install
or change live state.

Use the originating host's enrolled device, exact sender grant, trusted CA and
configured origin HTTPS port with `harklight-agent`. Read the standalone CLI command reference and the
`harklight-agent-send-listen` skill shipped with the server. The origin port is separate
from enrollment and relay ports. Preserve the returned session, request,
revision, event and operation identities; use the same session for listening,
ACK, reply, revision, retraction and result. The CLI's durable journal and
`retry --op-id` recover an uncertain operation without a second send.

The approved full-form sender carries an `answer_ui` object with schema
`attention-client.actions/1` on every send. A question uses:

```json
{"schema":"attention-client.actions/1","actions":[{"id":"reply","label":"Reply","kind":"text"}]}
```

A decision uses one `kind:"value"` action per choice plus text Reply. Each value
action submits its exact JSON value; text Reply submits the entered JSON string.
An informational message uses exactly this one action, with no Reply or choices:

```json
{"schema":"attention-client.actions/1","actions":[{"id":"seen","label":"Seen","kind":"value","value":{"acknowledgment":"seen"}}]}
```

Before that sender support is installed, continue otherwise-supported sends
through the installed session CLI with its actual controls. The app compatibility
form remains during this transition; it is not another agent send route. The
0.1.1 sender can send plain questions and choices, but plain sends omit `answer_ui`,
choices omit text Reply, and Seen is unavailable. Report a specific requested form
it cannot express; do not fabricate a descriptor or suspend other supported sends.
Invalid descriptors are never permission to guess an answer.

After the approved full-form sender is installed, sends and revisions, including
clearing choices, retain the required valid `actions/1` descriptor. The source
candidate uses `send --informational` for Seen, mutually exclusive with `--choice`;
use that flag only after its approved release is installed. Documentation does
not make a flag or capability exist.

The server stores the descriptor as an opaque object. Before removing the app's
compatibility form, release and install sender support for `actions/1` on every
send, then obtain an authorized read-only recount showing no open message without
that schema. Do not build closure or conversion code for this cutover. Recount
just before removal rather than relying on a past snapshot.

If the recount finds an incompatible open message, bring its exact identity to
the server and client maintainers. The only proposed treatment is an existing
ordinary terminal path that appends closure events and leaves the original message,
receipts, revisions and feed entries byte-identical. Both person and sender see
closed without an answer, never answered or `no_comment`, and the sender can
observe the closure. Still-needed work is re-sent as new requests through the one
route within the applicable authority. This guidance authorizes no live closure
or resend. If no existing terminal kind fits, bring the exact mismatch to the
maintainer before proposing a new kind or mechanism.

No conversion of any kind is part of this cutover: no stored rewrite, server/app
read-time converter or one-time client upgrade. Answered, retracted and expired
messages remain readable; preserve drafts and pending answers without conversion
or silent loss. Raise an incompatibility rather than inventing a migration. Live
changes require their approved release/install route, never a manual data edit.
Closure is not an answer or authorization to perform the requested action.

Session acceptance is not human attention. Opening a message is not Seen.
Listener attachment or printed output is not an originating-session ACK. Report
the actual route and the installed CLI's unsupported reason when handoff is not
available; another host's key or an invented controller cannot supply it.

## Search or answer older messages as an agent

After the owner has registered the agent key and granted the relevant sender scope, use the agent endpoint:

```sh
"$PACKAGE/bin/harklight" agent search --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --text "next step"
"$PACKAGE/bin/harklight" agent get --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --message request-1
```

`search` can also use `--state`, `--limit` and the returned `--cursor`; `get` returns the exact visible message under current read scope. To answer an open message, use the acceptance sequence returned by `get`, a caller-held UUIDv4 operation ID, an intent reference meaningful to your workflow and an answer file containing one JSON value:

```sh
"$PACKAGE/bin/harklight" agent respond --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --message request-1 \
  --acceptance "$ACCEPTANCE_SEQ" --op "$OP_ID" --intent-ref "$INTENT_REF" \
  --answer-file answer.json
```

The tool displays the exact target and asks for `ANSWER` before submission. Reuse the same operation ID and answer after an uncertain result; a different answer needs a new deliberate decision and can lose to an already committed terminal response. Current grants and revocation apply to every search, get and response. An agent response records the relay outcome; it does not execute a source action or prove APNs display.

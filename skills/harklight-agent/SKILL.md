---
name: harklight-agent
version: "0.1.0"
description: "Operate a self-hosted Harklight server from an agent or script: enroll a sender device, send a message, read its reply, or use separately granted agent search and response tools."
---

# Operate Harklight

Use the `bin/harklight` shipped with the server package. Ask the Harklight operator for the server address, certificate hostname, public CA certificate, person account and the sender ID you are allowed to use. Verify the CA through the operator's trusted channel; a URL or tailnet connection alone does not establish account trust. Keep identity directories private. The server package's `SELF_HOST.md` covers setup and port exposure; find it alongside `bin/harklight` in that package, even if this skill has been copied into a harness.

## Choose the right identity

An enrolled **device identity** can send under an exact sender grant, read its own person inbox and retrieve an origin-only terminal receipt. It does not become an owner or gain every sender ID. The current headless enrollment command creates this identity locally. The authorized person owner approves its displayed code and public-key fingerprint. For a sender, the owner grants the chosen ID during approval with `--senders`, or later with `owner grant-sender --device FINGERPRINT --sender ID`. Omit sender grants for a read/respond-only device.

For example, on the agent's machine, with values supplied by the operator:

```sh
"$PACKAGE/bin/harklight" enroll start --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$ENROLL_PORT" \
  --ca "$CA_PEM" --label "Script sender"
```

Give the approval code to the authenticated owner. The owner checks the intended person, label and key fingerprint before running `bin/harklight owner approve` with `--senders app:example` for this sender. Then check the saved request and prove current relay access:

```sh
"$PACKAGE/bin/harklight" enroll status --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$ENROLL_PORT" \
  --relay-port "$RELAY_PORT" --ca "$CA_PEM"
```

Keep the directory if a submission reply is lost. `enroll retry` uses the same local key; a `key_pending` result does not recover the old code or prove enrollment. Follow the command's expiry guidance, and treat `key_in_use` or an unknown status handle as unresolved until the owner reconciles it. This CLI is for headless agents and scripts; the native app's guided device joining is separate work.

A separately registered **agent identity** supports `agent search`, `agent get` and `agent respond` over the agent HTTPS endpoint. It cannot send a new message. `agent keygen --directory "$AGENT_DIR"` creates a local identity; the owner registers its exact public key with `owner agent-register`, then sets current read and respond sender scopes with `owner agent-grants-replace`. Enrollment and a sender grant on a device do not substitute for those agent grants.

## Send a document and get its reply

Write one JSON message document. The server requires `sender_id`, `message_id`, integer `created_at` and nonempty `title`; `body` is optional. Use a sender ID actually granted to this device, such as `app:example`. Keep `message_id` stable for this request and unique within that sender's messages. This `attention-client.actions/1` document gives the current mainline app an answer control:

```json
{"sender_id":"app:example","message_id":"request-1","created_at":1791930000000,"title":"Choose the next step","body":"Pick one option or write a note.","answer_ui":{"schema":"attention-client.actions/1","actions":[{"id":"continue","label":"Continue","kind":"value","value":"continue"},{"id":"note","label":"Write a note","kind":"text"}]}}
```

Save it as `message.json`, then send and check the origin receipt:

```sh
"$PACKAGE/bin/harklight" device send --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" \
  --ca "$CA_PEM" --message message.json
"$PACKAGE/bin/harklight" device receipt --directory "$DEVICE_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$RELAY_PORT" \
  --ca "$CA_PEM" --sender app:example --message-id request-1
```

`send` reports relay acceptance, not human attention or a source action. `receipt` is pending until a terminal event exists; after an answer it gives the committed answer, including the difference between JSON `null` and no answer. Only the original sending device can request that receipt. Another enrolled device can run `device read` to durably journal replayed inbox events before ACK, and `device respond --sender app:example --message-id request-1 --answer answer.json` with a file containing one JSON value. A response is deliberate and terminal; do not infer it from the message text or notification delivery.

In `attention-client.actions/1`, a `value` action submits its exact JSON value and a `text` action submits the person's entered text as a JSON string. Companion app revisions differ when `answer_ui` is absent: a newer native app offers a deliberate text reply, while an earlier app marks the request unsupported for answering. Use the structured action example when the installed client revision is uncertain. The app also recognizes `attention-client.ui/1` sender documents with scenes, illustrations and inputs; validate one against the receiving client before sending it. An unfamiliar or invalid explicit `answer_ui` is shown as unsupported and offers no app reply control; it is not permission to guess an answer. Other accepted JSON members have no guaranteed app rendering or execution behavior.

Keep the exact message file and identity directory for retries. `device send` and `device respond` save a UUID operation with the request's exact JSON before network submission and reuse it on identical retries. Changing content under an existing `(sender_id, message_id)` conflicts with the first accepted message. Choose a new message ID for a genuinely new request; do not change an uncertain request merely to make a retry pass. The local journal and cursor survive process restarts if the identity directory survives.

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

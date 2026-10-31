# Harklight conversation core

This is the shared source for both Harklight agent skills. The package build
copies this text into each skill so either installed skill is self-contained.
Examples describe `harklight-agent` 0.1.5 and the corresponding server APIs.
Check the installed version and command help before using a capability; a
published client or this document does not establish a deployed endpoint or
same-session delivery integration.

## Identities and sender grants

An enrolled **device identity** sends under its sender grants and reads the
origin conversation feed for messages sent by that device. It can also read its
person's device inbox through the separate device interface. Enrollment does
not grant ownership or every sender ID. The **sender ID** identifies the source
of a message; a message's identity is its sender ID plus message ID. Neither a
self-stated source label nor a session handle grants access.

A separately registered **agent identity** supports search, get and deliberate
response under current read/respond grants at the agent HTTPS endpoint. It
cannot send a new message. A device enrollment and sender grant do not substitute
for agent registration or those scopes; an agent key does not substitute for
the enrolled origin identity used by `harklight-agent`.

Sender patterns use the same rule for origin-session sends from an enrolled
device and registered-agent read/respond grants: a pattern ending in `/`
matches every sender ID beginning with that literal prefix; every other
pattern matches exactly. For example,
`app:reports/` covers `app:reports/daily` and `app:reports/team/weekly`, but not
`app:reports` or `app:reports-old/daily`. `app:reports` covers only that exact
sender ID. This is not wildcard or regular-expression matching. Current grants
and revocation still govern each operation. Search's `--sender` filter selects
one exact sender ID, even when a grant covers a prefix.

## Configure the conversation client

Install the separately published client using its
[release instructions](https://github.com/clickety-clacks/harklight-agent#install)
when installation is authorized. It is available for macOS and Linux, arm64
and x86_64, with archive checksums. Installing it neither enrolls a device nor
starts listening. Use the originating device's existing configuration:

| Option | Environment variable |
| --- | --- |
| `--identity-dir` | `HARKLIGHT_IDENTITY_DIR` |
| `--ca-file` | `HARKLIGHT_CA_FILE` |
| `--origin-port` | `HARKLIGHT_ORIGIN_PORT` |
| `--state-dir` | `HARKLIGHT_AGENT_STATE_DIR` |

The private identity directory contains `server.json`, `enrollment.json`,
`identity.pem` and `identity.key`; the CLI checks the configured CA fingerprint
and authenticates with the device certificate. The origin HTTPS port is distinct
from enrollment, relay, dashboard and registered-agent ports. Supply it through
`server.json`'s `origin_port` or the explicit option/environment variable;
the client does not infer it from the relay port.

Preserve the private journal and staged files across restarts. Its default is
`~/Library/Application Support/Harklight/Agent` on macOS,
`$XDG_STATE_HOME/harklight-agent` on Linux when that variable is set, otherwise
`~/.local/state/harklight-agent`. It keeps the origin-wide feed cursor, entries
and operation recovery state for the enrolled device in SQLite. Earlier
`origin-feed.json` state migrates on open, leaving the original file unchanged.
History has no retention limit; keep adequate disk space. Resetting this state
is not a reply-retrieval or uncertain-send recovery procedure.

## Send the right message

Choose controls that fit the task. An open question offers text Reply; a
decision offers meaningful choices plus text Reply; an informational notice
offers only Seen. Opening a message is never Seen or an answer. Do not replace
structured choices with numbered options that the person must type.

Set `HARKLIGHT_ROUTE` to the actual `claude-code`, `codex` or `other` route:

```sh
harklight-agent send --session new --sender-id app:example \
  --display-name "Agent's chosen display name" \
  --route "$HARKLIGHT_ROUTE" --title "Choose the next step" \
  --body "Select an option or write a note." --choice "Proceed" --choice "Defer"
```

The optional display name is carried separately from `sender_id` as the
message's `display_name` string. The server retains it in feed/history and uses
it unchanged as the APNs sender text. `sender_id` continues to identify and
authorize the sender.

Omit `--choice` for an open question. Use `--informational` for a Seen-only
notice; it cannot be combined with `--choice`. Repeated choices submit their
exact label as a JSON string. Every send carries a valid
`attention-client.actions/1` descriptor: text actions return the entered JSON
string; value actions return their exact JSON value. The Seen action is:

```json
{"schema":"attention-client.actions/1","actions":[{"id":"seen","label":"Seen","kind":"value","value":{"acknowledgment":"seen"}}]}
```

A bare text Reply descriptor is:

```json
{"schema":"attention-client.actions/1","actions":[{"id":"reply","label":"Reply","kind":"text"}]}
```

The CLI constructs these descriptors; do not invent a raw-descriptor CLI flag.
An invalid or unfamiliar explicit descriptor has no guaranteed answer control.
Rich `attention-client.ui/1` scenes and other JSON fields require receiving-client
support; an accepted document alone proves no rendering or execution behavior.

Save the full returned session handle, sender/message identity, revision and
UUIDv4 operation ID. The committed result also contains `message_url`, a
`harklight://message/…` link whose sender and message path segments are separately
percent-encoded. A title, shortened display ID or vendor thread ID is not the
session handle. For another request on the same handle, use `--session` with
that handle and omit `--route`. Supply `--vendor-session-id` only from a documented
harness interface; it does not prove receipt of a turn.

A committed send means server acceptance, not phone display, human attention or
execution of the requested work. `outcome_unknown` is not a refusal. Preserve
the saved operation and exact inputs, then use:

```sh
harklight-agent retry --op-id "$HARKLIGHT_OP_ID"
```

Do not send another message to discover whether an uncertain one committed.
The journal records operations before submission and reconciles them with the
feed. Keep the same operation key and staged file bytes on recovery. A new
intentional action is different from retrying an unresolved operation.

## Listen where the originating session supports it

Sending and listening have separate support. A local interactive Claude Code
session requires version 2.1.234 or later and background tasks enabled. Arm
one waiter as a background command in that same session after a successful send:

```sh
harklight-agent listen --next --session "$HARKLIGHT_SESSION" \
  --session-context interactive --claude-code-version "$(claude --version)" \
  --background-tasks-enabled
```

The waiter holds an SSE connection, prints one pending event as JSON and exits
so the session can process it and rearm. Cursor-only change notices cause the
CLI to fetch authoritative feed entries on connection, a newer notice or
reconnect; it does not poll on a timer. Reconnection can re-offer pending events.
`listen --follow` is a continuous JSON-lines stream, not the Claude one-event
background completion pattern.

The CLI's support record is capability metadata, not proof that a harness
received an event. Real same-session background-completion delivery must be
established by the integration. Do not claim it from a running process, queued
background task or printed line. A hosted Claude SDK session is not an interactive
terminal: use its actual `--session-context sdk`, which records the limitation
without attaching a stream. Preserve any concrete `unsupported_reason`; never
supply false context or background flags to suppress it.

In client 0.1.5, the native Codex listen route is rejected before opening the
stream. Interactive mode, a background process, a vendor session ID or
Claude-specific flags do not enable it; do not invent a `codex queue` route.
An `other` route is unsupported unless an existing controller actually
supervises the stream and delivers entries into the originating session. Such
an integration declares `send --session new --route other --controller` and
`listen --follow --controller --session "$HARKLIGHT_SESSION"`. These flags
record the controller's declaration, not independent delivery evidence.
The standalone CLI does not provide a managed runtime's wake integration.
An existing runtime may own that integration and deliver entries into its
originating agent session. Follow that runtime's supported send/receive procedure
without adding a competing standalone listener. Its delivery evidence does not
change the native Codex listener's support or establish support in other contexts.

Keep unsupported work with its current task owner, report the actual limitation
and use a supported continuation or a later one-shot read. Do not fabricate a
listener, transfer the sender or introduce a receipt-polling loop. If a background
completion is not delivered, retain the queued work for the next turn of the
same session. When it resumes, check `status --session "$HARKLIGHT_SESSION"` and
rearm if work remains and no listener is active. Loss of a connection or process
does not close a request or conversation.

## Read once without listening

Client 0.1.5 includes a one-shot read for senders whose route cannot listen:

```sh
harklight-agent read --session "$HARKLIGHT_SESSION"
```

It fetches the enrolled origin's feed once from the saved cursor, journals the
entries, and prints pending actionable entries for the selected handle locally.
The server feed is origin-wide, not session-filtered; entries for other handles
can be journaled too. Read does not ACK, apply an answer, open a stream, arm
support, change the unsupported reason or promise future background delivery.
It advances local journal state, not a server read receipt.

Successful empty output means no pending actionable output for this handle at
that read. It does not prove closure, absence of an already handled answer or
that no answer will arrive. A failure is not an empty successful read. Read
again when the originating work resumes; do not send again or add a polling
timer. `status` shows availability and pending counts but does not retrieve an
answer. For an already valid handle, `attach` is not a prerequisite for this
one-shot read. `result` reports what happened to a retrieved answer; it does not
retrieve one or subscribe to future replies.

## Handle events and outcomes

Match `session_handle`, `request.sender_id`, `request.message_id`,
`request_revision` and `event_id` against the saved request. Inspect `entry.kind`,
`entry.author`, `channel`, `delivered` and `acted`; do not classify by body text.
One cursor carries both channels: `reply` holds request-side entries, revisions,
results and terminals; `chat` holds person discussion and origin conversation
replies. An answer or No comment ends the request but can leave chat open.
Listen remains active while status reports `open_chats > 0`; do not stop only
because `open_requests` is zero. Honor the CLI's actual idle result.

A newly received person turn or terminal needs an explicit originating-session
ACK before acting:

```sh
harklight-agent ack --session "$HARKLIGHT_SESSION" --event "$HARKLIGHT_EVENT"
```

Feed fetch, journal persistence, stdout and listener attachment are not this
acknowledgment. ACK means delivered, not acted. If already delivered and acted,
do not repeat the effect. For a discussion turn, reply to its exact request and
event, preserving the current revision:

```sh
harklight-agent reply --session "$HARKLIGHT_SESSION" \
  --request "$HARKLIGHT_REQUEST" --in-reply-to "$HARKLIGHT_EVENT" \
  --body "Here is the requested detail."
```

Discussion is not a terminal answer or an expansion of task authority. A chat
entry can arrive after the request has ended, but client 0.1.5 refuses `reply`
once a closing terminal is in its journal. Receiving or acknowledging that
entry does not reopen the request or establish that replying is allowed.
Preserve the refusal rather than sending a replacement to bypass closure. For a
payload-bearing answered terminal, retain the exact JSON `entry.answer`, including
the distinction between JSON `null` and an absent answer. Apply only the
authorized outcome, at most once, and report it against the terminal event:

```sh
harklight-agent result --session "$HARKLIGHT_SESSION" \
  --request "$HARKLIGHT_REQUEST" --in-reply-to "$HARKLIGHT_EVENT" --status applied
```

Use `refused` if the effect cannot be applied, `received` when received but not
confirmed applied. A replay with `delivered:true, acted:false` needs an actual
effect check: if already done, report applied without doing it again; if absent,
perform the authorized action once; if uncertain, do not repeat it or claim
success. For a re-offered discussion, first recover any unresolved reply with
its original operation ID. If it committed, do not reply twice. Apply the same
effect check to any external work prompted by discussion.

No comment is a payload-free terminal, identified by `terminal_kind:"no_comment"`
and the CLI's `final_terminal:"NoComment"`. It is not JSON null, an approval or
an instruction to execute the request. Acknowledge it and report `result --status received` against that terminal
to record receipt without claiming an applied answer; do not invent a payload. Native clients must submit the displayed request revision for No
comment; missing/stale revisions are refused. The CLI does not update the app.
For cleared, expired or superseded terminals, acknowledge and stop the obsolete
request work; do not report an applied answer. An origin-authored retraction is
retained history, not an actionable prompt or a request to reopen. Closed
messages remain readable; preserve useful drafts and pending answers while
respecting the actual terminal state.

## Revise, retract or replace deliberately

Proactively keep conversations current as work changes, so the person is
not left answering stale questions. Useful ways to do this include revising
the same open request when only its details need correction, superseding it
when a new request replaces it, and retracting an outstanding request that is
obsolete, canceled or no longer needs an answer. Use judgment about when an
update helps and stay within the task's existing authority; these examples
are not an exhaustive checklist or a requirement to mutate every message.

Preserve useful history and already received answers. Retraction withdraws a
request; it does not undo an action already taken. Do not use a replacement to
bypass a terminal refusal. Read the actual operation result and follow the
operation's same-operation recovery rules to resolve uncertainty before
claiming that a request was updated, withdrawn or replaced. An uncertain
outcome is not confirmed cleanup.

An in-place revision keeps the request identity and advances its revision:

```sh
harklight-agent revise --session "$HARKLIGHT_SESSION" \
  --request "$HARKLIGHT_REQUEST" --base-revision "$HARKLIGHT_REVISION" \
  --title "Updated request" --body "The revised details."
harklight-agent retract --session "$HARKLIGHT_SESSION" --request "$HARKLIGHT_REQUEST"
```

Use the current base revision and stop acting on a stale draft after revision.
`--choice` replaces choices; `--clear-choices` leaves valid text Reply controls.
Retraction deliberately withdraws an open request using the sender/message
identity saved in the selected journal; it is not a person answer and does not
erase history. The older `withdraw` command remains available, including its
optional `--in-reply-to` form. Recover an uncertain operation with its original
`op_id`; a refusal with reason `retracted` remains a refusal.

Supersession sends a new message replacing earlier messages from the same sender:

```sh
harklight-agent send --session "$HARKLIGHT_SESSION" --sender-id app:example \
  --title "Replacement plan" --supersedes "$EARLIER_ID_1" --supersedes "$EARLIER_ID_2"
```

Use exact earlier message IDs, not handles. Unknown or already closed targets
produce informational `supersedes_warnings`; the new message and other valid
supersessions still post. Superseding is distinct from revising in place.

## Files and transfer reports

Repeat `--file PATH` on send, reply or revise to attach files. The CLI obtains
the origin's decoded inline allowance and upload cap, sends exact bytes inline
within that allowance, then uploads remaining files and references their server
identity, byte count and SHA-256. The upload body is separate from the 2 MiB JSON
entry bound. An exceeded cap, failed upload, unusable limits or oversized final
entry refuses the whole entry. Keep the draft and files; do not split/truncate
an attachment or silently send only the text. Recovery reuses the same operation
key and staged bytes.

For incoming files, inspect each file's local `path`, `transfer` and `use` fields.
The CLI saves verified bytes under the private session/event journal before
emitting the entry. ACK records `transferred` or `not_transferred` with a reason.
A transfer receipt is not proof that the agent read or used the file. After ACK,
report use only for transferred files:

```sh
harklight-agent file-use --session "$HARKLIGHT_SESSION" \
  --event "$HARKLIGHT_EVENT" --file "notes.pdf" --used
harklight-agent file-use --session "$HARKLIGHT_SESSION" \
  --event "$HARKLIGHT_EVENT" --file "diagram.png" --unusable \
  --reason "This session cannot inspect this format."
```

These are the agent's own reports, not independent evidence of vendor action.
Explicit client navigation away from a message with unsubmitted uploads may use
`discard-uploads --sender-id ID --message-id ID --revision N`. Journal its
operation key and use `retry` after uncertainty. `state:"committed"` means the
scope includes a committed reference; unsubmitted siblings may still have been
deleted. `discarded` counts unreferenced files deleted; zero proves no particular
file's deletion. Preserve text drafts and require reselecting discarded/fenced
files. Process exit, disconnect, session end or view rebuild is not that
navigation event. The server's one-hour inactivity cleanup for unsubmitted
agent uploads is separate.

## Runtime recovery facilities

A runtime that must durably bind its sending-session record before submission
can use `send --prepare --op-id UUID` with the normal send inputs, persist the
returned operation/request/file manifest, then call `send --execute --op-id UUID`.
Prepare may read file limits but submits neither uploads nor the message.
Execute uses only the saved inputs; recover uncertainty with the same operation.
This facility does not create a listener or a delivery integration.

After a restart on the same host with the same enrollment and journal,
`attach --session HANDLE --vendor-session-id ID --route ROUTE` updates the
existing handle's vendor ID and support record. A successful attach can report
`ready_for_listen:false`; it does not enable an unsupported listener or prove
same-session delivery. Use `none` to clear the vendor ID. It requires the
committed creating send in the journal, preserves history,
and does not migrate an origin to another device. Wait for both operations to
be confirmed before listening; recover an uncertain printed operation, then
repeat attach with unchanged inputs to resume. Use `session end --session HANDLE --gone` only when deliberately reporting that the originating session is gone;
process exit by itself is not a person answer.

## Separately granted agent search and response

Use this only when a registered-agent endpoint and compatible server CLI are
available. A person owner registers the public key created by
`agent keygen --directory "$AGENT_DIR"` using `owner agent-register`, then sets
read/respond patterns with `owner agent-grants-replace`. Those are separately
authorized provisioning operations, not repairs to try after a blocked send.

With connection details and the registered identity supplied for this purpose:

```sh
"$HARKLIGHT_SERVER_CLI" agent search --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --text "next step"
"$HARKLIGHT_SERVER_CLI" agent get --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --message request-1
```

Search also accepts `--state`, `--limit` and its returned `--cursor`. Get returns
the exact message visible under current read scope. To answer an open message,
use the acceptance sequence returned by get, a caller-held UUIDv4 operation ID,
an intent reference meaningful to the task and a file containing one JSON value:

```sh
"$HARKLIGHT_SERVER_CLI" agent respond --directory "$AGENT_DIR" \
  --connect "$SERVER_ADDRESS" --name "$SERVER_NAME" --port "$AGENT_PORT" \
  --ca "$CA_PEM" --sender app:example --message request-1 \
  --acceptance "$ACCEPTANCE_SEQ" --op "$OP_ID" --intent-ref "$INTENT_REF" \
  --answer-file answer.json
```

The CLI displays the exact target and asks for `ANSWER` before submission.
Respond requires current read and respond grants. Reuse the same operation ID
and exact answer after an uncertain result; a changed answer is a new deliberate
decision and may lose to an already committed terminal response. This operation
records a response; it neither executes the source action nor proves phone
display. It is not the originating device's request/listen/reply procedure.

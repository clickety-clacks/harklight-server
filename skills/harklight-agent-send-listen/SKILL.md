---
name: harklight-agent-send-listen
version: "0.1.1"
description: Send a request through an existing Harklight sender grant and keep its originating agent session listening for request-bound turns.
---

# Send and listen with Harklight

Use the `harklight-agent` host CLI and the existing enrolled device identity.
The CLI is released from <https://github.com/clickety-clacks/harklight-agent>;
install it with `brew install clickety-clacks/tap/harklight-agent` or from that
repository's release tarballs. The server package does not bundle it. The CLI
does not enroll devices, request sender grants, add trust roots, or start a
replacement agent. Ask the operator for the existing identity directory, CA
file and origin HTTPS port when those values are not already configured. The
sender ID must already be granted to this device. See the CLI's README in that
repository for file attachments, retries, the disposable integration fixture
and the full command reference.

The CLI uses the configured origin HTTPS endpoint. Its port is distinct from
the relay port and is never inferred from it. Set `--origin-port` or
`HARKLIGHT_ORIGIN_PORT` when the enrolled `server.json` has no `origin_port`.
Keep the journal directory so uncertain operations can be recovered.

## Start a request

Create a new Harklight session handle for the request and keep the exact value
printed by the CLI:

```sh
harklight-agent send --session new --sender-id app:example \
  --route claude-code \
  --title "Choose the next step" --body "Pick one option or write a note."
```

The route is required when creating the handle and becomes part of that
handle's server support record. For another request using the same handle,
omit `--route` so the route record stays attached to that handle.

If the harness exposes its exact session or thread ID through a documented
interface, it may be supplied as `--vendor-session-id`. Do not infer one from
a display name or use it as proof that the harness received a turn.

Arm a `listen --next` waiter in the same turn as every successful send. For a
local interactive Claude Code terminal, run it as a background Bash command so
it remains held on the Harklight SSE stream while the session is idle:

```sh
harklight-agent listen --next --session "$HARKLIGHT_SESSION" \
  --session-context interactive \
  --claude-code-version "$(claude --version)" \
  --background-tasks-enabled
```

Do not substitute `listen --follow` for this Claude Code waiter pattern. A
follow process writes continuously, while the blocking mode prints one pending
entry and exits so the session can handle it and arm the next waiter.

## Handle each waiter result

The waiter writes one JSON line. A change notice is not a conversation entry;
the CLI fetches the authoritative feed and journals each entry before printing
it. Read `entry.kind`, `entry.author`, the exact request identity, revision,
`event_id`, `delivered`, and `acted` fields. Do not classify an entry by its
text.

Run `ack` before acting on a newly printed person turn or terminal entry:

```sh
harklight-agent ack --session "$HARKLIGHT_SESSION" --event "$HARKLIGHT_EVENT"
```

The acknowledged response is evidence that the originating session received
that exact event. SSE attachment, feed fetch, journal write, stdout output or
background-task queue acceptance alone do not mark it delivered. If the
entry was already delivered and acted, do not act on it again.

The CLI records the listening support check at arm. Claude Code listening
requires an interactive terminal, Claude Code 2.1.234 or later, and background
tasks enabled. If a prerequisite is missing, the CLI records `unsupported` and
attaches no stream. Restore the prerequisite and rearm this same handle; do not
create another request or session. The support record is capability metadata,
not evidence of handoff.

For an entry with `files`, inspect each file's `path`, `transfer` and `use`
fields. The CLI saves and verifies each inline or uploaded file before it prints the event.
After `ack`, the response records each file as transferred or not transferred.
Do not treat a server transfer receipt as evidence that this session read or
used the file. For each file this session can inspect, report its own outcome:

```sh
harklight-agent file-use --session "$HARKLIGHT_SESSION" \
  --event "$HARKLIGHT_EVENT" --file "plan.pdf" --used
```

When it cannot use a transferred file, report that separately:

```sh
harklight-agent file-use --session "$HARKLIGHT_SESSION" \
  --event "$HARKLIGHT_EVENT" --file "diagram.png" --unusable \
  --reason "This session cannot inspect this format."
```

Do not report use for a file the CLI marked `not_transferred`. The use report is
the originating agent's statement; it does not prove that the vendor read or
acted on the bytes.

Client upload cleanup belongs to an explicit person navigation event. Do not
run `discard-uploads` because an agent process exits, an SSE connection drops,
a session ends, or a view rebuilds. The client preserves its text draft and
marks discarded or fenced upload references unready until the person selects
the files again. This is separate from the server's one-hour inactivity sweep
for unsubmitted agent uploads.

For a discussion turn, check the exact request and revision, then reply naming
the event that prompted the reply:

```sh
harklight-agent reply --session "$HARKLIGHT_SESSION" \
  --request "$HARKLIGHT_REQUEST" --in-reply-to "$HARKLIGHT_EVENT" \
  --body "The request is complete."
```

Use `revise` with the request's current base revision when changing the
request. Use `withdraw` only when the origin is intentionally ending an open
request; a reply does not close it.

For an `answered` terminal entry, apply the answer at most once and report the
result against that terminal event:

```sh
harklight-agent result --session "$HARKLIGHT_SESSION" \
  --request "$HARKLIGHT_REQUEST" --in-reply-to "$HARKLIGHT_EVENT" \
  --status applied
```

If an answered entry is re-offered with `delivered:true` and `acted:false`, first
check whether the earlier effect took hold. If it is present, report `applied`
without applying the answer again. If it is absent, apply the answer once and
report `applied` or `refused`. If the effect is uncertain, do not apply the
answer again or report `applied`; report `received` and say that the outcome
needs checking. Keep the original event, request and revision identities in the
result.

For a re-offered discussion turn, first retry any operation whose outcome is
unknown with its original `op_id`. If the earlier reply committed, do not post
another one. If it did not commit and the turn asks for no action outside
Harklight, reply normally. If it asks for an outside action, check whether that
action already took effect before doing it again: when present, do not repeat
it and reply that it is done; when absent, perform it once and reply with the
outcome; when uncertain, do not repeat it and reply that the outcome is
unconfirmed and needs checking. If you cannot tell whether the action had
started, treat the result as uncertain. Never claim an action completed when
its outcome cannot be determined.

Use `--status refused` when an answered-entry effect cannot be applied.
For `cleared`, `expired` or `superseded` terminal entries, acknowledge and stop
work on that request. They are not approval and do not receive a result.
An origin-authored `retracted` terminal is retained as request history and is
not emitted as an actionable prompt. Retraction ends the open request: do not
answer it, report a result, resume work against it, or treat it as a request to
reopen. A revision keeps the same request identity but advances its revision;
do not use a draft or action bound to an older revision after an update.

If the CLI reports an unknown operation outcome, preserve the UUIDv4 `op_id`
and retry that operation with the same key. Do not create a second operation to
guess whether the first one committed. The CLI reconciles persisted operations
and feed entries when the listener reconnects.

At the beginning of each later Claude Code turn, check the exact handle:

```sh
harklight-agent status --session "$HARKLIGHT_SESSION"
```

If the request is open or an entry is pending and there is no active listener,
start another background `listen --next` waiter in this same session. Continue
until the listener prints `{"kind":"idle","open_requests":0}`. Connection
loss, process exit and the idle line do not themselves close a request; only a
server terminal entry does.

## Availability and supported routes

Claude Code's local interactive background Bash commands can continue after a
final response, but may stop when the session exits, output exceeds 5 GB, or
macOS/Linux reports critical memory pressure after an idle period. The
background-completion handoff into the same session remains an unverified
assumption here: this work has not confirmed it with a real session. Do not claim that a
running SSE listener or printed line proves Claude Code received a turn. If a
completion notification is not delivered, treat the turn as queued and use the
next turn of the same session to rearm; never replace it with timer polling or
another agent. See the official
[background command reference](https://code.claude.com/docs/en/tools-reference#background-commands)
and [interactive mode reference](https://code.claude.com/docs/en/interactive-mode#background-bash-commands).

Codex listening is not supported yet: it needs a confirmed real Codex session
result first. Do not call `codex queue` or claim that a Codex session received a turn. `status` and
`listen` report that boundary; sending remains available.

A harness that is neither Claude Code nor Codex can declare its own continuous
listener by minting the handle with `--route other --controller` on `send` and
arming it with `listen --follow --controller`. Declaring `--controller` is that
harness's own statement that it runs and supervises the `--follow` process;
it is not a stronger delivery guarantee than Claude Code or Codex get. The CLI
refuses `--controller` without those companion flags before any network call.

Use `--file` with `send`, `reply` or `revise` only when the origin reports file
limits. The CLI uses the published decoded inline allowance and uploads any
remaining files as exact bytes, bound to the same operation, request and
revision. The receiver fetches each uploaded file, verifies its size and
SHA-256, writes the exact bytes, then reports its transfer outcome in `ack`.
The CLI submits the complete entry or none of it. If the published cap is
exceeded, upload fails, or the final JSON request exceeds its existing bound,
keep the draft and selected files; do not send only the text or split an
attachment. A retry after an unknown upload response reuses the same operation
key and staged bytes. The server publishes `none` for self-hosted upload with
no product-imposed cap or its configured finite SaaS cap; the CLI does not
choose a cap.

# Harklight protocol 0.2 shared messaging

This document defines the agreed Step 1 wire contract. It incorporates the
client contract approval, the approved design, and the delivery owner’s
subsequent rulings on organization affiliation, group posting and chat.

## Capabilities and connection scope

Authenticated `GET /v2/me` uses the existing Me resource for person and Agent
connections. `features.shared_messages` and `features.groups` are booleans.
Omit an unavailable feature; absence means false. Groups require shared
messages and use the same opt-in. When shared messaging is available, Me
adds `principal.organization_ids: OpaqueId[]`, the authenticated person's
own affiliations only. There is no organization directory. Capable connections send
`Harklight-Features: shared_messages` on each HTTP request, including SSE
reconnects. For person `GET /v2/changes` SSE only, EventSource may instead
send `features=shared_messages&continuation=<opaque token>` with the
existing `after`. Native Last-Event-ID retains precedence. Equivalent
header and query values normalize together. Conflicting values or duplicate
capability query parameters return `400 invalid_field`. JSON and other
routes continue to use headers. An explicit opt-in when the feature is
unavailable, or a shared write without opt-in, returns
`400 unsupported`. Older servers reject the extra strict write members;
clients check discovery before sending and report unsupported without trying
an individual send. Disabled groups return 404 for group routes and selectors.

Without opt-in, snapshots, direct item/entry/revision/file reads, feed
projections, counts and unread calculations exclude shared items. A shared-only
locator returns 404. Opt-in scopes page tokens and feed continuations to the authenticated
principal, feed route, normalized filters and capability set. Shared-enabled
person attention snapshots and shared-enabled Agent delivery-baseline headers
add `continuation: OpaqueId`, issued for the baseline’s `rev`. The token
identifies the scope and the earliest usable feed position; it is not an
event ID or a separate advancing checkpoint. Fetch a fresh baseline to
enable or disable capability. A client downgrading to legacy mode clears
both its token and saved cursor, even though the legacy wire format still
accepts bare numeric cursors.

For an opted-in JSON batch or SSE connection, send the token in
`Harklight-Continuation` (or the person SSE query form above) with the
existing `after` or `Last-Event-ID`.
On SSE the latter retains precedence. A missing, malformed, expired or
scope-mismatched token, a position below that token’s baseline, or a
shared token supplied without opt-in returns HTTP `410 cursor_expired`
before opening SSE. Never deliver or advance a cursor on that failure.
An already open SSE stream whose continuation expires emits the existing
id-less `closing` with reason `cursor_expired`. Change no SSE event IDs.
Shared snapshot and baseline pagination tokens enforce the same scope
and return 410 on mismatch. A correctly scoped cursor that the log has
compacted also returns 410 under the existing rules. The Server advances
over filtered rows in the same account log. Existing individual sends and individual read and feed
projections retain their current shapes.

On `PUT /v2/devices/me/push`, add optional
`features: ["shared_messages"]` to the existing binding body. The Server
stores capabilities for that authenticated device and binding revision only.
An update or rebind without the member sets no capabilities. Deletion clears
the binding. Other devices and browser requests cannot change this device's
eligibility. Preserve existing registration preconditions and replay; the optional
features member is part of the request bound to that revision. Project
badge counts, alert candidates, target and locator from the binding’s
visible items, including mixed and badge-only pushes. A legacy binding
never receives a shared target or a count increment attributable to one.
Enforce the binding revision and visibility again at delivery.

Use `Me.server.id` to scope identities across Server instances. Bind public
person profiles to the existing person ID. Owner-authorized
`PATCH /v2/me/profile` accepts exactly `{display_name:string,handle:string}`
and returns the updated Me with `principal.profile`. Both strings are
nonempty; handles use lowercase ASCII letters, digits, hyphens or underscores,
with Server-wide uniqueness. Each person has one handle per Server across
all their organizations. Handle uniqueness establishes identity and grants
no addressing authority; naming remains organization-scoped. Existing
request-size limits apply. A legacy
person may have a null handle and a Server-owned display fallback. Expose
profiles only to the person and through authorized conversation projections.
No global directory lookup is introduced.

## Send and revision

An opted-in `POST /v2/agent/messages` extends SendMessageBody with:

- `recipients` (required for this path),
  `{kind:"people",people:[OpaqueId,...],organization_id?:OpaqueId}`.
  The people list is nonempty. These are person IDs, including in
  `answer_policy.people`. Handles are not input selectors.
- `answer_policy` (optional, requests only), one of `{kind:"first"}`,
  `{kind:"named",people:[OpaqueId,...]}`, or `{kind:"everyone"}`.
  Default to first for requests. A named policy has a nonempty list and
  every named decider must be reached. Tightbeam decisions use exactly one
  named decider. The relay supplies the policy without a Tightbeam-specific
  identity field on the Server.

Each selector and policy is a closed object. Send admits one selector only.
Normalize duplicates to the first appearance before delivery; original body
digest still controls idempotency. An absent recipients field selects the existing
individual send shape; answer_policy alone is invalid. The Server stores explicit person-to-organization affiliations. The
authenticated agent’s existing registration and send right for the sender ID
authorize recipients when the sender and every recipient share one
organization on that Server. Resolve and record one organization for each
shared send, and check all affiliations in the send transaction. A send
spanning multiple organizations is refused. If a named send omits organization_id and exactly one organization is
common to the sender and all recipients, bind it. If several are common,
return `409 organization_required` with `retryable:false` and
`organization_ids: OpaqueId[]` listing only the common choices, in opaque
ID order. The caller can retry with a chosen organization ID. If none are
common, or a supplied organization is not common to all, return the
atomic `403 recipient_refused`. A group has one canonical organization.
Any unknown or out-of-organization recipient returns
`403 recipient_refused` with the standard error fields and
`retryable:false`. The entire send fails before delivery, message creation,
receipt or any other side effect. There are no per-recipient outcomes
in this error. Once all targets are authorized, report delivery availability
as `{person_id:OpaqueId,status:"reached"|"unreachable"}`.

On Hosted, business membership established by the seats component is
authoritative. A person may belong to several organizations. Over the
authenticated Hosted loopback interface,
`PUT /organizations/{business_ref}/members` accepts
`{person_ids:[OpaqueId,...]}` and idempotently replaces accepted member and
admin affiliations for that business. Affiliation never enrolls anyone into a group. Hosted sends the full set after changes
and during reconciliation. `DELETE /organizations/{business_ref}` removes
that business’s affiliations. Business type does not change affiliation;
existing seat and entitlement rules still govern delivery. Without that affiliation, a Hosted person
cannot use the shared send path. On a self-hosted Server, its owner manages
the one default organization with owner tooling. People the owner sets up
join that organization by default, with owner-controlled add and remove.
The Server uses this explicit relation and never matches names or emails.
The feature is available only where authoritative affiliation is enforced
and the authenticated person belongs to an organization. Ineligible
connections receive `400 unsupported` on shared sends. Legacy individual
account boundaries remain. Removing affiliation immediately ends live access to that organization’s
shared messages and groups. Serialize the removal with shared writes and
freeze authorized history, including files and Seen, through that cutoff.
Block subsequent replies, answers and updates, cancel waiting pushes and
future shared Shows, and emit only the local projection transition needed
to make the frozen view read-only.

With zero reached return `422 no_recipients` with the standard error envelope, retryable false, plus those outcomes for an
explicit people selector. If a named decider is otherwise eligible but
unreachable, return `422 decider_unreachable` with the same outcome fields.
Failure creates no message, rows, notifications or success receipt. A group
selector follows the disabled-feature rules above.

A successful named send returns `{rev:Position,message:SharedSentMessage,
recipients:[RecipientOutcome]}`. For Step 1, a shared send refuses any `supersedes` member, even an empty
array. Individual sends cannot supersede shared IDs. A shared result
carries exactly `rev,message,recipients`, with no `supersedes_warnings`. Reuse
the existing idempotency scope and receipt retention for Agent writes:
same authenticated sender scope, key and digest returns the original
committed result, including the bound organization, without recomputing it;
a changed body is 409. Replay uses the original commit
even after membership changes. Current authentication and permission checks
still apply. Never resolve a selector again to replay a committed success.

Sender writes reuse Agent message routes with shared ID as item_id.
Only an authenticated principal in the original sender authority may revise,
retract, post as sender, clear remaining obligations or close chat. A
different principal receives 404 for inaccessible locators. Recipient chat
posts use their own local item and authenticated identity; they cannot
access sender mutations. Agent revise reuses its body and result with
`If-Match` checked against the shared request revision in the commit.
The other shared Agent routes use the existing idempotency header and
receipt behavior, including chat close. Exact replay checks the stored
successful receipt before reevaluating mutable revision or room state.
A different digest returns 409. Retract and close serialize against
revision, participant writes and each other. An already retracted or
closed room uses the respective existing 409 typed error for a fresh write
that requires an open room. Reply does not require a new revision
precondition; stamp the revision at commit. The sender may explicitly close chat using
`POST /v2/agent/messages/{id}/chat/close` with an empty body and
`{rev,message}` result. Closure stops future chat and membership changes
for that conversation, preserving authorized history. Retraction closes
chat and withdraws for active recipients. Settlement alone leaves chat and
sender delivery open. A targeted agent reply sets `notify_person_id` to
one active participant; all active participants receive the same entry and
only the selected participant receives an alert. Standard replies use
regular eligibility for all active recipients.

## Resource extensions and examples

Opaque shared_id identifies both the message and its one chat. A person's
item_id is their local locator. Person entry_id is their local paging and
read-position locator; shared_entry_id identifies the logical entry across
projections. Sender entry_id is the shared_entry_id. Shared answer_id,
Seen event_id and chat shared_entry_id are distinct stable identifiers.
Their content is attributed to `Speaker = {kind:"person"|"sender"|"server",
id:OpaqueId,display_name:string,handle?:string|null}`, scoped by Me.server.id.
The sender ID represents the authenticated sender scope and sender identity.
Retain author roles person, origin and server. Use identity for authority
and attribution; a mutable profile string confers no authority. A shared request uses null for answer_policy only on
an FYI; shared FYIs record Seen and accept no answer mutation.

These are exact schema extensions. Types reuse the v2 OpaqueId, Rev,
Position, TimeMs and resource definitions. Fields written with a trailing
question mark are optional; all other members shown for the added objects
are required. Existing fields retain their definitions except where a
shared behavior is specified.

```text
SharedMember = {person: Speaker, seen_revision: positive integer | null,
                seen_at_ms: TimeMs | null}
SharedItem = Item + {shared: {shared_id: OpaqueId, chat_writable: boolean,
             can_answer: boolean, answer_policy: AnswerPolicy | null,
             members: SharedMember[], answers: PersonAnswer[]}}
SharedSentMessage = SentMessage + {shared: {shared_id: OpaqueId, chat_open: boolean,
                    answer_policy: AnswerPolicy | null, answers: AgentAnswer[],
                    organization_id: OpaqueId}}
SharedEntry = Entry + {shared_entry_id: OpaqueId, speaker: Speaker}
SharedAgentEntry = AgentEntry + {shared_entry_id: OpaqueId, speaker: Speaker}
PersonAnswerTerminal = Terminal + {answer_id: OpaqueId, speaker: Speaker,
                       by_this_person: boolean}
AgentAnswerTerminal = AgentTerminal + {answer_id: OpaqueId, speaker: Speaker}
PersonAnswer = {person_id: OpaqueId, terminal: PersonAnswerTerminal}
AgentAnswer = {person_id: OpaqueId, terminal: AgentAnswerTerminal}
SharedAnswerConflict = Error + {terminal: PersonAnswerTerminal}
SharedSeenChange = {rev: Rev, kind: "seen", data: {item_id: OpaqueId,
                    event_id: OpaqueId, request_revision: positive integer,
                    at_ms: TimeMs, speaker: Speaker}}
SharedAnswerChange = {rev: Rev, kind: "answer", data: {item_id: OpaqueId,
                      answer_id: OpaqueId, request_revision: positive integer,
                      person_id: OpaqueId, terminal: AgentAnswerTerminal}}
```

Person and Agent answer terminal extensions apply only to state answered.
Non-answer terminals use the existing schema and carry no answer identity.
In a shared advisory add `people: Speaker[]` for the subjects of a grouped
personal reminder. A shared send request uses the existing sender, session,
message and file_ids fields with the two additions defined above. Shared
successful top-level write envelopes are closed. In addition to SharedSendResult
and the answer receipt result, they are the following.

```text
SharedSendResult = {rev: Position, message: SharedSentMessage, recipients: RecipientOutcome[]}
SharedMessageResult = {rev: Position, message: SharedSentMessage}
SharedReviseResult = {rev: Position, message: SharedSentMessage, entry?: SharedAgentEntry}
SharedReplyResult = {rev: Position, entry: SharedAgentEntry}
SharedPersonItemResult = {rev: Position, item: SharedItem}
SharedPersonReplyResult = existing person WriteResult with entry: SharedEntry
                          and with item: SharedItem if returned
```

An opted-in connection may decode both legacy and extended payloads.
An opted-out connection receives no extension fields or shared kinds,
including when reading individual outcomes on an upgraded Server.

Person Item example, after Dana answered. Examples use illustrative IDs.

```json
{
  "item_id":"local_sam","rev":41,"sender":"example:release","request_revision":2,
  "message":{"title":"Launch date","created_at":100},"files":[],"seen":true,
  "chat":{"started":true,"available":true,"read_position":"local_e2","unread_replies":1},
  "rank":{"key":"a","section":"resolved"},
  "terminal":{"state":"answered","at_ms":300,"answer":{"kind":"choice","id":"thu","label":"Thursday"},
    "by_this_person":false,"answer_id":"answer_a",
    "speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}},
  "shared":{"shared_id":"shared_a","chat_writable":true,"can_answer":false,
    "answer_policy":{"kind":"first"},"answers":[],
    "members":[
      {"person":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"},
       "seen_revision":2,"seen_at_ms":290},
      {"person":{"kind":"person","id":"person_s","display_name":"Sam","handle":"sam"},
       "seen_revision":2,"seen_at_ms":295}
    ]}
}
```

Person Entry and sender AgentEntry for the same turn:

```json
{"entry_id":"local_e3","shared_entry_id":"entry_e3","item_id":"local_sam","rev":42,
 "position":17,"kind":"reply","author":"person","request_revision":2,"at_ms":310,"text":"Works for me",
 "speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}
```

```json
{"entry_id":"entry_e3","shared_entry_id":"entry_e3","item_id":"shared_a","rev":82,
 "position":12,"kind":"reply","author":"person","request_revision":2,"at_ms":310,"text":"Works for me",
 "speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}
```

Sender SentMessage after the answer:

```json
{"item_id":"shared_a","rev":80,"sender":"example:release","revision":2,
 "message":{"title":"Launch date","created_at":100},"files":[],"seen":true,
 "shared":{"shared_id":"shared_a","answer_policy":{"kind":"first"},"chat_open":true,"answers":[],"organization_id":"org_a"},
 "terminal":{"state":"answered","at_ms":300,"answer":"thu","outcome":{"kind":"choice","id":"thu","label":"Thursday"},
   "answer_id":"answer_a","speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}}
```

For shared sender projections `seen` means at least one active recipient
has Seen the substantive revision. Sender reads do not return a per-person
Seen roster in Step 1; opt-in Seen events identify the person. Answer and Seen event projections
identify their subject as specified below.

## Seen, answers and durable delivery

Require `{request_revision:positive integer}` on shared Seen writes.
Reject stale request revisions with the existing stale-request error. Store
one Seen fact per person per substantive revision, carry it forward across
progress-only revisions as in the individual model, and emit no new Seen
event on carry-forward or retry. Display both fields as null when unseen.
Publish Seen changes to active person projections as item upserts. A carried receipt projects the current revision with the original timestamp. Read
position is private to the viewing account and uses the existing write.
Count unread chat from the sender and other people, excluding the current
person's own words. `chat.last_origin_reply`, `chat.reply_rank_key` and
`chat.reply_group` retain the existing origin-only Replied semantics.
An authorized read-position write may name the viewer's local entry_id for
another person's visible reply in that same shared conversation.

Serialize membership, current revision, eligibility and answer commit in
one write. A stale revision receives the existing stale-request error.
A current but unauthorized answer receives 403; an otherwise eligible
loser under first/named receives the existing 409 already_answered with
the effective `terminal` added to the error, including the actual speaker.
The successful write returns the existing `{rev,item}` shape. Under first
or named, effective terminal is the shared settlement and by_this_person is
computed per viewer. Under everyone, keep one answer per subject in `shared.answers`. Each is
`{person_id:OpaqueId,terminal:PersonAnswerTerminal}` in person projections
and `{person_id:OpaqueId,terminal:AgentAnswerTerminal}` in Agent projections. The terminal has
viewer-relative by_this_person on the person side and raw answer plus outcome
on the sender side as applicable. Before closure, the sender has no top-level
terminal, and a person sees their own answer terminal, or null if pending.
A sender lifecycle clear ends remaining obligations and adds a sender
terminal with state cleared and no answer. Previously answered people keep
their own answer terminal; previously pending people see the clear with
by_this_person false. Both sides keep `shared.answers`. Later Chat changes
neither projection. Chat close by itself ends Chat only. An eligible person joining an open request gains an obligation.
Leaving ends an outstanding obligation and preserves the answer. Everyone mode may use an existing configured default to record an answer
with a system speaker and the subject person ID. It settles only that subject.
A default has by_this_person false and terminal outcome.kind default.
Speaker identity and request_revision are fixed at event admission and
retained across feed and pending recovery. Defer shared automatic defaults in
first and named modes. No default may settle a named decider's obligation.
Individual default behavior is preserved. Reminders
and bounces use personal attention state and stop for settled obligations.

Keep person feed kinds and envelopes. Roster, Seen and effective terminal
refreshes are item upserts; a chat reply is an entry upsert and changes
attention counts where applicable. On the opted-in Agent feed, keep existing
message, entry and advisory changes. Add two shared-only kinds for logical
events without a unique legacy resource:

```json
{"rev":79,"kind":"seen","data":{"item_id":"shared_a","event_id":"seen_s2",
 "request_revision":2,"at_ms":295,"speaker":{"kind":"person","id":"person_s","display_name":"Sam","handle":"sam"}}}
```

```json
{"rev":81,"kind":"answer","data":{"item_id":"shared_a","answer_id":"answer_a","request_revision":2,"person_id":"person_d",
 "terminal":{"state":"answered","at_ms":300,"answer":"thu","outcome":{"kind":"choice","id":"thu","label":"Thursday"},
 "answer_id":"answer_a","speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}}}
```

Each shared reply and answer becomes one logical sender delivery, independent
of person fan-out. A message upsert may also show settlement, but shared
clients submit answer work only from the answer change. Distinct changes have
distinct feed revisions (example changes are separate from their resource
projections). A shared chat receipt uses the existing entries receipt route.
Use `POST /v2/agent/messages/{id}/answers/{answer_id}/receipt` with the
existing AgentReceiptBody and exactly
`{rev,status,stage_outcome,terminal}` for a shared answer, including personal
defaults. Both stage_outcome and status retain their existing enum values.
Only the originating authorized Agent principal may acknowledge an admitted
answer on that shared message, after delivery selection via feed or baseline.
A not-yet-delivered authorized answer gets 409 not_answered; missing or
inaccessible answer IDs return 404. A receipt is per answer ID and never
acknowledges adjacent chat or other answers. The existing ack stage ordering,
handoff transition rules and operation replay apply. Same-key, same-body
replay returns the committed receipt; a new key with the already recorded
outcome reports already_recorded. Selection after a lost receipt response
uses the same answer ID and reconciles current delivery facts. A person Chat reply and a shared
answer enter the opted-in bounded delivery baseline; the latter uses the
answer change shape. The exact shared baseline header is
`{rev:Position,next_page:OpaqueId,answers:"all",shared_events:true,continuation:OpaqueId}`.
The page is `{rev:Position,changes:[AgentChange|SharedAnswerChange],next_page?:OpaqueId}`
with the header rev. Entries and answers carry stable IDs and their own request revisions.
Shared AnswerChange data.person_id identifies the subject, including for defaults. The page rev is the feed checkpoint; a resource's
rev or entry position does not replace it. The token is omitted from
legacy headers, as is shared_events. For example, a preparation response is

```json
{"rev":82,"next_page":"page_one","answers":"all","shared_events":true,"continuation":"feed_scope"}
```

With one pending answer the final page can be:

```json
{"rev":82,"changes":[{"rev":81,"kind":"answer","data":{"item_id":"shared_a",
 "answer_id":"answer_a","request_revision":2,"person_id":"person_d",
 "terminal":{"state":"answered","at_ms":300,"answer":"thu",
 "outcome":{"kind":"choice","id":"thu","label":"Thursday"},"answer_id":"answer_a",
 "speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}}}]}
```

With no pending work the final page is `{"rev":82,"changes":[]}`.
A losing first-answer response, for a different participant, is

```json
{"error":"already_answered","retryable":false,"request_id":"request_loser",
 "terminal":{"state":"answered","at_ms":300,"answer":{"kind":"choice","id":"thu","label":"Thursday"},
 "by_this_person":false,"answer_id":"answer_a",
 "speaker":{"kind":"person","id":"person_d","display_name":"Dana","handle":"dana"}}}
``` Apply pending work and checkpoints using the established semantics.
Ordinary historical reads do not submit turns as new work. Roster and Seen
refreshes are not pending chat or answer deliveries. Seen or roster recovery
uses current authorized projections; if an integration cursor expires,
historical Seen transitions are not guaranteed by the pending baseline.
Reminder advisories aggregate due people by message, revision and reason
within an evaluation pass; include those people and retain one advisory ID.

Dedupe across transport retry by Server identity and typed stable event
identity within one continuous store history. A restore can diverge from
external effects already applied. Operators restart consumers from a fresh
baseline after restoring, retaining any delivered-event audit needed to
reconcile prior side effects. A Server identity alone does not certify
continuity. Apply each baseline's scope and cursor independently.

## Groups and history

Group routes and selectors remain disabled during Step 1. The group wire
contract belongs to the groups slice. The following rulings constrain it.

People manage self-hosted groups through accepted invitations that explain
profile, reply and Seen visibility. Members may mute or leave. Agents with
ordinary registration and send rights may message a group in their own
organization about any work, without a separate posting grant. Agents cannot
manage groups.

Hosted defaults to disabled through seats and Terms/Privacy readiness.
Its component owns business eligibility and synchronizes only the explicitly
opted-in subset, replacing the roster whenever a person leaves the
business or group. Joining and leaving are the person’s own actions in the
Hosted component. The join choice explains that members can see their name,
replies and current-revision Seen. An admin cannot opt in anyone else. A
business seat alone does not join the group. Mute is a personal Server-side
notification setting and does not affect membership, Seen or content
delivery. The Hosted roster, consent and mute routes are deferred. Unknown
and out-of-organization groups will return the same 404.

Joiners to an open group conversation receive current message and chat
history. Departures freeze an authorized read-only view including content,
files, chat and Seen through departure and suppress waiting pushes. The
same cutoff will govern history pages and all direct locators. Future
shared Show requires an explicit authorized viewer and their exact local
item, with delivery canceled after departure. Step 1 adds no shared
Focus or Show behavior. Membership grants no screen authority.

## Conformance and release

- Mixed devices on the same account: only opted-in snapshots and feed list
  shared items and counts, only the opted-in push binding receives alerts,
  and the old connection gets 404 for shared item, revision, entry and file
  locators. Clear or rebind a token and confirm capability is reset.
- Send [Dana, Dana, Sam] with Sam unreachable: one shared message, one
  reachable person row, two outcomes. Replay after Sam becomes reachable
  returns the same committed response and adds no row. A zero-reached send
  leaves no rows and a changed body on an admitted key conflicts. A send
  mixing an eligible person and an out-of-organization ID fails atomically.
- Race two eligible answers in the same revision: exactly one settlement
  and sender event. Race an answer against revision and ensure only the
  accepted revision settles. An unauthorized named-decider attempt may still
  chat. A later entitled chat entry remains deliverable after answer.
- Remove a member between revision 2 and 3 and between two chat entries.
  In the groups slice, check snapshot, historical paging, files, Seen, feed and waiting push all
  respect departure. Confirm a new accepted member receives revision 3
  and all retained chat. Mute affects notifications only.
- Retry a lost feed page and recover with an expired cursor. One logical
  chat turn and answer survive under their stable IDs. A roster update
  creates no pending work. Verify independent device/agent checkpoints
  and replayed individual behavior. Lose the winning answer response and its
  sender receipt response, replay the receipt, then continue reading Chat.
  Test strict legacy result/baseline decoders with no opt-in and mixed
  push bindings with per-binding badge counts. Test invalid capability
  continuations in JSON and SSE before consuming any events.
- FORMAT32 and backup/restore are a separate Server storage slice. Rehearse
  migration and rollback on an isolated representative FORMAT31 copy,
  including rejection by the old loader and documented loss of later writes.
  Production enablement and installation are separate from this contract.

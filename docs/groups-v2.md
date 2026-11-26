# Harklight Server groups v2 contract r3

This revision incorporates the client review and Hosted provisioning
readback art_2b95a06d. Hosted can launch with a default group under its own
stable reference. The API supports multiple groups within a business.

The existing shared-message capability, strict legacy projections and revision
rules remain in force. Groups additionally require `harklight-features:
shared_messages,groups`. Groups are advertised as `features.groups:true` in
authenticated person and Agent `GET /v2/me` when the group gate is enabled
and the identity has an authoritative organization. Disabled group routes
and group selectors return the normal 404. Enabled group routes with a
missing group opt-in also return 404. Explicitly requesting the unavailable
capability on a shared baseline or feed returns the existing 400
`unsupported` response. Group visibility is part of the continuation scope.
People who use shared messages alone never receive group content.
For same-origin person SSE at `GET /v2/changes`, EventSource supplies
`features=shared_messages,groups` (URL-encoded as needed) and
`continuation=<opaque token>`. JSON and other routes continue to use headers.
Normalize equivalent flag sets across header and query, including order.
Conflicting values and duplicate query parameters return 400 `invalid_field`.
An explicit unavailable opt-in returns 400 `unsupported`. Capability changes
require a fresh baseline. Last-Event-ID precedence and 410 recovery remain
as specified in the shared messaging v2 contract.

## Self-hosted management

Person routes use the existing authenticated person principal and existing
idempotency/revision result conventions for writes. Each write requires the existing Idempotency-Key
UUID. Within the authenticated person scope the same key, method, path and
body returns the committed result, even after the invitation was consumed or
membership changed. A changed operation or body returns 409
`idempotency_conflict`. Only successful results commit a receipt. All write
preconditions below apply to a fresh operation, at commit, and use current
authority. These routes have no If-Match precondition and an If-Match header
is rejected with 400 `invalid_field`. Agent credentials cannot
enter these routes. All IDs are opaque. Group names are unique within their
organization using trimmed, case-insensitive comparison. The displayed
name preserves the entered casing. A rename preserves the group ID.
Unknown or inaccessible groups return the same 404.

- `GET /v2/groups` returns `{rev,groups:[Group],invitations:[Invitation]}`.
  People see groups they belong to or manage, and only their own pending
  invitations. `Group = {id,organization_id,name,manager:Speaker|null,members:
  [Speaker],member:bool,muted:bool}`. The roster contains current accepted
  members. The `member` and `muted` values describe the authenticated viewer,
  and `muted` is false for a nonmember. Pending invitees do not receive a group
  roster. A manager who leaves retains roster management while affiliated.
  Their former membership grants only frozen conversation history.
- `POST /v2/groups` with `{name}` returns `{rev,group:Group}`. Creation
  records both management and the creator's affirmative choice to join.
  The person can leave without losing the management role. Membership
  alone confers no management authority.
- `PATCH /v2/groups/{id}` with `{name}` returns `{rev,group:Group}`.
  Only the manager may rename. Repeating the current name succeeds without a revision change. A conflicting name returns 409
  `group_name_conflict`.
- `POST /v2/groups/{id}/invitations` with `{person_id}` returns
  `{rev,invitation:Invitation}` for the manager only. Target IDs are
  checked against the organization, and there is no person directory. A fresh invite operation for a pending
  target returns the existing invitation. Inviting an active member returns
  409 `already_member`. `Invitation =
  {id,group_id,group_name,invited_by:Speaker,person_id}`.
- `POST /v2/group-invitations/{id}/accept` with `{}` returns
  `{rev,group:Group}`. Only the named invitee may accept or decline a pending invitation. A stale or consumed invitation returns 404. Before
  submitting acceptance, the client explains that members can see the
  person's name, replies, answers and current-revision Seen. Declining via
  `POST /v2/group-invitations/{id}/decline` returns `{rev}`.
- `POST /v2/groups/{id}/leave` with `{}` returns `{rev}` and ends the
  caller's active membership. A fresh leave without membership returns 404. A person who leaves needs a fresh invitation to join.
- `PUT /v2/groups/{id}/mute` with `{muted:bool}` returns
  `{rev,group:Group}` for the member and affects only that person's
  notifications. A fresh mute without membership returns 404. An unchanged value succeeds without a revision change. The setting is private and does not affect attention,
  delivery, Seen or history.

Management changes are read through `GET /v2/groups`. An invitation is
not a shared conversation entry. Group sends and join/leave produce only
the existing opted-in shared item changes needed to update local
projections, never synthetic Chat or Answer events. Retained items describe
the group as of the latest authorized view. On departure, a single local
item change marks each affected conversation read-only.

## Agent send

The existing `POST /v2/agent/messages` accepts one closed recipient selector
`{kind:"group",group_id:OpaqueId}` or `{kind:"group",name:string}`.
An Agent with normal send rights can post to a known group ID in its
organization whether or not its person has joined the group. Self-hosted
Agents may resolve a name in their one organization. On Hosted the Agent
uses the opaque ID. `GET /v2/agent/groups` returns
`{rev,groups:[{id,organization_id,name}]}` for groups to which the
authenticated Agent's person has affirmatively opted in. This endpoint
provides neither a roster nor an organization-wide directory and does
not change posting authority. A nonmember who knows the ID can send,
and can recover its own minimal send receipt. The sender's shared
message/Chat access is scoped to the conversation it sent and contains
no group roster; members see the participant projection. There is no
additional posting grant. Agent
credentials cannot manage a group. Resolve the group, sender authority
and recipient roster in the send transaction. A name resolves only
in the sender's local organization. Unknown or out-of-scope selectors
return 404. A group has one canonical organization.

Success uses `{rev,message:SharedSentMessage,recipients:{kind:"group",
group_id:OpaqueId}}`. Errors for zero reached or an unavailable named
decider omit per-person outcomes. The receipt does not return roster
names, IDs or counts. The same operation returns its original receipt
after membership, group name or availability changes. Shared read
projections expose the group's opaque ID and name. Agent message reads
and feed events keep individually attributed speakers and answers
according to the existing shared-message contract.

## History

When a group has open Chat, a newly accepted member receives the current
content and prior Chat. The initial local Seen and read position are
personal. First and named settlement stays settled. Everyone can ask
a joiner while the request itself is open. Leaving the group freezes the
read-only local projection, revisions, files and Seen through the commit
and stops later delivery, replies, answers and waiting notifications. A
person invited back rejoins the open conversation at its current revision,
with their own previously recorded answers retained. Closed or withdrawn
conversations do not follow later invitations. A page token issued before
departure cannot cross the departure cutoff. Rejoining produces one item
projection update and exposes earlier Chat via history under its original
entry IDs. It does not redeliver old entries as new work. Organization removal also
removes active group membership and cancels pending invitations.

## Hosted interface

A single Server setting defaults off in Hosted mode. While off, group
routes return 404 and discovery and content provide no group hints.
Hosted's seats, Terms and Privacy approvals govern enablement. Hosted
uses the existing authenticated loopback interface and owns invitations,
opt-in and leave. The Server person routes expose reads and mute but no
Hosted group or roster edits.

### Hosted roster version ordering, replay and deletion

On the existing authenticated loopback interface,
`PUT /organizations/{business_ref}/groups/{group_ref}` accepts exactly
`{name:string,person_ids:[OpaqueId,...],version:integer}`. The version is a
nonnegative integer. The stable opaque Hosted reference is scoped to the
business and is distinct from the Server's public group ID. The organization
PUT comes first. An empty roster is valid. Trim the name and normalize it
to Unicode NFC, preserving display casing. Compare rosters as sets of IDs.

Hosted assigns the monotonically increasing version to the authoritative
consent snapshot in the same ordering as its membership changes. Retries
resend the identical snapshot and version. Reconciliation reads the
authoritative snapshot and never assigns a newer version to a stale roster.

The Server compares and applies in one serialized transaction. In order:

1. A tombstoned business/group reference returns 410 `group_deleted` for
   every PUT, including a retry of a formerly committed version.
2. A lower version returns 409 `stale_version`.
3. An equal version with the same normalized name and roster returns the
   original committed 200 `{group_id,rev}` without reapplying membership.
   It still returns that result if affiliation revocation pruned a member
   from the effective roster. Compare against the original submitted
   snapshot, never the effective membership. An equal version with different
   normalized content returns 409 `version_conflict`.
4. A new version requires the current authoritative organization and checks
   every proposed member against it. A missing organization gives 404.
   Any proposed person outside the organization gives 422 `not_affiliated`.
   Refused replacements leave version, roster and history untouched.
   Success commits the source version, normalized snapshot, effective
   membership and original 200 `{group_id,rev}`. The returned `rev` is the
   Server's positive group roster revision, advanced on each newly applied
   version. Rename preserves the group ID.

`DELETE /organizations/{business_ref}/groups/{group_ref}` returns 204. It
freezes authorized group history, removes active membership and durably
tombstones the reference. An identical DELETE returns 204 with no further
effect. Organization DELETE cascades to all its group references and
retains their tombstones. Recreating a group requires a fresh group_ref,
a fresh group_id and fresh explicit consent. The new group inherits no
history. Affiliation removal prunes effective group membership immediately
and leaves the stored source version and original replay result intact.

Hosted sends only the explicitly opted-in subset, after each consent or
affiliation change and during reconciliation. A business affiliation alone
never joins a group. Person management, invitation and leave routes return
404 in Hosted mode. Person reads and mute remain scoped to active members,
and the Hosted group manager is null.

## Verification

Check an invitation never enrolls before acceptance, forwarding an
invite fails, Agent edit fails, disabled Hosted leaks no group metadata,
group receipts omit identities, and a repeated send does not re-expand
its roster. Exercise prior Chat on join, cutoff on leave, rejoin after
fresh consent, active request policies, private mute, mixed-capability
devices, and Hosted replacement interleaved with affiliation revocation.
Verify a delayed PUT after a newer leave, an identical replay after
affiliation pruning, an equal-version conflicting request, and retries
of both PUT and DELETE after the reference has been tombstoned.

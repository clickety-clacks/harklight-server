# Route B bounded replay protocol

Route B adds stateless reads for a current inbox page and one item's current
state and conversation history. It does not change legacy replay, the existing
snapshot paging protocol, acknowledgments, read marks, opened reports, or push
behavior.

All request frames use protocol version `1`. The server echoes a string `ref`
when one is supplied. Errors use the existing `{"v":1,"type":"error",
"reason":"..."}` envelope and may include `field`.

## Limits

Both `inbox_list` and `conversation_page` default to 20 rows or frames and have
a maximum of 100. A larger positive limit is served as 100. `conversation_page`
also accepts `limit: 0` to fetch only the current row. `inbox_list` requires a
positive limit.

## `inbox_list`

Request:

```json
{"v":1,"type":"inbox_list","ref":"inbox-1","limit":20,"cursor":"<opaque>"}
```

`limit` and `cursor` are optional. The cursor is an unpadded base64url encoding
of a JSON object containing the last row's `sender_id` and `message_id`. Clients
must treat it as opaque. It is a position in the current server order, not a
delivery or acknowledgment cursor.

Success:

```json
{"v":1,"type":"ok","ref":"inbox-1","rows":[],"seq":0,"conversation_seq":0,"view_rev":"...","unresolved_count":0,"next_cursor":"<opaque>"}
```

`rows` contains up to `limit` rows. `next_cursor` appears only when more rows
follow. `unresolved_count` is the account's current push-badge count, including
unresolved rows beyond this page. `seq`, `conversation_seq`, `view_rev`, rows,
and the count describe the same serialized server state.

Each current row contains `sender_id`, `message_id`, `origin`,
`request_revision`, the current `message`, current `files`, `opened`,
`chat_started`, `chat_read_position`, `latest_origin_reply_seq`, the item's
latest `conversation_seq`, and current `availability`. It includes
`session_handle`, `terminal`, `origin_result`, and `priority_view` when
applicable. A recipient device also receives `as` (`message` or `knock`).
`priority_view` carries the cached `score` and effective `due_at` when present.
Message members and file entries are passed through unchanged. Rows do not
embed earlier revisions, chat turns, or turn-level facts.

## `conversation_page`

Request:

```json
{"v":1,"type":"conversation_page","ref":"chat-1","sender_id":"app:example","message_id":"item-1","before":900,"limit":50}
```

`sender_id` and `message_id` are required. `before` and `limit` are optional.
The page contains the newest frames below `before` (or all frames when absent),
returned in ascending `conversation_seq` order.

Success:

```json
{"v":1,"type":"ok","ref":"chat-1","row":{},"frames":[],"older":false,"seq":0,"conversation_seq":0,"view_rev":"..."}
```

`row` has the same shape as an `inbox_list` row. Each frame uses the existing
`conversation_open` projection. `older` reports whether an earlier frame
exists. `seq`, `conversation_seq`, `view_rev`, the row, and frames describe the
same serialized server state. An item the requesting person cannot see returns
`not_found`. The read works before sync and does not change session readiness,
sync mode, device cursors, push state, or persisted state.

## Bounded `conversation_open`

The existing request accepts an optional `bounded` field:

```json
{"v":1,"type":"conversation_open","since":900,"bounded":true}
```

Without `bounded: true`, existing behavior is unchanged. The bounded form still
requires a prior legacy `resync`, catches up from `since`, records the existing
conversation-opened state, and streams subsequent conversation events. Its
`conversation_resynced` frame omits account-wide chat read positions and adds
the current `view_rev`. The session receives `view_changed` notices from this
point onward. The `view_rev` is read in the same serialized step that enables
those notices. Bounded launch sends `inbox_list`, then `resync` from its `seq`,
then this request from its `conversation_seq`; it does not send `view_get`.

Older servers answer `inbox_list` with `unknown_type` and do not change sync
mode, so a client can fall back to legacy replay on the same connection.

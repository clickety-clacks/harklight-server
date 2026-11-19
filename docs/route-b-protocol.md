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

## Connection roles

A device WebSocket may declare a role in its upgrade request, so one stream
connection can sync while separate request connections read and mutate without
waiting on it or closing it:

```text
GET /relay?role=stream HTTP/1.1
GET /relay?role=request HTTP/1.1
```

A connection that declares no role (`GET /relay`) is a legacy stream: its frame
set, replies, displacement and close behavior are unchanged. Any other target is
refused with `400 Bad Request`, which is how an older server answers a declared
role; a client can fall back to one legacy connection on that answer. Roles
apply to the device WebSocket only. The raw TLS transport is always legacy.

A declared role receives this frame before any other:

```json
{"v":1,"type":"connection_admitted","connection_id":"c_...","role":"request"}
```

`connection_id` is opaque and safe to log. The server logs it when the
connection opens, closes or has a frame refused.

| Role | Accepts | Refuses with `wrong_connection_role` |
|---|---|---|
| `stream` | `resync`, `ack`, `conversation_open`, `conversation_ack`, `inbox_start`, `inbox_page`, `inbox_reconcile`, `inbox_receipts`, `inbox_checkpoint_ack`, `inbox_attach`, `focus_report` | every other frame type, including `inbox_list`, `conversation_page`, `view_get` and every mutation |
| `request` | every other frame type: reads (`inbox_list`, `conversation_page`, `view_get`, `levels_get`) and mutations (`message_open`, `conversation_read`, `answer`, `discussion`, `levels_set`, `push_register` and the rest) | the stream frames above |

A refusal uses the existing error envelope and echoes `ref`. A request
connection never enters snapshot paging mode, holds no cursor, receives no
change notices and leaves the device's focus state alone, so the paging
checkpoint never refuses its mutations. Replies stay display-only and carry
`view_rev` as described above. Uploads use the separate upload endpoint and are
attached by a mutation on a request connection.

Opening a stream, declared or legacy, closes the device's earlier stream
connections. Opening a request connection closes nothing. Revoking or removing
the device closes every connection of the device.

Before the server closes a connection that declared a role, it sends a WebSocket
close frame whose reason is one of these tokens:

| Reason | Status | When |
|---|---|---|
| `superseded_by_new_stream` | 4409 | A newer stream of the same device opened |
| `device_revoked` | 4403 | The device was revoked or removed |
| `server_shutdown` | 1001 | The device listener stopped |
| `idle_timeout` | 4408 | Reserved; the server has no idle timeout today |
| `protocol_error` | 1002 | A frame could not be decoded |

Legacy connections close as before, without a close frame.

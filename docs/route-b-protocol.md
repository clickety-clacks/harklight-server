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

## Client API requests

While the `client_api` feature flag is on (see
[feature-flags.md](feature-flags.md)), the device WebSocket listener also
serves plain HTTPS requests, authorized by the same enrolled device
certificate:

```text
POST /client/v1 HTTP/1.1
Content-Type: application/json
Content-Length: <n>

{"v":1,"type":"inbox_list","ref":"list-1"}
```

The body is one device frame, exactly as it would be sent on the socket, and
the response body is the reply the socket would give. Served types are the
snapshot reads `inbox_list`, `conversation_page`, `view_get`, `levels_get` and
the mutations `message_open`, `conversation_read`, `discussion`, `answer`,
`answer_no_comment`, `lifecycle`, `levels_set`, `push_register`, `push_remove`
and `push_status`. Every other type is answered `wrong_endpoint`.

A request has no session. It holds no cursor, never enters snapshot paging
mode, is never refused by a paging checkpoint and closes no connection. Any
number may run in parallel beside the device's stream. Mutations de-duplicate by
`op_id` per device, so a retry on another connection or on the socket returns
the first reply with `"duplicate": true`.

| Status | When |
|---|---|
| 200 | The frame was served; the body is `ok` or an `error` reply |
| 400 | The body is not a JSON frame with `v` and `type`, or has no `Content-Length` |
| 403 | The device is no longer authorized |
| 404 | `client_api` is off, or the path is not `/client/v1`. A server without the API also answers this way. |
| 405 | A method other than `POST` |
| 413 | The body is larger than 2 MiB |
| 503 | The reply is `storage_unavailable` or `retry_later`; `Retry-After` is set |

Every response carries `Harklight-Request-Id`, an opaque ID that is safe to log
on both sides; the server logs it with the type, status, reason and duration.
The server closes the connection after each response.

## Device stream

Every device WebSocket is the device's change stream. Its `101` upgrade
response carries `Harklight-Connection-Id`, an opaque ID that the server also
logs at open and close. Opening a WebSocket closes only the device's earlier
WebSocket sessions; client API requests are never closed by it. Revoking or
removing the device closes its stream.

Every server-initiated close of a stream sends a WebSocket close frame first,
with the reason token as its reason text:

| Code | Reason | When |
|---|---|---|
| 4409 | `superseded_by_new_stream` | A newer stream of the same device opened |
| 4403 | `device_revoked` | The device was revoked or removed |
| 4408 | `idle_timeout` | Reserved; there is no idle timeout today |
| 1001 | `server_shutdown` | The device listener stopped |
| 1002 | `protocol_error` | A frame could not be decoded |

A socket that still sends reads and mutations keeps working as before.

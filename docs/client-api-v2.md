# Harklight client API v2: field-level contract

Status: **draft for the delivery owner's acceptance, with all rulings
applied.** Nothing in this file is implemented. It turns the client API r2 specification (work item
wi_e41bc552-9279-41d0-b015-07174b692dec, r2.4; §5.1 is unchanged from r2.3)
into exact JSON shapes. Once it is accepted, this file is the binding
field-level contract. The behavior rules — authentication, the version rule,
gap-freedom, retention and retries — are the specification's. This file only
fixes names, types and the closed token list. Open questions are listed at
the end. Until they are settled, the shapes they name may still change.

The API will be served behind the `client_api_v2` feature flag, off by default,
and becomes the only client protocol in Server 0.2.x.

## 1. Conventions

- Every body is a UTF-8 JSON object. Unknown members must be ignored by
  clients. Servers refuse unknown members in request bodies with
  `400 invalid_field`.
- **Two kinds of number.**
  - A resource `rev` (on Item, Entry, Settings and each Change) is an integer
    of at least 1: the change that last wrote that resource.
  - A **position** is the `rev` member of a read, write or batch response
    (`InboxPage`, `EntriesPage`, `ItemResponse`, `SettingsResponse`,
    `WriteResult`, `ChangeBatch`). It is an integer of at least 0 and means
    "reflects every change up to and including this one". An account with no
    changes reports position 0, so its first change, rev 1, is not skipped.
- **Cut-over seed.**
  - For a person who already has data when the server moves to this API,
    every existing resource is stamped `rev` 1 and the person's position
    starts at 1. Their log starts at rev 2, and the seed is never a change
    row.
  - A person with no data starts at position 0.
- `item_id`, `entry_id`, `file_id`, `page` and `Request-Id` are opaque strings
  of at most 128 characters. Clients compare them only for equality and never
  parse them.
- Times the server generates are integer milliseconds since the Unix epoch and
  are named `*_at_ms`.
- Message content from a sender (`Item.message`) is passed through unchanged, as
  today. Every other member is defined here.
- Absent and `null` mean the same thing unless a member says otherwise.

## 2. Headers

| Header | Direction | Rule |
|---|---|---|
| `Request-Id` | response | Every response, including errors and feeds. Opaque, safe to log. |
| `Idempotency-Key` | request | Required on every `POST`, accepted on `PUT` and `DELETE`. A UUIDv4. |
| `Idempotent-Replayed: true` | response | The stored response of an earlier request with the same key and digest. Nothing was applied. |
| `ETag` | response | On `GET` resources and on `GET /v2/settings`. On files, it is the quoted sha256. |
| `If-None-Match` | request | On `GET`. A match answers `304` with no body. |
| `If-Match` | request | Required on `PUT /v2/settings/priority-levels`. A mismatch answers `412 precondition_failed`. |
| `Retry-After` | response | Seconds. On every `503`. |
| `Last-Event-ID` | request | On the feed. Takes precedence over `?after`. |

## 3. Resources

Schemas use JSON Schema 2020-12. `$ref`s point into the `$defs` below.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "harklight-client-api-v2",
  "$defs": {
    "Rev": { "type": "integer", "minimum": 1, "description": "Version of one resource or change" },
    "Position": { "type": "integer", "minimum": 0, "description": "Reflects every change <= this rev" },
    "OpaqueId": { "type": "string", "minLength": 1, "maxLength": 128 },
    "TimeMs": { "type": "integer", "minimum": 0 },

    "FileRef": {
      "type": "object",
      "required": ["file_id", "name", "size", "sha256"],
      "properties": {
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "name": { "type": "string" },
        "size": { "type": "integer", "minimum": 0 },
        "sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" },
        "content_type": { "type": "string" }
      }
    },

    "Item": {
      "type": "object",
      "required": ["item_id", "rev", "sender", "request_revision", "message", "files",
                   "seen", "chat", "rank"],
      "properties": {
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "rev": { "$ref": "#/$defs/Rev" },
        "sender": { "type": "string", "description": "The sending app, today's sender_id" },
        "request_revision": { "type": "integer", "minimum": 1 },
        "message": { "type": "object", "description": "Current message content, passed through unchanged" },
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } },
        "seen": { "type": "boolean", "description": "Mark Seen for the current substantive revision (today's opened)" },
        "as": { "enum": ["message", "knock"], "description": "Present only when this client is a recipient, not the item's origin" },
        "chat": { "$ref": "#/$defs/Chat" },
        "availability": { "$ref": "#/$defs/Availability" },
        "terminal": { "oneOf": [{ "type": "null" }, { "$ref": "#/$defs/Terminal" }] },
        "result": { "$ref": "#/$defs/OriginResult" },
        "rank": { "$ref": "#/$defs/Rank" }
      }
    },

    "Chat": {
      "type": "object",
      "required": ["available", "started"],
      "properties": {
        "available": { "type": "boolean", "description": "The item has a conversation (today: a session_handle is present)" },
        "started": { "type": "boolean" },
        "read_position": { "oneOf": [{ "type": "null" }, { "$ref": "#/$defs/OpaqueId" }], "description": "entry_id read through" },
        "last_origin_reply": { "oneOf": [{ "type": "null" }, { "$ref": "#/$defs/OpaqueId" }], "description": "entry_id of the newest origin reply" }
      }
    },

    "Availability": {
      "type": "object",
      "required": ["state"],
      "properties": {
        "state": { "enum": ["listening", "not_listening", "gone", "unsupported"] },
        "last_detached_at_ms": { "$ref": "#/$defs/TimeMs" }
      }
    },

    "Terminal": {
      "type": "object",
      "required": ["state", "at_ms"],
      "properties": {
        "state": { "enum": ["answered", "cleared", "expired", "superseded", "retracted"] },
        "at_ms": { "$ref": "#/$defs/TimeMs" },
        "no_comment": { "type": "boolean", "description": "Answered as no comment" },
        "by_this_person": { "type": "boolean" }
      }
    },

    "OriginResult": {
      "type": "object",
      "required": ["status", "revision"],
      "properties": {
        "status": { "type": "string" },
        "result_ref": { "type": "string" },
        "revision": { "type": "integer", "minimum": 1 }
      }
    },

    "Rank": {
      "type": "object",
      "required": ["key"],
      "properties": {
        "key": { "type": "string", "description": "Sort ascending, byte-wise, to get the server's inbox order" },
        "score": { "type": "number" },
        "due_at_ms": { "$ref": "#/$defs/TimeMs" }
      }
    },

    "Entry": {
      "type": "object",
      "required": ["entry_id", "item_id", "rev", "position", "kind", "author", "at_ms", "request_revision"],
      "properties": {
        "entry_id": { "$ref": "#/$defs/OpaqueId" },
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "rev": { "$ref": "#/$defs/Rev" },
        "position": { "type": "integer", "minimum": 1, "description": "Creation order within the item; unique per item and never changes" },
        "kind": { "enum": ["reply", "answer", "lifecycle", "revision", "availability", "receipt"], "description": "lifecycle entries record the origin's terminal actions; clients read them but cannot send them" },
        "author": { "enum": ["person", "origin", "server"] },
        "at_ms": { "$ref": "#/$defs/TimeMs" },
        "request_revision": { "type": "integer", "minimum": 1 },
        "text": { "type": "string" },
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } },
        "in_reply_to": { "$ref": "#/$defs/OpaqueId" },
        "state": { "enum": ["active", "withdrawn", "retracted"], "description": "A withdrawal or retraction is an upsert of this member; entries are never removed" },
        "terminal": { "$ref": "#/$defs/Terminal" },
        "availability": { "$ref": "#/$defs/Availability" },
        "delivery": {
          "type": "object",
          "description": "Receipts for a person-authored entry",
          "properties": {
            "delivered": { "type": "boolean" },
            "acted": { "type": "boolean" }
          }
        }
      }
    },

    "PriorityLevel": {
      "type": "object",
      "required": ["name", "lower_bound", "bounce_after_days"],
      "properties": {
        "name": { "type": "string", "minLength": 1 },
        "lower_bound": { "type": "number", "minimum": 0, "maximum": 1 },
        "bounce_after_days": { "oneOf": [{ "type": "null" }, { "type": "number", "exclusiveMinimum": 0 }] }
      }
    },

    "Settings": {
      "type": "object",
      "required": ["rev", "priority_levels", "push"],
      "properties": {
        "rev": { "$ref": "#/$defs/Rev" },
        "priority_levels": { "type": "array", "items": { "$ref": "#/$defs/PriorityLevel" } },
        "push": {
          "type": "object",
          "description": "This device's push binding. Browsers get state unconfigured.",
          "required": ["state", "revision"],
          "properties": {
            "state": { "enum": ["unconfigured", "ready", "queued", "sending", "waiting_retry", "endpoint_invalid", "blocked_configuration", "canceled"] },
            "revision": { "type": "integer", "minimum": 0 },
            "environment": { "enum": ["production", "sandbox"] },
            "last_attempt_at_ms": { "$ref": "#/$defs/TimeMs" },
            "next_attempt_at_ms": { "$ref": "#/$defs/TimeMs" }
          }
        }
      }
    },

    "Counts": {
      "type": "object",
      "required": ["unresolved"],
      "properties": { "unresolved": { "type": "integer", "minimum": 0 } }
    }
  }
}
```

## 4. Reads

| Request | Response body |
|---|---|
| `GET /v2/inbox?limit=&page=` | `InboxPage` |
| `GET /v2/items/{item_id}` | `ItemResponse` |
| `GET /v2/items/{item_id}/entries?before=&limit=` | `EntriesPage` |
| `GET /v2/settings` | `SettingsResponse` |
| `GET /v2/files/{file_id}` | The file bytes, with `Content-Type`, `Content-Length` and `ETag` |

```json
{
  "$defs": {
    "InboxPage": {
      "type": "object",
      "required": ["rev", "items", "counts"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "items": { "type": "array", "items": { "$ref": "#/$defs/Item" } },
        "counts": { "$ref": "#/$defs/Counts" },
        "next_page": { "$ref": "#/$defs/OpaqueId" }
      }
    },
    "EntriesPage": {
      "type": "object",
      "required": ["rev", "entries", "older"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "entries": { "type": "array", "items": { "$ref": "#/$defs/Entry" } },
        "older": { "type": "boolean" }
      }
    },
    "ItemResponse": {
      "type": "object",
      "required": ["rev", "item"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "item": { "$ref": "#/$defs/Item" }
      }
    },
    "SettingsResponse": {
      "type": "object",
      "required": ["rev", "settings"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "settings": { "$ref": "#/$defs/Settings" }
      }
    }
  }
}
```

- `limit` on the inbox defaults to 50, with a maximum of 200. On entries it
  defaults to 50, with a maximum of 200. A larger value is served as the
  maximum. A non-positive or non-integer value is `400 invalid_field` with
  `field: "limit"`.
- Entries are ordered by `position`, ascending: the newest page when `before`
  is absent, otherwise the entries whose `position` is below that of the
  `before` entry. `before` is an `entry_id`. An upsert raises an entry's `rev`
  but never changes its `position`. Clients sort entries by `position` and
  version-gate them by `rev`.
- The inbox pages through every item the person can see, terminal ones
  included, in rank order. `next_page` is absent on the last page.

## 5. Writes

Every write that succeeds answers `200`, or `201` for `POST /v2/files`, with a
`WriteResult` holding the resources it changed at their new `rev`.

| Request | Body |
|---|---|
| `POST /v2/items/{item_id}/seen` | `{}` |
| `PUT /v2/items/{item_id}/read-position` | `{"entry_id": OpaqueId}` |
| `POST /v2/items/{item_id}/replies` | `{"text": string, "file_ids"?: [OpaqueId]}` |
| `POST /v2/items/{item_id}/answer` | `{"request_revision": int}` plus at most one of `"choice": string`, `"comment": string`, `"result": object` |
| `PUT /v2/settings/priority-levels` | `{"levels": [PriorityLevel]}` with `If-Match` |
| `PUT /v2/devices/me/push` | `{"token": string, "environment": "production" \| "sandbox", "revision": int}` |
| `DELETE /v2/devices/me/push` | none |
| `PUT /v2/devices/me/focus` | `{"item_id"?: OpaqueId, "ttl_s": int 1..3600}` |
| `POST /v2/files` | Raw bytes with `Content-Type` and `Content-Length` |

```json
{
  "$defs": {
    "WriteResult": {
      "type": "object",
      "required": ["rev"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "item": { "$ref": "#/$defs/Item" },
        "entry": { "$ref": "#/$defs/Entry" },
        "settings": { "$ref": "#/$defs/Settings" }
      }
    },
    "AnswerBody": {
      "type": "object",
      "required": ["request_revision"],
      "properties": {
        "request_revision": { "type": "integer", "minimum": 1 },
        "choice": { "type": "string", "minLength": 1 },
        "comment": { "type": "string" },
        "result": { "type": "object" }
      },
      "additionalProperties": false,
      "not": { "anyOf": [
        { "required": ["choice", "comment"] },
        { "required": ["choice", "result"] },
        { "required": ["comment", "result"] }
      ] }
    },
    "FileCreated": {
      "type": "object",
      "required": ["file_id", "sha256", "size"],
      "properties": {
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" },
        "size": { "type": "integer", "minimum": 0 }
      }
    }
  }
}
```

- **Answer.** The body carries at most one of `choice`, `comment` and
  `result`, which keeps today's single stored answer value:
  - `choice` is the `id` of one `value` action in the item's
    `attention-client.actions/1` `answer_ui`. The server stores that action's
    `value`. An `id` that names no such action is `400 invalid_field`, with
    `field: "choice"`.
  - `comment` is the typed reply of the item's `text` action, stored as the
    answer text. An item with no `text` action gets `400 invalid_field`, with
    `field: "comment"`.
  - `result` is the structured result of an `attention-client.ui/1`
    `answer_ui`, stored as today's answer value. On an item with any other
    `answer_ui`, it is `400 invalid_field`, with `field: "result"`.
  - With none of them, the answer is "no comment", today's
    `answer_no_comment`.
  - More than one of them is `400 invalid_field`.
- **Lifecycle** is not part of the client API. Terminal actions stay on the
  origin API, and their results are readable as `Item.terminal` and as
  `lifecycle` entries.
- **Read position** takes an `entry_id`, and the server compares entries by
  `position`, max-wins. Sending an entry at or below the current position
  answers `200` with the unchanged item.
- **Idempotency** is keyed by person and `Idempotency-Key`. It holds for at
  least the 30-day change-log retention window.
- **`DELETE /v2/devices/me/push`** answers `200` with `WriteResult` carrying
  `settings`.
- **`/v2/devices/me/*`** paths answer `403` to a browser principal.

## 6. The change feed

`GET /v2/changes?after=<rev>&limit=<n>`

```json
{
  "$defs": {
    "Change": {
      "type": "object",
      "required": ["rev", "kind", "data"],
      "properties": {
        "rev": { "$ref": "#/$defs/Rev" },
        "kind": { "enum": ["item", "item_removed", "entry", "counts", "settings"] },
        "data": {}
      },
      "allOf": [
        { "if": { "properties": { "kind": { "const": "item" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/Item" } } } },
        { "if": { "properties": { "kind": { "const": "item_removed" } } },
          "then": { "properties": { "data": {
            "type": "object", "required": ["item_id"],
            "properties": { "item_id": { "$ref": "#/$defs/OpaqueId" } } } } } },
        { "if": { "properties": { "kind": { "const": "entry" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/Entry" } } } },
        { "if": { "properties": { "kind": { "const": "counts" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/Counts" } } } },
        { "if": { "properties": { "kind": { "const": "settings" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/Settings" } } } }
      ]
    },
    "ChangeBatch": {
      "type": "object",
      "required": ["changes", "rev", "more"],
      "properties": {
        "changes": { "type": "array", "items": { "$ref": "#/$defs/Change" } },
        "rev": { "$ref": "#/$defs/Position", "description": "rev of the last change, or the request's after when changes is empty" },
        "more": { "type": "boolean" }
      }
    },
    "Closing": {
      "type": "object",
      "required": ["reason"],
      "properties": { "reason": { "enum": ["revoked", "server_shutdown"] } }
    }
  }
}
```

- **JSON batch** (`Accept: application/json`):
  - The body is a `ChangeBatch`.
  - `limit` defaults to 500, with a maximum of 500.
  - `changes` are in increasing `rev` order and gap-free.
  - `after=0`, or no `after`, means "from the start". It is served only if
    the person's log is retained from rev 1. Otherwise, including every person
    seeded at cut-over, it is `410 cursor_expired`, and the client reads a
    snapshot first.
- **SSE** (`Accept: text/event-stream`):
  - Each change is one event: `id: <rev>`, `event: <kind>`, and
    `data: <data as one line of JSON>`.
  - The server writes `: heartbeat` at least every 25 s and never ends a feed
    for idleness.
  - Before a server-initiated end, it sends `event: closing` with
    `data: Closing`. That event has **no `id:` line**, because it is not a log
    change and must not move `Last-Event-ID`.
- **`after` older than retention:** `410` with an `Error` body whose `error` is
  `cursor_expired`, in both forms.
- **Feed ID:** the feed's `Request-Id` is its ID. The server logs it at open,
  and at close with `revoked`, `server_shutdown` or `client_closed`.

## 7. Errors

```json
{
  "$defs": {
    "Error": {
      "type": "object",
      "required": ["error", "retryable", "request_id"],
      "properties": {
        "error": { "enum": ["not_found", "invalid_field", "stale_request", "already_answered",
                            "already_retracted", "idempotency_conflict", "precondition_failed",
                            "cursor_expired", "unauthorized", "forbidden", "retry_later",
                            "storage_unavailable", "payload_too_large", "unsupported"] },
        "field": { "type": "string" },
        "retryable": { "type": "boolean" },
        "request_id": { "$ref": "#/$defs/OpaqueId" }
      }
    }
  }
}
```

| `error` | Status | `retryable` | When (today's engine reason, where one exists) |
|---|---|---|---|
| `invalid_field` | 400 | false | Malformed body, unknown member or bad value (`invalid_field`, `bad_frame`) |
| `unauthorized` | 401 | false | Browser session missing or expired. Devices fail at TLS instead. |
| `forbidden` | 403 | false | The principal may not do this (`forbidden`, `not_person`, `not_origin`) |
| `not_found` | 404 | false | Unknown or invisible item, entry or file (`unknown_message`, `not_found`) |
| `stale_request` | 409 | false | `request_revision` is behind the item (`stale_revision`) |
| `already_answered` | 409 | false | The item is already answered or otherwise closed to this write (`closed`) |
| `already_retracted` | 409 | false | The origin retracted the item (`retracted`) |
| `idempotency_conflict` | 409 | false | Same key, different request (`operation_conflict`) |
| `precondition_failed` | 412 | false | `If-Match` does not match. Also a superseded push `revision` (`stale_push_binding`). |
| `cursor_expired` | 410 | false | `after` is older than retention |
| `payload_too_large` | 413 | false | Body or file over the limit (`too_large`, `upload_cap_exceeded`) |
| `unsupported` | 400 | false | The server does not serve this here, for example push with push disabled |
| `retry_later` | 503 | true | Load shedding, with `Retry-After` |
| `storage_unavailable` | 503 | true | The store could not commit, with `Retry-After` |

No other token is sent. Error bodies never contain free-form text, message
content, tokens or identities.

## 8. Carried over from today, and dropped

- **Carried:**
  - inbox row content, renamed: `opened` becomes `seen`,
    `chat_started`/`chat_read_position` become `chat`, `origin_result` becomes
    `result`, and `priority_view` becomes `rank`;
  - terminal state, availability, files and `as`;
  - conversation entries for replies, answers, lifecycle facts, request
    revisions, availability changes and delivery receipts;
  - priority levels and push status.
- **Dropped from client resources:**
  - `seq`, `conversation_seq`, `accepted_seq`, `latest_origin_reply_seq`,
    `view_rev`;
  - the origin device key and `session_handle`, which are replaced by
    `Chat.available` and the absence of `as`;
  - the `cleared` mapping of retractions for old clients (`retracted` is now a
    terminal state of its own);
  - `unsupported_reason` text, push `binding_id`, `profile_id` and
    `attempt_count`, and `op_id`s;
  - entry kinds that only agents act on (`file_use`, `file_transfer`, `acted`,
    `seen`, `delivery_ack` as separate entries; their effect shows up as
    `Entry.delivery`).

## 9. Rulings applied

The delivery owner's rulings on this draft are applied above:

1. **Answer** carries at most one of `choice` (an action `id`, and the server
   stores its `value`), `comment` (the typed reply of a `text` action) and
   `result` (`attention-client.ui/1` only). With none of them, the answer is
   no comment. More than one is `invalid_field`. This keeps today's answer
   semantics (rulings 1, 9 and 10).
2. **Lifecycle** is removed from the client API. No person-side clear or
   dismiss frame exists today: the only terminal write a person's device can
   make is `answer`, including its Seen choice and `answer_no_comment`. So
   there is nothing to map instead.
3. **Rank.** `rank.key` is a total-order key, sorted ascending bytewise, and
   the server emits every move:
   - moving one item rewrites only that item's key and emits one `item` event;
   - a full re-rank, such as after a priority-levels change, may rewrite many
     keys;
   - time-driven moves (due dates, bounce) are `item` events at the moment they
     take effect.
4. **Push** is APNs only. `stale_push_binding` answers `412`, and browsers get
   `403` on `/v2/devices/me/*`.
5. **`forbidden`** is a 403 and is not retryable.
6. **`closing`** has no `id:` line, never appears in a JSON batch and never
   moves the cursor.
7. **Positions** start at 0, and resource `rev`s at 1 (§1).
8. **Entry `position`** is immutable creation order. Paging, `before` and the
   read position compare by it.
9. **Structured answers:** an optional `result` object, accepted only for
   `attention-client.ui/1` items (see ruling 1 above).
10. **Answer members are mutually exclusive** (see ruling 1 above).

No questions are open.

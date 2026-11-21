# Harklight client API v2: field-level contract

Status: **accepted field contract.** It includes the delivery owner's rulings
1 to 10, the contract rulings of rounds 2 to 4, and the product owner's chat
and answer rules. It turns the client API r2 specification (work item
wi_e41bc552-9279-41d0-b015-07174b692dec, r2.5) into
exact JSON shapes and is the binding field-level contract. The behavior rules
are the specification's: authentication, the version rule, gap-freedom,
retention and retries. This file fixes names, types and the closed token list.

The API is implemented in slices behind the `client_api_v2` feature flag, off
by default. It becomes the only client protocol in Server 0.2.x.

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
  `"result": null` selects a structured answer (§5), and
  `"reply_group": null` identifies a reply row with no group (§4).

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
        "content_type": { "type": "string", "description": "The file's stored type, so a browser can build a typed blob to preview it" }
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
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" }, "description": "Every file the item offers: the current revision's files plus any earlier revision's files that the current answer_ui references by name. Answer forms validate against this list alone." },
        "seen": { "type": "boolean", "description": "Mark Seen for the current substantive revision (today's opened)" },
        "created_at_ms": { "$ref": "#/$defs/TimeMs", "description": "Display only: the record's stored acceptance time. Absent when the record has none; every item accepted by 0.2 has it." },
        "arrival": { "type": "integer", "minimum": 1, "description": "Today's per-person free-order arrival counter; orders the new-messages area. Absent on items from before free order, which sit in Rank.group earlier_messages." },
        "thread": { "type": "string", "description": "The item's effective group thread (possibly inherited). Group key is (sender, thread); members are adjacent in Rank.key order." },
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
      "required": ["started"],
      "properties": {
        "available": { "type": "boolean", "description": "The item has an agent conversation. Clients hide the composer when false; /replies then answers 400 unsupported. Not a listening indicator." },
        "started": { "type": "boolean" },
        "read_position": { "oneOf": [{ "type": "null" }, { "$ref": "#/$defs/OpaqueId" }], "description": "entry_id read through" },
        "last_origin_reply": { "oneOf": [{ "type": "null" }, { "$ref": "#/$defs/OpaqueId" }], "description": "entry_id of the newest origin reply" },
        "unread_replies": { "type": "integer", "minimum": 0, "description": "Agent replies after the person's read position, computed by the server" },
        "reply_group": { "oneOf": [{ "type": "null" }, { "enum": ["earlier_chat_replies", "earlier_chat_replies_unknown"] }], "description": "Present whenever the item has at least one agent chat reply, even when unread_replies is 0: today's verified or unknown reply row, or null where today's reply row has no group (ranked order, or free order's common group)" },
        "reply_rank_key": { "type": "string", "description": "Present whenever reply_group is: where the reply row sorts, in Rank.key's space" },
        "reply_priority": { "$ref": "#/$defs/ReplyPriority" }
      }
    },

    "PriorityLevel4": { "enum": ["now", "high", "normal", "low"] },

    "ReplyPriority": {
      "type": "object",
      "description": "Present with reply_group: the reply row's level (the highest among its unseen replies; with none unseen, today's row level), score and due time, as today",
      "required": ["level"],
      "properties": {
        "level": { "$ref": "#/$defs/PriorityLevel4" },
        "score": { "type": "number" },
        "due_at_ms": { "$ref": "#/$defs/TimeMs" }
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
        "answer": { "$ref": "#/$defs/AnswerOutcome" },
        "by_this_person": { "type": "boolean", "description": "Settled by any client of this person, not only this device" }
      }
    },

    "AnswerOutcome": {
      "type": "object",
      "description": "Today's person-facing answer_outcome, unchanged in shape. Never the origin-only raw answer. Seen by every principal that can see the item.",
      "required": ["kind"],
      "properties": {
        "kind": { "enum": ["seen", "choice", "reply", "result", "default", "no_comment"] },
        "id": { "type": "string", "description": "kind choice: the matched answer_ui action's id" },
        "label": { "type": "string", "description": "kind choice: that action's label" },
        "text": { "type": "string", "description": "kind reply: the plain-text answer" },
        "value": { "description": "kind result: the structured answer, any JSON value including null; kind default: the default answer's value" }
      }
    },

    "OriginResult": {
      "type": "object",
      "description": "An origin result. Item.result is the latest; each result fact is also a result Entry.",
      "required": ["status", "revision"],
      "properties": {
        "status": { "enum": ["received", "applied", "refused"] },
        "result_ref": { "type": "string" },
        "revision": { "type": "integer", "minimum": 1 }
      }
    },

    "Rank": {
      "type": "object",
      "required": ["key", "section"],
      "properties": {
        "key": { "type": "string", "description": "Sort ascending, byte-wise, to get the server's inbox order across and within sections" },
        "section": { "enum": ["current", "resolved"], "description": "Draw a heading where it changes; resolved takes today's History heading (closed list, §4)" },
        "group": { "enum": ["common", "earlier_messages"], "description": "Only for current items while the inbox is in free order; common has no heading" },
        "level": { "$ref": "#/$defs/PriorityLevel4", "description": "Present only when the inbox view entry carries priority, in baseline and fallback views. Omitted under a selected JEV ranking and for progress FYIs." },
        "score": { "type": "number", "description": "Present for current items wherever today's priority view has it" },
        "due_at_ms": { "$ref": "#/$defs/TimeMs", "description": "Present wherever today's priority view has it" }
      }
    },

    "Entry": {
      "type": "object",
      "required": ["entry_id", "item_id", "rev", "position", "kind", "author", "request_revision"],
      "properties": {
        "entry_id": { "$ref": "#/$defs/OpaqueId" },
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "rev": { "$ref": "#/$defs/Rev" },
        "position": { "type": "integer", "minimum": 1, "description": "Creation order, unique per person, compared only within an item; not dense (gaps are normal). Never changes." },
        "kind": { "enum": ["reply", "lifecycle", "revision", "availability", "result"], "description": "lifecycle is only the origin's retraction fact; a revision entry (request revision 2 and later) marks that a revision happened; a result entry is one origin result fact. The answer outcome is Item.terminal, not an entry." },
        "author": { "enum": ["person", "origin", "server"] },
        "at_ms": { "$ref": "#/$defs/TimeMs", "description": "Absent on entries from before the cut-over; every entry created by 0.2 carries it" },
        "request_revision": { "type": "integer", "minimum": 1 },
        "text": { "type": "string" },
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } },
        "in_reply_to": { "$ref": "#/$defs/OpaqueId" },
        "state": { "enum": ["active", "withdrawn", "retracted"], "description": "A withdrawal or retraction is an upsert of this member; entries are never removed" },
        "availability": { "$ref": "#/$defs/Availability" },
        "result": { "$ref": "#/$defs/OriginResult", "description": "On result entries, nested like availability" },
        "priority": {
          "type": "object",
          "description": "On agent reply entries: the reply's level and score from today's view",
          "required": ["level"],
          "properties": {
            "level": { "$ref": "#/$defs/PriorityLevel4" },
            "score": { "type": "number" }
          }
        },
        "delivery": {
          "type": "object",
          "description": "Receipts for a person-authored entry, folded in from today's receipt facts",
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
            "state": { "enum": ["unconfigured", "ready", "queued", "sending", "waiting_retry", "endpoint_invalid", "blocked_configuration", "canceled", "expired"] },
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
    },

    "Me": {
      "type": "object",
      "required": ["account", "features", "limits"],
      "properties": {
        "sender_names": { "type": "array", "items": { "type": "string" }, "description": "On an enrolled key, its authorized sender names and slash-terminated prefixes" },
        "account": {
          "type": "object",
          "required": ["state"],
          "properties": { "state": { "enum": ["active", "paused"] } }
        },
        "features": {
          "type": "object",
          "description": "Server-controlled client features still behind a flag at 0.2 (§4)",
          "additionalProperties": { "type": "boolean" }
        },
        "limits": {
          "type": "object",
          "required": ["upload_cap_bytes_per_file", "inline_decoded_bytes_per_entry", "answer_ui_limits", "retention_days"],
          "properties": {
            "upload_cap_bytes_per_file": { "oneOf": [{ "type": "integer", "minimum": 0 }, { "const": "none" }] },
            "inline_decoded_bytes_per_entry": { "type": "integer", "minimum": 0 },
            "answer_ui_limits": {
              "type": "object",
              "description": "Exactly the Server's published UI/1 limits (UI1.published_limits). Every 0.2 client renders up to them.",
              "required": ["document_bytes", "requires", "illustrations", "primitives_per_illustration", "primitives_total", "inputs", "actions", "scene_nodes", "scene_depth", "result_nodes", "result_depth", "result_point_path", "array_items", "object_members", "identifier_bytes"],
              "properties": {
                "document_bytes": { "type": "integer", "minimum": 0 },
                "requires": { "type": "integer", "minimum": 0 },
                "illustrations": { "type": "integer", "minimum": 0 },
                "primitives_per_illustration": { "type": "integer", "minimum": 0 },
                "primitives_total": { "type": "integer", "minimum": 0 },
                "inputs": { "type": "integer", "minimum": 0 },
                "actions": { "type": "integer", "minimum": 0 },
                "scene_nodes": { "type": "integer", "minimum": 0 },
                "scene_depth": { "type": "integer", "minimum": 0 },
                "result_nodes": { "type": "integer", "minimum": 0 },
                "result_depth": { "type": "integer", "minimum": 0 },
                "result_point_path": { "type": "integer", "minimum": 0 },
                "array_items": { "type": "integer", "minimum": 0 },
                "object_members": { "type": "integer", "minimum": 0 },
                "identifier_bytes": { "type": "integer", "minimum": 0 }
              }
            },
            "retention_days": { "const": 30 }
          }
        }
      }
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
| `GET /v2/items/{item_id}/revisions/{n}` | `RevisionResponse`, for any request revision `n` from 1 to the current one |
| `GET /v2/settings` | `SettingsResponse` |
| `GET /v2/files/{file_id}` | The file bytes, with `Content-Length` and `ETag`; its type as described under Files below |
| `GET /v2/me` | `Me`. Answerable while the account is paused. |

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
    "RevisionResponse": {
      "type": "object",
      "required": ["rev", "revision"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "revision": {
          "type": "object",
          "required": ["request_revision", "message", "files"],
          "properties": {
            "request_revision": { "type": "integer", "minimum": 1 },
            "message": { "type": "object", "description": "Passed through unchanged" },
            "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } }
          }
        }
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
- **Sections** are a closed list taken from today's inbox order
  (`Placement.list_items`):
  - `current`: items still waiting on the person, in today's ranked or free
    order;
  - `resolved`: settled items, newest first. Clients draw today's History
    heading here.

  `rank.key` orders items across and within sections. While the inbox is in
  free order, `current` items also carry `Rank.group` (`common`, which has no
  heading, or `earlier_messages`). Clients use `Rank.level` for the priority
  mark when present, otherwise compare `Rank.score` with the settings levels
  when present. If neither field is present, draw no mark.

  An item with an agent chat reply carries `Chat.reply_group` and
  `Chat.reply_rank_key` even when all replies are read. The key places the
  reply row among item rows in `Rank.key`'s space. Its presence identifies a
  reply row even when the group is null. Clients keep their own row rules,
  and the Replied tab draws all reply rows.
- **Results.** `Item.result` is the latest origin result. Each result fact is
  also an Entry of kind `result`, with its own `position`, so the thread keeps
  its status turns. `status` is today's closed list: `received`, `applied`,
  `refused`.
- **Files.**
  - **To a device**, a file is served with its stored `Content-Type`:
    - a file uploaded through `POST /v2/files` keeps the type it was sent
      with;
    - a file attached inline before the cut-over is
      `application/octet-stream`.
  - **To a browser**, `GET /v2/files/{file_id}` is served exactly as today's
    browser download:
    - `Content-Type: application/octet-stream`;
    - `Content-Disposition: attachment; filename*=UTF-8''<percent-encoded name>`;
    - `Content-Length` and the sha256 `ETag`.

    It is never inline on the web origin. `FileRef.content_type` carries the
    stored type, so the web client can build a typed blob to preview the file.
- **`GET /v2/me` features.** It carries only the server-controlled client
  features still behind a flag at 0.2. Today's three app flags are
  classified as follows:
  - `progress_inbox`: on for everyone, so it is retired and not carried;
  - `server_matched_push_routing`: carried; off unless the operator turns it
    on;
  - `priority_level_editing`: carried; off unless the operator turns it on.

## 5. Writes

Every write that succeeds answers `200`, or `201` for `POST /v2/files`, with a
`WriteResult` holding the resources it changed at their new `rev`.

| Request | Body |
|---|---|
| `POST /v2/items/{item_id}/seen` | `{}` |
| `PUT /v2/items/{item_id}/read-position` | `{"entry_id": OpaqueId}` |
| `POST /v2/items/{item_id}/replies` | `{"text": string, "file_ids"?: [OpaqueId]}` |
| `POST /v2/items/{item_id}/answer` | `{"request_revision": int}` plus at most one of `"choice": string`, `"comment": string`, `"result": any JSON value` |
| `PUT /v2/settings/priority-levels` | `{"levels": [PriorityLevel]}` with `If-Match` |
| `PUT /v2/devices/me/push` | `{"token": string, "environment": "production" \| "sandbox", "revision": int}` |
| `DELETE /v2/devices/me/push` | none |
| `PUT /v2/devices/me/focus` | `{"screen": "inbox" \| "message" \| "chat" \| "settings" \| "signed_out" \| "not_in_front", "item_id"?: OpaqueId, "visible_entry_id"?: OpaqueId, "ttl_s": int 1..3600}` (§5.1) |
| `POST /v2/files` | Raw bytes with `Content-Type`, `Content-Length` and `Content-Disposition: attachment; filename*=UTF-8''<percent-encoded name>` |

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
        "settings": { "$ref": "#/$defs/Settings" },
        "show": { "$ref": "#/$defs/Show", "description": "Only on a focus write, when this device has a pending show" }
      }
    },
    "Show": {
      "type": "object",
      "required": ["show_id", "device_id", "item_id", "target", "held_ms"],
      "properties": {
        "show_id": { "$ref": "#/$defs/OpaqueId" },
        "device_id": { "$ref": "#/$defs/OpaqueId" },
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "target": { "enum": ["message", "chat"] },
        "held_ms": { "type": "integer", "minimum": 0 }
      }
    },
    "AnswerBody": {
      "type": "object",
      "required": ["request_revision"],
      "properties": {
        "request_revision": { "type": "integer", "minimum": 1 },
        "choice": { "type": "string", "minLength": 1 },
        "comment": { "type": "string" },
        "result": { "description": "Any JSON value, null included; presence selects a structured answer" }
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
      "required": ["file_id", "name", "sha256", "size"],
      "properties": {
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "name": { "type": "string", "minLength": 1 },
        "sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" },
        "size": { "type": "integer", "minimum": 0 }
      }
    }
  }
}
```

- **Answer.** An item is answerable while its `terminal` is null, including
  FYIs. Clients offer only Seen for FYIs and send `choice: "seen"`. Once
  `terminal` is set, an answer is `409 already_answered`, or
  `already_retracted` for a retraction. The body carries at most one of
  `choice`, `comment` and `result`, which keeps today's single stored answer
  value.
  - `choice` is the `id` of one `value` action in the item's
    `attention-client.actions/1` `answer_ui`. The server stores that action's
    `value`. An `id` that names no such action is `400 invalid_field`, with
    `field: "choice"`.
  - `comment` is a plain-text answer. The server accepts it on any answerable
    item, with or without an `answer_ui`, including one with an answer screen.
  - `result` is the structured result of an `attention-client.ui/1`
    `answer_ui`: **any JSON value**, null included, stored unchanged.
    **Presence selects it**: `"result": null` is a structured answer whose
    value is null. On an item with any other `answer_ui`, a present `result`
    is `400 invalid_field`, with `field: "result"`.
  - With none of them, the answer is "no comment", today's
    `answer_no_comment`.
  - More than one of them present is `400 invalid_field`.
  - The answer outcome then reads as `Terminal.answer`: `choice` with the
    action's `id` and `label`, `reply` with `text` for a plain-text answer
    (including on a UI/1 item), `result` with `value` for a structured
    answer, `seen`, `default` or `no_comment`. An FYI answered with
    `choice: "seen"` yields `{"kind":"seen"}`. An answer with no matching
    outcome leaves `Terminal.answer` absent. Answers stored before 0.2 on
    UI/1 items keep today's shape-based outcomes.
- **File name.** `POST /v2/files` requires the name in
  `Content-Disposition: attachment; filename*=UTF-8''<percent-encoded name>`
  (RFC 6266, RFC 5987). A missing or empty name is `400 invalid_field`, with
  `field: "name"`. Replies keep `file_ids`, because the name belongs to the
  file.
- **Replies** are accepted in every item state on an item with an agent
  conversation (`Chat.available`): open, answered, terminal, retracted but
  still visible, and while the agent is not listening. A reply to an agent
  that is not listening is stored and delivered when the agent next reads.
  The API encodes no chat-closing rule, and availability is informational.
  On an item sent without a conversation (`Chat.available: false`), a reply
  is `400 unsupported`. `404` is only for items the person cannot see.
- **Lifecycle** is not part of the client API. Terminal actions stay on the
  origin API. Their results are readable as `Item.terminal`, and a retraction
  also as a `lifecycle` entry.
- **Read position** takes an `entry_id`, and the server compares entries by
  `position`, max-wins. Sending an entry at or below the current position
  answers `200` with the unchanged item.
- **Idempotency** is keyed by person and `Idempotency-Key`, and it stores
  **successes only**, as today's receipts do. It holds for at least the
  30-day change-log retention window.
  - A repeat with the same key and digest returns the stored success with
    `Idempotent-Replayed: true`.
  - A refused request is not stored, so a retry with the same key is
    evaluated again.
  - A different digest under a stored key is `409 idempotency_conflict`.
  - Two clients sending the same key at the same moment may both apply. Keys
    are UUIDv4 values minted once per intent by one client, so this is not a
    production pattern, and nothing defends against it.
- **`DELETE /v2/devices/me/push`** answers `200` with `WriteResult` carrying
  `settings`.
- **`/v2/devices/me/*`** paths answer `403` to a browser principal.

## 5.1 Focus and held shows

Focus and push-a-view keep today's behavior (the focus spec as shipped).
Only their transport changes, and they are device-only.

- **`PUT /v2/devices/me/focus`** takes today's report fields:
  - `screen`: today's set, including `not_in_front`;
  - `item_id`: required for `message` and `chat`, absent for other screens;
  - `visible_entry_id`: optional for `chat`, absent for other screens;
  - `ttl_s`: an integer from 1 to 3600. Clients send 120 and refresh every
    60 s while in front.

  Report screen changes and send `not_in_front` on backgrounding. Debounce
  changes within a screen by one second and renew within `ttl_s` while in
  front. Send IDs only, keep network and disk work off the main thread, and
  avoid reporting per scroll frame. Clients serialize focus writes per
  device. With one `PUT` in flight, the latest report replaces any queued one.

  Its `reported_at` rule is today's: the first report, or a change to an
  in-front screen. An unchanged refresh maintains liveness without changing
  rank or default-target order. A lapsed `ttl_s` ends focus, listing and
  targetability immediately, like today's
  session end. A fresh accepted report restores them. A device is opted in to
  shows only by an accepted focus report, and `signed_out` opts it out.
- **Held shows** are memory-only. They are **never change-log rows**, and
  nothing about focus or shows is stored. A show is
  `{show_id, device_id, item_id, target: "message" | "chat", held_ms}`, where
  `held_ms` is its age when the Server writes it. Each show addresses exactly
  one device. An accepted show survives a `ttl_s` lapse without expiring.
  The server holds the latest show until consumption, replacement, device
  removal or revocation, or server restart. Turning off an agent's see or
  push grant leaves accepted shows intact.

  The device receives it in `WriteResult.show` on its next focus write, or
  while it holds an open SSE feed, as an id-less `event: show` with a `Show`
  payload. The server uses the same `show_id` on both paths, and the client
  applies it once. SSE shows never appear in a JSON batch or replay.

  The server recomputes `held_ms` on every delivery. The client subtracts it
  from its local receipt time and compares that time with the person's own
  open time. Check latest-show and own-open order both on receipt and before
  applying the show. Preserve typing deferral and the draft. The first touch
  restores normal open and read behavior. A show itself never causes Seen, an
  answer, a reply, an open, a read, APNs, sound, haptics, or a device wake.

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
        "rev": { "$ref": "#/$defs/Position", "description": "The position the batch is complete through: its last change's rev, or later if trailing log rows have no client form, or the request's after when nothing was read (round 12)" },
        "more": { "type": "boolean" }
      }
    },
    "Closing": {
      "type": "object",
      "required": ["reason"],
      "properties": { "reason": { "enum": ["revoked", "account_paused", "server_shutdown", "cursor_expired", "storage_unavailable"] } }
    }
  }
}
```

- **JSON batch** (`Accept: application/json`):
  - The body is a `ChangeBatch`.
  - `limit` defaults to 500, with a maximum of 500.
  - `changes` are in increasing `rev` order and gap-free.
  - `rev` is the position the batch is complete through: its last change's
    `rev`, later if trailing log rows have no client form, or `after` when
    nothing was read. Clients move their cursor to it even when it is past
    the last change.
  - `after=0`, or no `after`, means "from the start". It is served only if
    the person's log is retained from rev 1. Otherwise, including every person
    seeded at cut-over, it is `410 cursor_expired`, and the client reads a
    snapshot first.
- **SSE** (`Accept: text/event-stream`):
  - Each change is one event: `id: <rev>`, `event: <kind>`, and
    `data: <data as one line of JSON>`.
  - The server writes `event: heartbeat` with `data: {}` and **no `id:`
    line** at least every 25 s, and never ends a feed for idleness. A heartbeat
    never moves the cursor and never appears in a JSON batch. Clients use it
    only for the 60 s liveness rule.
  - Before every server-initiated end, it sends `event: closing` with
    `data: Closing`. That event has **no `id:` line**, because it is not a log
    change and must not move `Last-Event-ID`. The reasons are:
    - `revoked` and `account_paused`: stop, and re-enrol or show "paused";
    - `server_shutdown` and `storage_unavailable`: reconnect with
      `Last-Event-ID` after backoff;
    - `cursor_expired`: the position fell out of retention mid-stream, so
      rebaseline as for `410`.
  - A held show for this device may arrive as an id-less `event: show`
    (§5.1).
  - A feed that drops with no `closing` is recorded with initiator
    `os_network` or `unknown`, and reconnected with `Last-Event-ID`.
- **`after` older than retention:** `410` with an `Error` body whose `error` is
  `cursor_expired`, in both forms. After either a 410 or an SSE closing with
  this reason, read every page of `GET /v2/inbox`, set the cursor to the first
  page's `rev`, and reopen the feed using that cursor.
- **Feed ID:** the feed's `Request-Id` is its ID. The server logs it at open,
  and at close with its `closing` reason, or `client_closed`.

## 6.1 The log is compacted

The change log holds only each resource's latest row. A row is rendered from
current state, in the same serialized step as reads and commits. Every change
to a resource's client-visible inputs writes a new row for that resource,
which removes the older one. So the state rendered at row rev N is that
resource's state as of N.

What clients see:

- The feed and batches are gap-free, but may skip intermediate states of a
  resource. Each resource appears at most once per catch-up, at its latest
  `rev`. Clients must not rely on seeing every intermediate state.
- Chat turns are their own Entry resources, so no turn is ever skipped.
- `item_removed` rows are kept as tombstones for the retention window.
- A log row may have no client form (for example a read-position fact), but
  every client-visible effect of the fact it records also writes the
  affected resource's row. A read position writes the item's row, so another
  device sees `Chat.read_position`, `unread_replies` and `reply_priority`
  change.

The Server tests completeness by replaying the log, from empty and from a
mid-log cursor, and comparing the result with a fresh snapshot.

## 6.2 Push payload

A 0.2 push keeps today's APNs alert, badge and background form, and today's
visible text. Its `attention` member changes:

```json
{
  "attention": {
    "v": 2,
    "type": "sync",
    "server": "<32 hex characters, today's server identity>",
    "locator": {
      "v": 2,
      "binding_id": "<uuid>",
      "person_id": "<person>",
      "device_id": "<device>",
      "item_id": "<item_id>",
      "rev": 42,
      "target": "chat"
    }
  }
}
```

- `server` is unchanged, because a device can be enrolled with several
  Servers.
- `item_id` and `rev` name the change that caused the push.
- `sender_id`, `message_id` and `acceptance_seq` are dropped.
- `target` is present only for a chat push, as today.
- The app routes by `server` and `item_id`. If its cursor is already at or
  past `rev`, there is nothing to fetch; otherwise it fetches a JSON batch
  from its cursor.

## 7. Errors

```json
{
  "$defs": {
    "Error": {
      "type": "object",
      "required": ["error", "retryable", "request_id"],
      "properties": {
        "error": { "enum": ["not_found", "invalid_field", "stale_request", "already_answered",
                            "already_retracted", "already_closed", "not_answered", "idempotency_conflict", "precondition_failed",
                            "cursor_expired", "unauthorized", "forbidden", "retry_later",
                            "storage_unavailable", "payload_too_large", "unsupported",
                            "account_paused"] },
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
| `unauthorized` | 401 | false | Browser session missing or expired. A device revoked while its connection is open (each request re-checks enrollment; HTTP/1.1 also gets `Connection: close`). New device connections fail at TLS instead. |
| `forbidden` | 403 | false | The principal may not do this (`forbidden`, `not_person`, `not_origin`) |
| `account_paused` | 403 | false | The account is paused (`inactive_account`). Every `/v2` request except `GET /v2/me` gets it; open feeds get `closing` with `account_paused`. |
| `not_found` | 404 | false | Unknown or invisible item, entry or file (`unknown_message`, `not_found`) |
| `stale_request` | 409 | false | `request_revision` is behind the item (`stale_revision`) |
| `already_answered` | 409 | false | Only on `/answer`: the item's `terminal` is already set (`closed`) |
| `already_retracted` | 409 | false | The origin retracted the item (`retracted`); also an agent revise or retract on that item |
| `already_closed` | 409 | false | A closed message refuses agent revise, retract or lifecycle, a result conflicts with one already recorded, a file-use report conflicts, or a file is already attached (§9.2) |
| `not_answered` | 409 | false | The agent tried to record a result before an answer |
| `idempotency_conflict` | 409 | false | Same key, different request (`operation_conflict`) |
| `precondition_failed` | 412 | false | `If-Match` does not match. Also a superseded push `revision` (`stale_push_binding`). |
| `cursor_expired` | 410 | false | `after` is older than retention |
| `payload_too_large` | 413 | false | Body or file over the limit (`too_large`, `upload_cap_exceeded`) |
| `unsupported` | 400 | false | The server does not serve this here: push with push disabled, or a reply on an item with no agent conversation |
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
  - conversation entries for replies, retractions, request revisions and
    availability changes, with delivery receipts folded into
    `Entry.delivery`;
  - the answer outcome, as `Terminal.answer`;
  - today's inbox sections and unread-reply counts, as `Rank.section` and
    `Chat.unread_replies`;
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

## 9. Agent API

An enrolled key may send as an agent and receive as a device. Requests under
`/v2/agent` use mutual TLS on the device listener. Browser sessions and
grant-scoped client principals cannot use these routes. A principal sees its
own sent messages and their entries, scoped to the account and enrolled key.
An unknown or out-of-scope item or entry answers `404 not_found`.

The headers in §2 and the closed error and retry rules in §7 apply. An
agent's idempotency scope is its account and enrolled key. Every `POST`
requires a UUIDv4 `Idempotency-Key`. `PUT` and `DELETE` accept one. A replay
matches the original method, target, parameters and request contents and
returns the stored success with `Idempotent-Replayed: true`. Refusals are
evaluated again. A different request under a stored key answers
`409 idempotency_conflict`. The retention and simultaneous-request rules in
§5 apply. Show receipts live in relay memory alongside held shows. The
server evaluates a show replay after a restart as a new request.

`GET /v2/me` keeps `Me` and adds `sender_names: [string]` for an enrolled
key. An empty array means the key has no sender names. A name ending in
`/` authorizes that prefix, as today. The client's existing `Me.limits`
provides the file caps and retention window.

### 9.1 Resources and reads

| Request | Response body |
|---|---|
| `GET /v2/agent/messages?page=&session=&sender=&legacy_message_id=` | `SentMessagesPage` |
| `GET /v2/agent/messages/{item_id}` | `SentMessageResponse` |
| `GET /v2/agent/messages/{item_id}/entries?before=&limit=` | `AgentEntriesPage` |
| `GET /v2/agent/messages/{item_id}/revisions/{n}` | `AgentRevisionResponse` |
| `GET /v2/agent/focus` | `AgentFocus` |

The following definitions extend the shared `$defs`. Read positions and
resource versions use the agent's own log, independent of the person's log.
An empty log is at position 0. Rollout seeds retained resources at 1 for
agents with existing data. Clients apply newer versions of each resource.

```json
{
  "$defs": {
    "AgentSession": { "type": "string", "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$" },
    "AgentTerminal": {
      "type": "object",
      "required": ["state", "at_ms"],
      "properties": {
        "state": { "enum": ["answered", "cleared", "expired", "superseded", "retracted"] },
        "at_ms": { "$ref": "#/$defs/TimeMs" },
        "answer": { "description": "The raw stored answer, including an explicit null. Present exactly when an answer value was stored." },
        "outcome": { "$ref": "#/$defs/AnswerOutcome" },
        "delivery": { "$ref": "#/$defs/AgentDelivery" }
      }
    },
    "SentMessage": {
      "type": "object",
      "required": ["item_id", "rev", "sender", "revision", "message", "files", "delivered", "seen"],
      "properties": {
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "rev": { "$ref": "#/$defs/Rev" },
        "sender": { "type": "string" },
        "session": { "$ref": "#/$defs/AgentSession" },
        "legacy_message_id": { "type": "string", "description": "Agent-chosen message_id carried over from before cut-over" },
        "revision": { "type": "integer", "minimum": 1 },
        "message": { "type": "object", "description": "Current admitted sender content" },
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } },
        "created_at_ms": { "$ref": "#/$defs/TimeMs" },
        "terminal": { "$ref": "#/$defs/AgentTerminal" },
        "delivered": { "type": "boolean", "description": "Delivery recorded for the current substantive revision" },
        "seen": { "type": "boolean", "description": "Mark Seen recorded for the current substantive revision" },
        "result": { "$ref": "#/$defs/OriginResult" },
        "availability": { "$ref": "#/$defs/Availability" },
        "advisories": { "type": "array", "items": { "$ref": "#/$defs/AgentAdvisory" } }
      }
    },
    "AgentEntry": {
      "type": "object",
      "required": ["entry_id", "item_id", "rev", "position", "kind", "author", "request_revision"],
      "properties": {
        "entry_id": { "$ref": "#/$defs/OpaqueId" },
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "rev": { "$ref": "#/$defs/Rev" },
        "position": { "type": "integer", "minimum": 1 },
        "kind": { "enum": ["reply", "lifecycle", "revision", "availability", "result"] },
        "author": { "enum": ["person", "origin", "server"] },
        "request_revision": { "type": "integer", "minimum": 1 },
        "at_ms": { "$ref": "#/$defs/TimeMs" },
        "text": { "type": "string" },
        "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } },
        "in_reply_to": { "$ref": "#/$defs/OpaqueId" },
        "state": { "enum": ["active", "withdrawn", "retracted"] },
        "availability": { "$ref": "#/$defs/Availability" },
        "result": { "$ref": "#/$defs/OriginResult" },
        "priority": {
          "type": "object",
          "required": ["level"],
          "properties": {
            "level": { "$ref": "#/$defs/PriorityLevel4" },
            "score": { "type": "number" }
          }
        },
        "delivery": { "$ref": "#/$defs/AgentDelivery" }
      }
    },
    "AgentDelivery": {
      "type": "object",
      "properties": {
        "delivered": { "type": "boolean" },
        "acted": { "type": "boolean" },
        "outcome": { "enum": ["handed", "not_handed"] },
        "reason": { "type": "string" },
        "agent": { "type": "string" },
        "rerouted": { "type": "boolean" },
        "file_uses": { "type": "array", "items": { "$ref": "#/$defs/AgentFileUse" } }
      }
    },
    "AgentRevisionResponse": {
      "type": "object",
      "required": ["rev", "revision"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "revision": {
          "type": "object",
          "required": ["request_revision", "message", "files"],
          "properties": {
            "request_revision": { "type": "integer", "minimum": 1 },
            "message": { "type": "object" },
            "files": { "type": "array", "items": { "$ref": "#/$defs/FileRef" } }
          }
        }
      }
    },
    "SentMessagesPage": {
      "type": "object",
      "required": ["rev", "messages"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "messages": { "type": "array", "items": { "$ref": "#/$defs/SentMessage" } },
        "next_page": { "$ref": "#/$defs/OpaqueId" }
      }
    },
    "SentMessageResponse": {
      "type": "object",
      "required": ["rev", "message"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "message": { "$ref": "#/$defs/SentMessage" }
      }
    },
    "AgentEntriesPage": {
      "type": "object",
      "required": ["rev", "entries", "older"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "entries": { "type": "array", "items": { "$ref": "#/$defs/AgentEntry" } },
        "older": { "type": "boolean" }
      }
    },
    "AgentFocus": {
      "type": "object",
      "required": ["clients"],
      "properties": {
        "clients": { "type": "array", "items": { "$ref": "#/$defs/FocusDevice" } }
      }
    },
    "FocusDevice": {
      "type": "object",
      "required": ["device_id", "device_name", "screen", "reported_at_ms", "in_front", "connected"],
      "properties": {
        "device_id": { "$ref": "#/$defs/OpaqueId" },
        "device_name": { "type": "string" },
        "screen": { "enum": ["inbox", "message", "chat", "settings", "signed_out", "not_in_front"] },
        "reported_at_ms": { "$ref": "#/$defs/TimeMs" },
        "in_front": { "type": "boolean" },
        "connected": { "type": "boolean" },
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "visible_entry_id": { "$ref": "#/$defs/OpaqueId" }
      }
    }
  }
}
```

- The message list includes retained terminal messages. It sorts by original
  acceptance, newest first, with a stable tie-breaker. Pass back the opaque
  `next_page` as `page` with the same filters. The last page omits `next_page`.
  `sender` and `legacy_message_id` are exact-match filters. The latter
  matches only pre-cut-over messages. Agents can use the two filters
  together to resolve their old references to `item_id`. The
  `session` filter is a UUIDv4. Malformed filters answer `400 invalid_field`
  with the field. Message pages contain up to 50 messages.
  A token binds the account, key and filters. Pages report their observed
  position, and may see edits made after page one.
- Historical revision reads follow §4 and return `404 not_found` for
  unavailable revisions.
- `PUT` compares against the message's integer `revision`. The message
  read's quoted `ETag` covers the entire resource version for conditional
  `GET`, including changes to terminal state and receipts.
- Entry ordering and `before` follow §4, including the default limit of
  50 and maximum of 200. `older` is true when another page
  precedes the returned one. Agent entries keep the common Entry fields and
  add the agent’s receipt details. Retraction retains its lifecycle entry.
  Answers and other terminal actions are read from `SentMessage.terminal`.
  A message without a session has no agent conversation and returns an
  empty entry page.
- Focus requires a see grant. The listing follows §5.1, with current devices
  ordered by last focus activity and then by device ID. A lapsed report ends
  listing and targetability immediately. See and push grants are independent
  and off by default. Focus is scoped to the enrolled key's account. A
  caller without the see grant receives `403 forbidden`. The v2 projection
  maps today's `client_id` to `device_id`, `reported_at` to
  `reported_at_ms`, `sender_id` plus `message_id` to `item_id`, and
  `visible_event_id` to `visible_entry_id`.

### 9.2 Writes

| Request | Body | Success body |
|---|---|---|
| `POST /v2/agent/messages` | `SendMessageBody` | `AgentWriteResult` with `message` |
| `PUT /v2/agent/messages/{item_id}` | `ReviseMessageBody`, with `If-Match: <revision>` | `AgentWriteResult` with `message` |
| `POST /v2/agent/messages/{item_id}/retract` | `RetractMessageBody` | `AgentWriteResult` with `message` and optional acted `entry` |
| `POST /v2/agent/messages/{item_id}/lifecycle` | `AgentLifecycleBody` | `AgentWriteResult` with `message` |
| `POST /v2/agent/messages/{item_id}/replies` | `AgentReplyBody` | `AgentWriteResult` with `entry` |
| `POST /v2/agent/messages/{item_id}/result` | `AgentResultBody` | `AgentWriteResult` with `message` and `entry` |
| `POST /v2/agent/messages/{item_id}/receipt` | `AgentReceiptBody` | `AgentReceiptResult` with `message` |
| `POST /v2/agent/messages/{item_id}/entries/{entry_id}/receipt` | `AgentReceiptBody` | `AgentReceiptResult` with `entry` |
| `POST /v2/agent/messages/{item_id}/entries/{entry_id}/file-use` | `AgentFileUseBody` | `AgentFileUseResult` |
| `POST /v2/agent/messages/{item_id}/advisories/ack` | `AgentAdvisoryAckBody` | `AgentAdvisoryAckResult` |
| `POST /v2/agent/sessions/{session}/end` | `EmptyBody` | `AgentWriteResult` |
| `POST /v2/agent/show` | `AgentShowBody` | `AgentShowResult` |
| `POST /v2/files` | The §5 upload headers and raw bytes | `FileCreated` (201) |
| `DELETE /v2/files/{file_id}` | None | No body (204) |

Writes return 200 unless the table says otherwise. `AgentWriteResult` may
also include the message or entry affected by the same transaction.

```json
{
  "$defs": {
    "EmptyBody": { "type": "object", "additionalProperties": false },
    "AgentFileIds": { "type": "array", "items": { "$ref": "#/$defs/OpaqueId" } },
    "AgentMessageContent": {
      "type": "object",
      "required": ["title", "created_at"],
      "properties": {
        "title": { "type": "string", "minLength": 1 },
        "created_at": { "type": "integer" },
        "body": { "type": "string" },
        "message_type": { "enum": ["fyi", "request"] },
        "answer_ui": { "type": "object" },
        "callback": { "type": "object" },
        "open": {},
        "progress": {}
      },
      "not": { "anyOf": [{ "required": ["sender_id"] }, { "required": ["message_id"] }] }
    },
    "SendMessageBody": {
      "type": "object",
      "required": ["sender", "message"],
      "additionalProperties": false,
      "properties": {
        "sender": { "type": "string" },
        "session": { "$ref": "#/$defs/AgentSession" },
        "supersedes": { "type": "array", "items": { "$ref": "#/$defs/OpaqueId" } },
        "message": { "$ref": "#/$defs/AgentMessageContent" },
        "file_ids": { "$ref": "#/$defs/AgentFileIds" }
      }
    },
    "RetractMessageBody": {
      "type": "object",
      "additionalProperties": false,
      "properties": { "in_reply_to": { "$ref": "#/$defs/OpaqueId" } }
    },
    "ReviseMessageBody": {
      "type": "object",
      "required": ["message"],
      "additionalProperties": false,
      "properties": {
        "message": { "$ref": "#/$defs/AgentMessageContent" },
        "file_ids": { "$ref": "#/$defs/AgentFileIds" }
      }
    },
    "AgentLifecycleBody": {
      "type": "object",
      "required": ["action"],
      "additionalProperties": false,
      "properties": { "action": { "enum": ["answered", "cleared", "expired", "superseded"] } }
    },
    "AgentReplyBody": {
      "type": "object",
      "required": ["text"],
      "additionalProperties": false,
      "properties": {
        "text": { "type": "string" },
        "file_ids": { "$ref": "#/$defs/AgentFileIds" },
        "in_reply_to": { "$ref": "#/$defs/OpaqueId" }
      }
    },
    "AgentResultBody": {
      "type": "object",
      "required": ["status"],
      "additionalProperties": false,
      "properties": { "status": { "enum": ["received", "applied", "refused"] } }
    },
    "AgentReceiptBody": {
      "type": "object",
      "required": ["outcome"],
      "additionalProperties": false,
      "properties": {
        "outcome": { "enum": ["handed", "not_handed"] },
        "reason": { "type": "string", "minLength": 1 },
        "agent": { "type": "string", "minLength": 1 },
        "rerouted": { "type": "boolean" }
      },
      "if": { "properties": { "outcome": { "const": "not_handed" } } },
      "then": { "required": ["reason"], "not": { "required": ["rerouted"] } },
      "else": { "not": { "required": ["reason"] } }
    },
    "AgentReceiptResult": {
      "type": "object",
      "required": ["rev", "status", "stage_outcome"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "status": { "enum": ["recorded", "already_recorded", "superseded"] },
        "stage_outcome": { "enum": ["handed", "not_handed"] },
        "message": { "$ref": "#/$defs/SentMessage" },
        "entry": { "$ref": "#/$defs/AgentEntry" }
      }
    },
    "AgentFileUseBody": {
      "type": "object",
      "required": ["file_id", "outcome"],
      "additionalProperties": false,
      "properties": {
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "outcome": { "enum": ["used", "unusable"] },
        "reason": { "type": "string", "minLength": 1 }
      },
      "if": { "properties": { "outcome": { "const": "unusable" } } },
      "then": { "required": ["reason"] },
      "else": { "not": { "required": ["reason"] } }
    },
    "AgentFileUse": {
      "type": "object",
      "required": ["file_id", "outcome"],
      "properties": {
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "outcome": { "enum": ["used", "unusable"] },
        "reason": { "type": "string" }
      }
    },
    "AgentFileUseResult": {
      "type": "object",
      "required": ["rev", "entry", "file_id", "outcome"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "entry": { "$ref": "#/$defs/AgentEntry" },
        "file_id": { "$ref": "#/$defs/OpaqueId" },
        "outcome": { "enum": ["used", "unusable"] },
        "duplicate": { "type": "boolean" }
      }
    },
    "AgentAdvisoryAckBody": {
      "type": "object",
      "required": ["advisory_id"],
      "additionalProperties": false,
      "properties": { "advisory_id": { "$ref": "#/$defs/OpaqueId" } }
    },
    "AgentAdvisoryAckResult": {
      "type": "object",
      "required": ["rev", "advisory_id", "delivered", "already_delivered"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "advisory_id": { "$ref": "#/$defs/OpaqueId" },
        "delivered": { "const": true },
        "already_delivered": { "type": "boolean" }
      }
    },
    "AgentShowBody": {
      "type": "object",
      "required": ["item_id", "target"],
      "additionalProperties": false,
      "properties": {
        "item_id": { "$ref": "#/$defs/OpaqueId" },
        "target": { "enum": ["message", "chat"] },
        "device_name": { "type": "string", "minLength": 1 }
      }
    },
    "AgentShowResult": {
      "type": "object",
      "required": ["show_id", "device_id", "device_name", "state"],
      "properties": {
        "show_id": { "$ref": "#/$defs/OpaqueId" },
        "device_id": { "$ref": "#/$defs/OpaqueId" },
        "device_name": { "type": "string" },
        "state": { "enum": ["sent", "held"] }
      }
    },
    "AgentWriteResult": {
      "type": "object",
      "required": ["rev"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "message": { "$ref": "#/$defs/SentMessage" },
        "entry": { "$ref": "#/$defs/AgentEntry" },
        "supersedes_warnings": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["item_id", "reason"],
            "properties": {
              "item_id": { "$ref": "#/$defs/OpaqueId" },
              "reason": { "enum": ["invalid", "unknown", "closed"] }
            }
          }
        }
      }
    }
  }
}
```

- **Send and revise.** The server issues `item_id`. A sender needing a local
  reference maps it to that ID. The message has today's required `title`
  and integer `created_at`. Its `body`, `answer_ui`, `callback`, `open`,
  `progress` and other content follow today's message validation. The
  request's top-level `sender` supplies sender identity on send. Neither
  `sender_id` nor `message_id` is a content input. Existing immutability
  and inheritance rules for message content, answer UI and progress apply
  on revise. Files are supplied by `file_ids`. An unauthorized sender
  answers `403 forbidden`. Revise requires `If-Match` with the current
  integer `revision`; a missing or mismatched precondition answers
  `412 precondition_failed`. A retracted message refuses revision with
  `409 already_retracted`. Progress updates are revisions of an FYI using
  its `progress` member. On send, `supersedes` closes earlier messages of
  the same agent under today’s transition rules. The response carries
  `supersedes_warnings` for any targets that cannot be superseded.
- **Lifecycle and retract.** Both stamp the time on the server. Lifecycle
  follows the existing transition rules. A repeat retraction under a new
  key answers `409 already_retracted`. A closed message that refuses the
  transition answers `409 already_closed`. A malformed lifecycle action
  answers `400 invalid_field` with `field: "action"`. When retract names a
  person entry in `in_reply_to`, it also marks that entry acted. Handoff
  still uses the receipt endpoint.
- **Replies.** Either side may reply any number of times in every item
  state, including after answer or retraction. If supplied, `in_reply_to`
  identifies an entry in that conversation. A send without a session has no
  conversation (§5), and replies to it answer `400 unsupported`. Feed
  activity is informational. There is no `accepts_replies` member.
- **Result.** Applies to the current answer. Before an answer it returns
  `409 not_answered`. `received` can advance to `applied` or `refused`.
  A conflicting result or a transition after a final result returns
  `409 already_closed`. A malformed status answers `400 invalid_field`
  with `field: "status"`. The server derives `result_ref` from the
  operation, as today. `already_answered` is reserved for the person's
  `/answer`.
- **Receipt.** A message receipt targets that item's answer. An entry
  receipt targets a person-authored chat turn of that item. `not_handed`
  requires a non-blank reason and may advance to `handed`. A handoff
  already recorded as `handed` stays handed. The optional `agent` is
  non-blank. `rerouted` is valid only for `handed`. The response reports
  whether the request recorded a new handoff, found that outcome already
  recorded, or was superseded by the existing handoff. It also carries the
  effective `stage_outcome` and affected message or entry.
- **File use.** The file must belong to the referenced delivered turn.
  Report `used` or `unusable`, with a reason required for `unusable`.
  An identical use report is a duplicate. A different report for the same
  file and turn conflicts with the existing one and answers
  `409 already_closed`.
- **Advisory acknowledgment.** Supply the advisory's ID from the item.
  The response reports whether it was already marked delivered.
- **End session.** `404 not_found` for an unknown session of this enrolled
  key. Otherwise it marks the session gone for the person. The affected
  messages change availability on the feed and keep their conversations.
- **Show.** A push grant is required (`403 forbidden` without it). The item
  must be in the same account. The server resolves an explicit
  `device_name` under the push grant alone. Otherwise it chooses the most
  recently active listed device when the request is made. An unknown item
  or missing target device answers `404 not_found`. It holds or sends the
  view as in §5.1 and returns the resolved device name and delivery state.
- **Files.** An agent uses the §4 device download response for
  `GET /v2/files/{file_id}`, including its stored type and quoted sha256
  ETag. It can download files it is authorized to read through its own
  messages and their conversations. An agent may discard only its own
  upload while it is unattached. An unknown or inaccessible file answers
  `404 not_found`. An attached upload answers
  `409 already_closed`.
- Durable changes use the same transaction as the corresponding per-agent
  change rows. Focus and held shows use relay memory as in §5.1.

### 9.3 Agent change feed

`GET /v2/agent/changes?after=<rev>&session=<session>&limit=<n>`

The JSON and SSE transports follow §6, including batch limits,
`Last-Event-ID` precedence, heartbeat at least every 25 seconds, and the
closed `Closing.reason` list. The feed applies §6.1 compaction. On
`410 cursor_expired` or a closing with `cursor_expired`, read all pages
of `GET /v2/agent/messages` with the same session filter, rebuild each open
item's entries and advisory state, then reopen at the first message page's
position. On ordinary reconnect, send `Last-Event-ID`. A filtered feed
can skip agent-wide revisions.

```json
{
  "$defs": {
    "AgentChange": {
      "type": "object",
      "required": ["rev", "kind", "data"],
      "properties": {
        "rev": { "$ref": "#/$defs/Rev" },
        "kind": { "enum": ["message", "message_removed", "entry", "advisory"] },
        "data": {}
      },
      "allOf": [
        { "if": { "properties": { "kind": { "const": "message" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/SentMessage" } } } },
        { "if": { "properties": { "kind": { "const": "message_removed" } } },
          "then": { "properties": { "data": {
            "type": "object", "required": ["item_id"],
            "properties": { "item_id": { "$ref": "#/$defs/OpaqueId" } }
          } } } },
        { "if": { "properties": { "kind": { "const": "entry" } } },
          "then": { "properties": { "data": { "$ref": "#/$defs/AgentEntry" } } } },
        { "if": { "properties": { "kind": { "const": "advisory" } } },
          "then": { "properties": { "data": {
            "type": "object", "required": ["item_id", "advisory"],
            "properties": {
              "item_id": { "$ref": "#/$defs/OpaqueId" },
              "advisory": { "$ref": "#/$defs/AgentAdvisory" }
            }
          } } } }
      ]
    },
    "AgentAdvisory": {
      "type": "object",
      "required": ["advisory_id", "advisory_type", "request_revision", "text"],
      "properties": {
        "advisory_id": { "$ref": "#/$defs/OpaqueId" },
        "advisory_type": { "enum": ["unseen_interval_elapsed", "possible_contradictions"] },
        "delivered": { "type": "boolean" },
        "request_revision": { "type": "integer", "minimum": 1 },
        "text": { "type": "string" },
        "accepted_at_ms": { "$ref": "#/$defs/TimeMs" },
        "due_at_ms": { "$ref": "#/$defs/TimeMs" },
        "evaluated_at_ms": { "$ref": "#/$defs/TimeMs" },
        "level_name": { "type": "string" },
        "bounce_after_days": { "type": "number", "exclusiveMinimum": 0 }
      }
    },
    "AgentChangeBatch": {
      "type": "object",
      "required": ["rev", "changes", "more"],
      "properties": {
        "rev": { "$ref": "#/$defs/Position" },
        "changes": { "type": "array", "items": { "$ref": "#/$defs/AgentChange" } },
        "more": { "type": "boolean" }
      }
    },
    "AgentFocusEvent": {
      "type": "object",
      "required": ["focus"],
      "properties": { "focus": { "$ref": "#/$defs/AgentFocus" } }
    }
  }
}
```

- A filtered feed omits changes for messages outside the named session.
  Every feed still uses the agent-wide `rev`. A batch position can be later
  than the last returned change, including in an empty filtered batch.
  Feed connections are independent, and any number may cover a session.
- Answers and terminal close-outs appear as `message` changes, including
  on sends without a session. The per-agent `rev` orders these changes
  against chat entries in commit order.
- The first time a person-authored chat entry or answer is served on the
  feed, the server records Delivered once. The receipt write is the
  explicit handoff to the model. Delivery and receipt changes update the
  same entry or message resource.
- A person sees the agent or session as listening while there is a covering
  open feed, or a covering batch was fetched in the last 60 seconds. Chat
  is stored and delivered on the next read regardless of feed activity.
- SSE may send `event: focus` with an `AgentFocusEvent` to a key with the
  see grant. It has no `id:` line and is excluded from batches and replay.
  Switching the see grant off stops focus events within 15 seconds.
- Change-log verification replays the retained agent log from empty and
  compares the resulting sent-message map with a fresh paged read.

### 9.4 Legacy commands

| v2 route or disposition | Legacy command or surface |
|---|---|
| `POST /v2/agent/messages` | `send` |
| `PUT /v2/agent/messages/{item_id}` | `revise`, including progress |
| `POST /v2/agent/messages/{item_id}/retract` (round 19) | `withdraw`, `retract` |
| `POST /v2/agent/messages/{item_id}/lifecycle` | `lifecycle` |
| `POST /v2/agent/messages/{item_id}/replies` | `reply` |
| `POST /v2/agent/messages/{item_id}/result` | `result` |
| `POST /v2/agent/messages/{item_id}/receipt` for an answer, or `POST /v2/agent/messages/{item_id}/entries/{entry_id}/receipt` for a chat turn (round 20) | `ack2` |
| `POST /v2/agent/messages/{item_id}/entries/{entry_id}/file-use` (round 20) | `file_use` |
| `POST /v2/agent/sessions/{session}/end` | `session_end` |
| `POST /v2/files` | `upload` |
| `GET /v2/files/{file_id}` | File fetch |
| `DELETE /v2/files/{file_id}` | `discard_uploads` |
| `GET /v2/me` (agent spec r2 §1) | `file_limits` |
| `GET /v2/agent/messages/{item_id}`, or `GET /v2/agent/messages/{item_id}/revisions/{n}` for a selected revision | `message_get` |
| `GET /v2/agent/changes` with optional `session` (agent spec r2 §4) | `feed`, change notices, session listen, coverage stream |
| Automatic when the feed serves the turn (agent spec r2 §4) | `ack` (ACK1) |
| Retired. Feed activity supplies availability (agent spec r2 §§0, 4) | `support`, `attachment_update`, `status`, `coverage` |
| Retired. Feeds are independent (agent spec r2 §§0, 4) | Exclusive listener, reconnect takeover |
| `advisory` changes on `GET /v2/agent/changes` (agent spec r2 §4) | `advisory_feed` |
| `POST /v2/agent/messages/{item_id}/advisories/ack` (round 20) | `advisory_ack` |
| `GET /v2/agent/focus` (round 19) | `focus_get` |
| Ephemeral `focus` on the agent SSE feed (round 19) | Focus stream |
| `POST /v2/agent/show` (rounds 19–20) | `view_show` |
| Client item and entry reads, or `GET /v2/search` for the exact legacy sender and ID pair (rounds 19–20) | Granted-agent `get` |
| `GET /v2/search` (rounds 19–20) | Granted-agent `search` |
| `POST /v2/items/{item_id}/answer` (rounds 19–20) | Granted-agent `respond` |

For example, an agent reporting a result before the person answers gets
`409` with this body and a matching `Request-Id` response header.

```json
{"error":"not_answered","retryable":false,"request_id":"Lk4nY3_qtbE4R1c9"}
```

## 10. Rulings applied

The delivery owner's rulings on this draft are applied above:

1. **Answer** carries at most one of `choice` (an action `id`, and the server
   stores its `value`), `comment` (the plain-text answer, accepted on every
   answerable item) and `result` (`attention-client.ui/1` only). With none of them, the answer is
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
9. **Structured answers:** an optional `result` of any JSON type, accepted only for
   `attention-client.ui/1` items (see ruling 1 above).
10. **Answer members are mutually exclusive** (see ruling 1 above).

Rounds 2 to 4 of the contract rulings, and the chat and answer rules, are
applied as well. Round 4 wins where rounds conflict:

- **Answer outcome:** `Terminal.answer` is today's `answer_outcome`. There are
  no answer entries, and `Terminal.no_comment` is replaced by
  `answer.kind: "no_comment"`.
- **Entries:** a `lifecycle` entry is a retraction only, receipts fold into
  `Entry.delivery`, and `Entry.at_ms` is optional before the cut-over.
- **Earlier revisions** are read through
  `GET /v2/items/{item_id}/revisions/{n}`. Revision entries are markers.
- **Files:** `Item.files` includes earlier files the current screen
  references. Upload names travel in `Content-Disposition`, and content types
  are kept.
- **Answers:** `comment` is accepted on any answerable item.
- **Chat** never closes.
- **Feed:**
  - the heartbeat is an id-less `heartbeat` event;
  - paused accounts get `account_paused`;
  - a device revoked while its connection is open gets `401`.
- **Inbox:** `Rank.section`, `Chat.unread_replies`, `Item.created_at_ms` and
  push state `expired`.
- **`GET /v2/me`:** `account`, `features`, and `limits` (upload cap, inline
  budget, the published UI/1 caps and the 30-day retention).
- **Sections:** `Rank.section`, `Rank.group`, `Chat.reply_group` and
  `Chat.reply_rank_key` (rounds 5 and 6).
- **Compacted log:** §6.1.
- **Push payload:** §6.2.
- **Round 7:**
  - reply-row fields are present whenever the item has an agent chat reply;
  - `Item.result` is the latest result, and each result fact is also a
    `result` entry. This supersedes round 4 item 2 for results.
  - Its addendum adds `Entry.priority` on agent replies and
    `Chat.reply_priority`, and nests a result entry's `result`.
- **Rounds 8 to 17:**
  - `Item.created_at_ms` is optional and display-only, and `Item.arrival`
    orders the new-messages area (8);
  - `Item.thread` and adjacent group members (9);
  - answerable means `terminal` is null, FYIs included (10);
  - focus and held shows are transport-only and never log rows (11, §5.1);
  - the batch position, and a row for every client-visible effect (12);
  - idempotency stores successes only, and replies need a conversation (13);
  - `Rank.score` and `due_at_ms` wherever today has them (14);
  - `Terminal.answer` gains `choice`, `Rank.level` follows the view entry's
    priority, and the `Entry.position` wording (15 and its addendum);
  - `result` is any JSON value, with a `result` outcome kind (16);
  - the closing reasons `cursor_expired` and `storage_unavailable` (17).
- `Chat.reply_group` may be null, where today's reply row has no group.

No questions are open.

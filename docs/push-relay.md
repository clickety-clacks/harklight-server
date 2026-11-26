# Harklight push relay contract

The relay lets a self-hosted Server deliver APNs pushes for a registered
device. The app fetches its messages directly from its own Server.

## Configuration

The Server uses `ATTENTION_SERVER_PUSH_RELAY_URL` in `server.env` when Apple
push keys are absent. Direct delivery takes precedence when the keys are
configured. The relay URL is unset by default, and new installs include
`# ATTENTION_SERVER_PUSH_RELAY_URL=https://app.harklight.net` as a commented
setting. To enable it, set the URL and restart the Server. Both transports
support the same [device registration](client-api-v2.md#5-writes).

## Relay routes

Both routes use JSON with a 4 KiB request body limit. They are available in
`saas` mode with Apple push keys configured and return `404` otherwise.

### `POST /push-relay/v1/servers`

Register with the Server's opaque 32-hex-character identity from `GET /v2/me`.

```json
{"server": "<32 hex Server id>"}
```

A successful registration returns `201` with
`{"key": "v1.<server id>.<signature>"}`. The signature is an HMAC over the
Server identity under a secret generated when the relay starts. The relay
keeps no registration records, and the caller holds the key in memory.
Invalid registration requests return `400 {"error": "invalid_request"}`.
The proxy may return `429` with `Retry-After`.

### `POST /push-relay/v1/pushes`

Authenticate with `Authorization: Bearer <key>` from registration. For
example, a passive alert request is:

```json
{
  "token": "<hex APNs device token>",
  "environment": "production",
  "kind": "alert",
  "badge": 3,
  "interruption_level": "passive"
}
```

- `token`, `environment` and `kind` are required. Use the device's registered
  APNs environment (`production` or `sandbox`) and a kind of `alert` or
  `background`.
- `badge` is optional and carries the attention list count as a non-negative integer.
- The optional alert presentation fields are `interruption_level` (only
  `"passive"`) and `sound` (only `"default"`). A passive alert carries
  `interruption_level` and omits `sound`. An alert with sound carries
  `sound` and omits `interruption_level`. Background pushes omit both.

These are the only push request body fields. The relay derives the Server
identity from the verified key. The request carries no message text, title,
sender, item locator or sync cursor. The Server chooses the kind and
presentation before sending the request.

### APNs delivery

The relay sends to the requested APNs environment with the topic
`co.clicketyclacks.Harklight`. It uses the same type, priority and collapse
ID selection as direct delivery.

| Request | `apns-push-type` | `apns-priority` | `apns-collapse-id` |
| --- | --- | --- | --- |
| Alert | `alert` | `10` | `attention-sync` |
| Background with a badge | `alert` | `10` | `attention-terminal-sync` |
| Background without a badge | `background` | `5` | `attention-terminal-sync` |

Every push contains the [v2 sync hint](client-api-v2.md#62-push-payload)
with the authenticated Server identity and no locator. For example, the
passive alert request above sends:

```json
{
  "aps": {
    "alert": {"body": "New message in Harklight"},
    "badge": 3,
    "content-available": 1,
    "mutable-content": 1,
    "interruption-level": "passive"
  },
  "attention": {"v": 2, "type": "sync", "server": "<Server id from the key>"}
}
```

Alerts use the fixed body and collapse under a single ID. The relay copies
`interruption_level` to `aps.interruption-level` and `sound` to `aps.sound`
only when supplied, with no defaults. Background pushes carry
`aps.content-available: 1` and the badge when supplied.

### Push responses

An APNs response is returned as HTTP `200` with `apns_status` and, when
available, `reason`, `timestamp` (epoch milliseconds) and `retry_after_ms`.
The only forwarded reasons are `BadDeviceToken`, `ExpiredProviderToken`,
`ExpiredToken` and `Unregistered`. The relay refreshes an expired APNs
provider token and retries that APNs request internally. The calling Server
interprets the returned APNs outcome as it would for direct delivery.

```text
200 {"apns_status": 200}
200 {"apns_status": 410, "reason": "Unregistered", "timestamp": 1795617745404}
200 {"apns_status": 429, "retry_after_ms": 60000}
```

| Relay response | Meaning or caller action |
| --- | --- |
| `400 {"error": "invalid_request"}` | Invalid request. |
| `400 {"error": "unsupported"}` | Unknown request member or value. |
| `401 {"error": "unknown_key"}` | Register again and retry once. |
| Proxy `429` with `Retry-After` | Ordinary retry. |
| `5xx` or no response | Ordinary retry. |

# Relay memory

The Relay keeps the current Engine, its indexes, the current priority and rank
views, and the current snapshot encoding cache. Snapshot encoding runs in the
store owner, where the cached records and compressed fragments can share memory
with the snapshot. The SQLite worker receives the encoded binary and transaction
rows. Snapshot encoding and blob cleanup run in the owner. The read barrier
still serves eligible reads from the committed generation while the SQLite
worker writes.

Retained snapshot memory grows with the number and byte size of live records
(messages, conversation and lifecycle events, sessions, receipts, and other
stored facts), the keys in their indexes and projections, and the compressed
bytes of the current snapshot. The cache holds one generation. During a write
the owner can temporarily hold the prior and next generations, with unchanged
records and compressed fragments shared. Encoding can also allocate an output
binary as large as the compressed snapshot. BEAM heap and allocator capacity,
SQLite, active connections, and temporary request data add to process RSS.
Unique Agent Show receipts add a traffic-dependent term for their existing
30-day retention window.

## Reproduce

Use a Linux development environment with Elixir 1.19.5 and OTP 27.3.3.
The script accepts a fresh directory for the seed and reopens only a directory
it marked as synthetic. Run each case in two separate BEAM instances with
two schedulers. The Server application starts its Registry and the script
starts the Relay directly. It opens no network listeners.

```sh
export ATTENTION_SERVER_ENABLE=0 MIX_ENV=prod ERL_FLAGS='+S 2:2 +SDcpu 1 +SDio 1'
mix deps.get
mix compile
scratch=$(mktemp -d)
mix run --no-compile scripts/relay-memory.exs seed "$scratch/fixture" 1600 1600 10
mix run --no-compile scripts/relay-memory.exs measure "$scratch/fixture" 1600 10
```

Pass the desired final message count, discussion event count, and session count
to `seed`. The fixture contains 20 fewer messages so `measure` can add the
last 20 through the Relay. It then repeats 750 existing operations, cursor
reads and bounded requests to `GET /v2/inbox`. It records RSS after a 15-second
idle period.
The script measures a forced major GC separately and reports process, ETS,
binary, and field sizes. A flattened field size counts shared subterms for
each reference and serves as an upper bound for copied heap terms.

## Measurements

Linux x86-64, two schedulers, 4 GiB cap. The baseline was `97f9a78`.
Values are in MiB. Each case starts with a fresh synthetic store.

| Messages | Discussion events | Sessions | Peak RSS before → after | RSS after 15 s idle before → after |
| ---: | ---: | ---: | ---: | ---: |
| 400 | 400 | 10 | 383 → 148 | 126 → 109 |
| 1,600 | 1,600 | 10 | 1,462 → 250 | 286 → 137 |
| 5,000 | 5,000 | 10 | 3,220 → 522 | 877 → 266 |
| 1,600 | 1,600 | 1,600 | 1,247 → 311 | 405 → 158 |
| 1,600 | 16,000 | 10 | 3,561 → 646 | 876 → 257 |
| 2,400 | 100,000 | 10 | Killed at 4 GiB during open → 2,364 | — → 958 |

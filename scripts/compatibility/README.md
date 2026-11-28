# Isolated compatibility and diagnostics fixtures

The commit profiler is off by default. `AttentionServer.CommitProfile.capture/2`
returns `{:error, :disabled}` without installing trace hooks unless enabled by
an explicit `enabled: true` option or the `:attention_server, :commit_profile`
application setting. The diagnostics module makes no changes to the store or
normal request path.

Capture an already running **synthetic fixture** Relay with:

```elixir
{:ok, report} = AttentionServer.CommitProfile.capture(relay,
  enabled: true, duration_ms: 10_000, interval_ms: 20)
```

Run load concurrently. Duration is capped at 90 seconds, interval is at least
10 ms, stack aggregation retains at most 128 distinct entries and eight frames
per stack, and each sample observes at most eight linked processes. The caller
has a duration-plus-one-second deadline. A private OTP 27 trace session is
removed when collection ends or its tracer dies. Capture errors never kill the
Relay. No arguments, return values, messages, dictionaries or stored payloads
are collected. No diagnostics are automatically enabled on startup.

The report contains coverage counts, aggregate Relay/linked-process MFA stacks,
Relay queue samples and per-function runtime counters. Missing function matches
are missing coverage, not zero cost. Runtime counters are not wall latency;
traced callees have separate attribution. Linked stacks include the supervisor,
so separate that denominator from the Relay and persistence-worker samples.
Sampling can wait on the target scheduler. Measure baseline and enabled runs at
the same load and resource budget before interpreting profiler overhead.

The package carries a small, fresh-state smoke fixture:

```sh
ATTENTION_SERVER_ENABLE=0 HARKLIGHT_FIXTURE_DIR=/absolute/new-private-directory \
  FIXTURE_SCRIPT=/absolute/package/fixtures/compatibility/profile.exs \
  /absolute/package/bin/attention_server eval \
  'Code.require_file(System.fetch_env!("FIXTURE_SCRIPT"))'
```

The directory must not exist. The script creates six fresh synthetic stores,
runs 100 warm-up and 100 measured sends in each, and alternates three disabled
and enabled pairs. It writes `profile.json` and stops each Relay. Coverage checks
reject an incomplete capture. Delete the fixture directory after retaining the
report. This is a small matched overhead smoke, not a multi-organization stream
capacity benchmark or release-history gate. It starts no listener or provider.

One isolated 2 CPU / 4 GiB run on OTP 28.5 / Elixir 1.19.5 measured 4.4%,
30.9% and 37.6% longer send bands with capture enabled (three paired 100-send
bands, after equal warm-up). These short runs are noisy and are not a capacity
prediction. Capture is an intrusive measurement: keep it opt-in and compare a
matched disabled run for every workload. The disabled path installs no hooks.

## Portable history and API scenarios

Run the shared contract from source with:

```sh
ATTENTION_SERVER_ENABLE=0 mix test test/compatibility_scenarios_test.exs
```

Run from an extracted package, inside an isolated synthetic test environment:

```sh
package=/absolute/package
fixture=/absolute/new-synthetic-fixture
"$package/fixtures/compatibility/run.sh" "$package" "$fixture" seed
"$package/fixtures/compatibility/run.sh" "$package" "$fixture" verify
```

`seed` requires a new directory and creates synthetic certificates, two people,
a local organization, a group/invitation, four messages, a revision, conversation
replies, an answer, a shared read position, an attachment, an Agent session and
handoff receipt. It writes `manifest.json`. `verify` reopens that history and
compares every recorded resource field, cursor result and attachment byte, then
retries the handoff receipt. The ExUnit entry also verifies rejection of an
intentionally corrupted manifest and exercises 410 after real retention cleanup.

The scenario checks fixed expected content independently before recording the
manifest. Operation IDs, session ID and content are fixed; Server-assigned IDs,
certificate identities and wall timestamps are captured, not assumed reproducible
across independently seeded databases. Compare the same seeded history before
and after an upgrade or migration. Keep the original manifest as the expected
input; never regenerate it from the candidate. `differences/2` returns field paths
and values. The fixture refuses changed IDs as well as changed content.

The manifest includes the initial inbox snapshot, saved cursor, catch-up rows,
paginated-baseline equivalence, Last-Event-ID precedence and idempotency outcome.
It ends its synthetic Agent session before the durable snapshot: an active
Agent's listening status intentionally resets on restart. Exact fields, including
resource revisions, are compared without a blanket revision/timestamp filter.
The portable runner exercises Router/Plug request handling, not TLS transport,
SSE socket draining, real iPhone/iPad/Web rendering or a multi-node deployment.
Those remain separate acceptance checks.

After retaining the continuity fixture, test expiration on a disposable copy:

```sh
"$package/fixtures/compatibility/run.sh" "$package" "$fixture" expire
```

`expire` advances the injected clock by 31 days and triggers the existing scheduled
cleanup message, then requires 410 for the saved cursor. It intentionally makes
that fixture unsuitable for subsequent continuity verification. No SQL, Store
structs or persistence formats are referenced by the scenarios. Future backends
can preserve the `run(relay, fixture)` and `verify(relay, fixture, manifest)` entry
points, supply their Relay options to `start/2`, or own startup/provisioning around
the same public API calls. The package needs OpenSSL for synthetic certificates.
No listener or external provider is started. Remove the synthetic directory when
finished; retain manifests and reports without its private certificate keys.

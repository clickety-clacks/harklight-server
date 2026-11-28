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

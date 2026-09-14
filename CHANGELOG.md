# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ErrUnsupportedOnTransport`, a typed refusal for a protocol message the
  selected transport cannot carry. It classifies `RetryNever`: the same request
  over the same transport fails identically, and no new connection changes
  that.

  It has one reachable cause today, and it is load-bearing. geode 0.8.15 serves
  `Execute` over gRPC as a SERVER-STREAMING RPC — the engine pushes the schema
  and then every data page itself — so there is no client-driven `PULL` and
  `GeodeService` declares no method that could carry one. The driver's `Rows`
  pages with `PULL`, so **a gRPC result longer than the engine's gRPC page
  (128 rows) cannot be paged**. It now stops with this error instead of the
  previous opaque `unknown message type`, so a short result set can never be
  mistaken for a complete one. Callers that must page large results use the
  QUIC transport.

- Query-plan metadata. `Pool.Explain` / `Pool.Profile` (and the same pair on
  `Session` and on the driver `Conn`) return typed `*ExplainPlan` and
  `*ProfileReport` values, so a retrieval path can RECORD which access path a
  seed query used instead of guessing. `ExplainPlan.UsesIndex()` is the
  assertion that matters and it FAILS CLOSED: an empty plan, a nil plan and a
  plan the driver could not read all report `false`.

  Arguments are bound as parameters through the same `bindParameters` path a
  real query uses, so the plan describes the parameterised statement — no
  value is ever concatenated into the statement text.

  Both calls are gated on the engine advertising `explain` / `profile` in
  `SHOW CAPABILITIES`, and the gate fails closed in both directions: a
  capability the engine does not list, AND a capability probe that could not be
  completed, refuse the call with an error matching the new
  `ErrCapabilityUnsupported` before anything reaches the wire. That matters
  most for `PROFILE`, which EXECUTES the statement. The probe result is cached
  per connection, so it costs one extra round trip per connection lifetime.
  `ErrCapabilityUnsupported` classifies `RetryNever`.

  Wire shape against the pinned engine (0.8.15): EXPLAIN and PROFILE are NOT
  row-shaped. The QUIC and gRPC handlers answer with a dedicated
  `ExplainPayload` / `ProfilePayload` protobuf submessage and no result rows
  (`src/server/quic_connection_handler.zig`, `src/transport/grpc_server.zig`);
  the metric/value text rows documented in the engine's
  `docs/EXPLAIN_PROFILE.md` are produced by an execution-layer path those
  transports never reach. Both shapes are decoded, so the driver keeps working
  against an engine that answers either way.

  Three engine facts the typed values surface rather than hide:

  - `ProfileReport.Columns` is 0 whenever the engine answered with the
    structured payload — no column count is on the wire in either payload
    variant (`src/proto/geode.proto:417-436`). Only the text-row form carries
    one.
  - `ProfileReport.Measured()` reports whether the timings are real. A
    concrete TENANT scope demotes a data-touching read off geode 0.8.15's
    planned read runtime (GAP-0846/GAP-0509), and the statement fallback that
    then answers builds per-operator times from a deterministic synthetic
    model rather than a clock. A tenant-bound connection therefore gets
    `profile_version` 1 and synthetic timings; `Measured()` returns false and
    the numbers must be read as plan shape, not latency. The guard is a
    disjunction (`src/execution/planned_read_runtime.zig:246`): an
    RLS-evaluating session is demoted too, so dropping the tenant does not buy
    measured timings back.
  - `ProfileReport.UsesIndex()` and `IndexNames()` report index evidence ONLY
    from a measured profile, and never from the engine's `heuristic_index`
    placeholder. geode 0.8.15's statement fallback appends a `NodeIndexScan`
    operator for any equality predicate with NO catalog lookup
    (`src/server/profile_payload.zig:39`) and names it `heuristic_index`
    (`:194-195`); the catalog lookup runs only for EXPLAIN
    (`src/server/quic_connection_handler.zig:6526`). Ungated, PROFILE claimed
    an index for an unindexed `WHERE n.note = ?`. Ask EXPLAIN for the access
    path; PROFILE's answer on `profile_version` 1 is not evidence.

  `ErrPlanKindMismatch` (`RetryNever`) refuses a plan response whose payload
  does not match the request: the two payloads share one response oneof, so
  taking whichever arrived handed a `PROFILE` caller a zero-valued report with
  a nil error — indistinguishable from a brute-force scan.


- Retry classes. `Classify(err)` maps any error to a `RetryClass` —
  `RetryNever`, `RetrySameConn`, `RetryNewConn` or `RetryBackoff` — so a bulk
  ingest caller can tell "re-run this on the same connection" from "re-acquire
  first", "wait first" and "stop". `RetryClass.String()` returns a stable
  lowercase name (`never`, `same_conn`, `new_conn`, `backoff`) suitable as a
  metric label. Classification FAILS CLOSED: an error the driver does not
  recognise is `RetryNever`, so a worker never loops on a permanent failure it
  cannot read.

  The table, with the engine source each server-side entry is pinned to
  (geode 0.8.15 `src/server/proto_util.zig`):

  - `RetryNever` — context cancellation and deadlines; `ErrReadOnly`,
    `ErrTenantRequired`, `ErrTenantScopeRequired` and the other deterministic
    client-side refusals; `ErrPoolDraining`; GQLSTATE classes 00/01/02, 0A, 22,
    23, 28, 42 (including 42501 permission denied), 53 (executor budget
    exceeded — the working set does not shrink on a retry) and 57 (query
    cancelled by the stall watchdog); `08006`, the clustered-write timeout whose
    outcome is UNKNOWN and which the engine documents as retryable ONLY with an
    idempotency key; `08P01` password-reset-required; and anything unclassified.
  - `RetrySameConn` — GQLSTATE class 40 (40001 serialization failure, 40502
    deadlock). Nothing applied and the connection is fine.
  - `RetryNewConn` — `ErrBadConn`, `driver.ErrBadConn`, `ErrSessionRetired`,
    `ErrClosed`, `ErrInvalidState`, `ErrTxDone`, any `*TransportError`, GQLSTATE
    class 08 and class 25 — and the engine's `ClusterNotLeader` /
    `ClusterWriteNotReplicable` refusals, which arrive as 40001 but say "retry
    against the current leader", so retrying on the same connection would ask
    the same follower forever.
  - `RetryBackoff` — `ErrPoolExhausted` and GQLSTATE class 58 (system error,
    also the engine's catch-all for an unmapped error).

  `RetryPolicy.Classes map[RetryClass]bool` narrows which classes `Retry`
  retries; nil means every class except `RetryNever`. `Retry` honours the class:
  a `RetryBackoff` wait is JITTERED (drawn from the upper half of the policy's
  exponential delay) so a herd refused by one shared resource does not return in
  lockstep, while the per-connection classes keep the exact policy delay. An
  error `Retry` declines to retry is returned wrapped with the class that
  stopped it; `errors.Is`, `errors.As`, `IsDriverError` and `Classify` all still
  work through the wrapper.

  `Pool.RetrySession(ctx, policy, fn)` is the class-aware form for a caller that
  needs one connection across an attempt: it owns the `Session`, so on a
  `RetryNewConn` failure it closes the pinned connection and pins a fresh one
  before the next attempt — which plain `Retry` cannot do, having no handle on
  the connection. It also absorbs the SONNY-1227 pin-after-retirement case,
  whose first write fails with a `driver.ErrBadConn`-matching error.

  `IsRetryableError` is now exactly `Classify(err) != RetryNever` and is
  DEPRECATED in favour of `Classify`. It answers `true` for strictly more errors
  than before — a bare `ErrBadConn` or `ErrPoolExhausted` used to report `false`
  because neither implements the `Error` interface — and for no fewer.

- Pool health probing and graceful drain. `Pool.Health(ctx)` returns a
  `HealthStatus` (`Healthy`, `PooledConns`, `LiveConns`, `InFlightStreams`,
  `LastProbeErr`, `LastProbeAt`) whose zero value is unhealthy — health fails
  closed — and whose `LastProbeErr` is a static classification rather than the
  underlying error, so a readiness endpoint cannot leak a transport error's
  remote address. Behind it, `(*QUICPool).Probe(ctx)` verifies every pooled QUIC
  connection with a REAL round trip (a protocol-level PING on a long-lived probe
  stream) and evicts the ones that do not answer, so `acquire` can never hand
  out a connection the server has forgotten. A pool-owned goroutine runs the
  probe every `Config.HealthProbeInterval` (DSN `?health_probe_ms=N`, default
  1000 ms, 0 disables); it starts lazily on first use, never runs concurrently
  with itself, and is stopped and waited for by `Drain` and `Close`.
  `Pool.Drain(ctx)` is the graceful counterpart to `Close`: new work is refused
  with `ErrPoolDraining` (including `Pool.Session`, which reports it instead of
  blocking), in-flight statements are given until `ctx` expires, and only then
  is everything closed; it is idempotent and closes the pool even if `ctx`
  expires first.

  A probe never closes a stream on a connection that is still in the pool.
  A failed probe ABANDONS its stream — it takes `probeStrikes` (2) consecutive
  failures to evict a connection, so at strike 1 the connection is still
  serving, and geode 0.8.15 answers a stream closing on a pooled connection by
  tearing the whole connection down (SONNY-1227). The next probe opens a fresh
  stream; abandoned streams are closed with the connection itself, and a
  connection that has abandoned `probeStrikes` of them is condemned, which
  bounds them. Two budgets follow from that: a reused stream gets the
  steady-state budget, while a replacement stream gets it plus a stream-setup
  allowance, because the server materialises a probe stream when its first
  frame arrives (~70 ms measured) and charging that to a bare round trip was
  enough to evict a healthy connection after one slow moment. A probe that
  fails because the CALLER's context ended is reported but records no strike
  and does not disturb the stream, so a short or cancelled `Health` context can
  never condemn the pool. A connection no HELLO has run on yet is skipped by
  the probe and is counted neither live nor healthy.

  This replaces a local-only liveness check that could not see an orphaned
  connection: after an ungraceful server restart the pool kept handing out a
  pre-restart connection and every statement on it waited out its caller's full
  deadline (measured: 6.000 s of a 6 s budget against engine 0.8.15).
  Downstream watchdogs written to work around that are no longer needed
  (SONNY-806).

- Transaction pinning: `Pool.Session(ctx)` checks one connection out of the
  pool and returns a `*Session` whose `ExecContext`, `QueryContext` and
  `TX(ctx, opts, fn)` all ride that one connection — and therefore one QUIC
  stream — so a multi-statement ingest or a run of transactions cannot straddle
  connections. `Session.ConnID()` reports the pinned connection's identity
  (also exposed on the driver connection as `(*Conn).ConnID()`), and
  `Session.BoundGraph()` the graph it is bound to. `TX` runs the whole callback
  on the pinned connection, commits on a nil return, rolls back on an error,
  and rolls back then re-panics on a panic; a nested `TX` — or any statement
  sent to the `Session` instead of to the `*sql.Tx` — reports `ErrSessionBusy`
  instead of deadlocking. Once the pinned connection fails (a cancelled query
  retires it, a transport error marks it), the `Session` is retired rather than
  silently re-pinned to a different connection: every method then returns
  `ErrSessionRetired`, which matches both `ErrSessionClosed` and
  `driver.ErrBadConn`. `Close` is idempotent and releases the connection
  exactly once. Caller contract after a retirement (SONNY-1227): geode 0.8.15
  closes the whole pooled QUIC connection when the stream that carried its
  handshake goes away, so a `Session` pinned immediately after a retirement may
  fail on its first statement with an error matching `driver.ErrBadConn` —
  close that `Session` and pin again. `Pool.Session` deliberately does not hide
  this behind a validating round trip at pin time (SONNY-806).
- Capability discovery: `Connector.ServerInfo(ctx)` reports the engine's
  `SHOW VERSION` string verbatim, the raw ordered `SHOW CAPABILITIES` rows
  (`ServerInfo.Rows`), the boolean feature matrix built only from rows whose
  value is exactly `true`/`false` (`Capabilities`, with `Has`/`Names`; unknown
  features are unsupported), the version-valued metadata rows kept out of that
  map (`Metadata`, currently only `sonny_profile_version`), and the HELLO
  response metadata. Values are never coerced or trimmed, duplicate and empty
  feature names are rejected, and a non-boolean value from a feature that is
  not known metadata is an error rather than a silent `false`
  (`ErrMalformedCapabilityRows`). Feature-name constants (`CapVectorKNN`,
  `CapReadOnlyExecute`, ...) cover all 59 rows geode 0.8.15 emits and are
  pinned to `testdata/capabilities_0.8.15.golden` (SONNY-806).
- The server's HELLO capabilities were previously decoded and discarded. They
  are now captured per connection and exposed via `(*Conn).HelloCapabilities()`
  and `ServerInfo.HelloCapabilities`, alongside `(*Conn).BoundGraph()` and
  `(*Conn).SessionID()`. HELLO capabilities and `SHOW CAPABILITIES` are
  different vocabularies; neither is derived from the other (SONNY-806).

### Fixed

- A failed liveness probe could leak one server-side probe stream per failure.
  The bound on abandoned probe streams was evaluated only in
  `recordProbeFailure`, which the probe round skips whenever the round's context
  ended — so a connection that failed legitimately and was then outlasted by a
  slower connection in the same round kept its stream, took no strike, and
  stayed pooled. The bound is now enforced where the list grows
  (`abandonProbe`), so it holds on every path, and an eviction the bound
  condemned happens even on the cancelled path. What it protects is the peer's
  QUIC stream credit: an abandoned stream is never closed or FINished, so the
  server keeps its state and its `MAX_STREAMS` credit charged against the
  connection.

- `(*DriverError).Unwrap`, `(*TransportError).Unwrap`, `(*SecurityError).Unwrap`
  and `(*DriverError).Is` no longer panic on a nil receiver. `errors.Is` and
  `errors.As` call these on whatever they are handed, so a typed-nil error that
  escaped a caller would take down every error-inspection path it reached.

### Security

- Read-only connections: `?read_only=true` (aliases `readonly`, `read-only`)
  refuses mutating statements client-side with `ErrReadOnly` before any bytes
  reach the wire, and sets `ExecuteRequest.read_only` so the engine refuses
  them too with GQLSTATE `42501` regardless of role. Classification
  (`IsMutatingStatement`) fails closed: empty, unparseable and unrecognised
  statements, unknown `CALL` targets, and any `;`-separated program containing
  a write are all treated as mutating (SONNY-806).
- `?` placeholders are no longer inlined into the query text. Every argument
  now crosses the wire as a typed `proto.Param` (`$_gqN`), so no argument value
  can reach the GQL parser as source text (SONNY-806).

### Changed

- Query arguments are now bound as: `database/sql`'s `driver.Value` scalar set
  (`nil`, `string`, `[]byte`, `bool`, `int64`, `float64`); `time.Time`, encoded
  as the historical `fmt.Sprintf("%v", t)` string (round-trippable via
  `ScanTime`); other Go integer/float types and anything implementing
  `driver.Valuer`, narrowed by `database/sql`'s default converter (`int` ->
  `int64`, `float32` -> `float64`, etc.); and slices, arrays and maps, which
  keep their structural `list_val`/`map_val` encoding. Any other struct or
  type is rejected with an error instead of being silently formatted into a
  string.
- Parameter names beginning with `_gq` are reserved by the driver and rejected.
- `ParseDSN`: a DSN option repeated across the query string now resolves to
  its LAST occurrence for every option, not the first, so a DSN can be
  extended by appending overrides (e.g. a shared base DSN plus a per-call
  `&tenant=...&graph=...`) (SONNY-806).
- `BatchExec` and the `graph` pipelines now retry pool exhaustion with backoff
  (previously failed fast), a consequence of `ErrPoolExhausted` classifying
  `RetryBackoff` under the new retry classes (SONNY-806).

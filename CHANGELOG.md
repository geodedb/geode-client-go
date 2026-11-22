# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `sql.Result.RowsAffected()` now reports what an `Exec` changed instead of
  always 0 (SONNY-2822), so a conditional `MATCH ... SET` can tell a match from
  a miss. It reads `DataPage.update_counts`, which the engine sends on the
  final page from geode a6de87812 (first release after 0.8.25): nodes and
  relationships created plus deleted, or, for a statement that only sets
  properties or labels, property assignments plus label changes. The per-kind
  counts are available from `(*geode.Result).UpdateCounts()`. Against an
  engine that sends no counts the result is 0, as before.

### Changed

- The vendored `proto/geode.proto` is synced with geode main c4bd96acc
  (`UpdateCounts`, `CancelRequest`/`CancelResponse`, savepoint response
  errors, and the Savepoint, RollbackTo, Backup, Restore and UploadBackup
  RPCs).

- Engine pin moved to **geode 0.8.22** (`geodedb/geode:0.8.22`, manifest-list
  digest `sha256:867d93375470fd18c60336036196a9ef9715a1922e063e8787dc05d8e798ddce`).
  Renovate moved the `.gitlab-ci.yml` service on 2026-10-12 but not the other
  pin sites, so `TestIntegration_PinnedEngineVersion`, `ServerInfo*` and
  `GRPCServerInfo` failed on main. The Makefile default, `defaultGeodeImage` and
  `pinnedEngineVersion` now match it. The capability golden passed unchanged
  against 0.8.22.

## [v0.2.0] - 2026-09-16

The GraphRAG driver surface (SONNY-806 / GEO-GO-UP-001).

Engine pin: this release is developed and tested against **geode 0.8.18**
(`geodedb/geode:0.8.18`, manifest-list digest
`sha256:5ebbc7c5b8d13375749e7c1df8a77e54935bc609321cd4d8107d8e26f370c6f1`).
`engine_pin_test.go` asserts the running engine reports it, and the 0.8.18
capability contract is pinned as a 59-row golden file. Behaviour described
below as an "engine fact" was, unless the entry says otherwise, established
against **0.8.15** and re-checked against 0.8.18 only where a test asserts it;
an engine fact may change with the engine.

0.8.18 is the first PUBLISHED image carrying the 0.8.16 fix set. The engine's
`v0.8.16` and `v0.8.17` tags exist but never produced a container image, so
neither is pinnable. Moving from 0.8.15 to 0.8.18 changed exactly one row of
the capability contract — `graph_procedure_tenancy` flipped `false` -> `true`
(SONNY-1326) — and the golden was regenerated through the documented
`GEODE_UPDATE_GOLDEN=1` path and renamed to
`testdata/capabilities_0.8.18.golden`. `tenant_scope_fail_closed` and
`tenant_binding_server_verified` are still `false` at 0.8.18.

The pin moved to **0.8.18** on 2026-09-16 and the fixes below were measured,
not assumed. Two results matter and they point in opposite directions.

**The engine got better.**

- **SONNY-1229 / GAP-SONNY806-02 (Critical) is FIXED.** Against 0.8.18,
  `TestIntegration_CrossTenantReadIsDenied` PASSES. Tenant binding is an
  isolation boundary on the `MATCH` read path again. (Engine `0beb108b`, with
  the graph-procedure and subquery halves in SONNY-1318 / SONNY-1326.)
- **SONNY-1560 / GAP-SONNY806-07 (High) is FIXED in shape and in blast
  radius.** 0.8.15 parked for ~30 s and let a DIFFERENT session's transaction
  fail mid-way. 0.8.18 surfaces a typed gRPC `ResourceExhausted` —
  `stream window exhausted: peer stopped draining the pushed result` — after
  5632 of 20000 rows in **0.62 s**, and
  the cross-session blast radius is gone: `TestIntegration_GRPCBatchExecPastOnePage`
  no longer fails around statement 29. It still failed, at a statement number
  that moved run to run, for an unrelated driver reason — see the
  finished-`Execute`-stream entry under Fixed — and passes now.
  `TestIntegration_GRPCLargePushedResultFailsLoudly` was RED against it:
  it enumerated `io.EOF`-with-every-row or `context.DeadlineExceeded` and had
  no arm for a promptly surfaced typed error, so the engine's IMPROVEMENT
  tripped the test's `default` case. It now has an arm for a surfaced
  `*TransportError`, keyed on the driver's own error type rather than on
  message text, and that arm still fails the test if a complete result arrives
  with an error attached. The invariant the test exists to protect — never a
  silent truncation — is untouched: a short result reported as a clean `io.EOF`
  is still a hard failure, whatever the engine version.
- The capability contract moved by exactly one row:
  `graph_procedure_tenancy` `false` -> `true`. `tenant_scope_fail_closed` and
  `tenant_binding_server_verified` are still `false`.

**The driver got worse, and it is now fixed.**

- **The SONNY-1219 anchor stream dropped the session's tenant and graph scope
  on every request stream (GAP-SONNY806-08, Critical, DRIVER defect) — FIXED.**
  Moving HELLO onto a dedicated anchor stream made the sibling request streams
  skip it by design, and HELLO is what tells the engine which tenant and which
  graph a stream is. A tenant-bound session read **zero rows, including rows it
  wrote itself**, with no error anywhere: a silent wrong answer. Bisected
  against the engine, so it could not be blamed on the bump:

  | driver commit | engine | `ParamsBindInPropertyMaps` | `CrossTenantReadIsDenied` |
  |---|---|---|---|
  | `29d373b` (pre-anchor) | 0.8.15 | PASS | FAIL - the old cross-tenant LEAK |
  | `29d373b` (pre-anchor) | **0.8.18** | **PASS** | **PASS** |
  | `05b4653` (anchor) | 0.8.15 | FAIL - `no rows in result set` | FAIL - `tenant-a saw 0 of its own nodes` |
  | `3c14794` (anchor) | 0.8.18 | FAIL - `no rows in result set` | FAIL - `tenant-a saw 0 of its own nodes` |
  | **this change** | **0.8.18** | **PASS** | **PASS** |

  `05b4653` is the introducing commit. The up-to-8 "redundant" per-stream
  HELLOs it removed were what carried the binding onto each request stream.

  **The fix binds every stream rather than reverting the anchor.** Reverting
  would have been smaller, and at 0.8.15 it would even have worked — but only
  by accident. At 0.8.15 the engine destroyed a pooled connection as soon as
  its first request stream closed, so almost every request stream was a FIRST
  stream performing its own handshake; the unbound-sibling window existed and
  was simply rarely survivable. Engine 0.8.18 (`f80a812d`) keeps a connection
  alive until its LAST stream drains, so siblings now live — and a plain revert
  would leave them alive AND unbound, reproducing this same defect. So
  `pooledHello` now performs an unconditional HELLO on every request stream,
  and `pooledGraphInit` reads the binding from the stream's own handshake
  response rather than from the pooled connection's. There is no state in which
  a request stream can be issued unbound. The anchor stream is kept for the one
  thing it is actually for: owning the QUIC connection's lifetime so a finished
  request cannot take its siblings down with it (SONNY-1219).

  The cost is one round trip per logical connection, which is what the driver
  paid before `05b4653`.

  Also RED for the same reason and now green:
  `TestIntegration_ParamsAreNotInQueryText`,
  `TestIntegration_ParamsBindTimeValue`,
  `TestIntegration_ProfileTimingsAreSyntheticOnTheTenantBoundPath`.

  Regression coverage, written before the fix and RED against it:
  `TestIntegration_TenantBoundSessionReadsItsOwnWritesOnEveryStream` (a
  tenant-bound session writes and reads across four simultaneously held request
  streams, and a second tenant must see none of it) and, with no engine needed,
  `TestAnchor_RequestStreamCarriesTenantAndGraphBinding` /
  `TestAnchor_EveryRequestStreamBindsItself`, which assert on the WIRE that
  each request stream's own transport carries a `HelloRequest` naming the
  configured tenant, graph and role.

Known engine limit on the gRPC wire: **a single `Execute` stalls past roughly
64 KiB of pushed payload** (GAP-SONNY806-07 / SONNY-1560, High —
engine/grpc-zig does not act on HTTP/2 `WINDOW_UPDATE`). gRPC is usable for
small results only at 0.8.15 and **QUIC remains the release transport**; a
stalled result surfaces as an error, never as a short result set that looks
complete. The stall's blast radius is not confined to the caller that
triggers it: the engine parks for ~30 s while it holds the flow-control
window open, and a **different session's** multi-statement transaction issued
against the same engine process during that window can fail mid-way with
`driver.ErrBadConn` (measured: `TestIntegration_GRPCBatchExecPastOnePage`
fails around statement 29 of 647 when run immediately after the stall
reproducer, and passes alone). See `docs/GAP_ANALYSIS.md` for the full
measurement. **Fixed at the pinned 0.8.18** (`33a39499`, `3d3b1b60`, grpc-zig
v0.1.10): the stall is now a prompt typed `ResourceExhausted` and the
cross-session blast radius is gone. The paragraph above describes 0.8.15 and is
kept as the record of what was measured there.

### Added

- gRPC transport parity lane. ENG-4658 requires the feature set to pass
  "QUIC/TLS 1.3 **and** gRPC integration/race tests". The gRPC transport shares
  `Conn`, `Rows`, the read-only classifier, the parameter binder, the plan
  decoder and the `Session` layer with QUIC, but NOT the transport itself and
  NOT the QUIC connection pool — so none of the QUIC evidence carried over by
  construction. `integration_grpc_test.go` now mirrors each property against a
  real gRPC listener on the SAME pinned engine, with a `GRPC` infix so the
  parity is greppable (`TestIntegration_ParamsBindInPropertyMaps` ->
  `TestIntegration_GRPCParamsBindInPropertyMaps`): typed parameter and vector
  binding, the client read-only gate AND the engine's separate gRPC `read_only`
  floor, strict tenancy, cancellation (prompt abort, wrapped
  `context.Canceled`, connection retired), typed EXPLAIN/PROFILE payloads with
  the index control arm, `ServerInfo`, and `Session` pinning.

  A `make test-integration-docker-grpc-race` target and a
  `integration:geode:grpc` CI job run the lane under `-race`.

  Connecting to the engine's gRPC listener needs two things the obvious DSN
  does not give you, both now handled by the harness, the Makefile and CI:
  the listener is **TLS-only** in a release build (so
  `insecure_tls_skip_verify=true`, not `tls=0`, against the engine's
  self-signed certificate), and `--listen-grpc 0.0.0.0:PORT` binds **loopback**
  anyway — the address argument is parsed and discarded — so the server needs
  `GRPC_ZIG_BIND_ADDR=0.0.0.0` before a published container port reaches it.

- `Transport.PagesArePushed() bool`, the predicate that says who drives paging
  on a wire: `false` on QUIC (the driver sends a `PullRequest` for each page
  after the first), `true` on gRPC (the server pushes every page and the driver
  only reads). `Rows.fetchNextPage` and `Conn.absorbPlanFrame` branch on it.

  This is a **breaking change for anyone implementing `Transport` outside this
  package**: the method must be added, and the safe answer for a PULL-driven
  wire is `false`.

- `ErrUnsupportedOnTransport`, a typed refusal for a protocol message the
  selected transport cannot carry. It classifies `RetryNever`: the same request
  over the same transport fails identically, and no new connection changes
  that.

  Its live cause is the five `QuicClientMessage` oneof arms `GeodeService`
  declares no method for — savepoint, rollback_to, backup, restore,
  upload_backup — which end in `GRPCTransport.SendProto`'s default arm instead
  of an opaque `unknown message type`. The `PULL` arm is kept as a defensive
  assertion for a caller that reaches past the page loop. **It does not cap any
  result set**; see the paging fix below.

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
  pinned to `testdata/capabilities_0.8.18.golden` (SONNY-806).
- The server's HELLO capabilities were previously decoded and discarded. They
  are now captured per connection and exposed via `(*Conn).HelloCapabilities()`
  and `ServerInfo.HelloCapabilities`, alongside `(*Conn).BoundGraph()` and
  `(*Conn).SessionID()`. HELLO capabilities and `SHOW CAPABILITIES` are
  different vocabularies; neither is derived from the other (SONNY-806).
- Typed vectors. `geode.Vector` (a `[]float32`) binds an embedding as a typed
  vector parameter, so a query vector never reaches the parser as text; the
  call shape is `knn('property', ?, k, 'metric')`, with the property name and
  the metric as literals. Dimensions are capped at 65535 and an oversize vector
  is refused before the wire. Engine facts at 0.8.15, each asserted rather than
  assumed: a dimension mismatch answers GQLSTATE `22000` and a metric mismatch
  `42704`, only `vectorf32($v)` storage arms the vector index (SONNY-1224), and
  `knn` silently downgrades to a scan when another graph shares the property
  name (SONNY-1223) (SONNY-806).
- Session tenancy and role binding. `?tenant=` (alias `tenant_id`) and `?role=`
  bind the session at HELLO, so every statement on that connection inherits
  them; `?strict_tenancy=true` refuses a config with no concrete tenant at
  parse time with `ErrTenantRequired`, and the engine's own refusal (GQLSTATE
  `42501`) surfaces as the typed `ErrTenantScopeRequired` so callers gate
  startup on `errors.Is` instead of matching engine prose. **Tenant binding is
  not an isolation boundary at 0.8.15** — see SONNY-1229 and its known-RED
  reproducer above (SONNY-806).
- The protobuf bindings were regenerated from the pinned engine's
  `src/proto/geode.proto`, which is what makes the typed parameter, vector,
  read-only, plan and gRPC-service surfaces above reachable at all; `make
  proto-check` fails when the generated code drifts from the schema
  (SONNY-806).

### Fixed

- **Every COMPLETED gRPC request now finishes its `Execute` stream, not just a
  successful `ExecContext`** (SONNY-806, Lane D Task 16e). Task 16d finished the
  stream on one path out of four. Three others still handed a pooled connection
  back with a live server-streaming RPC on it — a statement the ENGINE REFUSED
  (`ExecContext`'s `DriverError` branch returned early), an `EXPLAIN` or
  `PROFILE` (`plan.go`'s `collectPlanFrames` never finished one at all), and a
  `Rows` read to its FINAL page (`Rows.Close` cancelled every pushed-page
  stream, finished or not). The next statement on that connection then reset
  the stream, and the engine's vendored grpc-zig escalates ONE stream reset
  into a whole-connection teardown, which reaches the caller as
  `Unavailable: error reading from server: EOF`. That is what turned pipeline
  2855806396's gRPC lane red at
  `TestIntegration_GRPCExplainBindsArgumentsRatherThanInliningThem`, one
  statement after a `CREATE INDEX` the engine refused with 42P07. A refusal and
  a final page are both COMPLETE answers, so both are now finished the way a
  successful statement is; `CancelActiveStream` stays for the one case that
  needs it — a result the engine may still be pushing, which must never be
  drained. `GRPCTransport.ExecStreamResets` counts the resets the driver still
  makes the engine handle so a regression is a number, not a race: it is 0 for
  a workload in which every request completed.
- **The sustained-ingest reproduction no longer writes 30,000 nodes into the
  graph every other test bulk-deletes** (SONNY-806, Lane D Task 16e).
  `TestConn_SustainedSingleConnIngest_NoWedge` now binds its own graph. On CI's
  ONE `services:` engine — not the testcontainers harness, which gives each
  test a fresh engine and hides all of this — its 30,000 twenty-property nodes
  outlived it, because its cleanup ran under a fixed 30 s budget and threw the
  result away. `cleanupTestData`'s whole-graph `MATCH (n) DETACH DELETE n` then
  inherited them under the same fixed budget, and on a slower runner that
  delete no longer fitted: `TestDocker_CreateNode` burned 34 s and gave up,
  `TestDocker_CreateMultipleNodes` queued behind the delete nobody had
  cancelled and died at its deadline, and every test after it did the same
  until the 20-minute package timeout. QUIC carries no cancel message, so an
  abandoned statement is not a cancelled one — measured against 0.8.18, a
  `MATCH (n) DETACH DELETE n` over 8,000 nodes abandoned at a 2 s deadline
  reported `context deadline exceeded` to the caller and the NEXT `CREATE`, on
  a different pool over a different QUIC connection, took 10.5 s; the seven
  after it took 3 ms each.
- **A cleanup that did not finish now says so** (SONNY-806, Lane D Task 16e).
  `cleanupTestData` fails the test instead of discarding the error — a graph
  that could not be cleaned is a graph the next test cannot trust — and
  `cleanupLabel` logs it. The silent `_, _ =` is what let one over-budget
  delete cascade through forty later tests without a single line naming it.
- **A statement abandoned at a deadline no longer reads like a statement that
  did not run** (SONNY-806, Lane D Task 16e). `Conn.ExecContext` annotates a
  context error raised while the engine was executing: the statement was
  abandoned by this client, not cancelled, the engine is still running it, and
  later statements on that graph queue behind it. The wrap is `%w` on the
  original, so `errors.Is(err, context.DeadlineExceeded)` and every
  classification built on it are unchanged; only what a human reads differs.
- **A closed `*sql.DB` no longer leaks its QUIC pool — which was leaking ENGINE
  stream workers, not just client memory** (SONNY-806, Lane D Task 16d). The
  `Connector` owns the shared `QUICPool` and nothing else can reach it, but it
  implemented no `Close`, so `sql.DB.Close` — which closes its connector only
  when the connector is an `io.Closer` — never closed the pool. Every closed DB
  left its dialed QUIC connections up (10 s keep-alives against a 120 s idle
  timeout, so the engine could not reap them either), each holding its anchor
  stream, plus a 1 Hz health-probe loop holding a probe stream on every one.

  Each of those streams pins one worker of the engine's SHARED stream pool for
  the stream's whole life (geode 0.8.18 `default_quic_stream_worker_count = 64`,
  `src/server/main.zig:264`; released only when `handleAdditionalStream`
  returns, `src/server/quic_connection_handler.zig:2891`). Against CI's one
  long-lived service engine the 65th `sql.Open`/`db.Close` cycle therefore found
  no worker left and its handshake was never answered: **`failed to receive
  HELLO response: context deadline exceeded`**, which is what turned every
  `TestDocker_*` and `TestLoad_*` after `TestDocker_MatchRelationshipPattern`
  RED. It never showed up locally because the testcontainers harness gives each
  test a fresh engine.

  Measured against one pinned 0.8.18 container: the engine's
  `geode_runtime_quic_active_connections` climbed 1:1 with the cycles and never
  came down (66 live connections, 133 stream handlers, after 65 closed DBs), and
  the wall tracked the worker count exactly — `GEODE_QUIC_STREAM_WORKERS=8`
  moved it from the 65th cycle to the 9th. With the probe loop disabled the leak
  is anchor-only and the wall moves to 320 = 256 detached first-stream owners
  (`quic_connection_handler.zig:176`) + 64 pool workers. After the fix the same
  80-cycle loop leaves the engine at 0 connections and 0 stream handlers.

  `Connector.Close` now closes the pool, and the deprecated one-shot
  `Driver.Open` — which builds a private connector per call that nothing else
  can ever close — hands that pool's lifetime to the connection it returns.
  Regression coverage, RED before the fix:
  `TestIntegration_RepeatedOpenCloseDoesNotExhaustOneEngine` (80 cycles against
  one engine; failed at cycle 66 with the CI error verbatim),
  `TestIntegration_ClosedDBLeavesNoPooledConnection`, and, with no engine,
  `TestConnector_IsAnIOCloser` / `TestDBClose_ClosesTheConnectorsPool`.

- **A completed gRPC `Execute` stream is now finished, not left for the next
  statement to reset** (SONNY-806, Lane D Task 16d). Every `Execute` rides its
  own server-streaming RPC and the driver stops reading as soon as it has the
  answer, so the RPC was left dangling and the NEXT `Execute` tore it down with
  an `RST_STREAM` — one reset per statement. The engine's vendored grpc-zig
  races that reset against its own handler: it logs `gRPC streaming: failed to
  send initial response error=PeerGone` and then drops the **whole TCP
  connection** rather than the one stream (`error: Handler error:
  error.PeerGone`). `TestIntegration_GRPCBatchExecPastOnePage` therefore died
  mid-transaction with `rpc error: code = Unavailable desc = error reading from
  server: EOF` at a statement number that moved run to run — 3 in CI, 5, 84 and
  138 across local reproductions against one shared engine.

  This corrects the claim above that the 0.8.18 bump made that test pass: the
  statement-29 failure it used to show was the GAP-SONNY806-07 blast radius and
  is indeed gone, but a second, independent defect was underneath it.

  `GRPCTransport.FinishExecStream` now half-closes and drains a finished
  request to its trailers (bounded by `finishExecStreamBudget`, falling back to
  the old reset), and `Conn.ExecContext` calls it once the response is complete.
  A result the engine is still pushing is still cancelled, never drained —
  `CancelActiveStream` is unchanged. Side effect of not stranding streams: the
  gRPC lane got much faster on one shared engine
  (`GRPCPagedResultIsConsumedAcrossPushedPages` 45.3 s -> 3.1 s,
  `GRPCCancelMidStream` 24.9 s -> 1.2 s). Regression coverage, RED before the
  fix: `TestIntegration_GRPCCompletedExecLeavesNoStreamToReset`. The engine
  losing a connection over one reset stream is an ENGINE defect and keeps its
  own ticket.

- **A statement that fails on the transport now says why** (SONNY-806, Lane D
  Task 16d). `transportErrBadConn` collapsed every `*TransportError` into the
  bare `driver.ErrBadConn` sentinel. That retires the connection correctly and
  then tells the caller nothing, and inside a transaction — where `database/sql`
  cannot retry and hands the error straight back — a 647-statement
  `Pool.BatchExec` failed with exactly `statement 3: driver: bad connection`.
  It now returns the same `badConnError` wrapper `markBadConn` uses, so
  `errors.Is(err, driver.ErrBadConn)` still retires the conn while
  `errors.As(err, &te)` and `Classify` keep working and the caller sees the
  cause. Covered by `TestTransportErrBadConn_KeepsTheCause`.

- **The multiplex probe tests no longer hard-code one developer's home
  directory** (SONNY-806, Lane D Task 16d). `probeGeodeBinary` was
  `/home/benji/src/devnw/.../zig-out/bin/geode` (committed in `2134501`, before
  this branch), so `TestPooledHello_ConcurrentFirstStreams`,
  `TestMultiplexServerProbe` and `TestMultiplex_FindCreate_And_Merge_NoWedge`
  failed at 0.00 s everywhere else — `fork/exec ...: no such file or directory`
  — including CI. The binary is now resolved from `GEODE_PROBE_BINARY` (or the
  pre-existing `GEODE_BINARY`), then from `geode` on `PATH`; with none of those
  the tests SKIP with that reason, because they launch a LOCAL engine that CI
  does not have. An env var naming a missing binary is still a hard failure.
  `TestNoDeveloperMachinePathsInSources` keeps such a path from coming back and
  runs in `make test-unit`.

- **Each pooled QUIC connection is anchored on its own HELLO stream, so a
  finished request no longer takes the connection down with it** (SONNY-1219).
  geode makes the first stream on a connection the connection's owner: when it
  closes, the engine destroys the whole connection and every sibling stream
  dies with `NO_ERROR (remote)`. With `MaxIdleConns=0` every `sql.Conn` closes
  right after its query, so the first request stream on each pooled connection
  closed almost immediately and took its seven siblings with it — a burst of 64
  requests over 8 dialed connections returned `ok=8 err=56`.

  `QUICPool.dialLocked` now opens a dedicated **anchor stream** the moment a
  connection is dialed, performs the HELLO on it, and holds it open for the
  connection's whole life (`quic_pool.go`, `pooledConn.anchor`). Every request
  stream is a sibling that may come and go freely, and no request's `Close` is
  ever the connection's death. The anchor is opened directly on the connection,
  so it consumes no admission slot and counts against neither `max_streams` nor
  `max_streams_per_conn`; it carries exactly one exchange, the HELLO, and after
  that only a watcher reads it. `pooledHello` is therefore a pure fast path for
  every request stream, which also removes the burst of up-to-8 concurrent
  redundant HELLOs a freshly dialed connection used to see.

  The watcher is the health half (Task 11): the peer closing or resetting the
  anchor condemns the connection, takes it out of the pool at once so `acquire`
  can never hand it out again, and DRAINS it — the QUIC connection is closed
  when its last in-flight request stream finishes, not under it. The watcher
  reads with a context that carries no deadline and no cancellation, so it arms
  no read deadline and writes no frame: QUIC idle detection and the health
  probe behave exactly as before.

  The drain is enforced in the PRUNE as well, not only in `retire`. `retire`
  condemns in two steps that are not atomic against the pool lock — it stores
  `pc.dead`, then queues for `p.mu` in `remove` — so a goroutine already holding
  the lock ran `acquire` → `pruneDeadLocked` inside that window, saw `isDead()`
  report true for a merely draining connection and discarded it, closing the
  QUIC connection under the request streams still in flight. Reordering `retire`
  would not have closed it: `isDead()` also turns true on its own once the peer
  cancels the connection's context. `pruneDeadLocked` now drops a retiring
  connection from the pool but leaves the close to the release that finishes its
  last stream, and reports it as retired rather than as a failed liveness probe.

  Two consequences of retiring-by-draining are **recorded, not fixed**, and
  neither is exercised by a test here:

  - `Drain` does not wait for a RETIRED connection's last streams. It counts
    in-flight streams over `p.conns`, and a retired connection has already left
    that slice, so a drain can return while one of its request streams is still
    finishing. `Pool.Health` has the same blind spot for the same reason. The
    stream is not orphaned — it holds its admission slot and closes the
    connection through `release` — but the drain's "all in-flight statements
    finished" guarantee now excludes it. The window is narrow (an anchor lost
    during a drain).
  - `transportErrBadConn` (`conn.go`) still collapses a transport error on the
    QUERY path to a bare `driver.ErrBadConn`, discarding the cause. It could now
    use `markBadConn` and keep both, which is strictly better; it is left alone
    because it is outside the reported defect and would change error text on
    paths this work did not measure.

- **A connection-level HELLO failure reaches `database/sql` as
  `driver.ErrBadConn`, so it re-dials instead of failing the caller**
  (SONNY-1219). `Connector.Connect` returned the raw `*TransportError`, and
  `database/sql` re-dials only for `driver.ErrBadConn` — so every caller in a
  burst whose connection had been torn down under it saw the transport error
  directly. `Connect` now marks a connection-level failure (`markBadConn`,
  `driver.go`): the error satisfies `errors.Is(err, driver.ErrBadConn)` for
  `database/sql` AND still yields its `*TransportError` to `errors.As`, so
  `Classify` keeps returning `RetryNewConn` — the correct class; this is not
  resource pressure and `RetryBackoff` would be wrong. A DIAL failure is
  deliberately excluded: there was no connection to be bad, and reporting
  `ErrBadConn` would make `database/sql` pay the connect timeout three times
  over before surfacing the same outage. The gRPC HELLO is excluded for the
  same reason at one remove: `grpc.NewClient` is lazy and establishes nothing,
  so an unreachable gRPC server produces no dial error at all — the first RPC
  is the `Handshake`, and the outage surfaces on the `handshake` op instead.

  The anchor handshake is bounded at **10 s** when the caller set no deadline of
  its own. That bound is the operative one for an ordinary DSN rather than a
  fallback for an unusual one: `connect_timeout` defaults to NONE, so nothing
  else limits how long a silent server can hold the pool lock while
  `dialLocked` waits for its HELLO. Dialing and handshaking off the pool lock
  remains the real fix and is still a follow-up.

- **An abandoned gRPC result no longer wedges the next statement on the same
  session** (SONNY-806). Closing a `Rows` before its final page — with a
  perfectly live context, which is ordinary caller behaviour — left geode's
  server-streaming `Execute` pushing pages nobody would read. The engine's push
  loop holds the session mutex across the send
  (`src/transport/grpc_server.zig:1065-1090`,
  `entry.mutex.lockUncancelable`) and `handleExecute` (`:1935`) takes that same
  mutex, so once the unread pages filled the HTTP/2 flow-control window the
  loop parked INSIDE the critical section and the next `Execute` on that
  session blocked behind it until its own context expired. Nothing had failed,
  so the connection was still `IsValid` and `database/sql` handed it straight
  to the next statement.

  `GRPCTransport` now keeps the cancel function of the context each `Execute`
  rides and fires it in three places: the next `Execute` (which would otherwise
  overwrite the stream and orphan the old RPC), `Close`, and every
  `Rows.Close` on a pushed-page transport — final page or not. On the first of
  those it fires **before** the new `Execute` is issued, not after: cancelling
  the superseded RPC while the next one is already in flight on the same
  session poisons the session at engine 0.8.15 — a 647-statement
  `Pool.BatchExec` then fails around statement 29 with `driver.ErrBadConn`.
  Same grpc-zig weakness as GAP-SONNY806-07 / SONNY-1560; ordering the
  `RST_STREAM` ahead of the new request keeps the engine's view of the
  session single-threaded. Cancelling — rather than marking the connection
  failed — is deliberate: the RPC is per-`Execute`, so tearing it down leaves
  the session and the gRPC `ClientConn` usable and the connection stays in
  the pool. Only a desynchronised connection (a cancelled query context, a
  transport error) is still retired.

  `Rows.Close` on a FULLY-READ result now cancels too, not only an early one.
  Before, the only things that ever retired a finished RPC's context were the
  next `Execute` on the SAME connection or the transport's own `Close`; between
  this `Close` returning the connection to `database/sql`'s pool and either of
  those firing, the context sat alive as a child of the caller's own context,
  torn down whenever THAT context's unrelated deadline or cancellation
  happened to fire — unordered against whichever statement next borrowed the
  connection. That race is the same `driver.ErrBadConn` shape measured above,
  now closed for the final-page case as well as the early one.

  Regression coverage: `TestIntegration_GRPCEarlyCloseDoesNotWedgeTheSession`
  (bounded follow-up statement on the same driver `Conn`),
  `TestRows_PushedPages_EarlyCloseCancelsTheStream` /
  `TestRows_PushedPages_FullReadAlsoCancels` /
  `TestGRPCTransport_NextExecuteCancelsThePreviousStream`.

- **The trailing-frame drain no longer runs on a pushed-page transport**
  (SONNY-806). `Conn.drainTrailingFrames` hands `ReceiveProto` a fresh
  background context with a 1 ms timeout. Only the context an `Execute` stream
  was created from binds that stream, so on gRPC that deadline could not
  unblock `Recv` at all: the drain could park on the wire while holding the
  connection mutex. Worse, on a non-final release there IS something to read,
  and reading it would have discarded a page of the caller's result rather than
  a stray trailing frame. The drain is now skipped when
  `Transport.PagesArePushed()`; the cancel above ends the stream outright, so
  there is nothing left to drain. `ReceiveProto`'s doc states the precondition.

- **gRPC results are no longer capped at the engine's first 128-row page**
  (GAP-SONNY806-04, SONNY-806). Any statement returning more than 128 rows over
  gRPC — `Pool.BatchExec` included — used to fail after exactly 128 rows with a
  `*TransportError` wrapping `ErrUnsupportedOnTransport`.

  This was a **driver** defect, not a protocol asymmetry. geode 0.8.15 serves
  `Execute` over gRPC as a server-streaming RPC and pushes the whole result,
  page by page, until one is marked final; `GRPCTransport.ReceiveProto` already
  returned those pages. `Rows.fetchNextPage` asked for the next page anyway with
  a `PullRequest` the service has no method to answer, so the write failed and
  the read that would have succeeded never happened. The page loop now consumes
  the pushed pages on transports that report `PagesArePushed()`, and the QUIC
  PULL path is unchanged.

  Cancellation semantics are identical on both wires: a cancel while waiting for
  a page reports the wrapped `ctx.Err()` (never a transport error) and retires
  the connection.

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
- **BREAKING (v0.2.0) — the `Transport` interface gained
  `PagesArePushed() bool`.** Anything implementing `Transport` outside this
  package must add it; the safe answer for a PULL-driven wire is `false`. It is
  a hard interface method rather than an optional type assertion on purpose:
  every transport must answer it, and the two failure modes of a missing answer
  are silent — a PULL-driven default on a pushed wire truncates results at the
  engine's first page, and a pushed default on QUIC stalls the reader waiting
  for a page nobody will send. A compile error at the one place the question is
  asked is cheaper than either. By contrast the tear-down capability added
  alongside it (`CancelActiveStream`) IS an optional assertion: only a
  stream-oriented transport has anything to cancel, and a transport with
  nothing to tear down should not have to implement a no-op.
- `Conn.hello` returns `*proto.HelloResponse` (internal; noted for forks).
- A cancelled `Rows` reports `context.Canceled` from `rows.Err()` where it
  previously surfaced a transport error, and marks the connection unusable so
  `database/sql` discards it.

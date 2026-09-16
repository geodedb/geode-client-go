# GAP Analysis

Last updated: 2026-09-16
Reviewed by: Adversarial Security Review Agent (2026-03-21); divelife bulk-seed investigation (2026-06-17); Lane D tenancy/role binding (SONNY-806 / GEO-GO-UP-001, 2026-09-13)
Repository: devnw/codepros/geode/geode-client-go
Branch: `eng-4622/geode-go`. Each finding below names the commit that filed or
closed it; `git log` is the authority, not a SHA copied into this header.
Review type: Comprehensive adversarial security review (Round 6 - no new findings); BUG-0001 closed 2026-03-21; BUG-0002 filed 2026-06-17 (high-concurrency connection-establishment stall); GAP-SONNY806-01 and GAP-SONNY806-02 filed 2026-09-13 against engine 0.8.15; GAP-ENG4658-02 (pooled connections never verified with network I/O) closed 2026-09-13; GAP-SONNY806-04 filed and closed 2026-09-14 (gRPC paging — a DRIVER defect, Lane D Task 14/14b); GAP-SONNY806-05 and GAP-SONNY806-06 filed and closed 2026-09-14 (abandoned gRPC stream and the trailing-frame drain — both DRIVER defects, Lane D Task 14c); GAP-SONNY806-07 / SONNY-1560 filed 2026-09-14 (ENGINE: a gRPC result stalls past ~64 KiB of pushed payload)

## Summary

Open findings by severity:

- Critical: 0
- Fixed 2026-09-16: GAP-SONNY806-02 (Critical, cross-tenant read leak) and
  GAP-SONNY806-07 / SONNY-1560 (High, gRPC stall past ~64 KiB) by the engine
  bump to 0.8.18, both verified; GAP-SONNY806-08 (Critical, DRIVER — the
  anchor stream dropped tenant/graph scope) by binding every request stream
  with its own HELLO (Lane D Task 16b), verified
- Medium: 2 (GAP-SONNY806-01 — engine refuses `$param` in CALL arguments, filed 2026-09-13;
  GAP-SONNY806-03 — engine ignores the `--listen-grpc` bind address, filed 2026-09-14)
- Low: 1 (BUG-0002 — downgraded 2026-06-17)
- Info: 0

Closed findings: 33 (Issues #1-#23, #25-#28; BUG-0001; GAP-ENG4658-02;
GAP-SONNY806-04 — gRPC paging, driver defect, closed 2026-09-14;
GAP-SONNY806-05 — abandoned gRPC stream wedges the session, driver defect,
closed 2026-09-14; GAP-SONNY806-06 — trailing-frame drain on a pushed-page
transport, driver defect, closed 2026-09-14; GAP-SONNY806-08 — the anchor
stream dropped the session's tenant and graph scope, driver defect, closed
2026-09-16)
Won't Do findings: 1 (Issue #24)

Top risks (remaining):

- **GAP-SONNY806-02 (Critical) — FIXED at engine 0.8.18**, verified 2026-09-16.
  geode 0.8.15 did not apply the session tenant filter on the `MATCH` read
  path, so two sessions binding different tenants at HELLO read each other's
  rows even with `GEODE_STRICT_TENANCY=1`. `TestIntegration_CrossTenantReadIsDenied`
  now PASSES against 0.8.18 with a correctly bound driver.

- **GAP-SONNY806-07 (High, engine ticket SONNY-1560)** — geode 0.8.15 cannot
  finish pushing a gRPC result larger than roughly 64 KiB. The stream stalls
  and stays stalled until a ~30 s engine-side timeout, whatever the client
  does; the same query over QUIC returns 50 000 rows in ~1.5 s. gRPC is
  therefore usable for SMALL results only at this engine version, and QUIC
  remains the release transport.

- **BUG-0002 (Low, downgraded)** — when a single `*sql.DB` opens MORE concurrent
  connections than the QUIC `pool_size` (default 16), multiple streams multiplex
  over each pooled connection and a small fraction of operations intermittently
  time out (~0.06% at 32-over-16; non-crashing, surviving ops continue at full
  rate). The CATASTROPHIC symptom originally attributed here — bulk ingests
  freezing to ~0 nodes/sec — was actually a **geode SERVER crash** (checkpoint vs
  concurrent-writer data race), fixed server-side in geode commit `7dcac17e`. The
  remaining driver behaviour is a robustness nit; mitigate by keeping concurrent
  connections ≤ `pool_size` (divelife `data-ingest` clamps to 16).

## Open Findings

### GAP-SONNY806-07 (SONNY-1560): a gRPC result stalls after roughly 64 KiB of pushed payload

- **Severity:** High (functional cap on an entire transport; a caller sees a
  hang, then a context error, instead of its rows)
- **Engine ticket:** SONNY-1560
- **Component:** geode ENGINE 0.8.15 gRPC path / the vendored grpc-zig — **not
  the Go driver**. The driver is reading; grpc-go emits HTTP/2
  `WINDOW_UPDATE`s as the application consumes each page.
- **Status:** **FIXED at the pinned engine 0.8.18**, verified 2026-09-16.
  The fix landed in geode 0.8.16 (`33a39499` releases the session mutex between
  pushed gRPC pages and bounds a stalled stream; `3d3b1b60` lets the library's
  `RST` end an exhausted stream and pins the flow-control seam; grpc-zig
  v0.1.10) and ships in 0.8.18. Measured: the oversized result now ends in
  **0.62 s** with a typed gRPC `ResourceExhausted` - `stream window exhausted:
  peer stopped draining the pushed result` - after 5632 of 20000 rows, instead
  of parking ~30 s, and `TestIntegration_GRPCBatchExecPastOnePage` PASSES
  immediately after it (it used to fail around statement 29), so the
  cross-session blast radius is gone. The measurements below describe 0.8.15
  and are kept as the record of what was found there. **Follow-up, done
  2026-09-16 (Task 16b):** `TestIntegration_GRPCLargePushedResultFailsLoudly`
  was RED because its `switch` accepted only `io.EOF`-with-every-row or
  `context.DeadlineExceeded` and had no arm for a promptly surfaced typed
  error. It now has one, keyed on the driver's own `*TransportError` rather
  than on message text, and the no-silent-truncation invariant is unchanged: a
  short result reported as a clean `io.EOF` is still a hard failure. (Filed
  2026-09-14, SONNY-806 / GEO-GO-UP-001 Lane D Task 14c.)
- **Reproducer:** `TestIntegration_GRPCLargePushedResultFailsLoudly` logs the
  stall point; any oversized result reproduces it, e.g.
  `FOR i IN range(0, 19999) RETURN i` over gRPC.

**Observed.** A single server-streaming `Execute` delivers roughly 64 KiB of
pushed payload and then sends nothing further. The driver blocks until its
context expires (or ~30 s, when an engine-side timeout eventually fires). The
stall point moves with ROW WIDTH, not row count, which is what identifies it as
flow control rather than a row limit. Measured against the pinned image
(`geodedb/geode:0.8.15`), each row a plain integer plus an optional string
literal:

| Query | Rows delivered | Approx. payload | Outcome |
|-------|----------------|-----------------|---------|
| `FOR i IN range(0, 999) RETURN i` | 999 / 999 | ~11 KiB | completes in 23 ms |
| `FOR i IN range(0, 999) RETURN i, '<64 B>'` | 768 / 999 | ~58 KiB | stalls |
| `FOR i IN range(0, 999) RETURN i, '<256 B>'` | 128 / 999 | ~35 KiB | stalls |
| `FOR i IN range(0, 49999) RETURN i` | 5632 / 49999 | ~66 KiB | stalls |
| the same 50 000-row query over **QUIC** | 49999 / 49999 | — | completes in 1.5 s |

**Assessment.** ~64 KiB is exactly HTTP/2's default initial stream window. The
engine appears to send until that window is exhausted and never to act on the
`WINDOW_UPDATE` frames that replenish it, so the stream cannot make progress
again. The driver has no lever: it already consumes every page as it arrives,
which is what makes grpc-go issue the updates in the first place.

**Consequences.**

- gRPC is usable for small results only at 0.8.15. Anything past ~64 KiB —
  which is a few thousand narrow rows, or a few hundred wide ones — hangs.
  QUIC is the release transport; `docs`/`README` say so.
- It sets the ceiling for the Task 14b paging fix. Pushed-page consumption is
  correct and proven (`TestIntegration_GRPCPagedResultIsConsumedAcrossPushedPages`,
  647 rows across five page boundaries), but it cannot deliver what the engine
  will not send.
- It is also why cancelling an abandoned RPC (GAP-SONNY806-05) cannot make an
  oversized abandoned result cheap: the engine is parked inside its push loop
  and does not act on the client's `RST_STREAM` either.
- **The blast radius crosses sessions, not just callers (SONNY-1560).** An
  oversized result parks the ENGINE — not just the client's stream — inside
  its push loop for the ~30 s engine-side timeout, and that park is scoped to
  the engine's session handling, not to the one stalled RPC. While it is
  parked, a **different session's** multi-statement transaction on the same
  engine process can fail mid-way with `driver.ErrBadConn`, not just the
  session that triggered the stall. Measured directly:
  `TestIntegration_GRPCBatchExecPastOnePage` (647 statements, a different
  session/connection than the stall) fails around statement 29 with
  `driver.ErrBadConn` when it runs immediately after
  `TestIntegration_GRPCLargePushedResultFailsLoudly`, and passes cleanly when
  run alone; `waitForGRPCEngineToRecover`
  (`integration_grpc_test.go`) exists solely to wait out the ~30-35 s the
  engine needs before a NEW session's batched work is reliable again. A caller
  that triggers one oversized gRPC result and then keeps issuing unrelated
  work on other connections/sessions against the same engine should expect
  that other work to fail for roughly 30 s, not just the query that
  overflowed.

**Requested engine change.** Process HTTP/2 `WINDOW_UPDATE` on the server side
so a stream's flow-control window is replenished as the client consumes, and
honour `RST_STREAM` while a send is parked.

### GAP-SONNY806-03: `--listen-grpc <addr>:<port>` ignores the bind address

- **Severity:** Medium (operability; silently unreachable listener)
- **Component:** geode ENGINE 0.8.15 (`src/server/main.zig`) — not the Go driver
- **Status:** **Open** (filed 2026-09-14, SONNY-806 / GEO-GO-UP-001 Task 14)

`src/server/main.zig:2122-2152` splits `--listen-grpc` into `grpc_address` and
`grpc_port`, then passes only the PORT to
`grpc_transport.GeodeGrpcServer.init(gpa, grpc_port, cert_path, key_path,
grpc_config)`. `grpc_address` is used solely in the "gRPC server started" log
line. The vendored grpc-zig binds `GrpcServer.DEFAULT_BIND_ADDRESS =
"127.0.0.1"` (grpc-zig `grpc_zig-0.1.8-7FnvpKz7BQAM3rNTfY4gV_dBgDfEB4JScP6cE--HzBl9`
— the content hash geode's `build.zig.zon:305-310` resolves `ref=v0.1.9` to —
`src/server.zig:138`, loopback-by-default by
design), so `serve --listen-grpc 0.0.0.0:50051` binds **127.0.0.1:50051** while
logging `address=0.0.0.0`.

Inside a container that means a published `-p …:50051/tcp` reaches nothing, and
the operator's only signal is a connection refusal. Verified against the pinned
image: `/proc/net/tcp` in the container shows `0100007F:C383` (127.0.0.1:50051)
with `--listen-grpc 0.0.0.0:50051`, and `00000000:C383` once
`GRPC_ZIG_BIND_ADDR=0.0.0.0` is set.

**Workaround (in use).** `GRPC_ZIG_BIND_ADDR=0.0.0.0` — grpc-zig's documented
opt-in (`src/server.zig:133-151`) — is set in the testcontainers harness
(`integration_main_test.go`), the `docker-start` recipe (`Makefile`) and the
`integration:geode` service (`.gitlab-ci.yml`).

**Requested engine change.** Pass the parsed `grpc_address` into
`GeodeGrpcServer.init` / `GrpcServer.initWithBind`, or refuse startup when the
requested address is not the one that will be bound.

### GAP-SONNY806-02: engine does not enforce tenant isolation on the MATCH read path

- **Severity:** Critical
- **Component:** geode ENGINE 0.8.15 (read path) — not the Go driver
- **Status:** **FIXED at the pinned engine 0.8.18**, verified 2026-09-16:
  `TestIntegration_CrossTenantReadIsDenied` PASSES against 0.8.18 — first with
  a pre-anchor driver (`29d373b`), and then on the branch itself once
  GAP-SONNY806-08 was fixed (Task 16b). The fix landed in
  geode **0.8.16** and ships in 0.8.18 (`0beb108b` applies tenant scope to engine-direct reads;
  `d0fe80fb`/`afcdbd6f`/`97bd9b83` extend it to graph procedures and
  introspection under SONNY-1318; `9ea03ccc`/`1d99e0e0` cover shortestPath,
  `COUNT{}`, `EXISTS{}` and pattern comprehensions and advertise
  `graph_procedure_tenancy` under SONNY-1326). The lane is pinned to 0.8.18 and
  has re-measured isolation; the capability golden moved with it
  (`testdata/capabilities_0.8.18.golden`). (Filed 2026-09-13, SONNY-806 /
  GEO-GO-UP-001.)
- **Reproducer:** `integration_tenancy_test.go` →
  `TestIntegration_CrossTenantReadIsDenied` (now GREEN; this is the P0 evidence
  and is deliberately not skipped)

**Observed.** With the engine started as `GEODE_STRICT_TENANCY=1` and two
tenants registered via `CALL geode.tenant.create('tenant-a'/'tenant-b')`, a node
written by a session bound to `tenant-a` is fully readable by a session bound to
`tenant-b` in the same graph:

```
strict tenancy confirmed; unscoped read refused as:
  geode: 42501: Tenant scope required: strict tenancy is enabled and this
  session has no concrete tenant scope
tenant-b saw 1 of tenant-a's nodes, want 0 — CROSS-TENANT LEAK
```

**Narrowed.** Three separate probes against engine 0.8.15:

1. Strict tenancy IS active — an unscoped (`"default"`) session is correctly
   refused with GQLSTATE 42501 (`src/server/proto_util.zig:240`), so both tenant
   sessions resolved some concrete, non-default scope from HELLO. That the
   driver delivers the *correct* tenant identifier into `HelloRequest`
   (`conn.go`) — not merely a non-default one — is shown below by probe 3: the
   cross-tenant `MATCH ... DELETE` matches zero rows, which is only possible if
   the CRUD path compared the exact tenant value `tenant-b`'s session sent
   against each node's stored `tenant_id`.
2. The leak is symmetric AND unbounded: a session binding a tenant id that was
   never registered (`tenant=t-never-registered`) also reads every row. That
   excludes the GAP-0846 hierarchy grant — an unregistered tenant participates
   in no hierarchy, so `passesTenantFilterSlice`
   (`src/execution/match_operations.zig:222-228`) could not have admitted it.
   The tenant filter is simply not consulted on this read path.
3. Writes ARE tagged correctly: a cross-tenant `MATCH ... DELETE` from
   `tenant-b` matched zero of `tenant-a`'s rows, because the CRUD path does
   apply the filter (`src/execution/crud_operations.zig:212-213`). So the defect
   is read-side only.

**Likely locus.** `readServedEngineDirect`
(`src/server/quic_connection_handler.zig:5858+`) lets a `.rows` read be served
engine-direct by `node_scan_stream` / the storage-native planned-read path, and
gates that ONLY on `rls_active` — never on tenant scope.
`src/server/node_scan_stream.zig` contains no tenant logic at all (its four
`tenant` mentions are comments about a `tenant = null` count precompute). The
file's own SAFETY note anticipates exactly this class of bug for RLS
("otherwise lifting the secondary gate while the path still lacks
passesRLSFilter leaks policy-denied rows") but no equivalent gate exists for
tenancy.

**Consistent with the engine's own capability rows** at 0.8.15:
`tenant_scope_fail_closed=false`, `tenant_binding_server_verified=false`,
`graph_procedure_tenancy=false`. At the pinned 0.8.18
(`testdata/capabilities_0.8.18.golden`) `graph_procedure_tenancy` is now
`true` (SONNY-1326); the other two are still `false`.

**Impact.** Any multi-tenant deployment on 0.8.15 leaks every tenant's data to
every other authenticated session bound to the same graph. Graph-level isolation
still holds (a session in a different graph sees zero rows).

**Driver-side mitigation:** none possible — isolation is a server-side property.
Until the engine fixes it, treat one graph per tenant as the only supported
isolation boundary.

### GAP-SONNY806-01: engine refuses `$param` in CALL `geode.fts.*` and `algo.*` arguments

- **Severity:** Medium
- **Component:** geode ENGINE 0.8.15 (`CALL` procedure argument binding) — not
  the Go driver
- **Status:** **Open** (filed 2026-09-13)
- **Tracking:** SONNY-778 (GEO-UP-002, FTS argument binding), SONNY-635
  (GEO-UP-005, bounded-traversal argument binding)

**FTS.** `src/execution/fts_dispatch.zig:103-104` (`geode.fts.search`),
`:191-192` (`geode.fts.phrase`), `:236-237` (`geode.fts.proximity`) and `:315`
(`geode.fts.rebuild`) read their arguments through `literalString`
(`fts_dispatch.zig:444-449`), which accepts only `ast.Expr.String`. A `$param`
in any `geode.fts.*` argument raises `TypeMismatch`.

**Traversal.** `src/execution/execution_helpers.zig:56-79` (`extractNodeId`)
accepts only an `Int` literal or an `Ident` resolved from `context.bindings` —
not `$param`. `extractU32FromMap` (`execution_helpers.zig:19-28`) reads config-map
values only from `Int` literals; `algo_dispatch.zig:286` and `:304` read
`maxDepth` through it. So `CALL algo.bfs.stream($nodeId, {maxDepth: $d})` is not
expressible. The procedures themselves are registered at
`algo_dispatch.zig:117` / `:119` / `:121` (`algo.bfs.stream`, `algo.dfs.stream`,
`algo.dijkstra.stream`) and are otherwise undocumented.

**Why it matters here.** SONNY-806 forbids interpolating source text into GQL,
so a BM25 seed cannot pass a user query as a parameter today.

**Workaround in this lane.** The driver never interpolates source text, paths,
identifiers or vectors. Tenant ids passed to `CALL geode.tenant.create` in
`integration_tenancy_test.go` are compile-time constants for exactly this
reason. Callers that need traversal resolve the start node through a preceding
parameterized `MATCH` binding in the same statement and render depth from an int
clamped to `[1,8]` via `strconv.Itoa` (fuzz-tested to match `^[0-9]+$`).

**Requested engine change.** Accept `ast.Expr.Param` in `geode.fts.*` arguments
and in `algo.*` node-id arguments and config-map values, resolving from
`ExecutionContext` query params — the same path `src/eval.zig:886` already uses
for `.Param`. Plus documented `CALL` syntax for `algo.bfs`/`dfs`/`dijkstra`.

### BUG-0002: high-concurrency connection establishment stalls (MaxOpenConns > QUIC pool)

- **Severity:** Low (downgraded 2026-06-17 — see update below; was High)
- **Component:** Transport / Connection pool (`quic_pool.go`, connector/HELLO path)
- **Status:** **Open** (filed 2026-06-17), **downgraded 2026-06-17**
- **2026-06-17 UPDATE — root cause was server-side, not the driver:** the
  catastrophic symptom (bulk ingest freezing to ~0 nodes/sec, connections dying)
  was a **geode SERVER CRASH** — `checkpointAll` held `mg.rwlock` SHARED while
  bulk writers also held it SHARED and mutated the indexes, so the checkpoint's
  `label_index.scan()` raced `addNode()` and panicked (`index out of bounds`),
  killing the process under load. Fixed in geode commit `7dcac17e` (checkpoint
  now takes the EXCLUSIVE lock); validated crash-free at 64 concurrent writers on
  host and in a container. With the server fixed, the driver no longer "stalls":
  a single `sql.DB` at 32–64 connections over the default 16-conn pool now sustains
  full throughput with only a small fraction (~0.06%) of intermittent per-op
  timeouts from stream multiplexing — a robustness nit, hence the downgrade to
  **Low**. The originally-reported HELLO/open_stream deadline errors were
  predominantly the server dying mid-handshake. Recommended driver follow-up
  (optional): when concurrent demand exceeds `pool_size`, either grow the pool or
  fail fast with a clear error instead of intermittently timing out.
- **Symptom:** A single `sql.DB` opened against one geode endpoint with
  `db.SetMaxOpenConns(N)` for N greater than the QUIC pool size
  (`DefaultQUICPoolSize = 16`) **stalls**: many connections fail to establish
  with `failed to receive HELLO response: context deadline exceeded` /
  `transport read_length ... deadline exceeded`, and (with a larger
  `pool_size`) `open_stream ... deadline exceeded`. Surfaced by the divelife
  14.27M bulk seed (`data-ingest ingest --from-dump --workers 32`), which froze
  at ~0 nodes/sec.
- **Reproduction (minimal):** open `sql.Open("geode", "<host>?graph=g&insecure=true")`,
  `db.SetMaxOpenConns(32)`, launch 32 goroutines each looping
  `db.ExecContext(ctx, "MERGE (x:T {k: ?}) SET x.a = ?", key, "v")` with unique
  keys against a host (non-Docker) geode server.
  - **N=16:** clean, ~3100 ops/sec.
  - **N=32 (default pool):** ~8+ goroutines die on HELLO deadline; survivors continue.
  - **N=32 with `?pool_size=64&max_streams=256`:** WORSE — more goroutines fail
    with `open_stream` deadlines. So naively raising the pool size does NOT fix
    it; the limit is concurrent QUIC connection establishment from a single
    client endpoint, not pool slots.
- **Isolation (NOT the geode server):** the geode server handles 32 connections
  fine when they come from TWO client endpoints (two processes × 16 workers,
  same graph, ~259 nodes/sec sustained). Only a single client endpoint
  establishing >~16 QUIC connections at once fails. So the defect is on the Go
  driver side (QUIC connection establishment under burst / pool interaction),
  or in how the connector maps `database/sql` connections onto the QUIC pool.
- **Hypotheses to investigate:** (1) each `sql.Conn` requires a dedicated QUIC
  connection for its HELLO session and the pool's hard `maxSize` (16) cannot
  host more, so the 17th+ `Connector.Connect` blocks/times out on HELLO rather
  than multiplexing or failing fast; (2) a burst of ~32 simultaneous QUIC
  handshakes from one client transport overwhelms the dial/handshake path
  (client `quic-go` and/or server accept). Note `acquire()` already multiplexes
  *streams* over existing conns once at `maxSize`, so the failure is most likely
  in the per-`sql.Conn` HELLO/connection establishment, not stream admission.
- **Downstream impact / current mitigation:** divelife `data-ingest` now clamps
  its `*sql.DB` pool + worker fan-out to 16 (`maxGeodeConns`) so cranking
  `--workers` cannot deadlock the ingest (commit `126bef7`). This is a
  workaround; the driver should either (a) multiplex many `sql.Conn` sessions
  over the QUIC pool without a hard per-process connection ceiling, (b) grow the
  pool to satisfy `MaxOpenConns` (bounded), or at minimum (c) fail fast with a
  clear "connection pool exhausted (N > pool_size)" error instead of a 30 s
  HELLO timeout that presents as a hang.
- **Required completion:** profile the connector/HELLO path under a 32-connection
  single-endpoint burst (client `quic-go` dial + server accept), implement one
  of (a)/(b)/(c), and add a regression test that a single `sql.DB` with
  `SetMaxOpenConns(64)` sustains throughput (or fails fast) without stalling.
- **2026-06-17 update — sharpened root cause + two approaches tried & reverted:**
  - **Decisive finding: multiplexing concurrent streams over one QUIC connection
    does not work against the current server.** Forcing 4 streams onto 1
    connection (`pool_size=1`, `MaxOpenConns=4`) → 3 of 4 concurrent
    `ExecContext` callers fail with `context deadline exceeded`; only ~1 stream
    per connection makes progress. This is why exactly `N - pool_size` callers
    fail: with `pool_size` connections each carrying `ceil(N/pool_size)` streams,
    all but one stream per connection wedge. (The driver's whole pool model —
    "many `sql.Conn` streams multiplexed over a few QUIC connections" — is
    therefore incompatible with the server's per-connection stream handling.)
  - **Approach 1 (tried, reverted): dial off-lock.** `acquire()` held `p.mu`
    across `dialLocked()`'s QUIC handshake, serializing establishment; this
    explains why `pool_size=64` made the stall WORSE (more serial dials). I
    rewrote `acquire()` to dial outside the lock. It built + passed the unit
    suite but did NOT fix the stall (because the default `pool_size=16` still
    forces multi-stream, the real blocker) — reverted to keep the tree clean.
  - **Approach 2 (tried, reverted): serialize HELLO under `pc.mu`.** Hypothesised
    a concurrent-HELLO race; holding `pc.mu` across `conn.hello()` did NOT help
    and made `N=64` worse (the failure is stream multiplexing, not HELLO) —
    reverted.
  - **Likely correct fix (NOT yet implemented — needs care, possibly server-side
    too):** make the driver use **one QUIC connection per active `sql.Conn`** (no
    concurrent stream multiplexing) AND dial off-lock so establishing many
    connections is parallel; size/grow the pool to `MaxOpenConns` rather than
    capping at 16. This must be validated against the server's ability to accept
    a burst of N QUIC connections from a single client endpoint (a 2-endpoint
    test of 32 total connections works today; a 1-endpoint test of 32 needs
    confirmation), and may require a complementary server-side fix to support
    concurrent streams per connection. Treat as a dedicated transport workstream,
    not a single-spot patch.
  - **Current safe mitigation in production:** divelife `data-ingest` clamps its
    pool + worker fan-out to 16 (commit `126bef7`), which is validated to keep
    bulk ingest stall-free.
  - **2026-09-13 update — now intermittently visible in the integration lane.**
    Task 8b unblocked the strict-tenancy lane (SONNY-806), so the concurrency
    tests reach real load again instead of failing instantly on GQLSTATE 42501.
    Two consecutive full `make test-integration-docker` runs each produced ONE
    extra failure, a different test each time — `TestDocker_ConcurrentQueries`
    (10 goroutines, one `context deadline exceeded`, 30.4 s) and
    `TestLoad_MixedReadWrite` (`reader 8 iter 13: context deadline exceeded`,
    `pool={15 7 0 7 577 2.22s 15 0 0}`, 33.2 s). Same signature as the
    "~1 stream per connection makes progress" finding above, at the ~0.06 %
    rate already recorded. Both pass in isolation (3/3 for
    `TestDocker_ConcurrentQueries`). No new root cause — this is BUG-0002
    surfacing, and it is why the lane is not yet reliably green at 0 failures.

## Closed Findings

### GAP-SONNY806-08: the SONNY-1219 anchor stream dropped the session's tenant and graph scope

- **Severity:** Critical (a tenant-bound session read ZERO rows, including rows
  it wrote itself; a silent wrong answer, not an error)
- **Component:** this DRIVER (`conn.go` `pooledHello`, the SONNY-1219 anchor
  stream) — NOT the engine
- **Status:** **Closed** 2026-09-16, Lane D Task 16b, commit
  `fix(driver): bind every pooled request stream with its own HELLO`
- **Introduced by:** `05b4653 fix(pool): anchor each QUIC connection on a
  dedicated HELLO stream (SONNY-1219)`

**Observed.** Before `05b4653` every request stream performed its own HELLO.
HELLO is what carries `tenant` and the bound graph, and the engine applies both
PER STREAM. `05b4653` moved HELLO onto a dedicated anchor stream and made
request streams skip it — its commit message counts removing "the burst of
up-to-8 concurrent redundant HELLOs" as a benefit. They were not redundant.
Without them the engine served request streams with no concrete scope while the
driver reported the anchor's binding client-side, so a scoped read matched
nothing and a scoped write was invisible to the session that made it.

**Bisected against the engine**, which excludes the 0.8.18 bump as a cause:

| driver commit | engine | `ParamsBindInPropertyMaps` | `CrossTenantReadIsDenied` |
|---|---|---|---|
| `29d373b` (pre-anchor) | 0.8.15 | PASS | FAIL - the old cross-tenant LEAK |
| `29d373b` (pre-anchor) | **0.8.18** | **PASS** | **PASS** |
| `05b4653` (anchor) | 0.8.15 | FAIL - `no rows in result set` | FAIL - `saw 0 of its own nodes` |
| `7fe7b25` (anchor) | 0.8.18 | FAIL - `no rows in result set` | FAIL - `saw 0 of its own nodes` |
| **fix** | **0.8.18** | **PASS** | **PASS** |

**Why it hid.** The failure only shows on a SCOPED session. No unit test bound a
tenant, and the SONNY-1219 review that approved `05b4653` ran targeted `-race`
unit tests plus a burst load test, none of which bind one. The full integration
lane was never re-run on a quiet host between `05b4653` and Task 16.

**Fixed (DECISION-D-16b-01).** `pooledHello` performs an **unconditional**
HELLO on every request stream — no `isAuthed` fast path remains on that path —
and `pooledGraphInit` reads the binding from that stream's own HELLO response
rather than from the pooled connection's. `Conn.adoptHelloState` is deleted with
the assumption it encoded. The anchor stream is kept for the one thing it is
for: owning the QUIC connection's lifetime so a finished request cannot take its
siblings down with it (SONNY-1219).

Reverting the anchor was rejected on evidence. At 0.8.15 a revert would have
been correct only by accident — the engine destroyed a pooled connection the
moment its first request stream closed, so nearly every request stream WAS a
first stream doing its own handshake. Engine 0.8.18 `f80a812d` keeps a
connection alive until its LAST stream drains, so siblings now survive, and a
revert would leave them alive AND unbound: the same defect.

**Regression coverage.** `TestIntegration_TenantBoundSessionReadsItsOwnWritesOnEveryStream`
(a tenant-bound session writes and reads across four simultaneously held request
streams; a second tenant sees none of it) and, with no engine needed,
`TestAnchor_RequestStreamCarriesTenantAndGraphBinding` /
`TestAnchor_EveryRequestStreamBindsItself`, which assert on the wire that each
request stream's own transport carries a `HelloRequest` naming the configured
tenant, graph and role.

### GAP-SONNY806-05: an abandoned gRPC result wedged the next statement on the same session

- **Severity:** High (availability; a live, `IsValid` connection stalls the
  next statement until its context expires)
- **Component:** **Go DRIVER** — `grpc_transport.go` (`SendProto` Execute arm,
  `Close`), `rows.go` (`Rows.Close`). The engine behaviour it collides with is
  by design.
- **Status:** **Closed** (filed 2026-09-14, fixed 2026-09-14, SONNY-806 /
  GEO-GO-UP-001 Lane D Task 14c)
- **Reproducer:** `TestIntegration_GRPCEarlyCloseDoesNotWedgeTheSession`;
  unit coverage in `rows_pushed_test.go`
  (`TestRows_PushedPages_EarlyCloseCancelsTheStream`) and
  `grpc_transport_cancel_test.go`.

**Observed (while open).** A caller that stopped reading a large gRPC result
before its final page — `rows.Close()` after one page, with a live context —
left the connection valid and pooled, and the NEXT statement on that connection
blocked until its own context expired.

**Root cause — driver.** geode serves `Execute` as a server-streaming RPC and
pushes every page unasked. The driver stopped reading but never cancelled the
RPC, so the engine went on pushing into a flow-control window nobody drained.
The engine's push loop holds the session mutex across the send
(`src/transport/grpc_server.zig:1065-1090`, `entry.mutex.lockUncancelable`) and
`handleExecute` (`:1935`) takes that same mutex, so a filled window parked the
loop INSIDE the critical section. The next `Execute` on that session waited
behind it. Nothing failed, so `Conn.IsValid()` stayed true and `database/sql`
handed the connection straight on. A second `Execute` made it worse rather than
better: it overwrote `execStream` and orphaned the first RPC with its context
still live.

**Fix.** `GRPCTransport` keeps the cancel of the context each `Execute` rides
and fires it on the next `Execute`, on `Close`, and on a non-final `Rows.Close`
when `Transport.PagesArePushed()` (through the optional `streamCanceller`
assertion). The first of those fires BEFORE the new `Execute` goes out.
Ordering is not cosmetic and was measured: cancelling the superseded RPC after
the next one is already in flight on the same session poisons that session at
0.8.15, and `TestIntegration_GRPCBatchExecPastOnePage` (647 statements in one
transaction) then fails around statement 29 with `driver.ErrBadConn`. The
engine appears to mishandle a `RST_STREAM` that arrives while it is taking up
new work on the session — the same grpc-zig weakness GAP-SONNY806-07 (SONNY-1560) asks to
have fixed. Cancelling rather than `markConnFailed` is deliberate: the RPC is
per-`Execute`, so tearing it down leaves the session and the `ClientConn`
usable and the connection stays poolable. Retiring it would trade a wedge for
an eviction and a re-dial on every early `Close`.

**Measured, and the limit of the fix.** With a result the engine can finish
pushing, an early `Close` now leaves the session usable immediately: the next
statement on the same driver `Conn` answered in **0.65 ms** with the cancel
versus 2.3 ms without. With a result past the GAP-SONNY806-07 (SONNY-1560) ceiling neither
behaviour is usable — the engine is parked inside its push loop and ignores the
client's `RST_STREAM`, so the next statement took **34 s** without the cancel
and failed with `driver.ErrBadConn` after **30 s** with it. That is the
engine's stall surfacing as a connection error rather than a 34-second wait,
and `database/sql` evicts and retries on a fresh connection; it is not a
regression the cancel introduced, and it cannot be fixed driver-side. The
cancel is kept because orphaning an RPC is wrong on its own terms: the stream
stays open for as long as the caller's context lives, holding client and server
state nobody will ever read.

### GAP-SONNY806-06: the trailing-frame drain could park on the wire and eat a pushed page

- **Severity:** Medium (availability + silent truncation, on the gRPC wire only)
- **Component:** **Go DRIVER** — `conn.go` (`drainTrailingFrames`),
  `grpc_transport.go` (`ReceiveProto` precondition)
- **Status:** **Closed** (filed 2026-09-14, fixed 2026-09-14, SONNY-806 /
  GEO-GO-UP-001 Lane D Task 14c)
- **Reproducer:** `TestRows_PushedPages_FinalFlagStopsTheRead` asserts the
  frame queued behind the final page is still unread after `Close`.

**Observed (while open).** `Conn.releaseRows` ran a 1 ms blocking drain built
for QUIC's trailing metrics/heartbeat frames. It passed `ReceiveProto` a fresh
`context.Background()` deadline.

**Root cause — driver.** Only the context an `Execute` stream was created from
binds that stream: grpc-go ties the RPC to it and to nothing else. A background
1 ms deadline therefore does not unblock `Recv`, so the drain could park on the
wire while holding `c.mu` — the lock every other operation on that connection
needs. And on a non-final release there IS something to read on a pushed-page
wire, so a drain that succeeded would discard a page of the caller's result
rather than a stray trailing frame.

**Fix.** `drainTrailingFrames` returns immediately when
`Transport.PagesArePushed()`. The GAP-SONNY806-05 cancel ends the abandoned
stream outright, so nothing is left to drain. `GRPCTransport.ReceiveProto`'s
doc now states the precondition explicitly.

### GAP-SONNY806-04: gRPC results stopped at the engine's first pushed page

- **Severity:** High (silent-truncation class; fail-closed while open)
- **Component:** **Go DRIVER** — `rows.go` (`fetchNextPage`), `grpc_transport.go`.
  NOT an engine defect and NOT a protocol asymmetry.
- **Status:** **Closed** (filed 2026-09-14, fixed 2026-09-14, SONNY-806 /
  GEO-GO-UP-001 Lane D Task 14b)
- **Reproducer:** `integration_grpc_test.go` →
  `TestIntegration_GRPCPagedResultIsConsumedAcrossPushedPages` (5×128+7 rows,
  asserts the full count and the ordering); unit coverage in
  `rows_pushed_test.go`

**Observed (while open).** Over gRPC, any statement whose result exceeded 128
rows failed with a `*TransportError` wrapping `ErrUnsupportedOnTransport` after
exactly 128 rows. `Pool.BatchExec` was included, which is why the first round of
gRPC parity tests seeded row by row.

**Root cause — driver, not engine.** geode 0.8.15 serves `Execute` over gRPC as
a SERVER-STREAMING RPC and pushes the WHOLE result: `handleExecuteStreaming`
(`src/transport/grpc_server.zig:1009-1101`) sends the schema/first response and
then loops `buildStreamDataPageLocked` → `sendStreamResponse` until the page it
built is final, paging at `grpc_stream_page_size` (`grpc_server.zig:915` →
`src/client/quic_query_runner.zig:5` = **128 rows**), then ends the stream. Every
page was already on the wire, and `GRPCTransport.ReceiveProto` already returned
them off `execStream.Recv()`. The driver asked for them anyway:
`Rows.fetchNextPage` sent a `PullRequest` after every non-final page because the
QUIC protocol requires one, and `GeodeService` declares no `Pull` method
(`proto/geode_grpc.pb.go`, `GeodeService_ServiceDesc`), so the WRITE failed and
the read never happened.

The original diagnosis ("protocol asymmetry; gRPC cannot page") mistook the
driver's PULL for a requirement of the wire. It was not: the pages were there to
be read.

**Fix.** `Transport.PagesArePushed() bool` — false on QUIC (PULL-driven), true on
gRPC (server-pushed). `Rows.fetchNextPage` sends the `PullRequest` only when the
transport is PULL-driven and otherwise reads the next pushed page off the same
stream, honouring `DataPage.final` exactly as before. The QUIC path is unchanged.
The Task 9 cancellation contract holds on both wires: a cancel while parked
waiting for a page surfaces the wrapped `ctx.Err()` and retires the connection
(`GRPCTransport.ReceiveProto` reports the context error rather than the gRPC
`recv` status; the RPC is bound to the same context, so a parked `Recv` unblocks
on cancel). `Conn.absorbPlanFrame`'s page loop takes the same branch.

`ErrUnsupportedOnTransport` is kept and re-scoped: its live cause is the five
`QuicClientMessage` oneof arms `GeodeService` declares no method for (savepoint,
rollback_to, backup, restore, upload_backup); the `PULL` arm remains as a
defensive assertion for a caller that reaches past the page loop. It no longer
caps any result set.

**Residual limit (engine, not driver).** `handleExecuteStreaming` stops after
1000 pages as a safety limit (`grpc_server.zig:1069`), i.e. ~128,000 rows per
gRPC result set. Not reached by anything in this lane; recorded here so a future
short read over gRPC is diagnosed there rather than re-litigated as a paging bug.

### GAP-ENG4658-02: pooled connections were never verified with network I/O

- **Severity:** High (availability)
- **Component:** QUIC connection pool (`quic_pool.go`) / pool health
- **Status:** **Closed** (fixed 2026-09-13, SONNY-806 / GEO-GO-UP-001 Lane D
  Task 11)
- **Reproducer:** `integration_health_test.go` →
  `TestIntegration_RecoversFromServerRestart` (both arms: probed and unprobed)

**Observed.** `isQUICConnAlive` (`quic_pool.go`) decided whether a pooled QUIC
connection was usable by reading its local `quic.Conn` context and nothing else
— no network I/O. That is the right answer for a connection the client tore
down, and the wrong answer for one the SERVER forgot. When the Geode process
dies without closing its connections (a crash, an OOM kill, a `SIGKILL`
rollout, a network partition), no CONNECTION_CLOSE ever reaches the client:
the context stays live, `pruneDeadLocked` keeps the connection, `acquire` keeps
handing it out, and every statement on it waits out its caller's full deadline.
`quicMaxIdleTimeout` is 120 s, so the transport does not rescue those callers
either.

Measured against engine 0.8.15 on the restart test's control arm (an otherwise
identical pool with probing disabled — i.e. the pre-fix driver): the first
statement after a `SIGKILL` restart consumed **6.000 s of its 6 s deadline** and
then failed with `context.DeadlineExceeded`, three runs out of three.

A graceful `docker stop` does NOT reproduce it: the engine closes its
connections on SIGTERM, quic-go cancels the context, and even the unprobed pool
recovers in ~37 ms. The defect is specifically the ungraceful death — which is
the one that happens in production.

**Downstream impact.** `mentats/internal/geodegraph` carries a ~100-line
watchdog, `poolWatchdog` (`geodegraph.go:36-81`), whose entire job is to work
around this from outside the driver. It can now be retired once mentats picks
up this driver's probing fix — gated on `>= v0.2.0`, the first tagged release
that carries `(*QUICPool).Probe` — and not before: pinning an unreleased
commit would tie mentats' dependency to a moving target. Owner: `@maintainers`
(this driver's default CODEOWNERS entry) tracks the mentats-side removal
alongside the v0.2.0 release.

**Fix.** The pool verifies its connections with a real round trip and evicts
what does not answer:

- `(*QUICPool).Probe(ctx)` PINGs every pooled connection over a **long-lived
  probe stream**, concurrently, each on a bounded deadline. The stream is
  opened once per connection and reused, never opened and closed per probe:
  geode 0.8.15 tears the whole pooled QUIC connection down when a stream that
  carried a handshake goes away (SONNY-1227), so a probe that closed its stream
  would cause the outage it is meant to detect. It carries no HELLO, because on
  a pooled connection the server authenticates the connection — which is why
  every stream after the first already skips the handshake.
- **A failed probe abandons its stream, it does not close it.** Eviction takes
  `probeStrikes` (2) CONSECUTIVE failures, so at strike 1 the connection is
  still pooled and still carrying live request streams — closing its probe
  stream then is the SONNY-1227 teardown, i.e. the very outage being detected.
  The stream is dropped instead, the next probe opens a fresh one, and every
  abandoned stream is closed together with the connection in `discard` /
  `Close`. A connection that has abandoned `probeStrikes` streams is condemned,
  which bounds the abandoned set. Reproducers:
  `TestQUICPool_FailedProbeNeverClosesTheStreamOfALiveConn`,
  `TestQUICPool_EvictionClosesEveryProbeStream`,
  `TestIntegration_AFailedProbeLeavesTheLivePooledConnectionServing`.
- **Stream setup is budgeted per stream.** A reused probe stream gets the
  steady-state budget (`probeTimeout`); a REPLACEMENT stream — opened because a
  failure abandoned the last — gets that plus `probeStreamSetup` (500 ms),
  because the server materialises a probe stream on its first frame (~70 ms
  measured) and charging that to a bare round trip made the retry fail too,
  which is strike 2 and a healthy connection evicted. The first stream on a
  connection nothing has ever answered on still gets `probeWarmupTimeout` (5 s).
- **A caller's cancellation is not evidence.** A probe that fails because the
  caller's context ended is reported in the status but records no strike and
  leaves the probe stream alone, so a `Pool.Health` call with a short or
  already-cancelled context cannot condemn the pool it is reporting on.
- A connection that fails a probe is marked dead, removed from the pool and
  closed. `pruneDeadLocked` treats the probe verdict as a liveness signal
  alongside the local context check, so `acquire` can never hand one out. A
  connection no HELLO has run on yet is SKIPPED by the probe and counts as
  neither live nor healthy: nothing about it was verified.
- A pool-owned goroutine runs the probe on `Config.HealthProbeInterval`
  (DSN `?health_probe_ms=N`, default 1000, 0 disables). It starts lazily on the
  first `acquire`, never runs concurrently with itself, and is stopped and
  waited for by `Drain` and `Close`.
- `Pool.Health(ctx)` reads the last recorded result (cheap) and returns a
  `HealthStatus` whose zero value is unhealthy — health fails closed.
  `LastProbeErr` is a static classification, never the underlying error, so a
  readiness endpoint cannot leak a `*TransportError`'s remote address.
- `Pool.Drain(ctx)` refuses new work with `ErrPoolDraining`, waits for in-flight
  statements, then closes everything.

**Evidence after the fix** (engine 0.8.15, `SIGKILL` restart, probe interval
200 ms): the probe recorded the pool unhealthy 7 ms / 179 ms / 140 ms after the
engine was confirmed gone, the orphaned connection was evicted, and the first
post-restart statement succeeded in 331 ms / 237 ms / 400 ms — against the same
restart's control arm stalling the full 6 s every time. After the review fixes
above (the round that notices the kill now has to open a replacement probe
stream, and pays `probeStreamSetup` for it): unhealthy 638 ms after the kill,
first post-restart statement 555 ms, control 6.001 s.

### BUG-0001: QUIC stream desynchronization on sequential queries

- **Severity:** Medium
- **Component:** Transport / Connection
- **Status:** **Closed** (fixed 2026-03-21)
- **Documented:** [docs/BUG_STREAM_DESYNC.md](BUG_STREAM_DESYNC.md)
- **Downstream impact:** divelife-api `GET /v1/feed` returns HTTP 500 intermittently ([GAP-0046](https://gitlab.com/devnw/sealife/divelife-api/-/issues/61))
- **Root cause:** `releaseRows()` used only a non-blocking `TryReceiveProtoBuffered()` drain. Trailing server frames (metrics, heartbeat) still in the QUIC socket but not yet in the 64 KB `bufio.Reader` were missed, leaving them to be read as responses to the next query.
- **Fix:** Added a Phase 2 short-deadline blocking drain in `releaseRows()` (`conn.go`). After the non-blocking pass, a `context.WithTimeout(…, trailingFrameDrainTimeout)` blocking `ReceiveProto` loop runs until no metrics/heartbeat frames remain or the deadline elapses. Tests in `conn_stream_desync_test.go` (8 cases) reproduce the exact desync and verify the fix.

## Stack-ranked findings

| Rank | Severity | RiskScore | Finding | Component | CWE/OWASP | Evidence (file:line) | GitLab Issue | Fix Summary | Owner | SLA/Due | Status |
|------|----------|-----------|---------|-----------|-----------|----------------------|--------------|-------------|-------|---------|--------|
| 1 | High | 48 | Go toolchain pinned to vulnerable 1.24.0 (stdlib CVEs) (confidence::confirmed) | Supply chain | CWE-1104 | go.mod:3, .gitlab-ci.yml:22-30 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/28> | Upgraded Go toolchain to 1.24.12 in go.mod and CI; osv-scanner clean | TBD | 30d (2026-03-01) | **Closed** |
| 2 | Medium | 36 | CA/cert/key loading allows special files to bypass size limits (confidence::confirmed) | TLS config | CWE-400 | security.go:95-115, security.go:150-186 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/27> | Added readLimitedFile to enforce regular files and size limits; tests in security_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 3 | Medium | 36 | Unbounded inbound frame size (confidence::confirmed) | Transport/Protocol | CWE-400 / API4:2023 | transport.go:205, transport.go:254 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/22> | Added 32MB frame limit via readLineLimited; tests in transport_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 4 | Medium | 32 | Explicit CA path fails open to system CAs (confidence::confirmed) | TLS config | CWE-295 | security.go:96, security.go:135 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/23> | Explicit CA paths now fail closed; tests in security_test.go | TBD | 90d (2026-04-30) | **Closed** |
| 5 | Medium | 32 | CI components pinned to mutable @v1 tags (confidence::confirmed) | CI/CD supply chain | CWE-829 | .gitlab-ci.yml:10-58 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/24> | Won't Do: catalog has other controls; pinning not used | TBD | 90d (2026-04-30) | **Closed (Won't Do)** |
| 6 | Low | 24 | SBOM scan allowed to fail (confidence::confirmed) | CI/CD | CWE-693 | .gitlab-ci.yml:53-55 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/25> | SBOM scan now required (allow_failure: false) | TBD | 180d (2026-07-29) | **Closed** |
| 7 | Low | 24 | Gitleaks allowlist excludes tests/ and testdata/ (confidence::confirmed) | CI/CD secrets | CWE-798 | .gitleaks.toml:16-19 | <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/26> | Removed broad allowlist entries for tests/testdata | TBD | 180d (2026-07-29) | **Closed** |

## Findings by area

App/API/Auth

- Issue #27: CA/cert/key loading allows special files to bypass size limits. **Closed**
- Issue #22: Unbounded inbound frame size allows client memory exhaustion. **Closed**
- Issue #23: Explicit CA path fails open to system CAs (pinning bypass). **Closed**

Data

- No new findings in this round.

CI-CD

- Issue #28: Go toolchain pinned to vulnerable 1.24.0 (stdlib CVEs). **Closed**
- Issue #24: CI components pinned to mutable @v1 tags. **Closed (Won't Do)**
- Issue #25: SBOM scan allowed to fail. **Closed**
- Issue #26: Gitleaks allowlist excludes tests/ and testdata/. **Closed**

IaC

- No new findings in this round.

Container

- No new findings in this round.

Ops

- No new findings in this round.

## Quick wins / Medium / Strategic

Quick wins (low effort)

- Tighten gitleaks allowlist to specific safe fixtures (Issue #26). **Done**
- Require SBOM scan to pass on protected branches (Issue #25). **Done**

Medium (engineering effort)

- Add max inbound frame size and reject oversized frames (Issue #22). **Done**
- Fail closed when explicit CA path is missing or unreadable (Issue #23). **Done**
- Reject non-regular cert/key/CA files and enforce bounded reads (Issue #27). **Done**

Strategic (pipeline hardening)

- Pin CI catalog components to immutable SHAs and document update process (Issue #24). **Won't Do**
- Upgrade Go toolchain to patched version (Issue #28). **Done**

## Won't Do

- Issue #24: Marked Won't Do because ci-catalog has other compensating controls and version pinning will not be used.

## Historical findings (closed)

The following issues were addressed in prior reviews (Rounds 1-2) and are closed. See each issue for historical evidence and remediation details.

| Issue | Title | Status |
|-------|-------|--------|
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/1> | Security: Benchmark tool defaults insecure_tls_skip_verify=true (CWE-295) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/2> | Security: Gitleaks allowlist disables scanning for all source files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/3> | Security: Hostname validation function exists but is never called | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/4> | Security: Complex manual lock management in QueryContext may lead to lock leaks | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/5> | Security: Example code demonstrates insecure_tls_skip_verify=true pattern | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/6> | Security: CI pipeline allows test failures without blocking | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/7> | Security: Pre-commit CI skips golangci-lint including gosec security checks | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/8> | Security: No size limit on certificate file loading | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/9> | Hardening: Add client-side connection rate limiting | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/10> | Supply Chain: Compiled binary tracked in git repository | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/11> | Security: Client cert/key files lack size limits (DoS) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/12> | Supply Chain: CI catalog components use mutable @latest tags | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/13> | Security: Deprecated driver methods use unbounded context | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/14> | Security: gosec linter disabled for test files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/15> | Security: Gitleaks allowlists all test files | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/16> | Hardening: Pre-commit YAML check uses --unsafe flag | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/17> | Hardening: Error messages include network addresses | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/18> | Supply Chain: Docker images use mutable :latest tag in Makefile and tests | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/19> | Security: No upper bound on page_size configuration (potential DoS) | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/20> | Security: Benchmark CLI uses unbounded context.Background() | Closed |
| <https://gitlab.com/devnw/codepros/geode/geode-client-go/-/issues/21> | Hardening: CODEOWNERS missing security-sensitive code files | Closed |

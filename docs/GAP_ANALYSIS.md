# GAP Analysis

Last updated: 2026-09-13
Reviewed by: Adversarial Security Review Agent (2026-03-21); divelife bulk-seed investigation (2026-06-17); Lane D tenancy/role binding (SONNY-806 / GEO-GO-UP-001, 2026-09-13)
Repository: devnw/codepros/geode/geode-client-go
Commit: b27e102 → acf49a2 → 5f9a4b0 (see git log)
Review type: Comprehensive adversarial security review (Round 6 - no new findings); BUG-0001 closed 2026-03-21; BUG-0002 filed 2026-06-17 (high-concurrency connection-establishment stall); GAP-SONNY806-01 and GAP-SONNY806-02 filed 2026-09-13 against engine 0.8.15

## Summary

Open findings by severity:

- Critical: 1 (GAP-SONNY806-02 — engine cross-tenant read leak, filed 2026-09-13)
- High: 0
- Medium: 1 (GAP-SONNY806-01 — engine refuses `$param` in CALL arguments, filed 2026-09-13)
- Low: 1 (BUG-0002 — downgraded 2026-06-17)
- Info: 0

Closed findings: 28 (Issues #1-#23, #25-#28; BUG-0001)
Won't Do findings: 1 (Issue #24)

Top risks (remaining):

- **GAP-SONNY806-02 (Critical)** — geode engine 0.8.15 does not apply the session
  tenant filter on the `MATCH` read path. Two sessions that bind different
  tenants at HELLO read each other's rows in the same graph, even with
  `GEODE_STRICT_TENANCY=1`. This is an ENGINE defect, not a driver defect; the
  driver cannot work around it. Reproducer:
  `TestIntegration_CrossTenantReadIsDenied` (currently RED by design).

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

### GAP-SONNY806-02: engine does not enforce tenant isolation on the MATCH read path

- **Severity:** Critical
- **Component:** geode ENGINE 0.8.15 (read path) — not the Go driver
- **Status:** **Open** (filed 2026-09-13, SONNY-806 / GEO-GO-UP-001)
- **Reproducer:** `integration_tenancy_test.go` →
  `TestIntegration_CrossTenantReadIsDenied` (RED; this is the P0 evidence and is
  deliberately not skipped)

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
   sessions resolved a concrete scope from HELLO. The driver therefore does
   deliver `tenant` into `HelloRequest` (`conn.go`).
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

**Consistent with the engine's own capability rows** at 0.8.15
(`testdata/capabilities_0.8.15.golden`): `tenant_scope_fail_closed=false`,
`tenant_binding_server_verified=false`, `graph_procedure_tenancy=false`.

**Impact.** Any multi-tenant deployment on 0.8.15 leaks every tenant's data to
every other authenticated session bound to the same graph. Graph-level isolation
still holds (a session in a different graph sees zero rows).

**Driver-side mitigation:** none possible — isolation is a server-side property.
Until the engine fixes it, treat one graph per tenant as the only supported
isolation boundary.

### GAP-SONNY806-01: engine refuses `$param` in CALL geode.fts.*and algo.* arguments

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

## Closed Findings

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

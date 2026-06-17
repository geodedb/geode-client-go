# GAP Analysis

Last updated: 2026-06-17
Reviewed by: Adversarial Security Review Agent (2026-03-21); divelife bulk-seed investigation (2026-06-17)
Repository: devnw/codepros/geode/geode-client-go
Commit: b27e102 → acf49a2 (see git log)
Review type: Comprehensive adversarial security review (Round 6 - no new findings); BUG-0001 closed 2026-03-21; BUG-0002 filed 2026-06-17 (high-concurrency connection-establishment stall)

## Summary

Open findings by severity:

- Critical: 0
- High: 1 (BUG-0002)
- Medium: 0
- Low: 0
- Info: 0

Closed findings: 28 (Issues #1-#23, #25-#28; BUG-0001)
Won't Do findings: 1 (Issue #24)

Top risks (remaining):

- **BUG-0002** — a single `*sql.DB` with `SetMaxOpenConns` > ~16 (the QUIC pool
  size) deadlocks/stalls bulk workloads: connections beyond the pool fail their
  HELLO handshake. Caps real ingest concurrency per client process.

## Open Findings

### BUG-0002: high-concurrency connection establishment stalls (MaxOpenConns > QUIC pool)

- **Severity:** High
- **Component:** Transport / Connection pool (`quic_pool.go`, connector/HELLO path)
- **Status:** **Open** (filed 2026-06-17)
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

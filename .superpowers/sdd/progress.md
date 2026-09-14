# Multiplex driver SDD progress

Branch: multiplex-driver (off b5e9d17)
Plan: docs/superpowers/plans/2026-06-21-multiplex-driver.md

## Task 0 — De-risk gate: PASSED (commit 1348f03)

Proved the geode server multiplexes N concurrent streams on ONE QUIC connection
under find+create (MATCH+MERGE) load: 8 streams -> 23696 op pairs, final count
exact, no wedge / desync / deadline race.

ROOT CAUSE of the earlier false WEDGE (3 layered bugs, all now understood):

1. Probe did a per-stream HELLO -- wrong. Auth is per QUIC *connection*
   (conn.go pooledHello); secondary streams skip HELLO and query directly.
2. Probe CLOSED the HELLO stream before workers ran -- the FIN tore down the
   connection auth; server cleanly closed sibling streams (NO_ERROR).
3. Server flag: GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING defaults FALSE
   (server_init.zig:54). Without it the server only services the first stream
   per conn; additional streams hang. With it, the streamAcceptorLoop services
   them. Server side is correct + tested (REQ-QUIC-HC-030).

DEPLOYMENT REQUIREMENT carried forward: the multiplex client only works against
a server started with GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1. Every Task 1-6
test harness MUST set it.

## Next: Task 1 -- config max_streams_per_conn (default 8)

## Lane D (Geode Go driver, SONNY-806 / GEO-GO-UP-001) SDD progress

Branch: eng-4622/geode-go (worktree .worktrees/eng-4622-geode-go, base cd6e4fe = origin/main 2026-09-13)
Plan: sonny-upstream docs/superpowers/plans/2026-09-06-epic3-geode-go.md (Tasks 1–16 in this repo; 17–36 in go/geode)
Engine pin correction: geodedb/geode:0.8.15 (index digest sha256:96a194cfce01e59ee1dbe1504aa0ed79c0141f7e2e8b23389da18e73e6adc8ee), not 0.8.13.
Task 1 dispatched 2026-09-13 14:05 EDT from BASE cd6e4fe.
Task 1: complete (commits cd6e4fe..b298068: b7e747d pin+race lane+CI, b298068 Go default pin/engine_pin_test/auth fail-fast/reset-flow test; review Approved after I1-I3 fixes). SHOW VERSION returns "v0.8.15". 12 pre-existing read-after-write integration failures against 0.8.15 under separate triage; golangci-lint pre-commit pin bumped v2.11.4->v2.13.1 (blocked every commit). Minor: go_version 1.25.8 component inputs vs go 1.27 module.
Task 2: complete (commits ed45cea..98dc938: 2162133 proto regen from engine ebfd8812 + proto/CANONICAL, 650e860 CDC/backup/restore/upload drift guards, 98dc938 --data-dir SONNY-1218 workaround; review Approved). Minor open: protoc binary unpinned; CANONICAL_PROTO default path fails under worktree layout; new session_id/payload fields unused by call sites.
Task 3: complete (commit 153a81c: ? placeholders bind as typed proto.Param, rewritePlaceholders/inlineValue/gqlStringEscaper deleted; report .superpowers/sdd/task-3-report.md). Engine 0.8.15 confirmed to bind $name/$N inside property maps (eval.zig:886, match_operations.zig:517); integration proof passes with the inliner gone. Brief correction: int64 must use IntKind_INT, not BIGINT — BIGINT maps to .BigInt (proto_util.zig:58-63) and is stored as an ExtValue property (graph_store.zig:1438), which broke `WHERE n.c = 5` and sum(). Containers stay structural (ENG-3910) instead of the brief's strict scalar switch. Integration lane: 362 PASS / 1 FAIL (TestLoad_BurstExceedsPoolSize_NoDeadlock, SONNY-1219, pre-existing) / 3 SKIP. Open: graph name still interpolated into CREATE/USE GRAPH (conn.go:243/246, cfg.Graph unvalidated); scanner does not track double-quoted literals.
Task 3 review: Approved after fix cee86cb (time.Time binding restored via proto.MakeParam; ScanTime goStringerLayout added). Range ac85b28..cee86cb. Open minors: time.Time string Kind left UNSPECIFIED; scanner blind spots (double quotes, backslash escapes, comments) → Task 5 fuzz; cfg.Graph interpolated at conn.go:243/246/298/302 with no Config.Validate check → needs a task; named args unreachable on the prepared path (NumInput).
Task 4: complete (commit f870117 typed Vector params; review Approved C0/I0/M2). Engine facts at 0.8.15: dimension mismatch SQLSTATE 22000 / metric mismatch 42704 in DriverError.Code (Class empty); only vectorf32($v) storage arms the pin/index (SONNY-1224); knn silently downgrades to scan when another graph shares the property name (SONNY-1223). Minor: add NaN/subnormal unit cases (Task 5 fuzz).
Task 5: complete (commits 832dd25..ae59944: 4 fuzz targets + fuzz-params target; argument-independence property q==q2; named-name fuzzing; review Approved after I1/I2 fixes). Scanner blind spots (double-quoted identifiers first) need a scanner-fix task before v0.2.0.
Task 6: complete (commits abb3fce..7da816e: read-only Config/DSN, fail-closed classifier with comment stripping, whole-statement CALL allow-list, wire ReadOnly on every ExecuteRequest, pooled+standalone graph-init parity; review Approved after I1/I2 fixes). Deviation: read-only conns never CREATE GRAPH (graph must pre-exist). Follow-ups: readOnlyProcedures drift detection vs engine table; conservative leading WITH/UNWIND/UNION.
Task 7: complete (commits 4c260d5..b56454d: ServerInfo/Capabilities with strict contract parsing, HELLO capability capture, 0.8.15 golden (59 rows = contract), wire-shape unit tests; review Approved after I1 fix). Pool defect (Connect→Close→Connect on shared QUIC pool) filed as SONNY-1227; ServerInfo uses a dedicated connection by design.
Task 8: complete (commits 5f9a4b0..01ce7a2: StrictTenancy config/DSN, ErrTenantRequired + ErrTenantScopeRequired (message-qualified 42501), readonly-role engine refusal test, cross-tenant test as P0 reproducer (SONNY-1229, RED on purpose), docker-start auth/data-dir lane fix; review Approved after I1 fix). Engine gaps commented on SONNY-778/SONNY-635. Branch integration job stays red until SONNY-1229 lands; Task 16 gated.
Task 8b: complete (commits d7f920c + 02940a6: bootstrap tenant `it-harness` created once in TestMain and bound in sharedDSN; roDSN gains a tenant so the read-only-role refusal is proven rather than the 42501 tenant refusal; cross-tenant probe strips via dsnWithoutTenant; CI/Makefile `GEODE_TEST_TENANT`; ParseDSN repeated keys documented as last-occurrence-wins (GoDoc, DSN Format, CHANGELOG) and pinned for role/ca; harness tenant consistency test; ensureHarnessTenant fails fast except on TenantAlreadyExists). Lane 387/49/3 → 434 PASS/3 FAIL/3 SKIP; remaining: CrossTenantReadIsDenied (SONNY-1229 not in pinned 0.8.15), BurstExceedsPoolSize (SONNY-1219), one rotating BUG-0002 deadline under load. Review Approved after the docs fix. Worktree git index was found corrupt and rebuilt from HEAD (fsck clean).
Task 9: complete (commit 7010ec8: Rows.Next returns a wrapped ctx.Err() before each PULL and while blocked in receive via the transport's joined deadline watcher; cancelled Rows retires the Conn (IsValid false, ResetSession ErrBadConn); no goroutine per cancel; 4 integration + 6 unit tests; 0.8.15 has no discard/reset message so the stream is closed and documented; SONNY-1227 workaround confined to the concurrency test via per-worker pools; found and filed SONNY-1324 PULL desync >1000 rows). Review Approved C0/I0/M6.
Task 9b: complete (commit db94df6: abortOnCancel doc corrected for push-driven pages; Close reads final under r.mu; settledGoroutines requires n==last; TransportError-while-cancelled unit test; CancelMidStream bound = first page size). Minor 6 (non-cancel transport error not retiring the conn) folded into SONNY-1324.
Task 10: complete (commits 3ba25ff Pool.Session on database/sql's *sql.Conn with ConnID (monotonic connSeq), fail-closed IsValid probe via Raw, TX commit/rollback/re-panic, ErrSessionBusy on nesting, ErrSessionRetired after cancellation retirement; c6c7dff polish: post-retirement re-pin contract in godoc/CHANGELOG (SONNY-1227), checkPinned/adoptPinned agreement, TX observes fnErr). Review Approved (C0/I1 doc → fixed/M3). 14 unit + 4 integration tests.
Task 11: complete (commits 385490a health probe/drain: PING on a long-lived per-connection probe stream (never opened/closed per probe — SONNY-1227), HealthStatus zero-value unhealthy, Pool.Health/Drain, QUICPool.Probe, health_probe_ms; restart-recovery test with a control arm (detection ~0.6 s vs a 6 s stall); 72c2c96 fix pass: a failed probe ABANDONS its stream and streams are closed only when the connection leaves the pool, per-stream budget (reused/replacement/first), caller-ctx cancellation never strikes, unauthed conns not live, harness binds 127.0.0.1 with GEODE_TEST_HOST_PORT). Reviews: Needs fixes (Critical: stream close on a live conn) → Approved. Open: enforce the abandoned-stream bound inside abandonProbe (Task 12 prelude); PreparedStatementStorm 4/17 vs 0/11 recorded as BUG-0002 noise (acquire path untouched).

# Geode-client-go QUIC Multiplex Driver Re-Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-enable correct QUIC stream multiplexing (multiple concurrent independent streams per QUIC connection) so all workloads — bulk MERGE (taxa) and find+create (MATCH+MERGE-relationship, edges) — run at multiplex throughput (~2,000+ ops/s) with zero wedges, desync, or deadline races.

**Architecture:** The driver already opens an *independent* bidirectional QUIC stream per request (own request/response pair, own `bufio.Reader`, own per-stream `SetReadDeadline` — the v0.1.27 ctx-watcher-join fix is per-stream). The "no-multiplex" workaround (BUG-0002, commit `7cb9f83`) restricts the pool to **one in-flight stream per connection** (`selectFreeConnIndex` returns only conns with `active==0`; admission bound = `min(maxStreams, maxSize)`). That serializes concurrency to the *connection* count (~3× slower). Because each stream is already independent, there is **no cross-stream demux, no shared read buffer, and no shared deadline** between streams; the only historical blocker was a server crash on concurrent streams, since fixed (BUG-0002 downgraded to Low, `f51bf3d`). So the safe re-implementation is **localized to the pool's stream-per-connection policy + admission bound**; the transport and desync-drain logic stay unchanged.

**Tech Stack:** Go, `github.com/quic-go/quic-go v0.59.0`, geode QUIC protobuf protocol.

## Global Constraints

- Module `geodedb.com/geode`. Do NOT change the quic-go version (v0.59.0).
- Each request gets its OWN bidirectional QUIC stream via `pc.openStream(ctx)` → `qConn.OpenStreamSync`. Do NOT put multiple requests on one stream (the protocol has no request-IDs; per-stream demux is out of scope and unnecessary).
- PRESERVE the v0.1.27 `ReceiveProto` ctx-watcher join fix (transport.go ~298-318) — per-stream, correct.
- PRESERVE per-operation stream draining: `drainExecStream`, `releaseRows` double-drain, `BeginTx` `HasBufferedData()` check (conn.go) — per-stream desync guards.
- PRESERVE: HELLO + graph-init run exactly ONCE per QUIC connection (double-checked lock on `pc.authed` / `pc.graphBound`, conn.go ~205-250) — must hold when multiple streams open on the same conn.
- Test server binary: `/home/benji/src/devnw/codepros/geode/geode/zig-out/bin/geode`. Use FRESH temp data-dirs + free ports **≥3590**. NEVER touch system geodes (PID 1783, 529355) or ports 3141 / 3200 / 3580 (a validated edgefix run is on 3580).
- Server launch for tests: `GEODE_ADMIN_PASSWORD=seed GEODE_WAL_SYNC=deferred GEODE_INDEX_MERGE_KEYS_ONLY=1 GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1 GEODE_METRICS_PORT=<m> <binary> serve --listen 127.0.0.1:<p> --data-dir <tmp>`. DSN: `localhost:<p>?insecure=true&user=geode&pass=seed`. Reuse the existing integration harness (`GEODE_TEST_DSN` env override, integration_main_test.go) where possible.
- **CRITICAL SERVER FLAG (Task 0 finding):** The server gates stream multiplexing behind `GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1` (`quic_stream_multiplexing_enabled`, default **FALSE** — server_init.zig:54). Without it the server only services the FIRST stream per QUIC connection; additional streams are accepted at the QUIC layer but the `streamAcceptorLoop` never runs, so their queries HANG (read deadline-exceeded, not a clean close). The client multiplex driver is therefore INOPERABLE against a server started without this flag. EVERY test harness in Tasks 1-6 MUST set it, and it is a **deployment requirement** that must be surfaced in the final report (and ideally the driver should fail-fast or warn if multiplexing is configured but the server doesn't honor it). The server side is correct and tested (REQ-QUIC-HC-030, `test-quic-high-concurrency-e2e`); no server change is needed — only the flag.
- Branch off current `main` (which has 7cb9f83 + b5e9d17 + d9fc4e3). Work on branch `multiplex-driver`. Frequent commits. Do NOT push (report for review).

---

## Task 0: De-risk — prove the server handles N concurrent streams on ONE connection

**Rationale:** The entire plan rests on the premise that the server no longer wedges on concurrent streams per connection (BUG-0002's crash is fixed). Verify this BEFORE changing the pool. If it still wedges, STOP and report — the server needs work first.

**Files:**

- Create: `multiplex_server_probe_test.go` (build tag `//go:build integration`)

**Interfaces:**

- Consumes: existing test harness `dsnForTest(t)` / `GEODE_TEST_DSN`; `NewQUICTransport(ctx, addr, tlsCfg)` (transport.go), `Transport.SendProto`/`ReceiveProto`, the HELLO + a simple `MATCH (n) RETURN count(n)` exec.
- Produces: confidence (a passing test) that ≥8 concurrent streams on ONE `*quic.Conn` complete find+create + reads without wedging.

- [x] **Step 1: Write the probe test** — done (`multiplex_server_probe_test.go`).
- [x] **Step 2: Run it** — done.
- [x] **Step 3: Decision gate** — **PASSED** (commit `1348f03`). 8 concurrent streams on ONE conn ran find+create to 23696 op pairs, final count exact (23696==23696), no wedge. The initial false WEDGE was a probe bug (per-stream HELLO + closed HELLO stream) compounded by the server's `GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING` flag being off. Server multiplex is correct + tested; proceeding to Task 1. **All downstream task harnesses must set `GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1`** (see Global Constraints).
- [x] **Step 4: Commit** — done (`1348f03`).

---

## Task 1: Config — `maxStreamsPerConn`

**Files:**

- Modify: `config.go` (Config struct + DSN parse), `quic_pool.go` (NewQUICPool signature/fields)
- Test: `config_test.go` (or `quic_pool_nomux_test.go` sibling)

**Interfaces:**

- Produces: `Config.MaxStreamsPerConn int` (DSN key `max_streams_per_conn`, default `8`, min `1`); `QUICPool.maxStreamsPerConn int` field set from it.

- [ ] **Step 1: Write failing test** — `TestConfig_MaxStreamsPerConn`: parse a DSN `host?max_streams_per_conn=4` → `cfg.MaxStreamsPerConn == 4`; parse without the key → default `8`; parse `=0` → clamped to `1`.
- [ ] **Step 2: Run, verify fail** — `go test -run TestConfig_MaxStreamsPerConn -v`. Expected FAIL (field/parse missing).
- [ ] **Step 3: Implement** — add `MaxStreamsPerConn int` to `Config` (config.go), parse the DSN key with default 8 + min-1 clamp, thread it into `NewQUICPool` as `maxStreamsPerConn` field.
- [ ] **Step 4: Run, verify pass** — `go test -run TestConfig_MaxStreamsPerConn -v`. Expected PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(pool): add max_streams_per_conn config (default 8)"`

---

## Task 2: Pool selector — `selectConnWithCapacity` (replace the no-multiplex selector)

**Files:**

- Modify: `quic_pool.go` (selectFreeConnIndex → capacity-aware; acquire reuse logic ~202-228)
- Test: `quic_pool_nomux_test.go` (rename/extend; keep the file, update assertions)

**Interfaces:**

- Consumes: `pooledConn.active int32` (atomic stream count), `maxStreamsPerConn`.
- Produces: `selectConnWithCapacity(conns []*pooledConn, max int) int` — returns the index of a conn with `active < max` (prefer the least-loaded for even spread), else -1.

- [ ] **Step 1: Write failing test** — `TestSelectConnWithCapacity`: with `max=4` and conns at active=[4,2,0,4] → returns index 2 (the least-loaded under cap); with all at active=4 → returns -1; with `max=1` it behaves like the old no-multiplex selector (only active==0). Keep `TestSelectFreeConnIndex` semantics covered by the `max=1` case.
- [ ] **Step 2: Run, verify fail** — `go test -run TestSelectConnWithCapacity -v`. Expected FAIL.
- [ ] **Step 3: Implement** — add `selectConnWithCapacity(conns, max)` (scan for min `active` with `active < max`); update `acquire()` (quic_pool.go ~207) to call it with `p.maxStreamsPerConn` instead of `selectFreeConnIndex`. Increment `pc.active` atomically on selection (as today). Update the BUG-0002 comments to describe the multiplex policy + the cap.
- [ ] **Step 4: Run, verify pass** — `go test -run TestSelectConnWithCapacity -v`. Expected PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(pool): multiplex up to max_streams_per_conn streams per QUIC conn"`

---

## Task 3: Admission bound + dial policy for multiplexing

**Files:**

- Modify: `quic_pool.go` (admission `slotsCh` bound ~101-114; dial-new-vs-reuse decision ~213-228)
- Test: `quic_pool_nomux_test.go`

**Interfaces:**

- Consumes: `maxStreams`, `maxSize` (max conns), `maxStreamsPerConn`.
- Produces: admission bound = `min(maxStreams, maxSize * maxStreamsPerConn)` (total in-flight ceiling); dial a new conn only when no existing conn has capacity AND `len(conns) < maxSize`.

- [ ] **Step 1: Write failing test** — `TestNewQUICPool_AdmissionMultiplex`: with `maxSize=2, maxStreamsPerConn=8, maxStreams=64` → admission ceiling = 16 (2×8), not 2 and not 64; acquiring 16 succeeds without blocking, the 17th blocks until a release. (Replaces `TestNewQUICPool_AdmissionBoundedByPoolSize`'s pool_size-only assumption.)
- [ ] **Step 2: Run, verify fail** — `go test -run TestNewQUICPool_AdmissionMultiplex -v`. Expected FAIL.
- [ ] **Step 3: Implement** — change `admitBound` to `min(maxStreams, maxSize*maxStreamsPerConn)`; change the reuse/dial logic so a new conn is dialed only when `selectConnWithCapacity` returns -1 AND `len(conns) < maxSize`; otherwise reuse the capacity conn. Keep `release()` decrementing `pc.active` + returning the admission slot.
- [ ] **Step 4: Run, verify pass** — `go test -run TestNewQUICPool_AdmissionMultiplex -v`. Expected PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(pool): admission ceiling = conns*streams_per_conn; dial only when no conn has capacity"`

---

## Task 4: Per-conn HELLO/graph-init safety under concurrent first streams

**Files:**

- Modify (if needed): `conn.go` (`pooledHello` ~205-226, `pooledGraphInit` ~231-250)
- Test: `conn_multiplex_hello_test.go` (build tag `//go:build integration`)

**Interfaces:**

- Consumes: `pc.authed bool`, `pc.mu sync.Mutex` (double-checked lock).
- Produces: a guarantee that when K streams open *concurrently* as the first users of a freshly dialed conn, HELLO + graph-init run exactly once and the other K-1 streams wait for it (no double-HELLO, no use-before-auth).

- [ ] **Step 1: Write failing/repro test** — `TestPooledHello_ConcurrentFirstStreams`: dial a fresh conn, launch K=8 goroutines that all `acquire` + open a stream on the same new conn simultaneously, each running a query that requires auth; assert exactly one HELLO occurred (instrument via a counter or server-side msg count) and all 8 queries succeed. Run several times (race).
- [ ] **Step 2: Run, verify** — `go test -tags integration -run TestPooledHello_ConcurrentFirstStreams -v -count=3`. If it already passes (the double-checked lock holds), record that and skip the fix; if it races (double HELLO or use-before-auth), it FAILS here.
- [ ] **Step 3: Implement (only if Step 2 failed)** — make `pooledHello` hold `pc.mu` across the HELLO write+read (not just the `authed` flag check), so concurrent first-streams serialize on the HELLO; ensure a stream that arrives mid-HELLO waits for `authed` before sending its own request. Keep it deadlock-free (don't hold `pc.mu` while blocking on another stream).
- [ ] **Step 4: Run, verify pass** — `go test -tags integration -run TestPooledHello_ConcurrentFirstStreams -v -count=5`. Expected PASS every run.
- [ ] **Step 5: Commit** — `git commit -am "fix(pool): serialize per-conn HELLO across concurrent first streams"`

---

## Task 5: Concurrency regression — multiplexed find+create AND simple-merge under load

**Files:**

- Create: `multiplex_load_test.go` (build tag `//go:build integration`)

**Interfaces:**

- Consumes: the harness + the multiplex pool (now default 8 streams/conn).
- Produces: a regression that fails on the old no-multiplex (low conn count → low throughput) and passes with multiplexing (high throughput, no wedge, correct results), for BOTH workloads.

- [ ] **Step 1: Write the load test** — `TestMultiplex_FindCreate_And_Merge_NoWedge`: fresh server; configure the pool with `maxSize=4, maxStreamsPerConn=8` (16 concurrency over 4 conns); run 32 worker goroutines for ~20s doing a mix of (a) `MERGE (n:L {id:$i})` and (b) `MATCH (p:L {id:$a}),(c:L {id:$b}) MERGE (p)-[:R]->(c)`. Assert: (i) total ops/s > a floor that the 4-conn no-multiplex model can't reach (e.g. assert ops_completed during the window exceeds 4× a measured single-conn baseline, OR simply > N where N proves >4-way concurrency), (ii) a watchdog detects no 15s stall, (iii) final node + edge counts are exactly correct (no lost/dup writes), (iv) zero `ErrBadConn`/desync errors. Watchdog dumps goroutines on stall.
- [ ] **Step 2: Run, verify pass on the multiplex build** — `go test -tags integration -run TestMultiplex_FindCreate_And_Merge_NoWedge -v -timeout 300s -count=3`. Expected PASS all 3 (it's a race — run repeatedly).
- [ ] **Step 3: Verify it would catch a regression** — temporarily set `maxStreamsPerConn=1` (no-multiplex) in the test config and confirm the throughput assertion FAILS (proves the test detects loss of multiplexing); revert to the multiplex config.
- [ ] **Step 4: Run the FULL existing regression suite** — `go test -tags integration ./... -timeout 600s` and `go test ./...`. All existing desync/wedge/stale tests (TestConn_SustainedSingleConnIngest_NoWedge, TestConn_EdgeFindCreate_NoWedge, TestConn_SequentialQueries_NoDesync, conn_stream_desync_test.go, transport_receive_ctx_test.go) MUST still pass. Report exit codes + note any pre-existing failures.
- [ ] **Step 5: Commit** — `git add multiplex_load_test.go && git commit -am "test(multiplex): sustained find+create + merge under load, correctness + throughput"`

---

## Task 6: End-to-end scale validation (data-ingest, real workload)

**Files:**

- None (validation run). Optionally update `errors.go`/comments if a real-world edge case surfaces.

**Interfaces:**

- Consumes: the multiplex driver built into the data-ingest tool via a `replace` directive.

- [ ] **Step 1: Build data-ingest against the multiplex driver** — in `/home/benji/src/gl/gitlab.com/devnw/sealife/data-ingest` (branch `fix-edge-phase-client`, which has the source_id-index schema fix), `go mod edit -replace geodedb.com/geode=<this repo>` and `GOEXPERIMENT=jsonv2 go build -o /tmp/data-ingest-mplx ./cmd/data-ingest`. Restore go.mod after (don't commit the replace).
- [ ] **Step 2: Fresh server + taxa** — start a fresh server (port ≥3590, fresh dir), run `/tmp/data-ingest-mplx ingest --from-dump /home/benji/divelife-dump --workers 16 --chunk-size 100`. Confirm sustained ≥2,000 ops/s (multiplex; vs no-multiplex ~600/s) and completion of 14.27M taxa with the source_id index applied.
- [ ] **Step 2b: Edges** — `/tmp/data-ingest-mplx ingest --from-dump /home/benji/divelife-dump --edges --workers 8 --chunk-size 200`. Confirm `geode_edges_total` grows at thousands/sec to completion (no wedge), and a hierarchy traversal returns correct parent/child pairs.
- [ ] **Step 3: Parallel-read consistency during ingest** — during the edge run, fire concurrent reads (point-read by source_id, count, projection+LIMIT) and confirm they stay fast and the ingest does not stall (reads + multiplexed writes coexist).
- [ ] **Step 4: Report** — record taxa rate, edge rate, edge counts, traversal spot-check, and parallel-read latencies. Note the geode-client-go next-tag (e.g. v0.1.28) + that data-ingest's go.mod would bump to it once tagged.

---

## Self-Review notes

- **Spec coverage:** Task 0 de-risks the server premise; Tasks 1-3 implement multiplexing (config → selector → admission); Task 4 covers the per-conn HELLO concurrency edge; Task 5 is the concurrency regression (both workloads, correctness + throughput, + a negative control); Task 6 validates at real scale incl. parallel reads during ingest. The v0.1.27 deadline fix and per-stream drains are preserved by constraint, not modified.
- **Type consistency:** `selectConnWithCapacity(conns []*pooledConn, max int) int`; `Config.MaxStreamsPerConn int`; `QUICPool.maxStreamsPerConn int`; `pooledConn.active int32` (existing). `max=1` reduces the selector to the old no-multiplex behavior (back-compat).
- **Key risk:** Task 0. If the server wedges on concurrent streams per conn, the plan stops there and the server is fixed first.

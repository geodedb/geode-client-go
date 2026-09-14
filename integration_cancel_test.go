//go:build integration

// integration_cancel_test.go proves the streaming-cancellation contract
// (SONNY-806 / GEO-GO-UP-001, Lane D Task 9):
//
//   - Cancelling the query context mid-paging aborts promptly and surfaces
//     context.Canceled (wrapped), never a transport error.
//   - A cancelled Rows retires its Conn: IsValid() reports false so
//     database/sql discards the connection instead of handing a
//     desynchronised QUIC stream to the next borrower.
//   - Cancellation leaks no goroutines.
//   - The pool keeps serving queries afterwards.
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"
)

// cancelProbeRows is the seeded row count. It must exceed the engine's inline
// page cap so the result genuinely spans pages and the reader is still
// mid-stream when it cancels. 0.8.15 pushes label-scan results inline in pages
// of inline_results_max_rows (default 1000) — geode
// src/server/quic_connection_handler.zig:7547-7599 — and ExecuteRequest carries
// no page size, so the DSN's page_size only sizes PULLs, never the first page.
// At 500 rows every result arrived as a single final page and the cancel path
// under test was never reached; TestIntegration_CancelRetiresConn asserts that
// explicitly so this can never silently regress.
const cancelProbeRows = 2500

// cancelFirstPageSize is the engine's inline_results_max_rows default (0.8.15,
// geode src/server/quic_connection_handler.zig:7547) — the size of the single
// inline first page a MATCH result of this size arrives in before any PULL is
// issued. TestIntegration_CancelMidStream bounds its post-cancel read count
// against this, not a looser fraction of cancelProbeRows: draining the whole
// buffered first page (a regression to "stop issuing PULLs but still hand out
// what's already buffered") must fail the test just as surely as draining the
// whole result would.
const cancelFirstPageSize = 1000

// cancelTenant is the tenant every session in this file binds. The harness
// runs the engine with GEODE_STRICT_TENANCY=1, under which a session with no
// concrete tenant scope is refused GQLSTATE 42501 on its first tenant-scoped
// statement — so the seed writes and the MATCH reads here all need a real
// tenant. Constant, never interpolated from input: CALL geode.tenant.create
// reads its argument as a literal string, so the id has to be rendered into
// the statement text (see integration_tenancy_test.go).
const (
	cancelTenant = "cancel-tenant"
	cancelGraph  = "canceltest"
)

// cancelDSN builds a canceltest DSN bound to cancelTenant, with extra DSN
// parameters (page_size, pool_size) appended.
func cancelDSN(extra string) string {
	return sharedDSN + "&graph=" + cancelGraph + "&tenant=" + url.QueryEscape(cancelTenant) + extra
}

// ensureCancelTenant registers cancelTenant with the engine. geode.tenant.create
// is the engine's only tenant DDL surface (0.8.15 reports tenant_ddl=false in
// SHOW CAPABILITIES); re-registering an existing tenant is a no-op error the
// caller can ignore.
func ensureCancelTenant(ctx context.Context, t *testing.T) {
	t.Helper()
	admin, err := sql.Open("geode", sharedDSN+"&graph="+cancelGraph)
	if err != nil {
		t.Fatalf("sql.Open admin: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx,
		"CALL geode.tenant.create('"+cancelTenant+"')"); err != nil {
		t.Logf("CALL geode.tenant.create(%q): %v (may already exist)", cancelTenant, err)
	}
}

// cancelSeedOnce keeps the seed to one pass per test binary. The rows live in
// the shared engine's canceltest graph, so every test in this file sees them;
// re-MERGEing 2500 nodes per test would cost seconds for nothing.
var cancelSeedOnce sync.Once

// seedCancelRowsTimeout bounds the one-time cancel-probe seed. It is
// deliberately its own timeout rather than a parameter drawn from whichever
// test happens to run the seed first: cancelSeedOnce.Do means exactly one
// caller's context would otherwise govern a batch write every test in this
// file depends on, tying an unrelated test's own cancel/deadline semantics —
// the 120 s `base` each test's cancellation ASSERTIONS run under — to setup
// work that is not part of what that test is asserting.
const seedCancelRowsTimeout = 60 * time.Second

func seedCancelRows(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	var seedErr error
	cancelSeedOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), seedCancelRowsTimeout)
		defer cancel()
		stmts := make([]Statement, 0, n)
		for i := 0; i < n; i++ {
			stmts = append(stmts, Statement{
				Query: "MERGE (n:CancelProbe {i: ?})",
				Args:  []any{int64(i)},
			})
		}
		p := &Pool{db: db}
		seedErr = p.BatchExec(ctx, stmts)
	})
	if seedErr != nil {
		t.Fatalf("seed: %v", seedErr)
	}
}

// settledGoroutines returns the goroutine count once it has stopped falling,
// or the last observation when the budget expires. Runtime-owned goroutines
// (GC workers, the QUIC packet handlers of connections being torn down) exit
// asynchronously, so a bare NumGoroutine() delta right after a cancel is
// noise; this is the goleak-style settle loop.
func settledGoroutines(budget time.Duration) int {
	deadline := time.Now().Add(budget)
	last := runtime.NumGoroutine()
	stable := 0
	for time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(25 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			stable++
			if stable >= 4 {
				return n
			}
		} else {
			stable = 0
		}
		last = n
	}
	return last
}

// TestIntegration_CancelMidStream cancels while rows are still being paged
// and asserts the error is context.Canceled, not a transport error, that the
// abort is prompt, and that the pool keeps working afterwards.
func TestIntegration_CancelMidStream(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	db, err := sql.Open("geode", cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	ensureCancelTenant(base, t)
	seedCancelRows(t, db, cancelProbeRows)

	ctx, cancel := context.WithCancel(base)
	defer cancel()
	rows, err := db.QueryContext(ctx, "MATCH (n:CancelProbe) RETURN n.i")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}

	read := 0
	var cancelledAt time.Time
	for rows.Next() {
		read++
		if read == 5 {
			cancelledAt = time.Now()
			cancel()
		}
	}
	abortLatency := time.Since(cancelledAt)
	rows.Close()

	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("rows.Err() = %v, want context.Canceled", err)
	}
	// Prompt abort: the reader must not drain the rest of the result. A full
	// drain of 2500 rows is milliseconds on localhost, so 5s can only be
	// exceeded by a wedged read, never by ordinary slack.
	if abortLatency > 5*time.Second {
		t.Fatalf("cancellation took %v to abort the stream, want prompt", abortLatency)
	}
	if read >= cancelFirstPageSize {
		t.Fatalf("read %d rows after cancelling at row 5 (first-page size is %d); "+
			"cancel did not abort paging — it only stopped draining the buffered "+
			"first page", read, cancelFirstPageSize)
	}

	// The pool must still be usable: a cancelled stream may not wedge it.
	var one int64
	if err := db.QueryRowContext(base, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("query after cancellation: %v", err)
	}
	if one != 1 {
		t.Fatalf("post-cancel query returned %d, want 1", one)
	}
}

// TestIntegration_CancelRetiresConn drives the driver directly (no
// database/sql in the way) so the retire-the-connection half of the contract
// is asserted at its source: after a mid-stream cancel, Rows.Next must report
// the context error and Conn.IsValid() must be false so database/sql discards
// the connection rather than reusing a desynchronised QUIC stream.
func TestIntegration_CancelRetiresConn(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	seedDB, err := sql.Open("geode", cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer seedDB.Close()
	ensureCancelTenant(base, t)
	seedCancelRows(t, seedDB, cancelProbeRows)

	cfg, err := ParseDSN(cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	dc, err := connector.Connect(base)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer dc.Close()

	conn, ok := dc.(*Conn)
	if !ok {
		t.Fatalf("Connect returned %T, want *geode.Conn", dc)
	}

	ctx, cancel := context.WithCancel(base)
	defer cancel()

	rows, err := conn.QueryContext(ctx, "MATCH (n:CancelProbe) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	// QueryContext transfers ownership of the Conn mutex to the Rows, so an
	// unclosed Rows deadlocks the deferred dc.Close() on any t.Fatal path.
	// Rows.Close is idempotent, so the explicit close below still stands.
	defer rows.Close() //nolint:errcheck // asserted explicitly below

	// Paging probe. ExecuteRequest carries no page size (proto/geode.proto:100)
	// — the server sizes the first page itself and the DSN's page_size only
	// applies to PULL. If the whole result lands in that first, final page
	// there is no paging to cancel mid-stream and this test proves nothing,
	// so say so loudly rather than passing vacuously.
	gr, ok := rows.(*Rows)
	if !ok {
		t.Fatalf("QueryContext returned %T, want *geode.Rows", rows)
	}
	t.Logf("first page: %d rows, final=%v", len(gr.protoBuffer), gr.final)
	if gr.final {
		t.Fatalf("the %d-row result arrived in a single final page (%d rows): the "+
			"cancel path under test (mid-paging PULL) is never reached. Seed more "+
			"rows than the engine's first-page size.", cancelProbeRows, len(gr.protoBuffer))
	}

	dest := make([]driver.Value, len(rows.Columns()))
	for i := 0; i < 5; i++ {
		if err := rows.Next(dest); err != nil {
			t.Fatalf("Next(%d) before cancel: %v", i, err)
		}
	}

	cancel()

	start := time.Now()
	nextErr := rows.Next(dest)
	latency := time.Since(start)

	if !errors.Is(nextErr, context.Canceled) {
		t.Fatalf("Rows.Next after cancel = %v, want context.Canceled", nextErr)
	}
	if latency > 5*time.Second {
		t.Fatalf("Rows.Next took %v to observe cancellation, want prompt", latency)
	}

	// IsValid takes the Conn mutex, which QueryContext handed to the Rows, so
	// it can only be asked once the Rows has released it. That is also the
	// only order database/sql uses: it closes the Rows, then asks IsValid
	// before returning the connection to the idle pool.
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close: %v", err)
	}
	if conn.IsValid() {
		t.Fatal("Conn.IsValid() is true after a cancelled stream; " +
			"database/sql would recycle a desynchronised connection")
	}
}

// TestIntegration_CancelLeaksNoGoroutines repeats the cancel cycle and
// asserts the goroutine population returns to its warmed-up level. The
// blocking page read installs a context watcher goroutine per ReceiveProto;
// a cancel that abandoned one would show up here as linear growth.
func TestIntegration_CancelLeaksNoGoroutines(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancelBase()

	// pool_size=2 bounds the QUIC connections the driver may accumulate, so the
	// warm-up below can reach the pool's steady state. Every cancelled reader
	// retires its connection, so the driver re-dials constantly: without the
	// bound, the QUICPool would keep adding connections (up to the default 16)
	// and their quic-go goroutines would read as a leak.
	db, err := sql.Open("geode", cancelDSN("&page_size=10&pool_size=2"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	ensureCancelTenant(base, t)
	seedCancelRows(t, db, cancelProbeRows)

	cancelOnce := func() {
		ctx, cancel := context.WithCancel(base)
		defer cancel()
		rows, err := db.QueryContext(ctx, "MATCH (n:CancelProbe) RETURN n.i")
		if err != nil {
			return
		}
		read := 0
		for rows.Next() {
			read++
			if read == 3 {
				cancel()
			}
		}
		rows.Close()
	}

	// Warm up: dial the whole (2-connection) pool and start its long-lived
	// goroutines before taking the baseline, so pool growth is not counted as
	// a leak.
	for i := 0; i < 4; i++ {
		cancelOnce()
	}
	baseline := settledGoroutines(10 * time.Second)

	const iterations = 20
	for i := 0; i < iterations; i++ {
		cancelOnce()
	}
	after := settledGoroutines(15 * time.Second)

	// Each leaked cancel strands at least one goroutine — the ReceiveProto
	// context watcher — so a leaking driver lands near baseline+iterations.
	// The tolerance only absorbs a connection's worth of quic-go machinery in
	// flight when the sample is taken.
	const tolerance = 8
	if after > baseline+tolerance {
		t.Fatalf("goroutines grew from %d to %d over %d cancels (tolerance %d): leak",
			baseline, after, iterations, tolerance)
	}
	t.Logf("goroutines: baseline=%d after %d cancels=%d", baseline, iterations, after)
}

// TestIntegration_CancelUnderConcurrency runs cancels and reads together so
// -race can catch shared-state bugs in the paging path.
//
// Each worker gets its own *sql.DB, and therefore its own QUIC pool, because
// two engine-side defects make a SHARED pool untestable here and neither is
// about cancellation (both are evidenced in .superpowers/sdd/task-9-report.md):
//
//   - Retiring a cancelled reader's connection closes its QUIC stream, and
//     0.8.15 answers that by closing the whole pooled QUIC connection: sibling
//     streams mid-handshake on it fail with "failed to receive HELLO response:
//     ... NO_ERROR (remote)" (SONNY-1227).
//   - A multi-page result leaves an unmatched PULL response on its stream —
//     0.8.15 pushes every remaining page inline as an EXECUTE response AND
//     still answers the driver's PULLs (geode
//     src/server/quic_connection_handler.zig:7565-7600) while releaseRows'
//     drain stops at the first non-EXECUTE frame (conn.go drainBufferedFrames)
//     — so the next borrower of that connection reads "expected Execute
//     response, got pull". That one predates this task: it reproduces with the
//     cancellation fix reverted.
//
// Separate pools keep both out of the way, leaving the cancellation contract
// itself asserted under real concurrency and under -race.
func TestIntegration_CancelUnderConcurrency(t *testing.T) {
	const workers = 16

	base, cancelBase := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancelBase()

	seedDB, err := sql.Open("geode", cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer seedDB.Close()

	ensureCancelTenant(base, t)
	seedCancelRows(t, seedDB, cancelProbeRows)

	type outcome struct {
		read     int
		queryErr error
		rowsErr  error
		took     time.Duration
	}
	results := make([]outcome, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started := time.Now()

			db, err := sql.Open("geode", cancelDSN("&page_size=10&pool_size=2"))
			if err != nil {
				results[i] = outcome{queryErr: err, took: time.Since(started)}
				return
			}
			defer db.Close()

			ctx, cancel := context.WithCancel(base)
			defer cancel()
			rows, err := db.QueryContext(ctx, "MATCH (n:CancelProbe) RETURN n.i")
			if err != nil {
				results[i] = outcome{queryErr: err, took: time.Since(started)}
				return
			}
			defer rows.Close()
			read := 0
			for rows.Next() {
				read++
				if i%2 == 0 && read == 3 {
					cancel()
				}
			}
			results[i] = outcome{read: read, rowsErr: rows.Err(), took: time.Since(started)}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		t.Logf("worker %2d (cancels=%v): read=%d took=%v queryErr=%v rowsErr=%v",
			i, i%2 == 0, r.read, r.took, r.queryErr, r.rowsErr)
	}

	for i, r := range results {
		if r.queryErr != nil {
			t.Errorf("worker %d: query failed: %v", i, r.queryErr)
			continue
		}
		if i%2 == 0 {
			// Cancelling worker: the driver — not database/sql's own context
			// watcher — must have aborted it, promptly, with the context error.
			if !errors.Is(r.rowsErr, context.Canceled) {
				t.Errorf("worker %d cancelled at row 3 but rows.Err() = %v, want context.Canceled",
					i, r.rowsErr)
			}
			if r.read > cancelProbeRows/2 {
				t.Errorf("worker %d read %d of %d rows after cancelling at row 3: paging did not abort",
					i, r.read, cancelProbeRows)
			}
			continue
		}
		// Non-cancelling worker: a neighbour's cancellation may not cost it
		// rows or wedge it.
		if r.rowsErr != nil {
			t.Errorf("worker %d never cancelled but paging failed: %v", i, r.rowsErr)
			continue
		}
		if r.read != cancelProbeRows {
			t.Errorf("worker %d read %d rows, want %d", i, r.read, cancelProbeRows)
		}
	}

	// Pool health is asserted on a fresh sql.Open: a new pool proves the engine
	// and the driver's connect path still serve queries after a burst of
	// cancelled streams, without depending on either engine defect above.
	fresh, err := sql.Open("geode", cancelDSN(""))
	if err != nil {
		t.Fatalf("sql.Open after cancellation: %v", err)
	}
	defer fresh.Close()

	var one int64
	if err := fresh.QueryRowContext(base, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("query after concurrent cancellation: %v", err)
	}
	if one != 1 {
		t.Fatalf("post-cancel query returned %d, want 1", one)
	}
}

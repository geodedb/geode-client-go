//go:build integration

// integration_pin_test.go proves the connection-pinning contract
// (SONNY-806 / GEO-GO-UP-001, Lane D Task 10):
//
//   - Every transaction on one Session rides the SAME driver connection, while
//     an unpinned workload on the same pool churns other connections.
//   - A rolled-back transaction does not cost the caller its pin.
//   - A cancelled query retires the Session (the Task 9 path): every later
//     call fails closed instead of silently re-pinning to another connection.
//   - Close is idempotent, releases the connection exactly once, and leaks no
//     goroutines.
//   - Pinning changes nothing about the graph binding (Task 7) or strict
//     tenancy (Task 8): the session rides the DSN's graph and tenant.
package geode

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pinGraph is this file's graph. The tenant comes from sharedDSN
// (harnessTenant): under GEODE_STRICT_TENANCY=1 every statement below is
// tenant-scoped, so the writes here prove the pinned connection carries the
// DSN's tenant scope exactly as an unpinned one does.
const pinGraph = "pintest"

// pinDSN builds a pintest DSN with extra parameters appended. sharedDSN
// already binds harnessTenant; a repeated key resolves to its last occurrence
// (see applyQueryParams in config.go), so appending is safe.
func pinDSN(extra string) string {
	return sharedDSN + "&graph=" + pinGraph + extra
}

// rawConnID reports the identity of the driver connection database/sql
// currently has pinned to the Session, read through (*sql.Conn).Raw rather
// than from the Session's cached copy. Comparing it against Session.ConnID is
// what makes "the pin held" an observation rather than a tautology: if
// database/sql had handed the Session a different connection, the two would
// disagree.
func rawConnID(t *testing.T, s *Session) uint64 {
	t.Helper()
	var id uint64
	if err := s.conn.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			t.Fatalf("pinned connection is %T, want *geode.Conn", dc)
		}
		id = c.ConnID()
		return nil
	}); err != nil {
		t.Fatalf("Raw: %v", err)
	}
	return id
}

// TestIntegration_SessionPinsOneConnectionAcrossTransactions runs several
// sequential transactions on one Session while unpinned workers hammer the
// same pool, and asserts the Session never moved off its connection even
// though the pool opened and used others.
func TestIntegration_SessionPinsOneConnectionAcrossTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// pool_size=8 with MaxOpenConns=8 keeps the database/sql connection count
	// at or below the QUIC pool size: BUG-0002 (docs/GAP_ANALYSIS.md) makes a
	// small fraction of operations time out when more sql connections
	// multiplex over fewer QUIC connections, which would show up here as
	// unrelated flake.
	pool, err := NewPool(pinDSN("&pool_size=8"), WithMaxOpenConns(8), WithMaxIdleConns(8))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	s, err := pool.Session(ctx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	defer s.Close() //nolint:errcheck // asserted in the close test

	// Task 7: the pin does not move the session off the DSN's graph. The
	// server binds the graph at HELLO only once it exists, so the first
	// connection into a fresh engine falls back to CREATE GRAPH + USE GRAPH
	// and reports no binding (pooledGraphInit); assert the binding whenever
	// there is one rather than depending on which of the two paths ran.
	if g := s.BoundGraph(); g != "" && g != pinGraph {
		t.Fatalf("Session bound graph = %q, want %q", g, pinGraph)
	} else {
		t.Logf("session %d bound graph = %q", s.ConnID(), g)
	}

	pinned := s.ConnID()
	if got := rawConnID(t, s); got != pinned {
		t.Fatalf("ConnID = %d but database/sql pinned conn %d", pinned, got)
	}
	connsAtPin := connSeq.Load()

	// Unpinned churn: six workers issuing statements through the pool itself,
	// so database/sql is free to hand each one whichever connection it likes.
	stop := make(chan struct{})
	var churnOps atomic.Int64
	var maxOpen atomic.Int64
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := pool.ExecContext(ctx,
					"MERGE (n:PinChurn {w: ?, i: ?})", int64(w), int64(i)); err != nil {
					// The churn is background noise, not the assertion: a
					// failure here (BUG-0002 timeout, pool pressure) must not
					// fail the pin test. Record and keep going.
					t.Logf("churn worker %d op %d: %v", w, i, err)
					continue
				}
				churnOps.Add(1)
				if open := int64(pool.Stats().OpenConnections); open > maxOpen.Load() {
					maxOpen.Store(open)
				}
			}
		}(w)
	}

	const txCount = 5
	for i := range txCount {
		if got := rawConnID(t, s); got != pinned {
			t.Fatalf("before TX %d the Session rode conn %d, want %d", i, got, pinned)
		}
		if err := s.TX(ctx, nil, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "MERGE (n:PinProbe {i: ?})", int64(i)); err != nil {
				return err
			}
			var seen int64
			return tx.QueryRowContext(ctx,
				"MATCH (n:PinProbe {i: ?}) RETURN count(n)", int64(i)).Scan(&seen)
		}); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("TX %d: %v", i, err)
		}
		if got := rawConnID(t, s); got != pinned {
			t.Fatalf("after TX %d the Session rode conn %d, want %d", i, got, pinned)
		}
		if got := s.ConnID(); got != pinned {
			t.Fatalf("ConnID after TX %d = %d, want %d", i, got, pinned)
		}
	}

	close(stop)
	wg.Wait()

	// The churn has to have actually exercised other connections, or the "one
	// Session, one connection" claim is vacuous.
	if churnOps.Load() == 0 {
		t.Fatal("the unpinned workload completed no operations: nothing churned")
	}
	if maxOpen.Load() < 2 {
		t.Fatalf("pool never held more than %d open connection(s): the unpinned "+
			"workload never churned another connection", maxOpen.Load())
	}
	if connSeq.Load() <= connsAtPin {
		t.Fatalf("no driver connection was created after the pin (seq %d -> %d): "+
			"the pinned connection was the only one in play",
			connsAtPin, connSeq.Load())
	}
	t.Logf("pinned conn %d held across %d transactions; churn did %d ops, "+
		"pool peaked at %d open conns, driver conns created %d -> %d",
		pinned, txCount, churnOps.Load(), maxOpen.Load(), connsAtPin, connSeq.Load())

	// The committed rows are visible through the pool at large, so the pinned
	// transactions really committed rather than merely appearing to.
	var committed int64
	if err := pool.QueryRowContext(ctx, "MATCH (n:PinProbe) RETURN count(n)").Scan(&committed); err != nil {
		t.Fatalf("post-commit read: %v", err)
	}
	if committed < txCount {
		t.Fatalf("post-commit PinProbe count = %d, want at least %d", committed, txCount)
	}
}

// TestIntegration_SessionFailedTXStaysPinned asserts a rolled-back transaction
// leaves the Session on the same connection and still usable: a failed
// generation must not cost the caller its pin.
func TestIntegration_SessionFailedTXStaysPinned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := NewPool(pinDSN("&pool_size=4"), WithMaxOpenConns(4), WithMaxIdleConns(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	s, err := pool.Session(ctx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	defer s.Close() //nolint:errcheck // asserted in the close test

	pinned := s.ConnID()

	sentinel := errors.New("generation abandoned")
	err = s.TX(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "MERGE (n:PinRollback {k: ?})", "abandoned"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("TX = %v, want the callback error", err)
	}

	// The pin survived the rollback.
	if got := rawConnID(t, s); got != pinned {
		t.Fatalf("after a failed TX the Session rode conn %d, want %d", got, pinned)
	}
	if got := s.ConnID(); got != pinned {
		t.Fatalf("ConnID after a failed TX = %d, want %d", got, pinned)
	}

	// And the Session is still usable on that same connection.
	if err := s.TX(ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "MERGE (n:PinRollback {k: ?})", "committed")
		return err
	}); err != nil {
		t.Fatalf("TX after a failed TX: %v", err)
	}
	if got := rawConnID(t, s); got != pinned {
		t.Fatalf("after the second TX the Session rode conn %d, want %d", got, pinned)
	}

	var committed int64
	if err := pool.QueryRowContext(ctx,
		"MATCH (n:PinRollback {k: ?}) RETURN count(n)", "committed").Scan(&committed); err != nil {
		t.Fatalf("post-commit read: %v", err)
	}
	if committed < 1 {
		t.Fatalf("committed PinRollback count = %d, want at least 1", committed)
	}
}

// TestIntegration_SessionRetiresOnCancelledQuery drives the Task 9 cancel path
// through a Session: cancelling mid-stream retires the pinned connection, and
// the Session must then fail closed on every call rather than quietly
// continuing on a different connection.
//
// It reuses the Task 9 fixtures (cancelDSN, the CancelProbe seed) deliberately
// — the point is that the SAME retirement Task 9 proved at the driver level is
// what the Session observes.
func TestIntegration_SessionRetiresOnCancelledQuery(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancelBase()

	seedDB, err := sql.Open("geode", cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer seedDB.Close()
	ensureCancelTenant(base, t)
	seedCancelRows(t, seedDB, cancelProbeRows)

	pool, err := NewPool(cancelDSN("&page_size=10&pool_size=4"),
		WithMaxOpenConns(4), WithMaxIdleConns(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	s, err := pool.Session(base)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	defer s.Close() //nolint:errcheck // asserted below

	pinned := s.ConnID()

	qctx, cancelQuery := context.WithCancel(base)
	defer cancelQuery()

	rows, err := s.QueryContext(qctx, "MATCH (n:CancelProbe) RETURN n.i")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}

	read := 0
	for rows.Next() {
		read++
		if read == 5 {
			cancelQuery()
		}
	}
	rowsErr := rows.Err()
	if err := rows.Close(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Rows.Close: %v", err)
	}
	if !errors.Is(rowsErr, context.Canceled) {
		t.Fatalf("Rows.Err after cancel = %v, want context.Canceled "+
			"(read %d rows; the %d-row result must span pages for the cancel "+
			"path to be reached)", rowsErr, read, cancelProbeRows)
	}

	// Every entry point now fails closed, matching BOTH ErrSessionClosed (the
	// session is unusable) and driver.ErrBadConn (because the connection
	// failed).
	assertRetired(t, "ExecContext", func() error {
		_, err := s.ExecContext(base, "MERGE (n:PinAfterCancel {k: ?})", "x")
		return err
	}())
	assertRetired(t, "QueryContext", func() error {
		_, err := s.QueryContext(base, "MATCH (n:CancelProbe) RETURN count(n)")
		return err
	}())
	assertRetired(t, "TX", s.TX(base, nil, func(*sql.Tx) error {
		t.Error("TX ran its callback on a retired session")
		return nil
	}))

	// The Session did not re-pin: ConnID still names the connection that died.
	if got := s.ConnID(); got != pinned {
		t.Fatalf("retired Session reports conn %d, want the dead conn %d", got, pinned)
	}

	// Close on a retired Session is still clean and still idempotent.
	if err := s.Close(); err != nil {
		t.Fatalf("Close after retirement: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close after retirement: %v", err)
	}

	// The pool itself keeps working: only the Session was retired.
	//
	// This unpinned statement comes FIRST on purpose. Tearing the retired
	// connection down can leave one stale connection behind on the shared QUIC
	// pool (the Connect→Close→Connect defect, SONNY-1227): its first write
	// fails with driver.ErrBadConn. database/sql absorbs that for an unpinned
	// statement by retrying on another connection — and cannot for a pinned
	// one, by design, since retrying would mean handing the caller a different
	// connection than the one it asked to ride. A caller that pins straight
	// after a retirement therefore has to be ready to close the new Session
	// and pin again; measured on 0.8.15, the unpinned statement below is
	// enough to flush the stale connection.
	if _, err := pool.ExecContext(base, "MERGE (n:PinAfterCancel {k: ?})", "unpinned"); err != nil {
		t.Fatalf("unpinned statement after retirement: %v", err)
	}

	fresh, err := pool.Session(base)
	if err != nil {
		t.Fatalf("Session after retirement: %v", err)
	}
	defer fresh.Close() //nolint:errcheck // best effort in cleanup
	if fresh.ConnID() == pinned {
		t.Fatalf("a new Session was handed the retired connection %d", pinned)
	}
	if _, err := fresh.ExecContext(base, "MERGE (n:PinAfterCancel {k: ?})", "fresh"); err != nil {
		t.Fatalf("ExecContext on a fresh Session: %v", err)
	}
}

// TestIntegration_SessionCloseReleasesExactlyOnce opens and closes many
// Sessions, asserting the pool is left with nothing checked out, that a second
// Close is a no-op, that a closed Session refuses work, and that the cycle
// leaks no goroutines (measured like integration_cancel_test.go).
func TestIntegration_SessionCloseReleasesExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool, err := NewPool(pinDSN("&pool_size=2"), WithMaxOpenConns(2), WithMaxIdleConns(2))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	cycle := func(i int) {
		s, err := pool.Session(ctx)
		if err != nil {
			t.Fatalf("Session %d: %v", i, err)
		}
		if _, err := s.ExecContext(ctx, "MERGE (n:PinCycle {i: ?})", int64(i)); err != nil {
			_ = s.Close()
			t.Fatalf("ExecContext %d: %v", i, err)
		}
		for c := range 3 {
			if err := s.Close(); err != nil {
				t.Fatalf("Close #%d of session %d: %v", c+1, i, err)
			}
		}
		if _, err := s.ExecContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("ExecContext after Close = %v, want ErrSessionClosed", err)
		}
	}

	// Warm up so the QUIC pool's long-lived goroutines exist before the
	// baseline sample; otherwise pool growth reads as a leak.
	for i := range 4 {
		cycle(i)
	}
	baseline := settledGoroutines(10 * time.Second)

	const iterations = 30
	for i := range iterations {
		cycle(100 + i)
	}
	after := settledGoroutines(15 * time.Second)

	if stats := pool.Stats(); stats.InUse != 0 {
		t.Fatalf("pool has %d connections in use after %d closed sessions, want 0",
			stats.InUse, iterations)
	}
	if stats := pool.Stats(); stats.OpenConnections > 2 {
		t.Fatalf("pool holds %d open connections, want at most MaxOpenConns=2: "+
			"Close released more than once or not at all", stats.OpenConnections)
	}

	// A leaked session strands at least its connection's machinery; the
	// tolerance only absorbs quic-go teardown in flight when the sample lands.
	const tolerance = 8
	if after > baseline+tolerance {
		t.Fatalf("goroutines grew from %d to %d over %d session cycles (tolerance %d): leak",
			baseline, after, iterations, tolerance)
	}
	t.Logf("goroutines: baseline=%d after %d session cycles=%d", baseline, iterations, after)
}

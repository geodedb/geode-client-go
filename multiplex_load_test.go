//go:build integration

// multiplex_load_test.go — Task 5 of the multiplex-driver plan.
//
// Sustained, mixed find+create + simple-merge load over a multiplexed pool.
// To isolate stream multiplexing as the throughput driver, the pool is pinned
// to ONE QUIC connection (pool_size=1) with max_streams_per_conn=8 — eight
// concurrent independent streams on a single connection. It asserts:
//
//	(i)   correctness: exact final node + edge counts (no lost/dup writes),
//	(ii)  liveness: a watchdog detects no >15s stall (dumps goroutines on stall),
//	(iii) cleanliness: zero ErrBadConn / stream-desync errors,
//	(iv)  throughput proving real multiplexing (8-way on one conn beats the
//	      single-stream per-conn ceiling by ~2x).
//
// The same test body drives the max_streams_per_conn=1 negative control
// (Step 3) — single in-flight stream on the same one connection — where the
// throughput assertion is expected to FAIL (~1100 ops/s < the 1200 floor < the
// ~2360 ops/s multiplex rate), proving the test catches a regression to the old
// one-stream-per-conn policy.
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	mplxWorkers     = 32
	mplxDuration    = 20 * time.Second
	mplxStallWindow = 15 * time.Second

	// The multiplex config and the no-multiplex negative control share ONE QUIC
	// connection (pool_size=1) so the ONLY variable between them is
	// max_streams_per_conn. That isolates stream multiplexing as the cause of
	// the throughput difference: with =1 the single connection carries exactly
	// one in-flight stream (the legacy serial request/response path); with =8
	// up to eight requests run concurrently on the same connection. Sharing a
	// connection (rather than also scaling pool_size) keeps the control honest —
	// it removes connection count as a confound and yields a large, robust
	// throughput gap instead of the ~20% spread seen when the server's per-op
	// cost dominates across 4 connections.
	mplxPoolSize   = 1
	mplxStreamsPer = 8 // 8-way multiplex over ONE connection

	mplxOpTimeout = 15 * time.Second
	mplxLabel     = "MplxLoad"

	// Throughput floor proving real multiplexing. The single-stream (no-mux)
	// path tops out near the serial per-op latency ceiling on one connection;
	// 8-way multiplexing on the same connection runs several times faster. The
	// floor sits well above the measured single-stream rate and well below the
	// measured 8-way rate, so a regression to one-stream-per-conn fails it (see
	// the Step 3 negative control).
	mplxMinThroughpt = 1200 // ops/s
)

// startMplxServerDSN spins up a fresh mux-enabled geode server and returns a DSN
// configured with the given pool_size and max_streams_per_conn.
func startMplxServerDSN(t *testing.T, poolSize, streamsPerConn int) string {
	t.Helper()
	tmpDir := t.TempDir()
	port := findFreeUDPPort(t, 3590, 3699)
	metricsPort := findFreeUDPPort(t, port+1, 3799)
	startProbeServer(t, port, metricsPort, tmpDir)
	return fmt.Sprintf(
		"127.0.0.1:%d?insecure=true&user=geode&pass=seed&pool_size=%d&max_streams_per_conn=%d&max_streams=%d",
		port, poolSize, streamsPerConn, poolSize*streamsPerConn,
	)
}

// TestMultiplex_FindCreate_And_Merge_NoWedge is the Task 5 concurrency
// regression. It runs the multiplex configuration (8 streams on ONE conn) and
// asserts correctness, no wedge, zero bad-conn errors, and multiplex throughput.
func TestMultiplex_FindCreate_And_Merge_NoWedge(t *testing.T) {
	dsn := startMplxServerDSN(t, mplxPoolSize, mplxStreamsPer)
	res := runMplxLoad(t, dsn, mplxPoolSize, mplxStreamsPer)

	// (iii) zero bad-conn / desync errors.
	require.Zero(t, res.badConnErrs, "zero ErrBadConn/desync errors expected")
	require.Empty(t, res.errSamples, "load run produced errors: %v", res.errSamples)

	// (ii) no wedge.
	require.False(t, res.wedged, "watchdog detected a >15s stall (wedge)")

	// (iv) throughput proving real multiplexing. With max_streams_per_conn=1 the
	// single connection carries one in-flight stream (legacy serial path),
	// measured at ~1100 ops/s; max_streams_per_conn=8 multiplexes eight
	// concurrent streams on the same connection, measured at ~2360 ops/s. The
	// floor sits between the two so a regression to one-stream-per-conn fails it
	// — see the negative control in Step 3.
	require.Greater(t, res.opsPerSec, float64(mplxMinThroughpt),
		"multiplex throughput %.0f ops/s must exceed the multiplex floor %d ops/s",
		res.opsPerSec, mplxMinThroughpt)

	// (i) correctness: exact final node + edge counts.
	requireExactCounts(t, dsn, res)

	t.Logf("MULTIPLEX OK: %d ops in %v = %.0f ops/s; nodes=%d edges=%d; 0 bad-conn errors, no wedge",
		res.opsCompleted, res.elapsed.Round(time.Millisecond), res.opsPerSec,
		res.expectNodes, res.expectEdges)
}

type mplxResult struct {
	opsCompleted int64
	opsPerSec    float64
	elapsed      time.Duration
	badConnErrs  int64
	wedged       bool
	errSamples   []string

	// Expected counts, computed from what each worker actually wrote.
	expectNodes int64
	expectEdges int64
}

// runMplxLoad drives mplxWorkers goroutines for mplxDuration doing a mix of
//
//	(a) MERGE (n:L {id:$i})                                  — node upsert
//	(b) MATCH (p:L {id:$a}),(c:L {id:$b}) MERGE (p)-[:R]->(c) — edge find+create
//
// Each worker owns a disjoint id range so node/edge counts are exactly
// predictable. Every op uses a fresh bounded context so a wedge surfaces as a
// deadline error (not an unbounded hang); a watchdog also dumps goroutines if
// global forward progress stalls for mplxStallWindow.
func runMplxLoad(t *testing.T, dsn string, poolSize, streamsPerConn int) *mplxResult {
	t.Helper()

	db, err := sql.Open("geode", dsn)
	require.NoError(t, err, "open db")
	t.Cleanup(func() { _ = db.Close() })

	// Let the sql.DB pool open enough handles to use all stream slots:
	// pool_size * max_streams_per_conn logical connections.
	maxConns := poolSize * streamsPerConn
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, db.PingContext(pingCtx), "ping")
	pingCancel()

	t.Cleanup(func() { cleanupLabel(t, db, mplxLabel) })
	cleanupLabel(t, db, mplxLabel)

	var (
		opsDone    atomic.Int64
		badConn    atomic.Int64
		nodeWrites [mplxWorkers]int64 // nodes merged per worker (exact)
		edgeWrites [mplxWorkers]int64 // edges merged per worker (exact)
		errMu      sync.Mutex
		errSamples []string
		wg         sync.WaitGroup
	)

	recordErr := func(e error) {
		if errors.Is(e, driver.ErrBadConn) {
			badConn.Add(1)
		}
		errMu.Lock()
		if len(errSamples) < 10 {
			errSamples = append(errSamples, e.Error())
		}
		errMu.Unlock()
	}

	// Watchdog: dump goroutines + flag a wedge if no global progress for the
	// stall window.
	stallCtx, stopWatchdog := context.WithCancel(context.Background())
	var wedged atomic.Bool
	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		ticker := time.NewTicker(mplxStallWindow)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-stallCtx.Done():
				return
			case <-ticker.C:
				cur := opsDone.Load()
				if cur == last {
					buf := make([]byte, 2<<20)
					n := runtime.Stack(buf, true)
					t.Logf("WEDGE: no progress past %d ops for %v; goroutine dump:\n%s",
						cur, mplxStallWindow, buf[:n])
					wedged.Store(true)
				}
				last = cur
			}
		}
	}()

	deadline := time.Now().Add(mplxDuration)
	start := time.Now()

	for w := 0; w < mplxWorkers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			var i int64
			for time.Now().Before(deadline) && !wedged.Load() {
				// Op A: MERGE node. Disjoint id space per worker.
				nodeID := fmt.Sprintf("w%d-n%d", wid, i)
				ctxA, cancelA := context.WithTimeout(context.Background(), mplxOpTimeout)
				_, err := db.ExecContext(ctxA,
					fmt.Sprintf("MERGE (n:%s {id: ?})", mplxLabel), nodeID)
				cancelA()
				if err != nil {
					recordErr(fmt.Errorf("worker %d MERGE node %s: %w", wid, nodeID, err))
					return
				}
				nodeWrites[wid]++
				opsDone.Add(1)

				// Op B: edge find+create from the previous node to this one.
				// First op makes a self-edge so an edge always references
				// already-merged nodes (deterministic edge count).
				prevID := nodeID
				if i > 0 {
					prevID = fmt.Sprintf("w%d-n%d", wid, i-1)
				}
				ctxB, cancelB := context.WithTimeout(context.Background(), mplxOpTimeout)
				_, err = db.ExecContext(ctxB,
					fmt.Sprintf("MATCH (p:%s {id: ?}),(c:%s {id: ?}) MERGE (p)-[:R]->(c)",
						mplxLabel, mplxLabel),
					prevID, nodeID)
				cancelB()
				if err != nil {
					recordErr(fmt.Errorf("worker %d MERGE edge %s->%s: %w", wid, prevID, nodeID, err))
					return
				}
				edgeWrites[wid]++
				opsDone.Add(1)
				i++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	stopWatchdog()
	<-watchdogDone

	var expectNodes, expectEdges int64
	for w := 0; w < mplxWorkers; w++ {
		expectNodes += nodeWrites[w]
		expectEdges += edgeWrites[w]
	}

	ops := opsDone.Load()
	return &mplxResult{
		opsCompleted: ops,
		opsPerSec:    float64(ops) / elapsed.Seconds(),
		elapsed:      elapsed,
		badConnErrs:  badConn.Load(),
		wedged:       wedged.Load(),
		errSamples:   errSamples,
		expectNodes:  expectNodes,
		expectEdges:  expectEdges,
	}
}

// requireExactCounts opens a fresh single-conn db and asserts the final node and
// edge counts exactly match what the workers wrote (no lost or duplicated
// writes under concurrent multiplexed MERGEs).
func requireExactCounts(t *testing.T, dsn string, res *mplxResult) {
	t.Helper()
	// Strip pool tuning so the verification uses a clean single connection.
	vdsn := dsn
	if idx := strings.Index(vdsn, "?"); idx >= 0 {
		vdsn = vdsn[:idx] + "?insecure=true&user=geode&pass=seed"
	}
	db, err := sql.Open("geode", vdsn)
	require.NoError(t, err, "open verify db")
	defer db.Close()
	db.SetMaxOpenConns(1)

	gotNodes := scalarCount(t, db, fmt.Sprintf("MATCH (n:%s) RETURN count(n)", mplxLabel))
	gotEdges := scalarCount(t, db, fmt.Sprintf("MATCH (:%s)-[r:R]->(:%s) RETURN count(r)", mplxLabel, mplxLabel))

	require.Equal(t, res.expectNodes, gotNodes,
		"final node count must exactly match writes (no lost/dup nodes)")
	require.Equal(t, res.expectEdges, gotEdges,
		"final edge count must exactly match writes (no lost/dup edges)")
}

// scalarCount runs a single-row, single-int RETURN query and returns the value.
func scalarCount(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	row := db.QueryRowContext(ctx, query)
	var n int64
	require.NoError(t, row.Scan(&n), "scan count for %q", query)
	return n
}

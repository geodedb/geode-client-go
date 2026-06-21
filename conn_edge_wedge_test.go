//go:build integration

// conn_edge_wedge_test.go reproduces the taxonomy EDGE-phase wedge/hang: the
// find+create workload `MATCH (a),(b) MERGE (a)-[r:REL]->(b) SET r.run_id = ?`
// run at scale through the pooled QUIC driver.
//
// Unlike the node-MERGE sustained-ingest wedge (conn_nomux_wedge_test.go), the
// edge statement is a MATCH that binds two existing nodes and MERGEs a
// relationship between them — a query that returns a result page (the merged
// relationship row) which the driver must fully drain from the stream before
// the pooled connection is reused for the next statement. A drain that leaves
// trailing frames (or that returns before the server's final page) desyncs the
// reused stream: the next ExecContext reads a stale frame, the read parks, and
// the client wedges in ReceiveProto.
//
// The data-ingest edge phase (ExecChunkedConcurrent) drives this exact shape
// over `workers` goroutines sharing one *sql.DB pool. This test reproduces it
// with both workers=1 (the -w1 hang) and workers=8 (the -w8 futex wedge).
package geode

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// envInt reads an integer from the environment, falling back to def.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

const edgeWedgeLabel = "EdgeWedge"

// TestConn_EdgeFindCreate_NoWedge reproduces the EDGE-phase find+create wedge.
//
// It first creates a chain of nodes, then runs many concurrent
// MATCH+MERGE-relationship statements (the PARENT_OF/ACCEPTED_AS shape) over a
// pool sized like the data-ingest edge phase. A wedge manifests as a statement
// blocking past its per-op deadline; the watchdog dumps goroutines so the
// parked frame is captured.
func TestConn_EdgeFindCreate_NoWedge(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runEdgeFindCreate(t, workers)
		})
	}
}

func runEdgeFindCreate(t *testing.T, workers int) {
	// Scale is configurable so the same test serves as a fast CI guard and a
	// heavy reproduction. The edge find+create wedge is sustained-pressure
	// dependent (tens of thousands of ops over many workers), so default to a
	// large run; override via GEODE_EDGE_WEDGE_NODES / GEODE_EDGE_WEDGE_EDGES.
	nodeCount := envInt("GEODE_EDGE_WEDGE_NODES", 4000)
	edgeCount := envInt("GEODE_EDGE_WEDGE_EDGES", 6000)
	const (
		perOpLimit = 20 * time.Second
		chunkSize  = 200
	)

	db, err := sql.Open("geode", sharedDSN)
	require.NoError(t, err, "open db")
	defer db.Close()

	// Pool sized like the edge phase: one pooled QUIC conn per worker.
	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxLifetime(0)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, db.PingContext(pingCtx), "ping")
	pingCancel()

	label := fmt.Sprintf("%s%d", edgeWedgeLabel, workers)
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	// Seed nodes: source_id 0..nodeCount-1.
	for i := 0; i < nodeCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), perOpLimit)
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("MERGE (n:%s {source: ?, source_id: ?})", label),
			label, fmt.Sprintf("%d", i))
		cancel()
		require.NoErrorf(t, err, "seed node %d", i)
	}
	t.Logf("seeded %d nodes", nodeCount)

	var opsDone atomic.Int64
	stallCtx, stopWatchdog := context.WithCancel(context.Background())
	defer stopWatchdog()
	go func() {
		var last int64
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stallCtx.Done():
				return
			case <-ticker.C:
				cur := opsDone.Load()
				if cur == last {
					buf := make([]byte, 1<<20)
					n := runtime.Stack(buf, true)
					t.Logf("EDGE WEDGE: no progress past %d edge ops for 15s; goroutine dump:\n%s",
						cur, buf[:n])
				}
				last = cur
			}
		}
	}()

	// The find+create statement: bind two existing nodes, MERGE a relationship,
	// SET a property after the MERGE (so the MERGE key excludes run_id, keeping
	// re-runs idempotent — exactly TaxonEdgesBySourceID's shape).
	edgeQuery := fmt.Sprintf(
		`MATCH (a:%s {source: ?, source_id: ?}), (b:%s {source: ?, source_id: ?}) `+
			`MERGE (a)-[r:PARENT_OF]->(b) SET r.run_id = ?`, label, label)

	// Build the edge statements (a chain plus some fan-out).
	type edge struct{ parent, child int }
	edges := make([]edge, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges = append(edges, edge{parent: i % nodeCount, child: (i + 1) % nodeCount})
	}

	// Dispatch chunks over `workers` goroutines, mirroring ExecChunkedConcurrent.
	type chunk struct{ start, end int }
	var chunks []chunk
	for i := 0; i < len(edges); i += chunkSize {
		end := i + chunkSize
		if end > len(edges) {
			end = len(edges)
		}
		chunks = append(chunks, chunk{i, end})
	}
	idxCh := make(chan int, len(chunks))
	for i := range chunks {
		idxCh <- i
	}
	close(idxCh)

	start := time.Now()
	var wg sync.WaitGroup
	var firstErr atomic.Value
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range idxCh {
				if firstErr.Load() != nil {
					return
				}
				c := chunks[idx]
				for _, e := range edges[c.start:c.end] {
					ctx, cancel := context.WithTimeout(context.Background(), perOpLimit)
					_, err := db.ExecContext(ctx, edgeQuery,
						label, fmt.Sprintf("%d", e.parent),
						label, fmt.Sprintf("%d", e.child),
						"run-edge-wedge")
					cancel()
					if err != nil {
						firstErr.CompareAndSwap(nil, fmt.Errorf("edge %d->%d: %w", e.parent, e.child, err))
						return
					}
					opsDone.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		t.Fatalf("edge find+create WEDGE after %d ops: %v", opsDone.Load(), v.(error))
	}
	t.Logf("completed %d edge find+create ops over %d workers in %v with no wedge",
		opsDone.Load(), workers, time.Since(start).Round(time.Millisecond))
}

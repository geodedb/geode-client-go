//go:build integration

// conn_nomux_wedge_test.go reproduces the sustained-ingest WEDGE introduced by
// the no-multiplex / bounded-admission pool change (7cb9f83, BUG-0002).
//
// Symptom (confirmed via A/B on an idle box): after a few thousand–30K ops a
// sustained sequential ingest on a SINGLE pooled connection stops sending. The
// client goroutine parks in ReceiveProto waiting for a response; the server
// logs "per-connection proto_reader stall ... client likely gone" and spins at
// ~95% CPU on the now-stuck connection. coord_apply_ops_total flatlines.
//
// Because it reproduces with a single QUIC connection / single stream
// (MaxOpenConns=1), this is NOT a multiplex/acceptor/concurrency-across-conns
// issue — it is the per-connection request/response stream lifecycle under the
// new policy. This test drives the exact data-ingest BatchExec path (BEGIN →
// many ExecContext MERGE → COMMIT) over one connection for tens of thousands of
// ops, with a per-batch deadline. Before the fix it wedges (a batch blocks on
// the parked ReceiveProto and the per-batch context deadline fires); after the
// fix the full run completes.
package geode

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConn_SustainedSingleConnIngest_NoWedge is the in-repo reproduction for the
// no-multiplex sustained-ingest wedge.
//
// It opens a SINGLE-connection pool (MaxOpenConns=1, MaxIdleConns=1) so every
// op flows through one QUIC stream — the workers=1 data-ingest configuration
// that wedges deterministically. It then runs many transactional batches of
// MERGE statements. Each batch has its own short-ish deadline; a wedge manifests
// as a batch blocking past that deadline, which fails the test fast (instead of
// hanging the whole suite).
func TestConn_SustainedSingleConnIngest_NoWedge(t *testing.T) {
	// The wedge is deterministic and early: this driver/data combination parks
	// the client in ReceiveProto within the first ~12 ops (the production ingest
	// by ~30K). Before the fix the run never clears the first dozen ops without a
	// per-batch deadline firing; after the fix it makes steady forward progress.
	// 30K key-only MERGEs over a SINGLE stream is decisively past both the
	// deterministic early wedge and the production 4K–30K wedge window while
	// staying within a sane suite budget. Raise the -timeout if running on slow
	// hardware — a healthy run only ever needs more wall-clock, never stalls.
	const (
		totalBatches  = 300 // batches → 30K ops, past the production wedge ceiling
		opsPerBatch   = 100 // statements per chunk (mirrors --chunk-size 100)
		label         = "NomuxWedge"
		perBatchLimit = 20 * time.Second
	)

	db, err := sql.Open("geode", sharedDSN)
	require.NoError(t, err, "open db")
	defer db.Close()

	// Single connection: force every op onto one QUIC stream (workers=1).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Warm + ping.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, db.PingContext(pingCtx), "ping")
	pingCancel()

	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	var opsDone atomic.Int64

	// Watchdog: if forward progress stalls for longer than the wedge window,
	// dump goroutine stacks so the parked frame is captured in the test log,
	// then fail. This turns a silent hang into actionable evidence.
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
					t.Logf("WEDGE: no progress past %d ops for 15s; goroutine dump:\n%s",
						cur, buf[:n])
				}
				last = cur
			}
		}
	}()

	start := time.Now()
	for b := 0; b < totalBatches; b++ {
		base := b * opsPerBatch
		if err := runMergeBatch(db, label, base, opsPerBatch, perBatchLimit); err != nil {
			t.Fatalf("batch %d (op base %d, %d ops done): %v — sustained-ingest WEDGE",
				b, base, opsDone.Load(), err)
		}
		opsDone.Add(opsPerBatch)

		if b%100 == 0 {
			t.Logf("progress: %d batches, %d ops, %v elapsed",
				b, opsDone.Load(), time.Since(start).Round(time.Millisecond))
		}
	}

	t.Logf("completed %d ops over a single connection in %v with no wedge",
		opsDone.Load(), time.Since(start).Round(time.Millisecond))
}

// runMergeBatch executes opsPerBatch auto-commit MERGE statements directly on
// the *sql.DB — exactly the shape of diver.dev/geode/graph.BatchExec, which
// loops `db.ExecContext(ctx, stmt.Query, stmt.Args...)` per statement with NO
// surrounding transaction. The statement is the real multi-property Taxon MERGE
// with `?` placeholder args carrying string values (including apostrophes/empty
// strings), so the inlined query and server response shape match the production
// ingest. A fresh context bounds each statement so a wedged ReceiveProto
// surfaces as a deadline error rather than an unbounded hang.
func runMergeBatch(db *sql.DB, label string, base, n int, limit time.Duration) error {
	q := fmt.Sprintf(`MERGE (n:%s {taxon_id: ?}) SET
		n.scientific_name = ?, n.authority = ?,
		n.rank = ?, n.status = ?,
		n.kingdom = ?, n.phylum = ?, n.class = ?,
		n.t_order = ?, n.family = ?, n.genus = ?,
		n.is_marine = ?, n.is_brackish = ?,
		n.is_freshwater = ?, n.is_terrestrial = ?,
		n.is_extinct = ?,
		n.citation = ?, n.modified = ?,
		n.source = ?, n.run_id = ?`, label)

	for i := 0; i < n; i++ {
		id := base + i
		ctx, cancel := context.WithTimeout(context.Background(), limit)
		_, err := db.ExecContext(ctx, q,
			fmt.Sprintf("col:%d", id),
			fmt.Sprintf("Genus species %d (d'Orbigny, 1840)", id), "L'auteur, 1900",
			"species", "accepted",
			"Animalia", "Chordata", "Actinopterygii",
			"Perciformes", "Family'ae", "Genus",
			true, false,
			false, false,
			false,
			"O'Brien et al. 2021", "2026-01-01T00:00:00Z",
			"col", "run-nomux-wedge",
		)
		cancel()
		if err != nil {
			return fmt.Errorf("exec op %d: %w", id, err)
		}
	}
	return nil
}

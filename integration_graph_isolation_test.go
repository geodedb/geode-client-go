//go:build integration

// integration_graph_isolation_test.go — SONNY-806, Lane D Task 16e.
//
// The QUIC lane of pipeline 2855806396 did not fail because of anything a
// single test did wrong. It failed because of where one test put its data.
//
// TestConn_SustainedSingleConnIngest_NoWedge writes 30,000 twenty-property
// nodes. It cleans them up with a fixed 30s budget and, until this task, threw
// the result away. On CI's ONE `services:` engine — not the testcontainers
// harness, which gives every test a fresh engine and hides all of this — a
// delete that does not fit in 30s simply leaves the nodes there, because the
// QUIC protocol has no cancel message: the client gives up, the engine keeps
// going, and nobody is told. Measured against geode 0.8.18, this is not a
// theory: a `MATCH (n) DETACH DELETE n` over 8,000 nodes abandoned at a 2s
// deadline returned `context deadline exceeded` to the caller and the NEXT
// CREATE — on a different pool, over a different QUIC connection — took 10.5s
// waiting for the engine to finish it. The seven after that took 3ms each.
//
// Those 30,000 nodes then sat in the DEFAULT graph, which is the graph
// cleanupTestData bulk-deletes at the end of every TestDocker_* write test.
// On the runner that pipeline landed on, that delete no longer fitted in its
// own 30s budget either — so TestDocker_CreateNode burned 34s and gave up,
// TestDocker_CreateMultipleNodes' CREATE queued behind the abandoned delete and
// died at its deadline twice over, and every test after it did the same until
// the 20-minute package timeout.
//
// The fix is one line of DSN: the bulk ingest does not belong in the graph
// other tests bulk-delete. This test is what keeps it there.

package geode

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// graphIsolationLabel is written through nomuxWedgeDSN and must be visible
// ONLY there.
const graphIsolationLabel = "NomuxWedgeIsolationProbe"

// TestIntegration_SustainedIngestStaysOutOfTheSharedGraph writes through the
// same DSN the sustained-ingest reproduction uses and requires the shared
// default graph — the one cleanupTestData empties — not to see it.
func TestIntegration_SustainedIngestStaysOutOfTheSharedGraph(t *testing.T) {
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
	}

	ingest, err := sql.Open("geode", nomuxWedgeDSN())
	if err != nil {
		t.Fatalf("sql.Open(ingest): %v", err)
	}
	t.Cleanup(func() { _ = ingest.Close() })

	shared, err := sql.Open("geode", sharedDSN)
	if err != nil {
		t.Fatalf("sql.Open(shared): %v", err)
	}
	t.Cleanup(func() { _ = shared.Close() })

	// Registered AFTER the two closes so LIFO runs it FIRST, while both
	// handles are still open. It deletes the probe from BOTH graphs: against a
	// tree where the ingest DSN is not isolated the seed lands in the shared
	// graph, and a run that leaves it there poisons the next run's assertion.
	t.Cleanup(func() {
		for _, c := range []struct {
			db    *sql.DB
			graph string
		}{{ingest, nomuxWedgeGraph}, {shared, "the shared default graph"}} {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			_, err := c.db.ExecContext(ctx,
				"MATCH (n:"+graphIsolationLabel+") DETACH DELETE n")
			cancel()
			if err != nil {
				t.Errorf("cleanup of %s in %s: %v", graphIsolationLabel, c.graph, err)
			}
		}
	})

	const probes = 3
	for i := range probes {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := ingest.ExecContext(ctx,
			"CREATE (n:"+graphIsolationLabel+" {i: ?})", int64(i))
		cancel()
		if err != nil {
			t.Fatalf("seed %d through the ingest DSN: %v", i, err)
		}
	}

	// Control arm. Without it, a seed that silently wrote nothing would make
	// the assertion below pass for the wrong reason.
	if got := countLabel(t, ingest, graphIsolationLabel); got != probes {
		t.Fatalf("the ingest graph %q holds %d %s nodes, want %d — the seed did "+
			"not land where the test thinks it did, so the isolation assertion "+
			"below would be vacuous", nomuxWedgeGraph, got, graphIsolationLabel, probes)
	}

	if got := countLabel(t, shared, graphIsolationLabel); got != 0 {
		t.Fatalf("the SHARED default graph holds %d %s nodes: the sustained-ingest "+
			"reproduction writes into the graph cleanupTestData bulk-deletes. At "+
			"30,000 nodes that delete outgrows its budget on a slower runner, the "+
			"client abandons it, the engine keeps executing it because QUIC has no "+
			"cancel message, and every test after it queues behind the delete — "+
			"which is how pipeline 2855806396's QUIC lane died", got, graphIsolationLabel)
	}
}

// countLabel returns how many nodes carry label in db's bound graph.
func countLabel(t *testing.T, db *sql.DB, label string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var n int64
	if err := db.QueryRowContext(ctx,
		"MATCH (n:"+label+") RETURN count(n) AS c").Scan(&n); err != nil {
		t.Fatalf("count %s: %v", label, err)
	}
	return n
}

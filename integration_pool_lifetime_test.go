//go:build integration

// integration_pool_lifetime_test.go — SONNY-806, Lane D Task 16d.
//
// The engine-side half of the Connector.Close regression. The unit tests in
// driver_connector_close_test.go pin the wiring; this one pins the CONSEQUENCE,
// and it only shows up against a SINGLE LONG-LIVED engine — which is exactly
// what CI's `services:` container is and what the testcontainers harness is
// not, since that gives each test its own fresh engine and resets the leak
// every time.

package geode

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// openCloseCycles is how many times the regression drives the full
// sql.Open → ping → query → db.Close cycle against ONE engine.
//
// The number is chosen against the engine's measured wall, not picked round.
// Every leaked pool pins two engine stream-pool workers (its anchor stream and
// its health-probe stream) and geode 0.8.18 sizes that pool at
// default_quic_stream_worker_count = 64 (src/server/main.zig:264), so before
// the fix the 65th cycle found no worker left to service its HELLO and failed
// with "failed to receive HELLO response: context deadline exceeded" — the CI
// failure, reproduced exactly. 80 clears that wall with margin while still
// running in well under a minute. Confirmation that 64 is the governing number
// and not a coincidence: re-running the same loop against an engine started
// with GEODE_QUIC_STREAM_WORKERS=8 moved the wall to the 9th cycle.
const openCloseCycles = 80

// TestIntegration_RepeatedOpenCloseDoesNotExhaustOneEngine opens and closes a
// *sql.DB openCloseCycles times against the shared engine and requires every
// single cycle to complete.
//
// A closed *sql.DB must leave nothing behind on the server. It used to: nothing
// closed the Connector's QUIC pool, so each closed DB left its dialed QUIC
// connections up (10s keep-alives against a 120s idle timeout, so the engine
// could not reap them either), each connection kept its anchor stream open, and
// the pool's 1Hz health probe went on holding a probe stream on every one of
// them — and each of those streams pins an engine stream-pool worker until the
// stream closes (handleAdditionalStream, geode
// src/server/quic_connection_handler.zig:2891).
func TestIntegration_RepeatedOpenCloseDoesNotExhaustOneEngine(t *testing.T) {
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
	}

	for i := 1; i <= openCloseCycles; i++ {
		func() {
			db, err := sql.Open("geode", sharedDSN)
			if err != nil {
				t.Fatalf("cycle %d: sql.Open: %v", i, err)
			}
			defer func() {
				if cerr := db.Close(); cerr != nil {
					t.Fatalf("cycle %d: db.Close: %v", i, cerr)
				}
			}()

			// The ping is the assertion: it is the HELLO handshake, and it is
			// what a starved engine can no longer answer.
			pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := db.PingContext(pingCtx); err != nil {
				t.Fatalf("cycle %d of %d: ping failed — the engine ran out of "+
					"stream workers because earlier cycles leaked their pools: %v",
					i, openCloseCycles, err)
			}

			queryCtx, qcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer qcancel()
			var one int
			if err := db.QueryRowContext(queryCtx, "RETURN 1 AS x").Scan(&one); err != nil {
				t.Fatalf("cycle %d of %d: query failed: %v", i, openCloseCycles, err)
			}
			if one != 1 {
				t.Fatalf("cycle %d: RETURN 1 AS x = %d, want 1", i, one)
			}
		}()
	}
}

// TestIntegration_ClosedDBLeavesNoPooledConnection is the same defect observed
// from the driver's own side: after sql.DB.Close the connector's pool must hold
// no QUIC connection and no in-flight stream. Holding the *Connector is what
// makes that observable — a DB opened with sql.Open hides it.
func TestIntegration_ClosedDBLeavesNoPooledConnection(t *testing.T) {
	cfg, err := ParseDSN(sharedDSN)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	db := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if pooled, _ := connector.quicPool.counts(); pooled == 0 {
		t.Fatal("expected the ping to have dialed at least one pooled connection")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if !connector.quicPool.isClosed() {
		t.Fatal("sql.DB.Close left the connector's QUIC pool open")
	}
	if pooled, inFlight := connector.quicPool.counts(); pooled != 0 || inFlight != 0 {
		t.Fatalf("after db.Close the pool still holds conns=%d streams=%d", pooled, inFlight)
	}
}

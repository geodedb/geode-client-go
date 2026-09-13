//go:build integration

package geode

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

// readOnlyGraph is the graph both read-only integration tests target. A
// read-only connection deliberately does not run CREATE GRAPH IF NOT EXISTS on
// the way in (conn.go), so a writable connection has to bring it into
// existence first.
const readOnlyGraph = "rotest"

// openReadOnlyTestGraph creates readOnlyGraph over a writable connection and
// returns a read-only *sql.DB bound to it.
func openReadOnlyTestGraph(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()

	seed, err := sql.Open("geode", sharedDSN+"&graph="+readOnlyGraph)
	if err != nil {
		t.Fatalf("sql.Open (writable seed): %v", err)
	}
	// Force the connection (and therefore the CREATE GRAPH/USE GRAPH init) to
	// actually happen before the read-only DB is opened.
	if err := seed.PingContext(ctx); err != nil {
		seed.Close()
		t.Fatalf("ping (writable seed): %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close (writable seed): %v", err)
	}

	db, err := sql.Open("geode", sharedDSN+"&graph="+readOnlyGraph+"&read_only=true")
	if err != nil {
		t.Fatalf("sql.Open (read-only): %v", err)
	}
	return db
}

// TestIntegration_ReadOnlyRefusesBeforeWire proves the client-side gate: a
// plain mutation is refused with ErrReadOnly and never reaches the engine,
// while reads on the same connection keep working.
func TestIntegration_ReadOnlyRefusesBeforeWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openReadOnlyTestGraph(ctx, t)
	defer db.Close()

	_, err := db.ExecContext(ctx, "CREATE (n:ROProbe {k: ?})", "x")
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ExecContext err = %v, want ErrReadOnly", err)
	}
	// The client gate fires first, so the failure is ErrReadOnly and not the
	// server's 42501 — nothing left the process.
	var de *DriverError
	if errors.As(err, &de) {
		t.Fatalf("err = %v: a DriverError means the statement reached the engine", err)
	}

	// The same gate applies on the Query path...
	if _, err := db.QueryContext(ctx, "MATCH (n) DETACH DELETE n"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("QueryContext err = %v, want ErrReadOnly", err)
	}
	// ... and to a multi-statement program whose write is not the first
	// statement (the shape graph/batch.go builds).
	_, err = db.ExecContext(ctx, "MATCH (n:ROProbe) RETURN n;\nCREATE (m:ROProbe);")
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("batch err = %v, want ErrReadOnly", err)
	}

	var one int64
	if err := db.QueryRowContext(ctx, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("read on a read-only connection: %v", err)
	}
	if one != 1 {
		t.Errorf("RETURN 1 = %d, want 1", one)
	}
}

// TestIntegration_ReadOnlyWireFlagEnforced proves the server-side half of the
// gate is armed independently of the client classifier: an ExecuteRequest
// carrying read_only=true and an unambiguously mutating program is refused by
// the engine with GQLSTATE 42501, no matter what the driver thought of it
// (geode 0.8.15, src/server/quic_connection_handler.zig:6490-6493).
//
// The classifier is bypassed by driving the connection's transport directly
// rather than by a test-only switch in production code — this is the same
// ExecuteRequest ExecContext would build, minus the client gate.
func TestIntegration_ReadOnlyWireFlagEnforced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openReadOnlyTestGraph(ctx, t)
	defer db.Close()

	raw, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	var (
		code string
		msg  string
	)
	rawErr := raw.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			t.Fatalf("driver conn type = %T, want *Conn", dc)
		}
		if !c.cfg.ReadOnly {
			t.Fatal("conn cfg.ReadOnly = false, want true")
		}

		// Exactly what ExecContext sends, with the client-side classifier
		// skipped. If the engine's read_only floor were not armed, this
		// CREATE would succeed.
		send := proto.NewExecuteMessage(&proto.ExecuteRequest{
			Query:    "CREATE (n:ROWire {k: 'x'})",
			ReadOnly: c.cfg.ReadOnly,
		})
		if err := c.transport.SendProto(ctx, send); err != nil {
			return err
		}
		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			return err
		}
		exec := resp.GetExecute()
		if exec == nil {
			t.Fatalf("expected Execute response, got %s", describeServerMessage(resp))
		}
		if exec.GetError() == nil {
			t.Fatal("engine accepted a mutation with ExecuteRequest.read_only=true, want a refusal")
		}
		code = exec.GetError().GetCode()
		msg = exec.GetError().GetMessage()
		return nil
	})
	// The stream carried a server error frame, so retire this connection
	// rather than returning it to the pool.
	_ = raw.Close()
	if rawErr != nil {
		t.Fatalf("raw transport exchange: %v", rawErr)
	}

	t.Logf("engine refusal: code=%q message=%q", code, msg)
	if !strings.Contains(code, "42501") && !strings.Contains(msg, "42501") &&
		!strings.Contains(strings.ToLower(msg), "read-only") {
		t.Fatalf("engine error = (%q, %q), want GQLSTATE 42501 read-only refusal", code, msg)
	}

	// The write must not have landed. A fresh writable connection is used so
	// the check is not itself subject to the read-only gate.
	verify, err := sql.Open("geode", sharedDSN+"&graph="+readOnlyGraph)
	if err != nil {
		t.Fatalf("sql.Open (verify): %v", err)
	}
	defer verify.Close()

	var count int64
	if err := verify.QueryRowContext(ctx, "MATCH (n:ROWire) RETURN count(n)").Scan(&count); err != nil {
		t.Fatalf("verify count: %v", err)
	}
	if count != 0 {
		t.Errorf("MATCH (n:ROWire) count = %d, want 0 — the refused write landed", count)
	}
}

// TestIntegration_ReadOnlyTransaction pins the documented transaction
// behaviour: a transaction on a read-only connection begins and reads
// normally, a mutation inside it is refused client-side, and — because that
// refusal emits nothing — the transaction is left open and usable for further
// reads, with the caller still owning the Commit/Rollback decision.
func TestIntegration_ReadOnlyTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openReadOnlyTestGraph(ctx, t)
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx on a read-only connection: %v", err)
	}

	var before int64
	if err := tx.QueryRowContext(ctx, "RETURN ?", int64(7)).Scan(&before); err != nil {
		t.Fatalf("read inside tx: %v", err)
	}
	if before != 7 {
		t.Errorf("RETURN 7 = %d, want 7", before)
	}

	if _, err := tx.ExecContext(ctx, "CREATE (n:ROTx {k: ?})", "x"); !errors.Is(err, ErrReadOnly) {
		_ = tx.Rollback()
		t.Fatalf("mutation inside tx err = %v, want ErrReadOnly", err)
	}

	// The tx survives the refusal: nothing reached the wire, so the stream and
	// the server-side transaction are untouched.
	var after int64
	if err := tx.QueryRowContext(ctx, "RETURN ?", int64(8)).Scan(&after); err != nil {
		t.Fatalf("read after a refused mutation inside tx: %v", err)
	}
	if after != 8 {
		t.Errorf("RETURN 8 = %d, want 8", after)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit of a read-only tx: %v", err)
	}

	// An explicitly read-only tx works too, and the connection still works
	// after the tx closes.
	roTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx(ReadOnly): %v", err)
	}
	if _, err := roTx.ExecContext(ctx, "MERGE (n:ROTx {k: ?})", "y"); !errors.Is(err, ErrReadOnly) {
		_ = roTx.Rollback()
		t.Fatalf("mutation inside read-only tx err = %v, want ErrReadOnly", err)
	}
	if err := roTx.Rollback(); err != nil {
		t.Fatalf("Rollback of a read-only tx: %v", err)
	}

	var one int64
	if err := db.QueryRowContext(ctx, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("read after tx: %v", err)
	}
}

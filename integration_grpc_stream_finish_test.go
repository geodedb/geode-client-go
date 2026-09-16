//go:build integration

// integration_grpc_stream_finish_test.go — SONNY-806, Lane D Task 16d.
//
// Every gRPC Execute rides its own server-streaming RPC. The driver stops
// reading as soon as it has the answer, so an RPC it does not finish is left
// dangling — and the NEXT Execute tears that one down with an RST_STREAM before
// issuing itself (GRPCTransport.SendProto). A statement-per-RPC workload
// therefore emitted one RST_STREAM per statement.
//
// The engine's vendored grpc-zig races that reset against its own handler. It
// logs `gRPC streaming: failed to send initial response error=PeerGone` and
// then drops the WHOLE TCP connection rather than the single stream (`error:
// Handler error: error.PeerGone`), so TestIntegration_GRPCBatchExecPastOnePage
// died mid-transaction with `rpc error: code = Unavailable desc = error reading
// from server: EOF` at a statement number that moved run to run — 3 in CI, 5,
// 84 and 138 across local reproductions. Against the testcontainers harness
// each test gets its own engine, which is why it only showed up on CI's one
// shared service engine.
//
// The engine should not lose a connection over one reset stream, and that is
// its own ticket. This test pins the driver's side of it: a request the driver
// has finished with leaves NOTHING for the next one to reset.

package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"
)

// TestIntegration_GRPCCompletedExecLeavesNoStreamToReset is deliberately
// white-box. The black-box symptom is a race — it needs hundreds of statements
// and still moves around — whereas the state that causes it is exact and can be
// asserted after a single statement.
func TestIntegration_GRPCCompletedExecLeavesNoStreamToReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)

	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	for i := range 5 {
		args := []driver.NamedValue{{Ordinal: 1, Value: int64(i)}}
		if _, err := conn.ExecContext(ctx,
			"MERGE (n:GRPCFinishProbe {i: ?})", args); err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}

		transport.mu.Lock()
		stream, streamCancel := transport.execStream, transport.execCancel
		transport.mu.Unlock()

		if stream != nil {
			t.Fatalf("statement %d left its Execute RPC dangling: the next statement "+
				"will reset it, and the engine drops the whole connection on that race", i)
		}
		if streamCancel != nil {
			t.Fatalf("statement %d left an exec cancel func behind", i)
		}
	}
}

// TestIntegration_GRPCRefusedExecLeavesNoStreamToReset is the arm Task 16d
// missed (F-16d-01): a statement the ENGINE refuses.
//
// A refusal is a COMPLETE answer — the engine planned the statement, rejected
// it and will send nothing else — but ExecContext returned the DriverError
// straight from the error branch without finishing the request stream. The
// connection then went back to the pool with a live Execute RPC on it, and the
// next statement reset that RPC. A refused statement is not exotic: this is
// exactly what CI hit, where `CREATE INDEX … (tolerated if already present)`
// was refused with 42P07 Duplicate index name and the EXPLAIN immediately
// after it died with `Unavailable: error reading from server: EOF`.
func TestIntegration_GRPCRefusedExecLeavesNoStreamToReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	// A statement the engine parses and refuses, not one the driver rejects
	// before the wire: the defect is in what happens to the RPC the refusal
	// travelled on, so the statement HAS to reach the engine.
	const refused = "CREATE INDEX grpcfinishprobe_i_idx ON GRPCFinishProbe(i) USING btree"
	if _, err := conn.ExecContext(ctx, refused, nil); err != nil {
		t.Logf("first CREATE INDEX: %v (tolerated — it may already exist)", err)
	}
	transport.mu.Lock()
	transport.execStream, transport.execCancel = nil, nil
	transport.mu.Unlock()

	for i := range 5 {
		_, err := conn.ExecContext(ctx, refused, nil)
		var de *DriverError
		if !errors.As(err, &de) {
			t.Fatalf("round %d: re-creating an existing index returned %v, "+
				"want a DriverError refusal — the test is not exercising the "+
				"engine-refusal path at all", i, err)
		}

		transport.mu.Lock()
		stream, streamCancel := transport.execStream, transport.execCancel
		transport.mu.Unlock()

		if stream != nil {
			t.Fatalf("round %d: the refused statement (%s) left its Execute RPC "+
				"dangling; the next statement resets it and the engine drops the "+
				"whole connection on that race", i, de.Code)
		}
		if streamCancel != nil {
			t.Fatalf("round %d: the refused statement left an exec cancel func behind", i)
		}
	}

	if n := transport.ExecStreamResets(); n != 0 {
		t.Fatalf("refused statements reset %d live Execute RPCs; every reset is a "+
			"whole-connection teardown the engine has to survive, and a refusal "+
			"has nothing to reset", n)
	}
}

// TestIntegration_GRPCPlanLeavesNoStreamToReset covers EXPLAIN and PROFILE.
//
// Both ride the same per-statement Execute stream as ExecContext on a
// transport that has one, and the plan payload is the whole answer — yet
// planRoundTrip/collectPlanFrames never finished the stream. Every EXPLAIN and
// every PROFILE therefore armed the next statement on that connection with an
// RST_STREAM to emit.
func TestIntegration_GRPCPlanLeavesNoStreamToReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	if _, err := conn.ExecContext(ctx, "MERGE (n:GRPCFinishProbe {i: 0})", nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for i, step := range []struct {
		name string
		run  func() error
	}{
		{"Explain", func() error {
			_, err := conn.Explain(ctx, "MATCH (n:GRPCFinishProbe {i: ?}) RETURN n.i", int64(0))
			return err
		}},
		{"Profile", func() error {
			_, err := conn.Profile(ctx, "MATCH (n:GRPCFinishProbe) WHERE n.i = ? RETURN n.i", int64(0))
			return err
		}},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}

		transport.mu.Lock()
		stream, streamCancel := transport.execStream, transport.execCancel
		transport.mu.Unlock()

		if stream != nil {
			t.Fatalf("step %d (%s) left its Execute RPC dangling: the next statement "+
				"on this connection will reset it, and the engine turns one stream "+
				"reset into a whole-connection teardown", i, step.name)
		}
		if streamCancel != nil {
			t.Fatalf("step %d (%s) left an exec cancel func behind", i, step.name)
		}
	}

	if n := transport.ExecStreamResets(); n != 0 {
		t.Fatalf("EXPLAIN/PROFILE reset %d live Execute RPCs; a completed plan has "+
			"nothing left on its stream but the server's end-of-stream", n)
	}
}

// TestIntegration_GRPCDrainedRowsAreFinishedNotReset covers F-16d-02: a query
// read to its final page.
//
// Rows.Close cancelled the Execute RPC for EVERY close, final or not, so every
// gRPC QUERY emitted one RST_STREAM even though the engine had already
// finished pushing. Cancelling is right for a result the engine may still be
// sending and wrong for one that is over.
func TestIntegration_GRPCDrainedRowsAreFinishedNotReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	for i := range 3 {
		args := []driver.NamedValue{{Ordinal: 1, Value: int64(i)}}
		if _, err := conn.ExecContext(ctx, "MERGE (n:GRPCFinishProbe {i: ?})", args); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	rows, err := conn.QueryContext(ctx, "MATCH (n:GRPCFinishProbe) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	dest := make([]driver.Value, len(rows.Columns()))
	read := 0
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Next after %d rows: %v", read, err)
		}
		read++
	}
	if read == 0 {
		t.Fatal("read 0 rows: the drain assertion below would be vacuous")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}

	if n := transport.ExecStreamResets(); n != 0 {
		t.Fatalf("a result read to its final page reset %d live Execute RPCs; the "+
			"engine escalates one reset into a whole-connection teardown, so a "+
			"query-heavy workload kept the exposure ExecContext was fixed for", n)
	}
}

// TestIntegration_GRPCRefusedThenPlanOnOneConnection is the CI shape,
// black-box: the exact `refused CREATE INDEX → EXPLAIN` pair that failed
// pipeline 2855806396, repeated on ONE connection so every round leaves a
// dangling stream for the next round to reset.
//
// The escalation is racy on the engine's side, so the round count is what
// makes it a test rather than a coin flip. Against the pre-fix tree at 300
// rounds it failed on every attempt — at round 48, 44 and 2 across three runs,
// each time with the CI message verbatim:
//
//	geode: Conn.Explain: geode: transport recv to …: rpc error:
//	code = Unavailable desc = error reading from server: EOF
//
// At 12 rounds it passed pre-fix, which is exactly the trap: the symptom moves,
// the state that causes it does not, and that is why the three white-box tests
// above exist alongside this one. After the fix, 300 rounds pass in under a
// second (3/3 runs).
func TestIntegration_GRPCRefusedThenPlanOnOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	if _, err := conn.ExecContext(ctx, "MERGE (n:GRPCFinishProbe {i: 0})", nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	const createIdx = "CREATE INDEX grpcfinishprobe_i_idx ON GRPCFinishProbe(i) USING btree"
	if _, err := conn.ExecContext(ctx, createIdx, nil); err != nil {
		t.Logf("first CREATE INDEX: %v (tolerated — it may already exist)", err)
	}

	for i := range 300 {
		// Refused: the index exists now, so this is the 42P07 arm every time.
		if _, err := conn.ExecContext(ctx, createIdx, nil); err == nil {
			t.Fatalf("round %d: CREATE INDEX succeeded twice; the refusal arm is "+
				"not being exercised", i)
		}
		if _, err := conn.Explain(ctx,
			"MATCH (n:GRPCFinishProbe {i: ?}) RETURN n.i", "a' OR 1=1 --"); err != nil {
			t.Fatalf("round %d: EXPLAIN after a refused statement on the same "+
				"connection: %v", i, err)
		}
	}
}

// TestIntegration_GRPCRefusedQueryLeavesNoStreamToReset is F-16e-01: the
// review's "fifth path" ExecContext's fix did not cover.
//
// QueryContext's engine-refusal branches returned the DriverError straight
// from the error branch without finishing the request stream — no Rows is
// created on a refusal, so Rows.Close never runs to finish it on their
// behalf. Byte-for-byte the same defect TestIntegration_GRPCRefusedExecLeavesNoStreamToReset
// closed for ExecContext (F-16d-01), reached through a refused READ instead
// of a refused write: a syntax error is the simplest statement the engine
// parses and rejects without ever touching a graph, but the same gap is
// reachable through a cross-tenant-denied MATCH, a type error or a missing
// label/procedure — any refusal that returns via conn.go's QueryContext
// error branches.
func TestIntegration_GRPCRefusedQueryLeavesNoStreamToReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	const refused = "MATCH (n RETURN n"

	for i := range 5 {
		_, err := conn.QueryContext(ctx, refused, nil)
		var de *DriverError
		if !errors.As(err, &de) {
			t.Fatalf("round %d: a syntax error returned %v, want a DriverError "+
				"refusal — the test is not exercising the engine-refusal path at all",
				i, err)
		}

		transport.mu.Lock()
		stream, streamCancel := transport.execStream, transport.execCancel
		transport.mu.Unlock()

		if stream != nil {
			t.Fatalf("round %d: the refused query (%s) left its Execute RPC "+
				"dangling; the next statement resets it and the engine drops the "+
				"whole connection on that race", i, de.Code)
		}
		if streamCancel != nil {
			t.Fatalf("round %d: the refused query left an exec cancel func behind", i)
		}
	}

	if n := transport.ExecStreamResets(); n != 0 {
		t.Fatalf("refused queries reset %d live Execute RPCs; every reset is a "+
			"whole-connection teardown the engine has to survive, and a refusal "+
			"has nothing to reset", n)
	}

	// The connection must still be usable for a normal statement afterwards —
	// the whole point of finishing rather than leaving the stream live.
	if _, err := conn.ExecContext(ctx, "MERGE (n:GRPCFinishProbe {i: 999})", nil); err != nil {
		t.Fatalf("statement after the refused queries: %v", err)
	}
}

// TestIntegration_GRPCRefusedQueryThenQueryOnOneConnection is the CI shape for
// F-16e-01, black-box: a refused QUERY followed by a second, successful
// statement on the SAME connection, repeated enough rounds to make the
// engine's racy RST_STREAM-to-connection-teardown escalation a certainty
// rather than a coin flip (see TestIntegration_GRPCRefusedThenPlanOnOneConnection,
// which pins the same shape for ExecContext/EXPLAIN). Pre-fix this fails with
// the CI message verbatim:
//
//	geode: Pool.Query: geode: Conn.QueryContext: geode: transport recv to …:
//	rpc error: code = Unavailable desc = error reading from server: EOF
//
// Post-fix, every round succeeds.
func TestIntegration_GRPCRefusedQueryThenQueryOnOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)
	if _, err := conn.ExecContext(ctx, "MERGE (n:GRPCFinishProbe {i: 0})", nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const refused = "MATCH (n RETURN n"

	for i := range 300 {
		if _, err := conn.QueryContext(ctx, refused, nil); err == nil {
			t.Fatalf("round %d: syntax-error query succeeded; the refusal arm is "+
				"not being exercised", i)
		}

		rows, err := conn.QueryContext(ctx, "MATCH (n:GRPCFinishProbe {i: 0}) RETURN n.i", nil)
		if err != nil {
			t.Fatalf("round %d: query after a refused query on the same connection: %v", i, err)
		}
		n := drainDriverRows(t, rows)
		if err := rows.Close(); err != nil {
			t.Fatalf("round %d: rows.Close: %v", i, err)
		}
		if n == 0 {
			t.Fatalf("round %d: 0 rows back from the seeded node", i)
		}
	}
}

//go:build integration

// integration_grpc_test.go is the gRPC half of the ENG-4658 gate — "the feature
// set passes QUIC/TLS 1.3 AND gRPC integration/race tests" (SONNY-806 /
// GEO-GO-UP-001, Lane D Task 14).
//
// The gRPC transport shares Conn, Rows, the read-only classifier, the
// parameter binder, the plan decoder and the Session layer with QUIC, but NOT
// the transport itself and NOT the QUIC connection pool. Everything below the
// Transport interface is different code (grpc_transport.go), so none of the
// Task 3–13 QUIC evidence carries over by construction; it has to be asserted
// again on this wire.
//
// Every test here mirrors a QUIC test with a GRPC infix, so the parity is
// greppable:
//
//	TestIntegration_ParamsBindInPropertyMaps
//	TestIntegration_GRPCParamsBindInPropertyMaps
//
// Where gRPC genuinely cannot do what QUIC does, the mirror asserts the
// DOCUMENTED REFUSAL instead of skipping. A gap that is proven is a gap that
// cannot rot silently, and each one is recorded in
// .superpowers/sdd/task-14-report.md with engine/driver file:line evidence:
//
//   - no QUIC pool, so no probe stream and no Drain of pooled connections
//     (TestIntegration_GRPCHealthFallsBackToPing)
//   - a different HELLO capability set
//     (TestIntegration_GRPCServerInfo)
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

// Graphs used by this file. They are gRPC-only names so a failure here can
// never be confused with — or contaminated by — the QUIC lane's graphs, even
// though both transports address the SAME engine.
const (
	grpcParamGraph  = "grpcparamtest"
	grpcVecGraph    = "grpcvectest"
	grpcROGraph     = "grpcrotest"
	grpcCancelGraph = "grpccanceltest"
	grpcPagedGraph  = "grpcpagedtest"
	grpcWedgeGraph  = "grpcwedgetest"
	grpcBatchGraph  = "grpcbatchtest"
	grpcPlanGraph   = "grpcplantest"
	grpcPinGraph    = "grpcpintest"
)

// grpcStreamPageSize is the engine's gRPC stream page size: geode 0.8.15
// serves Execute over gRPC as a server-streaming RPC and pages it at
// query_runner.default_page_size (src/transport/grpc_server.zig:915 ->
// src/client/quic_query_runner.zig:5), i.e. 128 rows.
//
// Nothing here needs a PULL: the engine PUSHES every page until one is final
// (grpc_server.zig:1009-1101 handleExecuteStreaming) and the driver reads them
// (Transport.PagesArePushed, rows.go fetchNextPage). The constant matters
// because a test that means to cross a page boundary has to seed past it —
// seeding 128 rows or fewer would assert nothing about paging at all.
const grpcStreamPageSize = 128

// cancelSeedRows is the row count the cancellation tests seed: more than one
// pushed page, so a cancel lands with pages still in flight.
const cancelSeedRows = 2*grpcStreamPageSize + 5

// requireGRPCDSN returns the gRPC DSN or fails the test.
//
// It does NOT skip in container mode. The harness always starts the pinned
// engine with a gRPC listener and always publishes it
// (integration_main_test.go), so an empty grpcDSN there means the parity lane
// silently did not run — exactly the outcome Task 14 exists to prevent. In
// manual-server mode the operator opts in with GEODE_TEST_GRPC_DSN; without it
// there is genuinely no gRPC endpoint to ask, and only then is a skip honest.
func requireGRPCDSN(t *testing.T) string {
	t.Helper()
	if grpcDSN == "" {
		if testContainer == nil {
			t.Skip("manual-server mode without GEODE_TEST_GRPC_DSN: no gRPC endpoint to test")
		}
		t.Fatal("grpcDSN is empty in container mode: the gRPC parity lane did not run. " +
			"See containerGRPCDSN (integration_test.go) and the GRPC_ZIG_BIND_ADDR / " +
			"--listen-grpc wiring in integration_main_test.go.")
	}
	return grpcDSN
}

// grpcDB opens a *sql.DB over gRPC with extra DSN parameters appended.
// grpcDSN already binds harnessTenant; a repeated DSN key resolves to its last
// occurrence (applyQueryParams in config.go), so appending is safe.
func grpcDB(t *testing.T, extra string) *sql.DB {
	t.Helper()
	db, err := sql.Open("geode", requireGRPCDSN(t)+extra)
	if err != nil {
		t.Fatalf("sql.Open(grpc): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// grpcPool opens a *Pool over gRPC with extra DSN parameters appended.
func grpcPool(t *testing.T, extra string) *Pool {
	t.Helper()
	p, err := NewPool(requireGRPCDSN(t) + extra)
	if err != nil {
		t.Fatalf("NewPool(grpc): %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// -----------------------------------------------------------------------
// Task 3 / 4 — typed parameter binding
// -----------------------------------------------------------------------

// TestIntegration_GRPCParamsBindInPropertyMaps mirrors
// TestIntegration_ParamsBindInPropertyMaps. Placeholders must bind as typed
// proto.Param over gRPC too: the binder lives in params.go, but the wire that
// carries ExecuteRequest.params is grpc_transport.go's, and an
// ExecuteRequest whose params were dropped in transit would still parse — the
// query text alone is valid GQL — and silently return the wrong row.
func TestIntegration_GRPCParamsBindInPropertyMaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := grpcDB(t, "&graph="+grpcParamGraph)

	// A value that would break out of a single-quoted GQL literal if anything
	// on this path were still interpolating.
	const hostile = "a' } ) RETURN 1 // "
	if _, err := db.ExecContext(ctx,
		"MERGE (n:GRPCParamProbe {k: ?}) ON CREATE SET n.v = ?, n.i = ?, n.f = ?, n.b = ?",
		"probe-1", hostile, int64(42), 1.5, true,
	); err != nil {
		t.Fatalf("MERGE with bound params over gRPC: %v", err)
	}

	var (
		v string
		i int64
		f float64
		b bool
	)
	if err := db.QueryRowContext(ctx,
		"MATCH (n:GRPCParamProbe {k: ?}) RETURN n.v, n.i, n.f, n.b", "probe-1",
	).Scan(&v, &i, &f, &b); err != nil {
		t.Fatalf("MATCH with bound param over gRPC: %v", err)
	}
	if v != hostile {
		t.Errorf("n.v = %q, want %q (value was mangled — the gRPC path is interpolating)", v, hostile)
	}
	if i != 42 || f != 1.5 || !b {
		t.Errorf("scalars = (%d, %v, %v), want (42, 1.5, true)", i, f, b)
	}
}

// TestIntegration_GRPCParamsAreNotInQueryText mirrors
// TestIntegration_ParamsAreNotInQueryText: if the payload had become query
// text, the injected `DETACH DELETE n RETURN 1` tail would have run and the
// count would not be 1.
func TestIntegration_GRPCParamsAreNotInQueryText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := grpcDB(t, "&graph="+grpcParamGraph)

	const injection = "x'}) DETACH DELETE n RETURN 1 //"
	if _, err := db.ExecContext(ctx,
		"MERGE (n:GRPCParamProbe {k: ?})", injection); err != nil {
		t.Fatalf("MERGE with injection payload over gRPC: %v", err)
	}

	var n int64
	if err := db.QueryRowContext(ctx,
		"MATCH (m:GRPCParamProbe {k: ?}) RETURN count(m)", injection).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1 — the payload was treated as query text, not data", n)
	}
}

// TestIntegration_GRPCVectorParameterBinds mirrors
// TestIntegration_KNNWithBoundVector (Task 4): a geode.Vector must cross the
// gRPC wire as a typed parameter and be usable by knn().
func TestIntegration_GRPCVectorParameterBinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db := grpcDB(t, "&graph="+grpcVecGraph)

	seed := []struct {
		id  string
		vec Vector
	}{
		{"gv-a", Vector{1, 0, 0}},
		{"gv-b", Vector{0, 1, 0}},
		{"gv-c", Vector{0, 0, 1}},
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx,
			"MERGE (n:GRPCVecProbe {vid: ?}) ON CREATE SET n.gembedding = ?", s.id, s.vec); err != nil {
			t.Fatalf("seed %s over gRPC: %v", s.id, err)
		}
	}

	// Round-trip proof first: the stored property must be the vector that was
	// bound, element for element. This is the assertion that fails if the
	// gRPC path dropped or re-encoded the parameter.
	var got any
	if err := db.QueryRowContext(ctx,
		"MATCH (n:GRPCVecProbe {vid: ?}) RETURN n.gembedding", "gv-a").Scan(&got); err != nil {
		t.Fatalf("read back bound vector over gRPC: %v", err)
	}
	list, ok := got.([]any)
	if !ok || len(list) != 3 {
		t.Fatalf("n.gembedding = %#v, want a 3-element list", got)
	}
	t.Logf("bound vector round-tripped over gRPC as %v", list)

	if _, err := db.ExecContext(ctx,
		"CREATE INDEX grpcvecprobe_idx ON GRPCVecProbe (gembedding) USING vector WITH (metric = 'cosine')",
	); err != nil {
		t.Logf("CREATE INDEX (tolerated if already present): %v", err)
	}

	// The id of the node the query vector is nearest to. knn() returns node
	// ids (engine 0.8.15 src/gql/language_metadata.zig:1086-1098, return_type
	// LIST), so the assertion needs gv-a's id to compare against.
	var wantID int64
	if err := db.QueryRowContext(ctx,
		"MATCH (n:GRPCVecProbe {vid: ?}) RETURN id(n)", "gv-a").Scan(&wantID); err != nil {
		t.Fatalf("id(gv-a) over gRPC: %v", err)
	}

	var ids any
	if err := db.QueryRowContext(ctx,
		"RETURN knn('gembedding', ?, ?, 'cosine')", Vector{0.99, 0.01, 0}, 1,
	).Scan(&ids); err != nil {
		t.Fatalf("knn with a bound vector over gRPC: %v", err)
	}
	if ids == nil {
		t.Fatal("knn returned NULL over gRPC — the vector parameter did not reach the function")
	}
	t.Logf("knn(bound vector) over gRPC = %v (gv-a is id %d)", ids, wantID)

	// Non-nil is not enough: a parameter that arrived as the WRONG vector, or
	// as a zero vector, still returns a non-nil list of k ids. The nearest
	// neighbour of {0.99, 0.01, 0} under cosine is gv-a {1, 0, 0} and nothing
	// else, so only the right vector produces this answer.
	nearest, isList := ids.([]any)
	if !isList || len(nearest) != 1 {
		t.Fatalf("knn(k=1) over gRPC = %#v, want a 1-element list of ids", ids)
	}
	gotID, isInt := nearest[0].(int64)
	if !isInt {
		t.Fatalf("knn id = %#v (%T), want int64", nearest[0], nearest[0])
	}
	if gotID != wantID {
		t.Fatalf("knn nearest id = %d, want %d (gv-a): the bound query vector did not "+
			"steer the search", gotID, wantID)
	}
}

// -----------------------------------------------------------------------
// Task 6 — read-only enforcement
// -----------------------------------------------------------------------

// openGRPCReadOnly creates grpcROGraph over a writable gRPC connection and
// returns a read-only *sql.DB bound to it. A read-only connection does not
// CREATE GRAPH on the way in (conn.go), so the graph has to exist first.
func openGRPCReadOnly(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()

	seed := grpcDB(t, "&graph="+grpcROGraph)
	if err := seed.PingContext(ctx); err != nil {
		t.Fatalf("ping (writable gRPC seed): %v", err)
	}
	return grpcDB(t, "&graph="+grpcROGraph+"&read_only=true")
}

// TestIntegration_GRPCReadOnlyRefusesBeforeWire mirrors
// TestIntegration_ReadOnlyRefusesBeforeWire: the client-side classifier gates
// the statement before it reaches any transport, so a mutation is ErrReadOnly
// and never a server refusal.
func TestIntegration_GRPCReadOnlyRefusesBeforeWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openGRPCReadOnly(ctx, t)

	_, err := db.ExecContext(ctx, "CREATE (n:GRPCROProbe {k: ?})", "x")
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ExecContext err = %v, want ErrReadOnly", err)
	}
	// The client gate fires first, so the failure is ErrReadOnly and not the
	// server's 42501 — nothing left the process.
	var de *DriverError
	if errors.As(err, &de) {
		t.Fatalf("err = %v: a DriverError means the statement reached the engine", err)
	}

	if _, err := db.QueryContext(ctx, "MATCH (n) DETACH DELETE n"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("QueryContext err = %v, want ErrReadOnly", err)
	}
	if _, err := db.ExecContext(ctx,
		"MATCH (n:GRPCROProbe) RETURN n;\nCREATE (m:GRPCROProbe);"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("batch err = %v, want ErrReadOnly", err)
	}

	var one int64
	if err := db.QueryRowContext(ctx, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("read on a read-only gRPC connection: %v", err)
	}
	if one != 1 {
		t.Errorf("RETURN 1 = %d, want 1", one)
	}
}

// TestIntegration_GRPCReadOnlyWireFlagEnforced mirrors
// TestIntegration_ReadOnlyWireFlagEnforced. This is the half that does NOT
// carry over from QUIC: the engine enforces ExecuteRequest.read_only in a
// SEPARATE gRPC code path (geode 0.8.15
// src/transport/grpc_server.zig:2260-2264, CANARY
// REQ-EXEC-READ-ONLY-REQUEST-001 ASPECT=GrpcGate) from the QUIC handler's.
// The client classifier is bypassed by driving the transport directly, so the
// only thing that can refuse the CREATE is the server.
func TestIntegration_GRPCReadOnlyWireFlagEnforced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openGRPCReadOnly(ctx, t)

	raw, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	var code, msg string
	rawErr := raw.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			t.Fatalf("driver conn type = %T, want *Conn", dc)
		}
		if c.cfg.Transport != TransportGRPC {
			t.Fatalf("conn transport = %v, want TransportGRPC", c.cfg.Transport)
		}
		if !c.cfg.ReadOnly {
			t.Fatal("conn cfg.ReadOnly = false, want true")
		}

		send := proto.NewExecuteMessage(&proto.ExecuteRequest{
			Query:    "CREATE (n:GRPCROWire {k: 'x'})",
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
			t.Fatal("the gRPC handler accepted a mutation with ExecuteRequest.read_only=true, " +
				"want a refusal")
		}
		code = exec.GetError().GetCode()
		msg = exec.GetError().GetMessage()
		// Retire this connection. The exchange above ran one Execute outside
		// the Conn's own bookkeeping and the stream carried a server error
		// frame, so it must not be reused. sql.Conn.Close() RETURNS the
		// connection to the pool; returning driver.ErrBadConn from the Raw
		// callback is what discards it (database/sql sql.Conn.Raw closes the
		// Conn when f returns exactly driver.ErrBadConn).
		return driver.ErrBadConn
	})
	_ = raw.Close()
	if rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
		t.Fatalf("raw gRPC transport exchange: %v", rawErr)
	}

	t.Logf("gRPC engine refusal: code=%q message=%q", code, msg)
	if !strings.Contains(code, "42501") && !strings.Contains(msg, "42501") &&
		!strings.Contains(strings.ToLower(msg), "read-only") {
		t.Fatalf("engine error = (%q, %q), want a GQLSTATE 42501 read-only refusal", code, msg)
	}

	// The write must not have landed. Verified over gRPC, on a writable
	// connection, so the check is not itself subject to the read-only gate.
	verify := grpcDB(t, "&graph="+grpcROGraph)
	var count int64
	if err := verify.QueryRowContext(ctx,
		"MATCH (n:GRPCROWire) RETURN count(n)").Scan(&count); err != nil {
		t.Fatalf("verify count: %v", err)
	}
	if count != 0 {
		t.Errorf("MATCH (n:GRPCROWire) count = %d, want 0 — the refused write landed", count)
	}
}

// -----------------------------------------------------------------------
// Task 8 — strict tenancy
// -----------------------------------------------------------------------

// TestIntegration_GRPCStrictTenancyRefusesUnscopedSession is the gRPC half of
// Task 8's mode probe. The DSN's tenant reaches the engine through the gRPC
// Handshake handler, which passes req.tenant_id into createSession (geode
// 0.8.15 src/transport/grpc_server.zig:1758) — a different call site from the
// QUIC HELLO handler's. An unscoped gRPC session must therefore be refused
// GQLSTATE 42501 "Tenant scope required", exactly as the QUIC one is; if it is
// not, gRPC is a strict-tenancy bypass and every tenant assertion in the suite
// is void on this transport.
func TestIntegration_GRPCStrictTenancyRefusesUnscopedSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Scoped control arm first: the same statement on a tenant-bound gRPC
	// session must succeed, so a failure below is about the SCOPE and not
	// about the graph, the label or the transport.
	scoped := grpcDB(t, "&graph="+grpcParamGraph)
	var scopedCount int64
	if err := scoped.QueryRowContext(ctx,
		"MATCH (n:GRPCTenantProbe) RETURN count(n)").Scan(&scopedCount); err != nil {
		t.Fatalf("tenant-bound gRPC read: %v", err)
	}

	unscoped, err := sql.Open("geode",
		dsnWithoutTenant(requireGRPCDSN(t))+"&graph="+grpcParamGraph)
	if err != nil {
		t.Fatalf("sql.Open (unscoped gRPC): %v", err)
	}
	defer unscoped.Close()

	var n int64
	err = unscoped.QueryRowContext(ctx,
		"MATCH (n:GRPCTenantProbe) RETURN count(n)").Scan(&n)
	if err == nil {
		t.Fatalf("unscoped gRPC read succeeded (count=%d): strict tenancy is NOT enforced on "+
			"the gRPC path, so gRPC is a bypass of the tenant scope the QUIC lane proves. "+
			"The engine runs with GEODE_STRICT_TENANCY=1 (integration_main_test.go).", n)
	}
	if !errors.Is(err, ErrTenantScopeRequired) {
		t.Fatalf("unscoped gRPC read err = %v, want ErrTenantScopeRequired "+
			"(GQLSTATE 42501 \"Tenant scope required\")", err)
	}
	t.Logf("strict tenancy enforced over gRPC; unscoped read refused as: %v", err)
}

// -----------------------------------------------------------------------
// Task 9 — cancellation
// -----------------------------------------------------------------------

// seedGRPCCancelRows brings the cancel graph up to n rows over gRPC.
//
// One MERGE per row rather than Pool.BatchExec: the cancellation tests must not
// depend on the transaction/batch path to set up the row count they cancel in
// the middle of.
func seedGRPCCancelRows(ctx context.Context, t *testing.T, db *sql.DB, n int) {
	t.Helper()
	for i := range n {
		if _, err := db.ExecContext(ctx,
			"MERGE (n:GRPCCancelProbe {i: ?})", int64(i)); err != nil {
			t.Fatalf("seed row %d over gRPC: %v", i, err)
		}
	}
}

// TestIntegration_GRPCCancelMidStream mirrors TestIntegration_CancelMidStream:
// cancelling mid-read must surface context.Canceled (never a transport error),
// must abort promptly, must not hand out the rest of the buffered page, and
// must leave the pool usable.
func TestIntegration_GRPCCancelMidStream(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	db := grpcDB(t, "&graph="+grpcCancelGraph)
	db.SetMaxOpenConns(4)
	// More than one pushed page, so "abort mid-stream" is literal: the reader
	// is cancelled with pages still owed by the server-streaming RPC, not
	// merely with buffered rows left over. Seeding exactly one page (as this
	// test did while gRPC could not page at all) could only ever prove the
	// weaker property.
	seedGRPCCancelRows(base, t, db, cancelSeedRows)

	ctx, cancel := context.WithCancel(base)
	defer cancel()
	rows, err := db.QueryContext(ctx, "MATCH (n:GRPCCancelProbe) RETURN n.i")
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
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
	_ = rows.Close()

	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("rows.Err() = %v, want context.Canceled", err)
	}
	if abortLatency > 5*time.Second {
		t.Fatalf("cancellation took %v to abort the gRPC stream, want prompt", abortLatency)
	}
	// Cancelling at row 5 must stop the reader inside the FIRST pushed page:
	// it may not drain the rest of that page, and it certainly may not go on
	// reading the pages the engine is still pushing.
	if read >= grpcStreamPageSize {
		t.Fatalf("read %d of %d seeded rows after cancelling at row 5: the cancel did "+
			"not stop the reader (it got past the first %d-row page)",
			read, cancelSeedRows, grpcStreamPageSize)
	}

	var one int64
	if err := db.QueryRowContext(base, "RETURN ?", int64(1)).Scan(&one); err != nil {
		t.Fatalf("query after gRPC cancellation: %v", err)
	}
	if one != 1 {
		t.Fatalf("post-cancel gRPC query returned %d, want 1", one)
	}
}

// TestIntegration_GRPCCancelRetiresConn mirrors
// TestIntegration_CancelRetiresConn: it drives the driver directly so the
// retire-the-connection half of the Task 9 contract is asserted at its source.
// A gRPC Conn whose Rows was cancelled must report IsValid() == false and
// ResetSession() == driver.ErrBadConn, so database/sql discards it instead of
// reusing a connection whose server-streaming RPC was abandoned mid-flight.
func TestIntegration_GRPCCancelRetiresConn(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	seed := grpcDB(t, "&graph="+grpcCancelGraph)
	seedGRPCCancelRows(base, t, seed, cancelSeedRows)

	cfg, err := ParseDSN(requireGRPCDSN(t) + "&graph=" + grpcCancelGraph)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	dc, err := connector.Connect(base)
	if err != nil {
		t.Fatalf("Connect over gRPC: %v", err)
	}
	// The conn is deliberately retired below; Close is the cleanup, not an
	// assertion.
	defer dc.Close()

	conn, ok := dc.(*Conn)
	if !ok {
		t.Fatalf("Connect returned %T, want *geode.Conn", dc)
	}

	ctx, cancel := context.WithCancel(base)
	defer cancel()

	rows, err := conn.QueryContext(ctx, "MATCH (n:GRPCCancelProbe) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	// QueryContext hands the Conn mutex to the Rows, so an unclosed Rows would
	// deadlock the deferred Close on any t.Fatal path.
	defer rows.Close() // Close is asserted explicitly below; this is the safety net.

	dest := make([]driver.Value, len(rows.Columns()))
	for i := range 5 {
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
		t.Fatalf("Rows.Next took %v to observe cancellation over gRPC, want prompt", latency)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close: %v", err)
	}
	if conn.IsValid() {
		t.Fatal("Conn.IsValid() is true after a cancelled gRPC stream; database/sql " +
			"would recycle a connection whose Execute RPC was abandoned")
	}
	if err := conn.ResetSession(base); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ResetSession after a cancelled gRPC stream = %v, want driver.ErrBadConn", err)
	}
}

// -----------------------------------------------------------------------
// Task 13 — plan metadata
// -----------------------------------------------------------------------

// seedGRPCPlanProbe seeds the plan graph over gRPC and indexes GRPCPlanProbe(k).
// geode 0.8.15 decides has_index_seek from a real catalog lookup, so without
// the index every plan is a scan and the index assertion would be vacuous.
func seedGRPCPlanProbe(ctx context.Context, t *testing.T) *Pool {
	t.Helper()

	pool := grpcPool(t, "&graph="+grpcPlanGraph)
	for _, k := range []string{"a", "b", "c"} {
		if _, err := pool.ExecContext(ctx,
			"MERGE (n:GRPCPlanProbe {k: ?}) ON CREATE SET n.note = ?", k, "seed-"+k); err != nil {
			t.Fatalf("seed %q over gRPC: %v", k, err)
		}
	}
	if _, err := pool.ExecContext(ctx,
		"CREATE INDEX grpcplanprobe_k_idx ON GRPCPlanProbe(k) USING btree"); err != nil {
		t.Logf("CREATE INDEX (tolerated if already present): %v", err)
	}
	return pool
}

// TestIntegration_GRPCExplainShowsTheIndexBackedPlan mirrors
// TestIntegration_ExplainShowsTheIndexBackedPlan, control arm included: an
// indexed predicate must report an index access path over gRPC and an
// unindexed one must not.
func TestIntegration_GRPCExplainShowsTheIndexBackedPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedGRPCPlanProbe(ctx, t)

	indexed, err := pool.Explain(ctx, "MATCH (n:GRPCPlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Explain (indexed) over gRPC: %v", err)
	}
	t.Logf("indexed gRPC plan schema=%q ops=%d est_rows=%d\n%s",
		indexed.Schema, len(indexed.Ops), indexed.EstRows, strings.Join(indexed.Text, "\n"))
	if len(indexed.Ops) == 0 {
		t.Fatal("EXPLAIN over gRPC returned no operators — the payload did not decode")
	}
	if !indexed.UsesIndex() {
		t.Errorf("UsesIndex() = false over gRPC for a predicate covered by "+
			"grpcplanprobe_k_idx; ops = %+v", indexed.Ops)
	}

	scan, err := pool.Explain(ctx, "MATCH (n:GRPCPlanProbe {note: ?}) RETURN n.k", "seed-a")
	if err != nil {
		t.Fatalf("Explain (unindexed) over gRPC: %v", err)
	}
	if scan.UsesIndex() {
		t.Errorf("UsesIndex() = true over gRPC for an UNINDEXED property — the assertion "+
			"cannot tell a seek from a scan; ops = %+v", scan.Ops)
	}
}

// TestIntegration_GRPCExplainBindsArgumentsRatherThanInliningThem mirrors its
// QUIC twin: a value that would be a syntax error if spliced into the text
// must still plan, and must not appear in the plan text.
func TestIntegration_GRPCExplainBindsArgumentsRatherThanInliningThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := seedGRPCPlanProbe(ctx, t)

	const hostile = "a' OR 1=1 --"
	plan, err := pool.Explain(ctx, "MATCH (n:GRPCPlanProbe {k: ?}) RETURN n.k", hostile)
	if err != nil {
		t.Fatalf("Explain with a hostile argument over gRPC: %v", err)
	}
	if len(plan.Ops) == 0 {
		t.Fatal("no operators — the statement did not plan over gRPC")
	}
	for _, line := range plan.Text {
		if strings.Contains(line, "OR 1=1") {
			t.Fatalf("argument text appeared in the gRPC plan: %q", line)
		}
	}
}

// TestIntegration_GRPCProfileDoesNotClaimAnIndexItCannotProve mirrors its QUIC
// twin. geode 0.8.15's profile_version 1 fallback appends a NodeIndexScan named
// the literal "heuristic_index" for ANY explicit-WHERE equality, with no
// catalog lookup (src/server/profile_payload.zig:39,194-195) — and the gRPC
// handler builds the same payload. The driver gates UsesIndex()/IndexNames()
// on Measured(), so both must stay false here.
func TestIntegration_GRPCProfileDoesNotClaimAnIndexItCannotProve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedGRPCPlanProbe(ctx, t)

	// An explicit WHERE, not an inline property map: buildProfilePayload keys
	// on has_where_eq/has_filter_eq, and only the WHERE form sets them. An
	// inline map never reaches the fabrication branch and the control arm
	// would be vacuous.
	const unindexedWhere = "MATCH (n:GRPCPlanProbe) WHERE n.note = ? RETURN n.k"

	rep, err := pool.Profile(ctx, unindexedWhere, "seed-a")
	if err != nil {
		t.Fatalf("Profile (unindexed) over gRPC: %v", err)
	}
	t.Logf("unindexed gRPC profile version=%d measured=%v ops=%d",
		rep.ProfileVersion, rep.Measured(), rep.Operators)

	var fabricated bool
	for _, op := range rep.Ops {
		t.Logf("  op %d %s index_name=%q", op.Index, op.Op, op.IndexName)
		if isIndexAccessPath(op.Op) || op.IndexName != "" {
			fabricated = true
		}
	}
	if !fabricated {
		t.Log("NOTE: the engine no longer reports an index operator for an unindexed " +
			"predicate over gRPC; this control arm is now vacuous and the UsesIndex " +
			"gate should be re-evaluated")
	}

	if rep.UsesIndex() {
		t.Errorf("UsesIndex() = true over gRPC for an UNINDEXED property under PROFILE — "+
			"the version-%d fallback fabricates the index operator and the gate failed "+
			"open; ops = %+v", rep.ProfileVersion, rep.Ops)
	}
	if got := rep.IndexNames(); got != nil {
		t.Errorf("IndexNames() = %v over gRPC for an UNINDEXED property, want nil", got)
	}

	scan, err := pool.Explain(ctx, unindexedWhere, "seed-a")
	if err != nil {
		t.Fatalf("Explain (unindexed) over gRPC: %v", err)
	}
	if scan.UsesIndex() {
		t.Errorf("EXPLAIN over gRPC reported an index for the unindexed property: %+v", scan.Ops)
	}
}

// TestIntegration_GRPCPlanPayloadIsTypedAndKindChecked is the gRPC half of the
// two remaining Task 13 properties, asserted on ONE real engine response so
// neither can be faked:
//
//   - PAYLOAD-ONLY. geode 0.8.15 answers EXPLAIN over gRPC with an
//     ExplainPayload and NO schema and NO data page: handleExecuteStreaming
//     skips the page loop entirely for the .explain/.profile/.error payloads
//     (src/transport/grpc_server.zig:1024-1040). The text-row decoder is dead
//     code on this path, exactly as it is on QUIC.
//   - KIND MISMATCH. Feeding that real EXPLAIN payload to the decoder as if a
//     PROFILE had been asked for must raise ErrPlanKindMismatch rather than
//     yield an empty report. The engine cannot be made to answer the wrong
//     payload on demand, so the mismatch arm is driven with a genuine payload
//     off this wire rather than a hand-built one.
func TestIntegration_GRPCPlanPayloadIsTypedAndKindChecked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedGRPCPlanProbe(ctx, t)

	raw, err := pool.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("DB().Conn: %v", err)
	}

	rawErr := raw.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			t.Fatalf("driver conn type = %T, want *Conn", dc)
		}

		send := proto.NewExecuteMessage(&proto.ExecuteRequest{
			Query: "EXPLAIN MATCH (n:GRPCPlanProbe {k: 'a'}) RETURN n.k",
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
		if exec.GetError() != nil {
			t.Fatalf("EXPLAIN over gRPC errored: %v", exec.GetError())
		}

		if exec.GetExplain() == nil {
			t.Fatal("EXPLAIN over gRPC returned no ExplainPayload: the typed plan path " +
				"is not available on this transport")
		}
		if exec.GetSchema() != nil || exec.GetPage() != nil {
			t.Errorf("EXPLAIN over gRPC carried schema=%v page=%v alongside the payload; "+
				"0.8.15 answers payload-only",
				exec.GetSchema() != nil, exec.GetPage() != nil)
		}

		// Kind guard, driven with the payload the engine really sent.
		var out planFrames
		var pullID uint64
		c.mu.Lock()
		_, mismatch := c.absorbPlanFrame(ctx, exec, planProfile, &out, &pullID)
		c.mu.Unlock()
		if !errors.Is(mismatch, ErrPlanKindMismatch) {
			t.Errorf("absorbPlanFrame(explain payload, planProfile) = %v, want ErrPlanKindMismatch",
				mismatch)
		}
		// Retire this connection: the exchange consumed one Execute stream
		// outside the Conn's own bookkeeping. sql.Conn.Close() would hand it
		// straight back to the pool; only driver.ErrBadConn from the Raw
		// callback discards it.
		return driver.ErrBadConn
	})
	_ = raw.Close()
	if rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
		t.Fatalf("raw gRPC plan exchange: %v", rawErr)
	}
}

// -----------------------------------------------------------------------
// Task 7 / 10 — server info and Session pinning
// -----------------------------------------------------------------------

// TestIntegration_GRPCServerInfo mirrors TestIntegration_ServerInfo and pins
// the ONE place where gRPC's answer legitimately differs from QUIC's: the
// HELLO capability list is the transport's own
// ("GRPC PROTOBUF TLS1.3" — geode 0.8.15 src/transport/grpc_server.zig:1491),
// not the QUIC handshake's. The capability MATRIX (SHOW CAPABILITIES) and the
// version must be identical, because they describe the engine, not the wire.
func TestIntegration_GRPCServerInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := ParseDSN(requireGRPCDSN(t))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	info, err := connector.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo over gRPC: %v", err)
	}

	if info.Version != pinnedEngineVersion {
		t.Errorf("Version over gRPC = %q, want %q", info.Version, pinnedEngineVersion)
	}
	for _, feature := range []string{CapVectorKNN, CapExplain, CapProfile, CapReadOnlyExecute} {
		if !info.Capabilities.Has(feature) {
			t.Errorf("engine %s does not advertise %q over gRPC", info.Version, feature)
		}
	}

	// HELLO capabilities ARE captured over gRPC — they are simply a different
	// set from QUIC's. Asserting the transport token is what makes it
	// impossible for this to become an empty list unnoticed.
	t.Logf("gRPC HELLO capabilities: %v", info.HelloCapabilities)
	var sawGRPC bool
	for _, c := range info.HelloCapabilities {
		if c == "GRPC" {
			sawGRPC = true
		}
	}
	if !sawGRPC {
		t.Errorf("HELLO capabilities over gRPC = %v, want the transport to advertise GRPC",
			info.HelloCapabilities)
	}
}

// TestIntegration_GRPCSessionPinsOneConnection mirrors
// TestIntegration_SessionPinsOneConnectionAcrossTransactions for the part gRPC
// supports: several sequential transactions on one Session must all ride the
// same driver connection, and the identity database/sql has pinned must agree
// with the Session's own view of it.
//
// gRPC has no QUIC connection pool, so the QUIC test's "the pool churned other
// connections meanwhile" arm has no counterpart; the churn here runs through
// the same *sql.DB, which is the layer that actually does the pinning.
func TestIntegration_GRPCSessionPinsOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := grpcPool(t, "&graph="+grpcPinGraph)
	pool.DB().SetMaxOpenConns(4)
	pool.DB().SetMaxIdleConns(4)

	s, err := pool.Session(ctx)
	if err != nil {
		t.Fatalf("Session over gRPC: %v", err)
	}
	defer s.Close() // Close is asserted explicitly below; this is the safety net.

	if g := s.BoundGraph(); g != "" && g != grpcPinGraph {
		t.Fatalf("Session bound graph = %q, want %q", g, grpcPinGraph)
	}
	pinned := s.ConnID()
	if got := rawConnID(t, s); got != pinned {
		t.Fatalf("ConnID = %d but database/sql pinned conn %d", pinned, got)
	}

	for i := range 4 {
		// Unpinned traffic between the pinned transactions, so database/sql
		// has other connections in play and a lost pin would show up.
		if _, err := pool.ExecContext(ctx,
			"MERGE (n:GRPCPinChurn {i: ?})", int64(i)); err != nil {
			t.Fatalf("churn %d: %v", i, err)
		}
		if err := s.TX(ctx, nil, func(tx *sql.Tx) error {
			_, e := tx.ExecContext(ctx, "MERGE (n:GRPCPinProbe {i: ?})", int64(i))
			return e
		}); err != nil {
			t.Fatalf("Session.TX %d over gRPC: %v", i, err)
		}
		if got := s.ConnID(); got != pinned {
			t.Fatalf("after TX %d the Session reports conn %d, want the pinned %d", i, got, pinned)
		}
		if got := rawConnID(t, s); got != pinned {
			t.Fatalf("after TX %d database/sql holds conn %d, want the pinned %d", i, got, pinned)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
}

// -----------------------------------------------------------------------
// Documented gaps — asserted, never skipped
// -----------------------------------------------------------------------

// TestIntegration_GRPCPagedResultIsConsumedAcrossPushedPages is the
// GAP-SONNY806-04 regression test (Task 14b).
//
// geode 0.8.15 serves Execute over gRPC as a SERVER-STREAMING RPC: the engine
// pushes the schema/first response and then loops buildStreamDataPageLocked ->
// sendStreamResponse until the page it built is final, paging at
// grpc_stream_page_size (src/transport/grpc_server.zig:1009-1101, :915 ->
// src/client/quic_query_runner.zig:5 = 128 rows). Every page is on the wire
// unasked, and GRPCTransport.ReceiveProto has always returned them.
//
// Until Task 14b the driver asked anyway: Rows.fetchNextPage sent a PullRequest
// after every non-final page, GeodeService declares no Pull method, the WRITE
// failed, and a gRPC result stopped dead at 128 rows with a typed refusal. That
// refusal was a capitulation to a driver bug, not a property of the wire.
//
// This seeds five full pages plus a short one — the remainder matters, because
// an off-by-one page loop is invisible on an exact multiple — and demands ALL
// of it, IN ORDER. A short read, a duplicated page, a dropped page or a
// reordered one all fail here.
func TestIntegration_GRPCPagedResultIsConsumedAcrossPushedPages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	const want = 5*grpcStreamPageSize + 7 // 647: five full pushed pages + 7

	db := grpcDB(t, "&graph="+grpcPagedGraph)
	seedGRPCPagedRows(ctx, t, db, want)

	// Control arm: the engine really holds that many rows, so a short read
	// below is the driver's doing and not a thin seed.
	var total int64
	if err := db.QueryRowContext(ctx,
		"MATCH (n:GRPCPageProbe) RETURN count(n)").Scan(&total); err != nil {
		t.Fatalf("count over gRPC: %v", err)
	}
	if total != int64(want) {
		t.Fatalf("engine holds %d GRPCPageProbe rows, want exactly %d — the seed did not land",
			total, want)
	}

	r, err := db.QueryContext(ctx,
		"MATCH (n:GRPCPageProbe) RETURN n.i ORDER BY n.i")
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	defer func() { _ = r.Close() }()

	read := 0
	for r.Next() {
		var got int64
		if err := r.Scan(&got); err != nil {
			t.Fatalf("Scan at row %d: %v", read, err)
		}
		if got != int64(read) {
			t.Fatalf("row %d is n.i=%d, want %d: the pushed pages were reordered, "+
				"duplicated or dropped", read, got, read)
		}
		read++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("rows.Err() after %d of %d rows = %v; want nil — gRPC paging is "+
			"server-pushed and the driver must consume it (GAP-SONNY806-04)",
			read, want, err)
	}
	if read != want {
		t.Fatalf("read %d of %d rows with no error: a truncated result set was handed "+
			"to the caller as if it were complete", read, want)
	}
	t.Logf("gRPC paged read: %d rows across %d pushed pages, in order",
		read, (read+grpcStreamPageSize-1)/grpcStreamPageSize)
}

// seedGRPCPagedRows brings the paging graph up to n rows over gRPC.
//
// One MERGE per row, not Pool.BatchExec: this is the control arm for the
// paging test, so it must not depend on the code path under test. (BatchExec
// over gRPC has its own test below.)
func seedGRPCPagedRows(ctx context.Context, t *testing.T, db *sql.DB, n int) {
	t.Helper()
	for i := range n {
		if _, err := db.ExecContext(ctx,
			"MERGE (n:GRPCPageProbe {i: ?})", int64(i)); err != nil {
			t.Fatalf("seed row %d over gRPC: %v", i, err)
		}
	}
}

// grpcPushedRowCeiling is how much a single gRPC Execute can actually deliver
// against engine 0.8.15, measured: roughly 64 KiB of pushed payload, after
// which the stream stalls until a ~30 s engine-side timeout. See
// GAP-SONNY806-07 — it is an ENGINE/grpc-zig flow-control defect (the peer
// never acts on the client's HTTP/2 WINDOW_UPDATEs), not a driver gap, and the
// same query over QUIC returns 50 000 rows in ~1.5 s.
//
// The number matters to the tests below: a probe meant to span pushed pages
// must stay well UNDER it, or it is measuring the engine's ceiling instead of
// the driver's page loop.
const grpcPushedRowCeiling = 5632 // 44 pages of narrow integer rows

// grpcMultiPageQuery returns 1000 narrow rows — eight pushed pages, ~11 KiB,
// comfortably inside the ceiling — without seeding anything. `FOR ... IN
// range(...)` is the generator geode 0.8.15 accepts; `UNWIND range(...)` is a
// parse error at this version.
const grpcMultiPageQuery = "FOR i IN range(0, 999) RETURN i"

// TestIntegration_GRPCEarlyCloseDoesNotWedgeTheSession is the Task 14c
// regression test: abandoning a pushed result before its final page must leave
// the connection immediately reusable.
//
// Why it matters. geode serves Execute as a server-streaming RPC and pushes
// every page unasked. A driver that stops reading without cancelling leaves
// the engine pushing into a window nobody drains, and the push loop holds the
// session mutex across the send (src/transport/grpc_server.zig:1065-1090,
// entry.mutex.lockUncancelable) while handleExecute (:1935) takes the same
// mutex — so unread pages are the next statement's problem. The driver now
// cancels the abandoned RPC in Rows.Close, and skips the trailing-frame drain
// that would otherwise have eaten a page of the caller's result on this wire.
//
// The test drives the driver Conn directly: one connection is one engine
// session, with no database/sql retry to paper over a stall and no ErrBadConn
// translation to hide the cause.
//
// HONEST STATUS: this test does NOT fail against engine 0.8.15 without the
// fix, and it is recorded as a guard rather than a reproducer. Two measured
// engine facts explain why, and both are worth knowing before anyone "tightens
// it up" until it goes red:
//
//   - Before the fix the trailing-frame drain masked the wedge. On a
//     non-final release Conn.drainTrailingFrames loops over ReceiveProto with a
//     1 ms BACKGROUND deadline that does not bind a gRPC stream, so instead of
//     expiring it read pushed pages until the result ran out — slurping the
//     abandoned result synchronously under the connection lock. For a result
//     this size that is merely wasteful (the follow-up statement measured
//     2.3 ms without the cancel against 0.65 ms with it), so the assertion
//     below passes either way.
//   - Making the result big enough to fill the HTTP/2 window — which is what
//     would park the engine's push loop inside the session mutex and turn
//     waste into a wedge — is not possible at 0.8.15: the engine stalls at
//     roughly 64 KiB of pushed payload whatever the client does
//     (GAP-SONNY806-07), and then ignores the client's RST_STREAM too. Past
//     that ceiling the follow-up statement is slow with the fix and slower
//     without it (30 s to driver.ErrBadConn versus 34 s), which measures the
//     engine's stall, not the driver's cancellation.
//
// The failing-before-the-fix evidence for the cancel therefore lives in the
// unit lane, where the stream is a fake and the engine's ceiling is not in the
// way: TestRows_PushedPages_EarlyCloseCancelsTheStream and
// TestGRPCTransport_NextExecuteCancelsThePreviousStream. What this test adds is
// the end-to-end property on the real wire — an early Close leaves the session
// usable and the connection poolable — and it will start earning its keep as a
// reproducer the moment GAP-SONNY806-07 is fixed and grpcPushedRowCeiling can
// be raised.
func TestIntegration_GRPCEarlyCloseDoesNotWedgeTheSession(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	conn := grpcDriverConn(t, base, grpcWedgeGraph)

	// Control arm: the probe really does span several pushed pages, so the
	// early Close below abandons a result rather than ending a finished one.
	full, err := conn.QueryContext(base, grpcMultiPageQuery, nil)
	if err != nil {
		t.Fatalf("control QueryContext over gRPC: %v", err)
	}
	total := drainDriverRows(t, full)
	if err := full.Close(); err != nil {
		t.Fatalf("control Close: %v", err)
	}
	if total <= grpcStreamPageSize {
		t.Fatalf("the probe returned %d rows, which fits in one %d-row pushed page: "+
			"an early Close would abandon nothing", total, grpcStreamPageSize)
	}

	rows, err := conn.QueryContext(base, grpcMultiPageQuery, nil)
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on the first row: %v", err)
	}
	// Abandon the remaining pages with the query context still LIVE.
	if err := rows.Close(); err != nil {
		t.Fatalf("early Close: %v", err)
	}

	if !conn.IsValid() {
		t.Fatal("Conn.IsValid() is false after an early Close on a live context: " +
			"cancelling the abandoned RPC leaves the connection reusable, so it must " +
			"not be retired — retiring would cost an eviction and a re-dial every " +
			"time a caller stopped reading early")
	}

	// The next statement on this session must not be waiting on anything the
	// abandoned result left behind.
	next, cancelNext := context.WithTimeout(base, 15*time.Second)
	defer cancelNext()

	start := time.Now()
	after, err := conn.QueryContext(next, "RETURN 1", nil)
	if err != nil {
		t.Fatalf("statement after an early Close = %v (%v elapsed): the abandoned "+
			"server-streaming Execute is still owed pages and holds the session "+
			"mutex (grpc_server.zig:1065-1090); this Execute (:1935) waits behind it",
			err, time.Since(start))
	}
	oneDest := make([]driver.Value, len(after.Columns()))
	if err := after.Next(oneDest); err != nil {
		_ = after.Close()
		t.Fatalf("reading the row after an early Close: %v", err)
	}
	elapsed := time.Since(start)
	if err := after.Close(); err != nil {
		t.Fatalf("Close of the follow-up rows: %v", err)
	}
	t.Logf("statement after abandoning %d of %d pushed rows completed in %v",
		total-1, total, elapsed)

	// And the session is not merely usable once: a third statement proves the
	// cancel did not leave the connection in a one-shot state.
	third, err := conn.QueryContext(next, "RETURN 2", nil)
	if err != nil {
		t.Fatalf("second statement after an early Close: %v", err)
	}
	if err := third.Next(make([]driver.Value, len(third.Columns()))); err != nil {
		_ = third.Close()
		t.Fatalf("reading the second follow-up row: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("Close of the second follow-up rows: %v", err)
	}
}

// TestIntegration_GRPCLargePushedResultFailsLoudly pins the property that
// survives GAP-SONNY806-07 either way: a gRPC result the engine cannot finish
// pushing must reach the caller as an ERROR, never as a short result set that
// looks complete.
//
// At engine 0.8.15 a single Execute stalls after roughly 64 KiB of pushed
// payload (grpc-zig never acts on the client's WINDOW_UPDATEs), so this query
// stops early and the caller sees the context error. If the engine is fixed,
// the same test passes by returning every row. What it forbids — silent
// truncation — is the failure mode this lane exists to prevent, and it is the
// exact shape GAP-SONNY806-04 wore before Task 14b.
func TestIntegration_GRPCLargePushedResultFailsLoudly(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBase()

	const want = 20000 // ~235 KiB of narrow rows: far past the measured ceiling

	conn := grpcDriverConn(t, base, grpcWedgeGraph)

	ctx, cancel := context.WithTimeout(base, 20*time.Second)
	defer cancel()

	started := time.Now()
	rows, err := conn.QueryContext(ctx,
		fmt.Sprintf("FOR i IN range(0, %d) RETURN i", want-1), nil)
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	defer func() { _ = rows.Close() }()

	dest := make([]driver.Value, len(rows.Columns()))
	read := 0
	var stop error
	for {
		if stop = rows.Next(dest); stop != nil {
			break
		}
		read++
	}

	switch {
	case errors.Is(stop, io.EOF) && read == want:
		t.Logf("engine delivered all %d rows: GAP-SONNY806-07 appears fixed — "+
			"revisit grpcPushedRowCeiling and the gRPC caveats in README/CHANGELOG",
			read)
	case errors.Is(stop, io.EOF):
		t.Fatalf("SILENT TRUNCATION: %d of %d rows, reported as a clean EOF. A short "+
			"result set must never reach the caller as a complete one", read, want)
	case errors.Is(stop, context.DeadlineExceeded):
		t.Logf("engine stalled after %d of %d pushed rows and the driver surfaced the "+
			"context error (GAP-SONNY806-07, ~64 KiB ceiling)", read, want)
		if read > grpcPushedRowCeiling*2 {
			t.Logf("note: %d rows is well past the recorded ceiling of %d; the "+
				"measurement may need refreshing", read, grpcPushedRowCeiling)
		}
	default:
		t.Fatalf("after %d of %d rows the read stopped with %v; want either every row "+
			"or a surfaced error", read, want, stop)
	}

	_ = rows.Close()
	waitForGRPCEngineToRecover(base, t, started)
}

// waitForGRPCEngineToRecover blocks until the engine can once more do the kind
// of work the tests after this one do, and reports how long that took.
//
// It exists because GAP-SONNY806-07's blast radius is not confined to the
// caller that triggers it. When the engine stalls mid-push it stays parked
// with the client's RST_STREAM unread, and while it is parked a NEW session's
// multi-statement transaction is refused mid-way —
// TestIntegration_GRPCBatchExecPastOnePage, which runs immediately after this
// one, fails around statement 29 with driver.ErrBadConn, yet passes when run
// alone. That is worth measuring rather than dodging by reordering the file:
// the same thing will happen to a real caller that issues an oversized query
// and then keeps working.
//
// The probe is deliberately transaction-shaped rather than a bare RETURN 1. A
// single unary round trip answers within milliseconds while the engine is
// still poisoned for batched work, so a trivial probe reports a recovery that
// has not happened.
func waitForGRPCEngineToRecover(ctx context.Context, t *testing.T, stalledAt time.Time) {
	t.Helper()

	const (
		budget    = 120 * time.Second
		probeSize = 64 // past the ~29 statements at which the poisoned session fails
		// The engine's own timeout on a parked push fires around 30 s after
		// the send began. Until it does, the engine is still holding the
		// stalled result's session state, and a probe that succeeds before
		// then proves nothing: a trivial RETURN 1 answers in ~45 ms and a
		// 64-statement transaction in ~285 ms while the 647-statement batch
		// in the next test still dies at statement 29. Measured against
		// engine 0.8.15; both numbers are from this lane.
		engineStallTimeout = 35 * time.Second
	)

	stmts := make([]Statement, 0, probeSize)
	for i := range probeSize {
		stmts = append(stmts, Statement{
			Query: "MERGE (n:GRPCRecoveryProbe {i: ?})",
			Args:  []any{int64(i)},
		})
	}

	start := time.Now()
	deadline := start.Add(budget)

	var last error
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		if since := time.Since(stalledAt); since < engineStallTimeout {
			time.Sleep(engineStallTimeout - since)
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		pool := grpcPool(t, "&graph="+grpcWedgeGraph)
		last = pool.BatchExec(probeCtx, stmts)
		cancel()
		if last == nil {
			t.Logf("engine ran a %d-statement transaction again %v after the stalled "+
				"result, on attempt %d (GAP-SONNY806-07 blast radius)",
				probeSize, time.Since(start), attempt)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("engine could not run a %d-statement transaction within %v of the "+
		"stalled result; last error %v. GAP-SONNY806-07 has got worse: the stall "+
		"used to clear on its own", probeSize, budget, last)
}

// grpcDriverConn dials the gRPC endpoint and returns the driver Conn itself.
// One driver Conn is one engine session, which is what the session-level
// assertions above need; database/sql would be free to hand the follow-up
// statement a different connection and prove nothing.
func grpcDriverConn(t *testing.T, ctx context.Context, graph string) *Conn {
	t.Helper()
	cfg, err := ParseDSN(requireGRPCDSN(t) + "&graph=" + graph)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	dc, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect over gRPC: %v", err)
	}
	t.Cleanup(func() { _ = dc.Close() })

	conn, ok := dc.(*Conn)
	if !ok {
		t.Fatalf("Connect returned %T, want *geode.Conn", dc)
	}
	return conn
}

// drainDriverRows reads a driver.Rows to EOF and returns the row count.
func drainDriverRows(t *testing.T, rows driver.Rows) int {
	t.Helper()
	dest := make([]driver.Value, len(rows.Columns()))
	n := 0
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			return n
		}
		if err != nil {
			t.Fatalf("Next at row %d: %v", n, err)
		}
		n++
	}
}

// TestIntegration_GRPCBatchExecPastOnePage mirrors the QUIC batch path over
// gRPC at a size that used to be unreachable there.
//
// Pool.BatchExec runs every statement inside one transaction on one pinned
// connection (batch.go batchExecTx). Over gRPC that means a unary Begin, N
// server-streaming Executes and a unary Commit on the same session — and until
// Task 14b the gRPC parity lane seeded row by row specifically to stay under
// the engine's first page. Asserting a batch that crosses five page boundaries
// AND its committed effect keeps that limit from quietly coming back.
func TestIntegration_GRPCBatchExecPastOnePage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	const want = 5*grpcStreamPageSize + 7 // 647

	pool := grpcPool(t, "&graph="+grpcBatchGraph)

	stmts := make([]Statement, 0, want)
	for i := range want {
		stmts = append(stmts, Statement{
			Query: "MERGE (n:GRPCBatchProbe {i: ?})",
			Args:  []any{int64(i)},
		})
	}
	if err := pool.BatchExec(ctx, stmts); err != nil {
		t.Fatalf("BatchExec(%d statements) over gRPC: %v", want, err)
	}

	var total int64
	if err := pool.DB().QueryRowContext(ctx,
		"MATCH (n:GRPCBatchProbe) RETURN count(n)").Scan(&total); err != nil {
		t.Fatalf("count after BatchExec over gRPC: %v", err)
	}
	if total != int64(want) {
		t.Fatalf("BatchExec committed %d of %d rows over gRPC", total, want)
	}

	// And the rows read back whole, which is the half that needed the paging
	// fix: reading 647 rows crosses five pushed page boundaries.
	r, err := pool.DB().QueryContext(ctx,
		"MATCH (n:GRPCBatchProbe) RETURN n.i ORDER BY n.i")
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	defer func() { _ = r.Close() }()
	read := 0
	for r.Next() {
		var got int64
		if err := r.Scan(&got); err != nil {
			t.Fatalf("Scan at row %d: %v", read, err)
		}
		if got != int64(read) {
			t.Fatalf("row %d is n.i=%d, want %d", read, got, read)
		}
		read++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("rows.Err() after %d rows: %v", read, err)
	}
	if read != want {
		t.Fatalf("read back %d of %d batched rows", read, want)
	}
}

// TestIntegration_GRPCHealthFallsBackToPing pins the second gap. Pool.Health
// over QUIC reads a protocol-level PING taken on a long-lived per-connection
// probe stream and reports PooledConns/LiveConns from the QUIC pool (Task 11).
// A gRPC Pool owns no QUICPool, so there is no probe stream to take and no
// pooled-connection census to report: Health degrades to a round trip through
// database/sql (health.go:268-273).
//
// Asserting it means a future change that quietly wires a QUIC probe onto a
// gRPC pool — or that starts reporting a pool census gRPC does not have —
// fails here instead of shipping.
func TestIntegration_GRPCHealthFallsBackToPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := grpcPool(t, "&graph="+grpcParamGraph)

	if qp := pool.quicPool(); qp != nil {
		t.Fatalf("a gRPC Pool owns a QUICPool (%p): Health would probe a pool this "+
			"transport does not use", qp)
	}

	h := pool.Health(ctx)
	t.Logf("gRPC health: %+v", h)
	if !h.Healthy {
		t.Fatalf("Health over gRPC = %+v, want healthy against a live engine", h)
	}
	if h.LastProbeErr != "" {
		t.Errorf("Health.LastProbeErr = %q, want empty", h.LastProbeErr)
	}
	// The ping fallback reports the one connection it verified, never a QUIC
	// pool census.
	if h.PooledConns != 1 || h.LiveConns != 1 {
		t.Errorf("Health = (pooled %d, live %d), want (1, 1) from the ping fallback",
			h.PooledConns, h.LiveConns)
	}

	// Drain has no QUIC pool to close either, but must still be a clean
	// shutdown rather than an error or a hang.
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain over gRPC: %v", err)
	}
}

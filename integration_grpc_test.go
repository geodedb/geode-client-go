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
//   - no PULL, so no paging past the first gRPC page
//     (TestIntegration_GRPCPagedResultRefusesRatherThanTruncates)
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
	grpcPlanGraph   = "grpcplantest"
	grpcPinGraph    = "grpcpintest"
)

// grpcStreamPageSize is the engine's gRPC stream page size: geode 0.8.15
// serves Execute over gRPC as a server-streaming RPC and pages it at
// query_runner.default_page_size (src/transport/grpc_server.zig:915 ->
// src/client/quic_query_runner.zig:5), i.e. 128 rows. A gRPC result of at most
// this many rows arrives in the first page and needs no PULL; anything longer
// does, and the gRPC transport has no PULL to send.
const grpcStreamPageSize = 128

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

	var ids any
	if err := db.QueryRowContext(ctx,
		"RETURN knn('gembedding', ?, ?, 'cosine')", Vector{0.99, 0.01, 0}, 1,
	).Scan(&ids); err != nil {
		t.Fatalf("knn with a bound vector over gRPC: %v", err)
	}
	if ids == nil {
		t.Fatal("knn returned NULL over gRPC — the vector parameter did not reach the function")
	}
	t.Logf("knn(bound vector) over gRPC = %v", ids)
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
		return nil
	})
	// The stream carried a server error frame; retire this connection rather
	// than returning it to the pool.
	_ = raw.Close()
	if rawErr != nil {
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
// The seed deliberately does NOT use Pool.BatchExec: a batch returns a result
// set, and a result set longer than one gRPC page cannot be paged on this
// transport (see TestIntegration_GRPCPagedResultRefusesRatherThanTruncates).
// Seeding statement by statement keeps every response a single page.
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
	seedGRPCCancelRows(base, t, db, grpcStreamPageSize)

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
	// The whole result is buffered in one gRPC page, so "aborted" can only
	// mean "stopped handing out the buffered rows". Draining the page anyway
	// is the regression this bound catches.
	if read >= grpcStreamPageSize {
		t.Fatalf("read %d of %d buffered rows after cancelling at row 5: the cancel "+
			"did not stop the reader", read, grpcStreamPageSize)
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
	seedGRPCCancelRows(base, t, seed, grpcStreamPageSize)

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
		return nil
	})
	// The raw exchange consumed one Execute stream outside the Conn's own
	// bookkeeping; retire the connection rather than returning it.
	_ = raw.Close()
	if rawErr != nil {
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

// TestIntegration_GRPCPagedResultRefusesRatherThanTruncates pins the biggest
// real gap between the two transports.
//
// geode 0.8.15 serves Execute over gRPC as a SERVER-STREAMING RPC: the engine
// pushes the schema and then every data page itself and ends the stream
// (src/transport/grpc_server.zig:1008-1101 handleExecuteStreaming), so there is
// no client-driven PULL — and GeodeService declares no method that could carry
// one (proto/geode_grpc.pb.go, GeodeService_ServiceDesc). The driver's Rows
// pages with PULL (rows.go fetchNextPage), so a result longer than the engine's
// gRPC page (128 rows, grpc_server.zig:915 -> quic_query_runner.zig:5) ends at
// the first page.
//
// What this test refuses to allow is a SILENT short read. The reader must stop
// with a typed ErrUnsupportedOnTransport that names the transport limit — a
// caller must be able to tell "this wire cannot page" from "the query returned
// that many rows" — and Classify must answer RetryNever, because no retry and
// no other connection makes gRPC grow a PULL.
//
// Delete this test when the driver learns to consume the engine's pushed pages
// (or gRPC gains a PULL); until then it is the contract.
func TestIntegration_GRPCPagedResultRefusesRatherThanTruncates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const rows = grpcStreamPageSize + 40

	db := grpcDB(t, "&graph="+grpcCancelGraph)
	seedGRPCCancelRows(ctx, t, db, rows)

	// Control arm: the engine really does hold that many rows, so a short read
	// below is the transport's doing and not an empty graph.
	var total int64
	if err := db.QueryRowContext(ctx,
		"MATCH (n:GRPCCancelProbe) RETURN count(n)").Scan(&total); err != nil {
		t.Fatalf("count over gRPC: %v", err)
	}
	if total < int64(rows) {
		t.Fatalf("engine holds %d GRPCCancelProbe rows, want at least %d — the seed did not land",
			total, rows)
	}

	r, err := db.QueryContext(ctx, "MATCH (n:GRPCCancelProbe) RETURN n.i")
	if err != nil {
		t.Fatalf("QueryContext over gRPC: %v", err)
	}
	read := 0
	for r.Next() {
		read++
	}
	rowsErr := r.Err()
	_ = r.Close()

	t.Logf("gRPC paged read: %d of %d rows, err=%v", read, total, rowsErr)

	if rowsErr == nil {
		if int64(read) == total {
			t.Fatalf("gRPC returned all %d rows: the transport learned to page. "+
				"Remove this gap test and the ErrUnsupportedOnTransport arm in "+
				"GRPCTransport.SendProto.", total)
		}
		t.Fatalf("gRPC returned %d of %d rows with NO error: a truncated result set was "+
			"handed to the caller as if it were complete", read, total)
	}
	if !errors.Is(rowsErr, ErrUnsupportedOnTransport) {
		t.Fatalf("rows.Err() = %v, want ErrUnsupportedOnTransport naming the missing PULL", rowsErr)
	}
	if read != grpcStreamPageSize {
		t.Errorf("read %d rows before the refusal, want the engine's gRPC page size %d",
			read, grpcStreamPageSize)
	}
	if got := Classify(rowsErr); got != RetryNever {
		t.Errorf("Classify(rows.Err()) = %v, want RetryNever: no retry grows a PULL", got)
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

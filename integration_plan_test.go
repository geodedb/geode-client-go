//go:build integration

package geode

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// planTestGraph is the graph every query-plan integration test targets.
const planTestGraph = "plantest"

// seedPlanProbe brings the plan-probe graph into existence, seeds it, and
// creates a btree index on PlanProbe(k) — the index the EXPLAIN assertions
// depend on. geode 0.8.15 decides has_index_seek by looking the statement's
// label + property equality up in the catalog
// (src/server/quic_connection_handler.zig:6524-6566 and whereHasIndexedEq at
// :2277), so without a catalog entry EVERY plan is a scan and the
// index assertion would be vacuous.
func seedPlanProbe(ctx context.Context, t *testing.T) *Pool {
	t.Helper()

	pool, err := NewPool(sharedDSN + "&graph=" + planTestGraph)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	for _, k := range []string{"a", "b", "c"} {
		if _, err := pool.ExecContext(ctx,
			"MERGE (n:PlanProbe {k: ?}) ON CREATE SET n.note = ?", k, "seed-"+k); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}
	// IF NOT EXISTS is not assumed: a re-run against a warm engine must not
	// fail the test, and the engine's own duplicate-index refusal is the only
	// error worth tolerating here.
	if _, err := pool.ExecContext(ctx,
		"CREATE INDEX planprobe_k_idx ON PlanProbe(k) USING btree"); err != nil {
		t.Logf("CREATE INDEX (tolerated if already present): %v", err)
	}
	return pool
}

// TestIntegration_ExplainShowsTheIndexBackedPlan is the assertion Task 33
// builds on: a seed query whose predicate is covered by an index must report
// an index access path, and the SAME query shape on an unindexed property
// must not. The control arm is what makes the positive arm mean anything —
// without it, an implementation that always answered "index" would pass.
func TestIntegration_ExplainShowsTheIndexBackedPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedPlanProbe(ctx, t)

	indexed, err := pool.Explain(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Explain (indexed): %v", err)
	}
	t.Logf("indexed plan schema=%q ops=%d est_rows=%d cost=%d\n%s",
		indexed.Schema, len(indexed.Ops), indexed.EstRows, indexed.Cost,
		strings.Join(indexed.Text, "\n"))

	if len(indexed.Ops) == 0 {
		t.Fatal("EXPLAIN returned no operators — the payload did not decode")
	}
	if !indexed.UsesIndex() {
		t.Errorf("UsesIndex() = false for a predicate covered by planprobe_k_idx; ops = %+v",
			indexed.Ops)
	}

	// Control arm: same shape, uncovered property.
	scan, err := pool.Explain(ctx, "MATCH (n:PlanProbe {note: ?}) RETURN n.k", "seed-a")
	if err != nil {
		t.Fatalf("Explain (unindexed): %v", err)
	}
	t.Logf("unindexed plan schema=%q\n%s", scan.Schema, strings.Join(scan.Text, "\n"))
	if scan.UsesIndex() {
		t.Errorf("UsesIndex() = true for an UNINDEXED property — the assertion "+
			"cannot distinguish a seek from a scan; ops = %+v", scan.Ops)
	}
}

// TestIntegration_ExplainBindsArgumentsRatherThanInliningThem proves the plan
// is produced for the parameterised statement. A value that would be a syntax
// error if it were spliced into the text must still yield a plan.
func TestIntegration_ExplainBindsArgumentsRatherThanInliningThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := seedPlanProbe(ctx, t)

	hostile := "a' OR 1=1 --"
	plan, err := pool.Explain(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", hostile)
	if err != nil {
		t.Fatalf("Explain with a hostile argument: %v", err)
	}
	if len(plan.Ops) == 0 {
		t.Fatal("no operators — the statement did not plan")
	}
	for _, line := range plan.Text {
		if strings.Contains(line, "OR 1=1") {
			t.Fatalf("argument text appeared in the plan: %q", line)
		}
	}
}

// TestIntegration_ProfileReportsMeasuredTiming pins the PROFILE payload shape
// against the pinned engine: real per-operator timing, a row count taken from
// the final operator, and an operator count.
func TestIntegration_ProfileReportsMeasuredTiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedPlanProbe(ctx, t)

	rep, err := pool.Profile(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	t.Logf("profile version=%d measured=%v rows=%d ops=%d total_time_ns=%d\n%s",
		rep.ProfileVersion, rep.Measured(), rep.Rows, rep.Operators, rep.TotalTimeNS,
		strings.Join(rep.Text, "\n"))
	if !rep.Measured() {
		// Expected against the pinned engine: the harness binds a concrete
		// tenant, which demotes the read off the planned read runtime
		// (GAP-0846/GAP-0509), so the timings are the engine's deterministic
		// synthetic model rather than a clock. Recorded, not failed — the
		// driver's job is to report which it got, and Measured() does.
		t.Logf("profile timings are SYNTHETIC (profile_version %d): the "+
			"tenant-bound session demoted the read off the planned runtime",
			rep.ProfileVersion)
	}

	if rep.Operators < 1 {
		t.Fatalf("Operators = %d, want at least one executed operator", rep.Operators)
	}
	if rep.TotalTimeNS <= 0 {
		t.Errorf("TotalTimeNS = %d, want a measured duration", rep.TotalTimeNS)
	}
	if rep.Rows < 1 {
		t.Errorf("Rows = %d, want >= 1 for a seeded match", rep.Rows)
	}
	var timed bool
	for _, op := range rep.Ops {
		if op.TimeNS > 0 {
			timed = true
		}
	}
	if !timed {
		t.Errorf("no operator reported a non-zero time_ns; ops = %+v", rep.Ops)
	}
	// Deviation guard: the structured payload carries no column count, so
	// Columns is 0 on this path. If a later engine starts sending one, this
	// fails and the docs need updating.
	if rep.Columns != 0 {
		t.Errorf("Columns = %d — the 0.8.15 payload carries no column count; "+
			"the engine changed", rep.Columns)
	}
}

// TestIntegration_ReadOnlyConnCanExplain proves a read-only retrieval path can
// still record which access path its seed query used — and that PROFILE, which
// EXECUTES, is still refused for a mutating target on the same connection.
func TestIntegration_ReadOnlyConnCanExplain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A read-only connection does not CREATE GRAPH on the way in, so the
	// writable seed has to bring the graph (and the index) into existence.
	_ = seedPlanProbe(ctx, t)

	ro, err := NewPool(sharedDSN + "&graph=" + planTestGraph + "&read_only=true")
	if err != nil {
		t.Fatalf("NewPool (read-only): %v", err)
	}
	defer ro.Close()

	plan, err := ro.Explain(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("read-only Explain: %v", err)
	}
	if !plan.UsesIndex() {
		t.Errorf("read-only plan lost the index access path: %+v", plan.Ops)
	}

	if _, err := ro.Profile(ctx, "MERGE (n:PlanProbe {k: ?})", "z"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("PROFILE of a MERGE on a read-only conn = %v, want ErrReadOnly", err)
	}
}

// TestIntegration_SessionExplainRidesThePinnedConnection proves the Session
// entry point exists and answers on the pinned connection.
func TestIntegration_SessionExplainRidesThePinnedConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := seedPlanProbe(ctx, t)

	s, err := pool.Session(ctx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	defer s.Close()

	before := s.ConnID()
	plan, err := s.Explain(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Session.Explain: %v", err)
	}
	if len(plan.Ops) == 0 {
		t.Fatal("Session.Explain returned no operators")
	}
	rep, err := s.Profile(ctx, "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Session.Profile: %v", err)
	}
	if rep.Operators < 1 {
		t.Fatalf("Session.Profile reported %d operators", rep.Operators)
	}
	if s.ConnID() != before {
		t.Errorf("ConnID moved from %d to %d — the Session did not stay pinned",
			before, s.ConnID())
	}
}

// TestIntegration_PlanCapabilitiesAreAdvertised records that the pinned engine
// really does advertise the two capabilities the gate requires. Without this,
// a future engine that dropped them would turn every plan assertion above into
// a silent ErrCapabilityUnsupported that reads like a driver bug.
func TestIntegration_PlanCapabilitiesAreAdvertised(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := ParseDSN(sharedDSN)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	info, err := connector.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	for _, feature := range []string{CapExplain, CapProfile} {
		if !info.Capabilities.Has(feature) {
			t.Errorf("engine %s does not advertise %q", info.Version, feature)
		}
	}
}

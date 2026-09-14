package geode

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"geodedb.com/geode/proto"
)

func TestParseProfileMetrics(t *testing.T) {
	rows := [][2]string{
		{"profile", "planned_query_profile"},
		{"rows", "42"},
		{"columns", "1"},
		{"time_ns", "184213"},
		{"operators", "2"},
	}
	got := parseProfileMetrics(rows)
	if got.Rows != 42 || got.Columns != 1 || got.TotalTimeNS != 184213 || got.Operators != 2 {
		t.Fatalf("parseProfileMetrics = %+v", got)
	}
	if len(got.Text) != len(rows) {
		t.Errorf("len(Text) = %d, want %d", len(got.Text), len(rows))
	}
}

func TestParseProfileMetrics_FallbackVariant(t *testing.T) {
	rows := [][2]string{
		{"profile", "query_profile"},
		{"rows", "7"},
		{"columns", "2"},
		{"time_ms", "3"},
	}
	got := parseProfileMetrics(rows)
	if got.Rows != 7 || got.Columns != 2 {
		t.Fatalf("parseProfileMetrics = %+v", got)
	}
	if got.TotalTimeNS != 3_000_000 {
		t.Errorf("TotalTimeNS = %d, want 3000000 (3ms)", got.TotalTimeNS)
	}
}

func TestExplainPlan_UsesIndex(t *testing.T) {
	p := &ExplainPlan{Ops: []ExplainOp{{Kind: "IndexSeek"}, {Kind: "Project"}}}
	if !p.UsesIndex() {
		t.Error("UsesIndex() = false for a plan containing IndexSeek")
	}
	scan := &ExplainPlan{Ops: []ExplainOp{{Kind: "NodeScan"}}}
	if scan.UsesIndex() {
		t.Error("UsesIndex() = true for a NodeScan-only plan")
	}
	if (&ExplainPlan{}).UsesIndex() {
		t.Error("empty plan reported UsesIndex — must fail closed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire-shape decode (geode 0.8.15 sends a payload, not rows)
// ─────────────────────────────────────────────────────────────────────────────

// plannedExplainPayload is the shape geode 0.8.15 sends for a statement that
// compiled to the planned read runtime
// (src/server/explain_payload.zig buildExplainPayloadFromPlan): schema
// "planned_explain.v1", real operator names, cost 0 throughout.
func plannedExplainPayload() *proto.ExplainPayload {
	return &proto.ExplainPayload{
		Schema: "planned_explain.v1",
		Ops: []*proto.ExplainOp{
			{Idx: 0, Kind: "IndexSeek", EstRows: 1, Cost: 0},
			{Idx: 1, Kind: "Filter", EstRows: 1, Cost: 0},
			{Idx: 2, Kind: "Project", EstRows: 1, Cost: 0},
		},
		Totals:         &proto.ExplainTotals{EstRows: 1, Cost: 0},
		Properties:     &proto.ExplainProperties{Ordered: false, Limit: 0, Offset: 0},
		ProfileVersion: 2,
	}
}

// costExplainPayload is the trait-derived sketch geode 0.8.15 sends when the
// statement did NOT compile to the planned runtime
// (buildExplainPayload + buildOps): schema "cost_explain.v1", a Scan access
// path, heuristic costs, and the statement properties filled in.
func costExplainPayload() *proto.ExplainPayload {
	return &proto.ExplainPayload{
		Schema: "cost_explain.v1",
		Ops: []*proto.ExplainOp{
			{Idx: 0, Kind: "Scan", EstRows: 1000, Cost: 1000},
			{Idx: 1, Kind: "Expand", EstRows: 8000, Cost: 8000},
			{Idx: 2, Kind: "Project", EstRows: 8000, Cost: 800},
			{Idx: 3, Kind: "Sort", EstRows: 8000, Cost: 207},
			{Idx: 4, Kind: "Limit", EstRows: 10, Cost: 1},
		},
		Totals: &proto.ExplainTotals{EstRows: 1000, Cost: 10008},
		Properties: &proto.ExplainProperties{
			Ordered: true, Limit: 10, Offset: 5, Distinct: true,
			UnionMode: "ALL", UnionPartCount: 2,
		},
		ProfileVersion: 2,
	}
}

func TestExplainPlanFromPayload_PlannedSchema(t *testing.T) {
	plan := explainPlanFromPayload(plannedExplainPayload())

	if plan.Schema != "planned_explain.v1" {
		t.Errorf("Schema = %q", plan.Schema)
	}
	if len(plan.Ops) != 3 {
		t.Fatalf("len(Ops) = %d, want 3", len(plan.Ops))
	}
	if plan.Ops[0] != (ExplainOp{Index: 0, Kind: "IndexSeek", EstRows: 1, Cost: 0}) {
		t.Errorf("Ops[0] = %+v", plan.Ops[0])
	}
	if plan.EstRows != 1 || plan.Cost != 0 {
		t.Errorf("totals = est %d cost %d", plan.EstRows, plan.Cost)
	}
	if !plan.UsesIndex() {
		t.Error("UsesIndex() = false for a plan whose access path is IndexSeek")
	}
	if len(plan.Text) != len(plan.Ops) {
		t.Errorf("len(Text) = %d, want one line per op (%d)", len(plan.Text), len(plan.Ops))
	}
	if plan.ProfileVersion != 2 {
		t.Errorf("ProfileVersion = %d, want 2", plan.ProfileVersion)
	}
}

func TestExplainPlanFromPayload_CostSchemaCarriesProperties(t *testing.T) {
	plan := explainPlanFromPayload(costExplainPayload())

	if plan.Schema != "cost_explain.v1" {
		t.Errorf("Schema = %q", plan.Schema)
	}
	if !plan.Ordered || !plan.Distinct {
		t.Errorf("Ordered=%v Distinct=%v, want both true", plan.Ordered, plan.Distinct)
	}
	if plan.Limit != 10 || plan.Offset != 5 {
		t.Errorf("Limit=%d Offset=%d, want 10/5", plan.Limit, plan.Offset)
	}
	if plan.UnionMode != "ALL" || plan.UnionPartCount != 2 {
		t.Errorf("UnionMode=%q parts=%d", plan.UnionMode, plan.UnionPartCount)
	}
	if plan.EstRows != 1000 || plan.Cost != 10008 {
		t.Errorf("totals = est %d cost %d", plan.EstRows, plan.Cost)
	}
	// The whole point of the type: a Scan access path must NOT report an index.
	if plan.UsesIndex() {
		t.Error("UsesIndex() = true for a Scan-rooted cost plan")
	}
}

func TestExplainPlanFromPayload_NilAndEmptyFailClosed(t *testing.T) {
	for name, p := range map[string]*proto.ExplainPayload{
		"nil":           nil,
		"empty":         {},
		"no ops":        {Schema: "planned_explain.v1"},
		"nil sub-msgs":  {Schema: "x", Ops: nil, Totals: nil, Properties: nil},
		"empty op kind": {Ops: []*proto.ExplainOp{{}}},
	} {
		plan := explainPlanFromPayload(p)
		if plan == nil {
			t.Fatalf("%s: decoded to a nil plan", name)
		}
		if plan.UsesIndex() {
			t.Errorf("%s: UsesIndex() = true — the gate must fail closed", name)
		}
	}
}

func TestProfileReportFromPayload(t *testing.T) {
	p := &proto.ProfilePayload{
		ProfileVersion: 2,
		Ops: []*proto.ProfileOp{
			{OpIndex: 0, Op: "NodeIndexScan", Phase: "SCAN", InputRows: 100, Rows: 3,
				TimeNs: 1200, CumulativeTimeNs: 1200, IndexName: "idx_planprobe_k", SelectivityEst: 0.1},
			{OpIndex: 1, Op: "Project", Phase: "PROJECT", InputRows: 3, Rows: 3,
				TimeNs: 300, CumulativeTimeNs: 1500},
		},
		Totals:      &proto.ProfileTotals{TimeNs: 1500, PeakBytes: 128},
		TotalTimeNs: 1500,
	}
	rep := profileReportFromPayload(p)

	if rep.Operators != 2 {
		t.Errorf("Operators = %d, want 2", rep.Operators)
	}
	// Rows comes from the LAST operator's output.
	if rep.Rows != 3 {
		t.Errorf("Rows = %d, want 3", rep.Rows)
	}
	if rep.TotalTimeNS != 1500 {
		t.Errorf("TotalTimeNS = %d, want 1500", rep.TotalTimeNS)
	}
	if rep.PeakBytes != 128 {
		t.Errorf("PeakBytes = %d, want 128", rep.PeakBytes)
	}
	// Columns is NOT on the wire in either payload variant.
	if rep.Columns != 0 {
		t.Errorf("Columns = %d, want 0 — the payload carries no column count", rep.Columns)
	}
	if !rep.UsesIndex() {
		t.Error("UsesIndex() = false despite a named index on the scan operator")
	}
	if got := rep.IndexNames(); len(got) != 1 || got[0] != "idx_planprobe_k" {
		t.Errorf("IndexNames() = %v", got)
	}
	if len(rep.Text) == 0 || rep.Text[0] != "profile=planned_query_profile" {
		t.Errorf("Text[0] = %q, want the engine's own variant name", rep.Text)
	}
}

func TestProfileReportFromPayload_FallbackVariantIsNamedVersion1(t *testing.T) {
	rep := profileReportFromPayload(&proto.ProfilePayload{
		ProfileVersion: 1,
		Ops:            []*proto.ProfileOp{{Op: "NodeScan", Rows: 7}},
		TotalTimeNs:    42,
	})
	if rep.Text[0] != "profile=query_profile" {
		t.Errorf("Text[0] = %q, want profile=query_profile for version 1", rep.Text[0])
	}
	if rep.Rows != 7 {
		t.Errorf("Rows = %d, want 7", rep.Rows)
	}
	if rep.UsesIndex() {
		t.Error("UsesIndex() = true for a NodeScan-only profile")
	}
}

func TestProfileReportFromPayload_NilAndEmptyFailClosed(t *testing.T) {
	for name, p := range map[string]*proto.ProfilePayload{
		"nil":   nil,
		"empty": {},
	} {
		rep := profileReportFromPayload(p)
		if rep == nil {
			t.Fatalf("%s: decoded to a nil report", name)
		}
		if rep.UsesIndex() {
			t.Errorf("%s: UsesIndex() = true — must fail closed", name)
		}
		if rep.Rows != 0 || rep.Operators != 0 {
			t.Errorf("%s: %+v, want zeroes", name, rep)
		}
	}
}

func TestProfileReportFromPayload_TotalsFallbackAndSaturation(t *testing.T) {
	// total_time_ns absent → fall back to totals.time_ns.
	rep := profileReportFromPayload(&proto.ProfilePayload{
		Totals: &proto.ProfileTotals{TimeNs: 99},
	})
	if rep.TotalTimeNS != 99 {
		t.Errorf("TotalTimeNS = %d, want the totals fallback 99", rep.TotalTimeNS)
	}
	// An out-of-int64-range unsigned count saturates instead of going negative.
	rep = profileReportFromPayload(&proto.ProfilePayload{
		TotalTimeNs: math.MaxUint64,
		Ops:         []*proto.ProfileOp{{Rows: math.MaxUint64}},
	})
	if rep.TotalTimeNS != math.MaxInt64 || rep.Rows != math.MaxInt64 {
		t.Errorf("saturation failed: %+v", rep)
	}
}

func TestProfileReport_NilReceiverFailsClosed(t *testing.T) {
	var rep *ProfileReport
	if rep.UsesIndex() {
		t.Error("nil ProfileReport reported UsesIndex")
	}
	if rep.IndexNames() != nil {
		t.Error("nil ProfileReport returned index names")
	}
	var plan *ExplainPlan
	if plan.UsesIndex() {
		t.Error("nil ExplainPlan reported UsesIndex")
	}
}

func TestIsIndexAccessPath(t *testing.T) {
	for _, s := range []string{
		"IndexSeek", "index_seek", "INDEX SEEK", "NodeIndexScan",
		"VectorIndexScan", "ftsIndexSeek", "  IndexSeek  ",
	} {
		if !isIndexAccessPath(s) {
			t.Errorf("isIndexAccessPath(%q) = false", s)
		}
	}
	for _, s := range []string{
		"", "NodeScan", "AllNodesScan", "LabelScan", "Project", "Expand",
		"Filter", "Sort", "Limit", "SetOp", "Seek", "Index",
	} {
		if isIndexAccessPath(s) {
			t.Errorf("isIndexAccessPath(%q) = true", s)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Capability gate — fail closed
// ─────────────────────────────────────────────────────────────────────────────

// withFakeCapabilities pre-loads a connection's capability cache so the gate
// answers from the fake set instead of probing the engine. This is what lets a
// unit test prove the gate without a server: a capability the set does not
// list must refuse the call BEFORE anything reaches the wire.
func withFakeCapabilities(c *Conn, caps Capabilities) {
	c.caps.Store(&caps)
}

func TestRequirePlanCapability_FailsClosed(t *testing.T) {
	cases := []struct {
		name string
		caps Capabilities
		kind planKind
		want bool // want refusal
	}{
		{"explain advertised", Capabilities{CapExplain: true}, planExplain, false},
		{"profile advertised", Capabilities{CapProfile: true}, planProfile, false},
		{"explain advertised false", Capabilities{CapExplain: false}, planExplain, true},
		{"explain absent", Capabilities{CapProfile: true}, planExplain, true},
		{"profile absent", Capabilities{CapExplain: true}, planProfile, true},
		{"empty set", Capabilities{}, planExplain, true},
		{"nil set", Capabilities(nil), planProfile, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestConn(newMockTransport())
			withFakeCapabilities(c, tc.caps)
			err := c.requirePlanCapability(t.Context(), tc.kind)
			switch {
			case tc.want && err == nil:
				t.Fatal("capability gate allowed an unadvertised feature")
			case tc.want && !errors.Is(err, ErrCapabilityUnsupported):
				t.Fatalf("err = %v, want ErrCapabilityUnsupported", err)
			case !tc.want && err != nil:
				t.Fatalf("capability gate refused an advertised feature: %v", err)
			}
		})
	}
}

// TestConn_ExplainRefusedWithoutCapabilityEmitsNothing is the load-bearing
// half of "fail closed": a refusal must not put the statement on the wire, or
// a PROFILE would have executed before the gate noticed.
func TestConn_ExplainRefusedWithoutCapabilityEmitsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(c *Conn) error
	}{
		{"Explain", func(c *Conn) error { _, err := c.Explain(t.Context(), "MATCH (n) RETURN n"); return err }},
		{"Profile", func(c *Conn) error { _, err := c.Profile(t.Context(), "MATCH (n) RETURN n"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := newMockTransport()
			c := newTestConn(mt)
			withFakeCapabilities(c, Capabilities{"something_else": true})

			err := tc.run(c)
			if !errors.Is(err, ErrCapabilityUnsupported) {
				t.Fatalf("err = %v, want ErrCapabilityUnsupported", err)
			}
			mt.mu.Lock()
			sent := len(mt.sent)
			mt.mu.Unlock()
			if sent != 0 {
				t.Fatalf("%d messages reached the wire despite the capability refusal", sent)
			}
		})
	}
}

func TestCapabilityRefusalIsNeverRetried(t *testing.T) {
	if got := Classify(fmt.Errorf("geode: Pool.Explain: %w", ErrCapabilityUnsupported)); got != RetryNever {
		t.Errorf("Classify(ErrCapabilityUnsupported) = %v, want RetryNever", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end round trip over a mock transport
// ─────────────────────────────────────────────────────────────────────────────

func mockExplainMsg(p *proto.ExplainPayload) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Explain{Explain: p},
			},
		},
	}
}

func mockProfileMsg(p *proto.ProfilePayload) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Profile{Profile: p},
			},
		},
	}
}

func mockExecErrorMsg(code, message string) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Error{
					Error: &proto.Error{Code: code, Message: message},
				},
			},
		},
	}
}

// lastExecuteRequest returns the ExecuteRequest of the last message sent.
func lastExecuteRequest(t *testing.T, mt *mockTransport) *proto.ExecuteRequest {
	t.Helper()
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if len(mt.sent) == 0 {
		t.Fatal("nothing was sent")
	}
	req := mt.sent[len(mt.sent)-1].GetExecute()
	if req == nil {
		t.Fatalf("last message is not an Execute: %v", mt.sent[len(mt.sent)-1])
	}
	return req
}

// TestConn_ExplainDecodesThePayloadAndBindsArguments pins the 0.8.15 wire
// shape end to end AND the parameter-binding contract: the argument must
// travel as a typed proto.Param, never as text spliced into the statement.
func TestConn_ExplainDecodesThePayloadAndBindsArguments(t *testing.T) {
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true})
	mt.push(mockExplainMsg(plannedExplainPayload()))

	plan, err := c.Explain(t.Context(), "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "s3cret-value")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !plan.UsesIndex() {
		t.Error("UsesIndex() = false for the IndexSeek payload")
	}

	req := lastExecuteRequest(t, mt)
	if !strings.HasPrefix(req.GetQuery(), "EXPLAIN MATCH") {
		t.Errorf("query = %q, want an EXPLAIN prefix", req.GetQuery())
	}
	if strings.Contains(req.GetQuery(), "s3cret-value") {
		t.Fatalf("argument leaked into the statement text: %q", req.GetQuery())
	}
	if len(req.GetParams()) != 1 {
		t.Fatalf("params = %d, want 1 bound parameter", len(req.GetParams()))
	}
	if got := req.GetParams()[0].GetValue().GetStringVal().GetValue(); got != "s3cret-value" {
		t.Errorf("bound value = %q", got)
	}
}

func TestConn_ProfileDecodesThePayload(t *testing.T) {
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapProfile: true})
	mt.push(mockProfileMsg(&proto.ProfilePayload{
		ProfileVersion: 2,
		Ops: []*proto.ProfileOp{
			{OpIndex: 0, Op: "LabelScan", Phase: "SCAN", Rows: 5, TimeNs: 700},
		},
		TotalTimeNs: 700,
	}))

	rep, err := c.Profile(t.Context(), "MATCH (n:PlanProbe) RETURN n.k")
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	if rep.Rows != 5 || rep.Operators != 1 || rep.TotalTimeNS != 700 {
		t.Fatalf("report = %+v", rep)
	}
	if !strings.HasPrefix(lastExecuteRequest(t, mt).GetQuery(), "PROFILE MATCH") {
		t.Error("statement was not prefixed with PROFILE")
	}
}

// TestConn_ExplainTextRowFallback proves the driver still works against an
// engine that answers EXPLAIN with a `plan` text column instead of the
// structured payload. geode 0.8.15 never does over QUIC or gRPC, but the
// engine's own execution layer produces exactly these rows and the brief
// assumed them, so the path is implemented and tested rather than assumed away.
func TestConn_ExplainTextRowFallback(t *testing.T) {
	fastDrainTimeout(t)
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true})
	mt.push(mockSchemaMsg([]string{"plan"}))
	// Exactly the rows src/execution/statement_handlers.zig:1343-1371 builds:
	// an "EXPLAIN" banner, one indented row per plan step, a "Total cost: N"
	// trailer. It emits no clause headers, so neither does this corpus.
	mt.push(mockPageMsg([][]string{
		{"EXPLAIN"},
		{"  IndexSeek (cost: 120)"},
		{"  Project (cost: 8)"},
		{"Total cost: 128"},
	}))

	plan, err := c.Explain(t.Context(), "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if len(plan.Text) != 4 {
		t.Errorf("len(Text) = %d, want the 4 rows verbatim", len(plan.Text))
	}
	if len(plan.Ops) != 2 {
		t.Fatalf("Ops = %+v, want IndexSeek and Project only", plan.Ops)
	}
	if plan.Ops[0].Kind != "IndexSeek" || plan.Ops[0].Cost != 120 {
		t.Errorf("Ops[0] = %+v", plan.Ops[0])
	}
	if plan.Cost != 128 {
		t.Errorf("Cost = %d, want the Total cost row 128", plan.Cost)
	}
	if !plan.UsesIndex() {
		t.Error("UsesIndex() = false for a text plan containing IndexSeek")
	}
}

// TestConn_ExplainTextRowFallback_UnindentedPlannedForm covers the engine's
// OTHER text form (buildPlannedExplainResult), whose operator rows carry no
// indentation at all. An indentation-keyed parser would drop every one of them.
func TestConn_ExplainTextRowFallback_UnindentedPlannedForm(t *testing.T) {
	fastDrainTimeout(t)
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true})
	mt.push(mockSchemaMsg([]string{"plan"}))
	mt.push(mockPageMsg([][]string{{"IndexSeek"}, {"Filter"}, {"Project"}}))

	plan, err := c.Explain(t.Context(), "MATCH (n:PlanProbe {k: ?}) RETURN n.k", "a")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if len(plan.Ops) != 3 {
		t.Fatalf("Ops = %+v, want 3 unindented operators", plan.Ops)
	}
	if !plan.UsesIndex() {
		t.Error("UsesIndex() = false for the unindented planned text form")
	}
}

func TestConn_ProfileTextRowFallback(t *testing.T) {
	fastDrainTimeout(t)
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapProfile: true})
	mt.push(mockSchemaMsg([]string{"metric", "value"}))
	mt.push(mockPageMsg([][]string{
		{"profile", "planned_query_profile"},
		{"rows", "42"},
		{"columns", "1"},
		{"time_ns", "184213"},
		{"operators", "2"},
	}))

	rep, err := c.Profile(t.Context(), "MATCH (n:PlanProbe) RETURN n.k")
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	// Columns IS populated on this path — it is the only form that carries it.
	if rep.Rows != 42 || rep.Columns != 1 || rep.TotalTimeNS != 184213 || rep.Operators != 2 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestConn_PlanSkipsMetricsAndHeartbeatFrames(t *testing.T) {
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true})
	mt.push(mockHeartbeatMsg())
	mt.push(mockMetricsMsg())
	mt.push(mockExplainMsg(plannedExplainPayload()))

	plan, err := c.Explain(t.Context(), "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if len(plan.Ops) != 3 {
		t.Fatalf("Ops = %+v", plan.Ops)
	}
}

func TestConn_PlanSurfacesServerError(t *testing.T) {
	mt := newMockTransport()
	c := newTestConn(mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true})
	mt.push(mockExecErrorMsg("42601", "Syntax error"))

	_, err := c.Explain(t.Context(), "MATCH (n) RETURN n")
	var de *DriverError
	if !errors.As(err, &de) || de.Code != "42601" {
		t.Fatalf("err = %v, want a DriverError carrying 42601", err)
	}
	// A statement-level refusal is not a connection failure.
	if c.state == StateInError {
		t.Error("a server-side syntax error marked the connection failed")
	}
}

func TestConn_PlanReadOnlyGate(t *testing.T) {
	mt := newMockTransport()
	c := newConn(&Config{PageSize: 100, ReadOnly: true}, mt)
	withFakeCapabilities(c, Capabilities{CapExplain: true, CapProfile: true})

	// EXPLAIN of a read is allowed on a read-only connection.
	mt.push(mockExplainMsg(plannedExplainPayload()))
	if _, err := c.Explain(t.Context(), "MATCH (n:PlanProbe) RETURN n.k"); err != nil {
		t.Fatalf("read-only conn refused EXPLAIN of a MATCH: %v", err)
	}

	// PROFILE EXECUTES, so a mutating target must be refused before the wire.
	before := len(mt.sent)
	_, err := c.Profile(t.Context(), "MERGE (n:PlanProbe {k: ?})", "a")
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("err = %v, want ErrReadOnly", err)
	}
	if len(mt.sent) != before {
		t.Error("the refused PROFILE still reached the wire")
	}
}

// TestConn_PlanRefusedWhileRowsAreOpen pins the refusal on a COLD connection —
// no withFakeCapabilities, so the capability cache is empty and the gate would
// have to probe. The probe is a round trip, and putting SHOW CAPABILITIES on
// the wire in the middle of somebody else's result set is exactly what the
// ordering in planStatement exists to prevent: the caller must get
// ErrQueryInProgress, and nothing must reach the transport.
func TestConn_PlanRefusedWhileRowsAreOpen(t *testing.T) {
	mt := newMockTransport()
	c := newTestConn(mt)
	c.activeRows = &Rows{}

	if _, err := c.Explain(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrQueryInProgress) {
		t.Fatalf("Explain err = %v, want ErrQueryInProgress", err)
	}
	if _, err := c.Profile(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrQueryInProgress) {
		t.Fatalf("Profile err = %v, want ErrQueryInProgress", err)
	}
	mt.mu.Lock()
	sent := len(mt.sent)
	mt.mu.Unlock()
	if sent != 0 {
		t.Errorf("%d messages reached the wire; the refusal must precede the "+
			"capability probe", sent)
	}
}

// TestConn_PlanPayloadKindMismatchIsRefused covers BOTH directions of the I1
// defect: the two payloads share one ExecutionResponse oneof, so a decoder
// that takes whichever is present hands a PROFILE caller a zero-valued
// ProfileReport with a nil error — no operators, no timing, UsesIndex() false.
// That reads exactly like a brute-force scan, which is the wrong answer rather
// than an absent one.
func TestConn_PlanPayloadKindMismatchIsRefused(t *testing.T) {
	t.Run("explain payload answering PROFILE", func(t *testing.T) {
		mt := newMockTransport()
		c := newTestConn(mt)
		withFakeCapabilities(c, Capabilities{CapProfile: true})
		mt.push(mockExplainMsg(plannedExplainPayload()))

		rep, err := c.Profile(t.Context(), "MATCH (n) RETURN n")
		if !errors.Is(err, ErrPlanKindMismatch) {
			t.Fatalf("err = %v, want ErrPlanKindMismatch", err)
		}
		if rep != nil {
			t.Errorf("report = %+v, want nil on a refusal", rep)
		}
		if Classify(err) != RetryNever {
			t.Errorf("Classify = %v, want RetryNever", Classify(err))
		}
		if c.IsValid() != true {
			t.Error("the connection was marked failed; the stream is still in sync")
		}
	})

	t.Run("profile payload answering EXPLAIN", func(t *testing.T) {
		mt := newMockTransport()
		c := newTestConn(mt)
		withFakeCapabilities(c, Capabilities{CapExplain: true})
		mt.push(mockProfileMsg(&proto.ProfilePayload{
			ProfileVersion: 2,
			Ops:            []*proto.ProfileOp{{Op: "LabelScan", Rows: 5, TimeNs: 700}},
			TotalTimeNs:    700,
		}))

		plan, err := c.Explain(t.Context(), "MATCH (n) RETURN n")
		if !errors.Is(err, ErrPlanKindMismatch) {
			t.Fatalf("err = %v, want ErrPlanKindMismatch", err)
		}
		if plan != nil {
			t.Errorf("plan = %+v, want nil on a refusal", plan)
		}
	})
}

func TestPool_PlanEntryPointsGuardNilPool(t *testing.T) {
	var p *Pool
	if _, err := p.Explain(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrPoolNotInitialized) {
		t.Errorf("Pool.Explain on a nil pool = %v", err)
	}
	if _, err := (&Pool{}).Profile(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrPoolNotInitialized) {
		t.Errorf("Pool.Profile on a zero-value pool = %v", err)
	}
}

func TestSession_PlanEntryPointsGuardClosedSession(t *testing.T) {
	var s *Session
	if _, err := s.Explain(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Session.Explain on a nil session = %v", err)
	}
	if _, err := (&Session{}).Profile(t.Context(), "MATCH (n) RETURN n"); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Session.Profile on a zero-value session = %v", err)
	}
}

// TestProfileReport_IndexEvidenceIsGatedOnMeasured is the unit half of the
// fail-open defect. geode 0.8.15's statement-fallback profile appends a
// "NodeIndexScan" operator for ANY equality predicate without consulting the
// catalog (src/server/profile_payload.zig:39, version stamped at :321) and
// names it "heuristic_index" (:194-195). Neither is evidence of an index, so
// neither may be reported as one.
func TestProfileReport_IndexEvidenceIsGatedOnMeasured(t *testing.T) {
	fabricated := &ProfileReport{
		ProfileVersion: 1,
		Ops: []ProfileOpStat{
			{Op: "NodeIndexScan", Phase: "SCAN", IndexName: "heuristic_index"},
			{Op: "Project", Phase: "PROJECT"},
		},
	}
	if fabricated.UsesIndex() {
		t.Error("UsesIndex() = true on profile_version 1 — the engine fabricates " +
			"NodeIndexScan there with no catalog lookup; this must fail closed")
	}
	if got := fabricated.IndexNames(); got != nil {
		t.Errorf("IndexNames() = %v on profile_version 1, want nil", got)
	}

	// Even the measured profile must not read the placeholder as an identity.
	sentinelOnV2 := &ProfileReport{
		ProfileVersion: 2,
		Ops:            []ProfileOpStat{{Op: "NodeScan", IndexName: "heuristic_index"}},
	}
	if got := sentinelOnV2.IndexNames(); got != nil {
		t.Errorf("IndexNames() = %v, want nil — \"heuristic_index\" is the "+
			"engine's placeholder, not a catalog index", got)
	}
	if sentinelOnV2.UsesIndex() {
		t.Error("UsesIndex() = true from the heuristic_index placeholder alone")
	}

	// A real name on the measured profile still answers.
	catalogNamed := &ProfileReport{
		ProfileVersion: 2,
		Ops:            []ProfileOpStat{{Op: "NodeIndexScan", IndexName: "idx_planprobe_k"}},
	}
	if !catalogNamed.UsesIndex() {
		t.Error("UsesIndex() = false for a measured profile naming a catalog index")
	}
	if got := catalogNamed.IndexNames(); len(got) != 1 || got[0] != "idx_planprobe_k" {
		t.Errorf("IndexNames() = %v", got)
	}
}

func TestProfileReport_MeasuredFailsClosed(t *testing.T) {
	var nilRep *ProfileReport
	if nilRep.Measured() {
		t.Error("nil report claimed measured timings")
	}
	if (&ProfileReport{}).Measured() {
		t.Error("zero-value report claimed measured timings")
	}
	if (&ProfileReport{ProfileVersion: 1}).Measured() {
		t.Error("profile_version 1 is the synthetic statement fallback, not a measurement")
	}
	if !(&ProfileReport{ProfileVersion: 2}).Measured() {
		t.Error("profile_version 2 is the planned-runtime trace and IS measured")
	}
}

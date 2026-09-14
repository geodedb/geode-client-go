// plan.go — query-plan metadata: EXPLAIN and PROFILE as typed Go values.
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

// ErrCapabilityUnsupported reports that the engine does not advertise a
// capability the requested operation requires.
//
// The gate FAILS CLOSED in both directions: a capability the engine does not
// list is unsupported (Capabilities.Has), and a capability probe that could
// not be completed at all refuses the operation rather than assuming support.
// A retrieval path that records "this seed query used the vector index" must
// never silently record nothing because the engine quietly declined to answer.
var ErrCapabilityUnsupported = errors.New("geode: capability not advertised by the engine")

// Plan-decoding bounds. A plan is small by construction; these exist so a
// hostile or desynchronised server cannot make a caller allocate without
// bound while it waits for a payload that never arrives.
const (
	// maxPlanFrames bounds how many response frames one EXPLAIN/PROFILE may
	// consume before the driver calls the stream desynchronised.
	maxPlanFrames = 64
	// maxPlanRows bounds the text-row fallback form.
	maxPlanRows = 4096
	// nanosPerMilli converts the text-row fallback's time_ms to TotalTimeNS.
	nanosPerMilli = 1_000_000
	// profileMetricColumns is the column count of the text-row PROFILE form.
	profileMetricColumns = 2
)

// ExplainOp is one operator in a query plan, exactly as the engine's
// ExplainPayload reports it.
type ExplainOp struct {
	// Index is the operator's position in the plan, 0-based.
	Index uint32
	// Kind is the engine's own operator name — "IndexSeek", "LabelScan",
	// "AllNodesScan", "Expand", "Filter", "Project", "Sort", "Limit",
	// "SetOp" and so on. It is NOT normalised: the engine emits two
	// vocabularies (see ExplainPlan.Schema) and folding them would hide which
	// planner answered.
	Kind string
	// EstRows is the planner's estimated row count for this operator.
	EstRows int64
	// Cost is the planner's heuristic cost for this operator. It is 0 on the
	// planned-runtime schema, which does not cost its operators.
	Cost int64
}

// ExplainPlan is the planner's view of a statement.
//
// Costs are fixed heuristic constants in geode 0.8.15
// (src/planner/cost_seed.zig EstimatorConfig: scan_default 1000 rows,
// expand_degree 8, …), so the load-bearing signal is which access path the
// planner chose — IndexSeek versus a scan — not the absolute numbers.
//
// # Which schema answered
//
// geode 0.8.15 builds the payload two different ways and says which in
// Schema:
//
//   - "planned_explain.v1" — the statement compiled to the planned read
//     runtime, and Ops are that plan's real operators (IndexSeek, LabelScan,
//     AllNodesScan, ExpandOut, …). Cost is 0 throughout; the planned runtime
//     does not cost operators (src/server/explain_payload.zig
//     buildExplainPayloadFromPlan).
//   - "cost_explain.v1" / "cost_explain.v2" — the statement did not compile
//     to the planned runtime, so the engine synthesised a cost-model sketch
//     from the statement's traits: one access-path op (IndexSeek when a
//     catalog index covers the label+property equality, else Scan), then
//     Expand, optional Filter/Aggregate, Project, optional Sort/Limit/SetOp
//     (buildExplainPayload + buildOps). "v2" is the EXPLAIN CALIBRATED form.
//
// Both answer UsesIndex correctly; the vocabularies differ in everything else,
// which is why Schema is surfaced rather than hidden.
type ExplainPlan struct {
	// Schema is the payload schema identifier the engine stamped on the plan.
	// Empty when the plan came from the text-row fallback form.
	Schema string

	// Ops are the plan's operators in engine order.
	Ops []ExplainOp

	// EstRows and Cost are the plan totals.
	EstRows int64
	Cost    int64

	// Ordered, Limit, Offset, Distinct, UnionMode and UnionPartCount are the
	// planner's statement properties. UnionMode is "" when the statement has
	// no UNION, else one of ALL, DISTINCT, INTERSECT, EXCEPT, NONE.
	Ordered        bool
	Limit          uint64
	Offset         uint64
	Distinct       bool
	UnionMode      string
	UnionPartCount uint32

	// ProfileVersion is the payload's own version stamp (2 at 0.8.15).
	ProfileVersion uint32

	// Text is the plan rendered one line per operator, for logging. When the
	// engine answers with the structured payload — which it always does at
	// 0.8.15 over QUIC and gRPC — these lines are rendered FROM that payload
	// and are not a verbatim server string. When an engine answers `EXPLAIN`
	// with a `plan` text column instead, Text holds those rows verbatim.
	Text []string
}

// UsesIndex reports whether the plan contains an index access path.
//
// An empty plan reports false, and so does a nil one: an assertion built on
// this must fail closed. "The plan proves an index was used" and "the driver
// could not tell" are the same answer here, on purpose — the caller that
// wants to distinguish them checks len(Ops).
func (p *ExplainPlan) UsesIndex() bool {
	if p == nil {
		return false
	}
	for _, op := range p.Ops {
		if isIndexAccessPath(op.Kind) {
			return true
		}
	}
	for _, line := range p.Text {
		if isIndexAccessPath(line) {
			return true
		}
	}
	return false
}

// indexAccessPathTokens are the lowercased, separator-stripped substrings that
// name an index access path in either EXPLAIN vocabulary and in the engine's
// PROFILE operator names.
//
//   - "indexseek"  — planned_explain.v1 .IndexSeek and cost_explain .IndexSeek
//     (src/server/explain_payload.zig plannedOpName / buildOps).
//   - "indexscan"  — the PROFILE operator name "NodeIndexScan"
//     (src/server/profile_payload.zig buildProfilePayload).
//   - "vectorindex" / "ftsindex" — the retrieval access paths Task 33 asserts
//     on; kept even though 0.8.15 does not yet emit them as op kinds, so a
//     later engine that does is recognised rather than silently reported as a
//     brute-force scan.
//
//nolint:gochecknoglobals // package-level lookup table; the tests walk it
var indexAccessPathTokens = []string{
	"indexseek", "indexscan", "vectorindex", "ftsindex",
}

// isIndexAccessPath reports whether s names an index access path. Matching is
// case-insensitive and ignores the '_', '-' and ' ' separators, so
// "IndexSeek", "index_seek", "INDEX SEEK" and "NodeIndexScan" all match.
func isIndexAccessPath(s string) bool {
	folded := strings.Map(func(r rune) rune {
		switch r {
		case '_', '-', ' ', '\t':
			return -1
		}
		return r
	}, strings.ToLower(s))
	for _, tok := range indexAccessPathTokens {
		if strings.Contains(folded, tok) {
			return true
		}
	}
	return false
}

// ProfileOpStat is one executed operator's measurements.
type ProfileOpStat struct {
	// Index is the operator's position in the executed plan, 0-based.
	Index uint32
	// Op is the engine's operator name and Phase its phase label
	// ("SCAN", "EXPAND", "PROJECT", …).
	Op    string
	Phase string
	// InputRows and Rows are the operator's input and output row counts.
	InputRows int64
	Rows      int64
	// TimeNS is the operator's own measured time; CumulativeTimeNS is the
	// running total through this operator.
	TimeNS           int64
	CumulativeTimeNS int64
	// IndexName is the index this operator used, when it used one and the
	// engine named it. Empty otherwise — including for every operator on the
	// planned-runtime profile, which does not populate it at 0.8.15.
	IndexName string
	// SelectivityEst is the planner's selectivity estimate for an indexed
	// operator, 0 otherwise.
	SelectivityEst float64
}

// ProfileReport is the executed-statement profile.
//
// PROFILE EXECUTES the statement. Unlike EXPLAIN it is not safe to run against
// a mutating statement "just to see the plan", and the driver's read-only
// classifier refuses `PROFILE MERGE …` on a read-only connection for exactly
// that reason.
//
// PROFILE also returns NO RESULT ROWS: geode 0.8.15 answers the request with
// the profile payload alone, so a caller that needs both the rows and the
// profile must issue the statement twice.
//
// Whether the numbers are measured or modelled depends on which engine path
// answered; see Measured and TotalTimeNS.
type ProfileReport struct {
	// Rows is the row count the final operator emitted, and Columns the
	// result's column count.
	//
	// Columns is 0 whenever the engine answered with the structured payload,
	// which carries no column count — it is populated only by the text-row
	// fallback form. See the deviation note in .superpowers/sdd/task-13-report.md.
	Rows    int64
	Columns int64

	// TotalTimeNS is the execution time in nanoseconds.
	//
	// It is a REAL measurement only when Measured reports true. geode 0.8.15
	// demotes a data-touching read off the planned read runtime whenever the
	// session resolves to a concrete TENANT scope (GAP-0846/GAP-0509,
	// src/execution/planned_read_runtime.zig:219-247), and the statement
	// fallback that then answers builds its per-operator times from a
	// deterministic synthetic model — 10ns × (index+1) — rather than from a
	// clock (src/server/profile_payload.zig buildProfilePayload). A
	// tenant-bound Sonny connection therefore gets profile_version 1 and
	// synthetic timings. Treat them as plan SHAPE, not as latency.
	TotalTimeNS int64

	// Operators is the number of operators in the executed plan.
	Operators int64

	// ProfileVersion is the payload's version stamp: 2 for the planned-runtime
	// profile, 1 for the statement-fallback profile.
	ProfileVersion uint32

	// Ops are the per-operator measurements, empty on the text-row form.
	Ops []ProfileOpStat

	// PeakBytes is the payload's peak memory total. It is a deterministic
	// SYNTHETIC model on the statement-fallback path (geode 0.8.15
	// src/server/profile_payload.zig derives it from row counts, not from an
	// allocator), so it is reported but must not be read as a measurement.
	PeakBytes int64

	// Text is the report rendered as metric=value lines, using the same
	// metric vocabulary the engine's own text-row form uses (profile, rows,
	// columns, time_ns, operators). On the text-row form these are the
	// server's rows verbatim.
	Text []string
}

// Measured reports whether this report's timings are real measurements.
//
// True only for the planned-read-runtime profile (profile_version 2), which
// carries a per-operator RuntimeTrace. The statement-fallback profile
// (version 1) synthesises its per-operator times, so a caller that asserts on
// latency must gate on this. A nil report is not measured — fail closed.
func (r *ProfileReport) Measured() bool {
	return r != nil && r.ProfileVersion >= profilePlannedVersion
}

// UsesIndex reports whether any executed operator went through an index.
// A nil or empty report reports false — it fails closed exactly as
// ExplainPlan.UsesIndex does.
func (r *ProfileReport) UsesIndex() bool {
	if r == nil {
		return false
	}
	for _, op := range r.Ops {
		if op.IndexName != "" || isIndexAccessPath(op.Op) {
			return true
		}
	}
	return false
}

// IndexNames returns every distinct index name the executed plan reported, in
// operator order. It is empty when no operator named one.
func (r *ProfileReport) IndexNames() []string {
	if r == nil {
		return nil
	}
	var out []string
	seen := make(map[string]bool, len(r.Ops))
	for _, op := range r.Ops {
		if op.IndexName == "" || seen[op.IndexName] {
			continue
		}
		seen[op.IndexName] = true
		out = append(out, op.IndexName)
	}
	return out
}

// explainPlanFromPayload decodes the engine's structured ExplainPayload.
// A nil payload decodes to an empty, non-nil plan: an empty plan fails
// UsesIndex closed, which is the safe reading of "the engine said nothing".
func explainPlanFromPayload(p *proto.ExplainPayload) *ExplainPlan {
	plan := &ExplainPlan{}
	if p == nil {
		return plan
	}
	plan.Schema = p.GetSchema()
	plan.ProfileVersion = p.GetProfileVersion()
	plan.EstRows = p.GetTotals().GetEstRows()
	plan.Cost = p.GetTotals().GetCost()

	props := p.GetProperties()
	plan.Ordered = props.GetOrdered()
	plan.Limit = props.GetLimit()
	plan.Offset = props.GetOffset()
	plan.Distinct = props.GetDistinct()
	plan.UnionMode = props.GetUnionMode()
	plan.UnionPartCount = props.GetUnionPartCount()

	ops := p.GetOps()
	plan.Ops = make([]ExplainOp, 0, len(ops))
	plan.Text = make([]string, 0, len(ops))
	for _, op := range ops {
		plan.Ops = append(plan.Ops, ExplainOp{
			Index:   op.GetIdx(),
			Kind:    op.GetKind(),
			EstRows: op.GetEstRows(),
			Cost:    op.GetCost(),
		})
		plan.Text = append(plan.Text, fmt.Sprintf("%d %s est_rows=%d cost=%d",
			op.GetIdx(), op.GetKind(), op.GetEstRows(), op.GetCost()))
	}
	return plan
}

// profileReportFromPayload decodes the engine's structured ProfilePayload.
//
// Rows is taken from the LAST operator's output row count, which is what the
// statement returned: the planned-runtime trace records a real per-operator
// count, and the statement-fallback path sets every operator's count to the
// statement's row count, so the last one is correct on both. Columns is not on
// the wire in either form and stays 0.
func profileReportFromPayload(p *proto.ProfilePayload) *ProfileReport {
	rep := &ProfileReport{}
	if p == nil {
		return rep
	}
	rep.ProfileVersion = p.GetProfileVersion()
	rep.TotalTimeNS = clampU64(p.GetTotalTimeNs())
	if rep.TotalTimeNS == 0 {
		rep.TotalTimeNS = clampU64(p.GetTotals().GetTimeNs())
	}
	rep.PeakBytes = clampU64(p.GetTotals().GetPeakBytes())

	ops := p.GetOps()
	rep.Operators = int64(len(ops))
	rep.Ops = make([]ProfileOpStat, 0, len(ops))
	for _, op := range ops {
		rep.Ops = append(rep.Ops, ProfileOpStat{
			Index:            op.GetOpIndex(),
			Op:               op.GetOp(),
			Phase:            op.GetPhase(),
			InputRows:        clampU64(op.GetInputRows()),
			Rows:             clampU64(op.GetRows()),
			TimeNS:           clampU64(op.GetTimeNs()),
			CumulativeTimeNS: clampU64(op.GetCumulativeTimeNs()),
			IndexName:        op.GetIndexName(),
			SelectivityEst:   op.GetSelectivityEst(),
		})
	}
	if n := len(rep.Ops); n > 0 {
		rep.Rows = rep.Ops[n-1].Rows
	}
	rep.Text = renderProfileText(rep)
	return rep
}

// profileVariantName names the payload variant in the same vocabulary the
// engine's own text-row form uses, so a log line reads the same either way.
func profileVariantName(version uint32) string {
	if version >= profilePlannedVersion {
		return "planned_query_profile"
	}
	return "query_profile"
}

// profilePlannedVersion is the ProfilePayload.profile_version the planned
// read runtime stamps (geode 0.8.15 buildProfilePayloadFromTrace); the
// statement-fallback path stamps 1.
const profilePlannedVersion = 2

// renderProfileText renders the report as the engine's metric/value rows.
func renderProfileText(rep *ProfileReport) []string {
	text := []string{
		"profile=" + profileVariantName(rep.ProfileVersion),
		"rows=" + strconv.FormatInt(rep.Rows, 10),
		"columns=" + strconv.FormatInt(rep.Columns, 10),
		"time_ns=" + strconv.FormatInt(rep.TotalTimeNS, 10),
		"operators=" + strconv.FormatInt(rep.Operators, 10),
	}
	for _, op := range rep.Ops {
		line := fmt.Sprintf("%d %s/%s rows=%d time_ns=%d",
			op.Index, op.Op, op.Phase, op.Rows, op.TimeNS)
		if op.IndexName != "" {
			line += " index=" + op.IndexName
		}
		text = append(text, line)
	}
	return text
}

// clampU64 narrows an unsigned wire count to int64, saturating rather than
// wrapping. A negative row count or duration is never a useful answer.
func clampU64(v uint64) int64 {
	if v > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(v)
}

// explainClauseHeaders are the clause headers the engine's text-row EXPLAIN
// form emits between operator rows. They are not operators.
//
// The list is deliberately SHORTER than the GQL clause vocabulary: every entry
// here is a name the driver will discard, so a clause keyword that is ALSO a
// plan operator name must not appear. CREATE, MERGE, DELETE, SET, REMOVE,
// UNWIND, FILTER, LIMIT, CALL and UNION are all operator names in
// src/server/explain_payload.zig plannedOpName and
// src/execution/statement_handlers.zig plannedOpName, so listing them would
// erase real operators from the planned text form — including the ones a
// mutating plan consists of. None of the entries below is an operator name in
// either engine vocabulary.
//
//nolint:gochecknoglobals // package-level lookup table
var explainClauseHeaders = map[string]bool{
	keywordExplain: true, "MATCH": true, "RETURN": true, "WHERE": true,
	"ORDER": true, "ORDER BY": true, "SKIP": true, "WITH": true,
}

// explainTotalCostPrefix is the trailer row of the engine's legacy text-row
// EXPLAIN form (src/execution/statement_handlers.zig executeExplain).
const explainTotalCostPrefix = "Total cost:"

// parseExplainLine turns one text-row plan line into an operator.
//
// Lines that are not operators — the leading "EXPLAIN" banner, clause headers,
// the "Total cost: N" trailer, blanks — return ok=false and are kept only in
// Text.
//
// Indentation is NOT the test, deliberately. geode 0.8.15 has two text-row
// EXPLAIN forms and only one of them indents: the legacy form emits
// "  NodeScan (cost: 120)" under an unindented clause header, while
// buildPlannedExplainResult emits bare, unindented operator names
// ("IndexSeek", "LabelScan"). Keying on indentation would silently discard
// every operator of the planned form — the one that actually names IndexSeek.
func parseExplainLine(line string) (ExplainOp, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, explainTotalCostPrefix) {
		return ExplainOp{}, false
	}
	if explainClauseHeaders[strings.ToUpper(trimmed)] {
		return ExplainOp{}, false
	}

	op := ExplainOp{Kind: trimmed}
	// The legacy form appends " (cost: N)" to each operator name.
	if open := strings.LastIndex(trimmed, " (cost:"); open >= 0 {
		op.Kind = strings.TrimSpace(trimmed[:open])
		rest := strings.TrimSuffix(strings.TrimSpace(trimmed[open+len(" (cost:"):]), ")")
		if n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64); err == nil {
			op.Cost = n
		}
	}
	if op.Kind == "" {
		return ExplainOp{}, false
	}
	return op, true
}

// explainPlanFromRows folds the text-row `plan` column into a plan. It is the
// fallback for an engine that answers EXPLAIN with rows instead of the
// structured payload; geode 0.8.15 never does over QUIC or gRPC.
func explainPlanFromRows(lines []string) *ExplainPlan {
	plan := &ExplainPlan{Text: append([]string(nil), lines...)}
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), explainTotalCostPrefix) {
			rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), explainTotalCostPrefix))
			if n, err := strconv.ParseInt(rest, 10, 64); err == nil {
				plan.Cost = n
			}
			continue
		}
		if op, ok := parseExplainLine(line); ok {
			op.Index = uint32(len(plan.Ops)) //nolint:gosec // G115: bounded by maxPlanRows
			plan.Ops = append(plan.Ops, op)
		}
	}
	return plan
}

// parseProfileMetrics folds the metric/value rows the text-row PROFILE form
// returns into a typed report. Both documented variants are handled:
// planned_query_profile reports time_ns, query_profile reports time_ms
// (geode 0.8.15 docs/EXPLAIN_PROFILE.md, "Profiling Metrics").
func parseProfileMetrics(pairs [][2]string) ProfileReport {
	rep := ProfileReport{}
	for _, kv := range pairs {
		rep.Text = append(rep.Text, kv[0]+"="+kv[1])
		if kv[0] == "profile" && kv[1] == "planned_query_profile" {
			rep.ProfileVersion = profilePlannedVersion
		}
		n, err := strconv.ParseInt(strings.TrimSpace(kv[1]), 10, 64)
		if err != nil {
			continue
		}
		switch kv[0] {
		case "rows":
			rep.Rows = n
		case "columns":
			rep.Columns = n
		case "time_ns":
			rep.TotalTimeNS = n
		case "time_ms":
			rep.TotalTimeNS = millisToNanos(n)
		case "operators":
			rep.Operators = n
		}
	}
	return rep
}

// planKind selects which statement prefix and which payload a plan round trip
// asks for.
type planKind int

const (
	planExplain planKind = iota
	planProfile
)

// The two statement keywords, named once so the classifier's allow-list, the
// statement prefix and the clause-header table cannot drift apart.
const (
	keywordExplain = "EXPLAIN"
	keywordProfile = "PROFILE"
)

func (k planKind) keyword() string {
	if k == planProfile {
		return keywordProfile
	}
	return keywordExplain
}

func (k planKind) opName() string {
	if k == planProfile {
		return "Conn.Profile"
	}
	return "Conn.Explain"
}

func (k planKind) capability() string {
	if k == planProfile {
		return CapProfile
	}
	return CapExplain
}

// planFrames is what one EXPLAIN/PROFILE round trip collected: either the
// structured payload the engine sent, or the text rows a fallback form sent.
type planFrames struct {
	explain *proto.ExplainPayload
	profile *proto.ProfilePayload
	columns []string
	rows    [][]string
}

// planCapabilities returns this connection's cached capability matrix,
// reading SHOW CAPABILITIES from the engine once per connection.
//
// The probe is per-connection rather than per-Pool so the gate is answered by
// the SAME engine that will answer the statement, and cached so the cost is
// one extra round trip per connection lifetime rather than one per call. A
// failed probe is NOT cached: the next call re-probes rather than inheriting
// a refusal from a transient failure.
func (c *Conn) planCapabilities(ctx context.Context) (Capabilities, error) {
	if cached := c.caps.Load(); cached != nil {
		return *cached, nil
	}
	rows, err := c.showCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	caps, _, err := parseCapabilityRows(rows)
	if err != nil {
		return nil, err
	}
	c.caps.Store(&caps)
	return caps, nil
}

// requirePlanCapability refuses the operation unless the engine advertises the
// capability it needs. It fails closed on a probe failure as well as on a
// missing capability — see ErrCapabilityUnsupported.
func (c *Conn) requirePlanCapability(ctx context.Context, kind planKind) error {
	caps, err := c.planCapabilities(ctx)
	if err != nil {
		return fmt.Errorf("%w: %s: capability probe failed: %w",
			ErrCapabilityUnsupported, kind.capability(), err)
	}
	if !caps.Has(kind.capability()) {
		return fmt.Errorf("%w: %s", ErrCapabilityUnsupported, kind.capability())
	}
	return nil
}

// Explain returns the plan for a statement without executing it.
//
// Arguments are bound as parameters exactly as they would be for the real
// query (bindParameters), so the plan reflects the parameterised statement
// rather than a hand-edited copy. No argument value is ever written into the
// query text.
func (c *Conn) Explain(ctx context.Context, query string, args ...any) (*ExplainPlan, error) {
	frames, err := c.planStatement(ctx, planExplain, query, args)
	if err != nil {
		return nil, err
	}
	if frames.explain != nil {
		return explainPlanFromPayload(frames.explain), nil
	}
	return explainPlanFromRows(flattenPlanRows(frames.rows)), nil
}

// Profile executes a statement and returns its execution profile.
//
// PROFILE runs the statement. It returns no result rows at geode 0.8.15 —
// only the profile — so a caller that needs both must issue the statement
// twice.
func (c *Conn) Profile(ctx context.Context, query string, args ...any) (*ProfileReport, error) {
	frames, err := c.planStatement(ctx, planProfile, query, args)
	if err != nil {
		return nil, err
	}
	if frames.profile != nil {
		return profileReportFromPayload(frames.profile), nil
	}
	rep := parseProfileMetrics(pairPlanRows(frames.rows))
	return &rep, nil
}

// planStatement gates on the engine's capability matrix, binds the arguments
// and runs one EXPLAIN/PROFILE round trip.
func (c *Conn) planStatement(
	ctx context.Context, kind planKind, query string, args []any,
) (*planFrames, error) {
	op := kind.opName()
	if err := c.requirePlanCapability(ctx, kind); err != nil {
		return nil, fmt.Errorf("geode: %s: %w", op, err)
	}
	nv, err := c.planNamedValues(args)
	if err != nil {
		return nil, fmt.Errorf("geode: %s: %w", op, err)
	}
	frames, err := c.planRoundTrip(ctx, kind, query, nv)
	if err != nil {
		return nil, fmt.Errorf("geode: %s: %w", op, err)
	}
	return frames, nil
}

// planNamedValues converts the caller's ...any arguments the way database/sql
// would before handing them to a driver: the connection's own
// CheckNamedValue first, then the default parameter converter for anything it
// skips. Bypassing that would let an `int` reach the encoder as an `int`,
// which the typed binder does not accept.
func (c *Conn) planNamedValues(args []any) ([]driver.NamedValue, error) {
	out := make([]driver.NamedValue, 0, len(args))
	for i, a := range args {
		nv := driver.NamedValue{Ordinal: i + 1}
		if named, ok := a.(sql.NamedArg); ok {
			nv.Name, nv.Value = named.Name, named.Value
		} else {
			nv.Value = a
		}
		switch err := c.CheckNamedValue(&nv); {
		case err == nil:
		case errors.Is(err, driver.ErrSkip):
			v, cErr := driver.DefaultParameterConverter.ConvertValue(nv.Value)
			if cErr != nil {
				return nil, fmt.Errorf("argument %d: %w", i+1, cErr)
			}
			nv.Value = v
		default:
			return nil, err
		}
		out = append(out, nv)
	}
	return out, nil
}

// planRoundTrip sends one EXPLAIN/PROFILE statement and reads its answer.
//
// geode 0.8.15 answers both with a DEDICATED protobuf payload, not with rows:
// the QUIC handler (src/server/quic_connection_handler.zig, the .explain and
// .profile arms) and the gRPC handler (src/transport/grpc_server.zig) both
// build an ExplainPayload / ProfilePayload and send exactly ONE
// ExecutionResponse carrying it. The text-row forms documented in
// geode/docs/EXPLAIN_PROFILE.md are produced by
// src/execution/statement_handlers.zig, which those transports never reach for
// a statement whose analysis kind is .explain or .profile. Both shapes are
// handled here anyway so the driver keeps working against an engine that
// answers either way.
func (c *Conn) planRoundTrip(
	ctx context.Context, kind planKind, query string, args []driver.NamedValue,
) (*planFrames, error) {
	stmt := kind.keyword() + " " + query
	if err := validate.Query(stmt); err != nil {
		return nil, &SecurityError{Type: constOpInput, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return nil, driver.ErrBadConn
	}
	if c.activeRows != nil {
		return nil, ErrQueryInProgress
	}

	// Bind `?` placeholders to typed wire parameters. No argument value is
	// ever written into the statement text (SONNY-806).
	bound, protoParams, err := bindParameters(stmt, args)
	if err != nil {
		return nil, err
	}
	// The read-only classifier sees the whole `EXPLAIN <stmt>` /
	// `PROFILE <stmt>` text, so `EXPLAIN MATCH …` passes on a read-only
	// connection while `PROFILE MERGE …` — which would actually write — does
	// not. That asymmetry is deliberate: PROFILE executes.
	if err := c.guardReadOnly(bound); err != nil {
		return nil, err
	}

	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:    bound,
		Params:   protoParams,
		ReadOnly: c.cfg.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		c.markConnFailed()
		return nil, transportErrBadConn(err)
	}

	return c.collectPlanFrames(ctx, kind)
}

// collectPlanFrames reads response frames until the plan is complete.
// Callers must hold c.mu.
func (c *Conn) collectPlanFrames(ctx context.Context, kind planKind) (*planFrames, error) {
	out := &planFrames{}
	pullID := uint64(0)
	for frame := 0; frame < maxPlanFrames; frame++ {
		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			c.markConnFailed()
			return nil, transportErrBadConn(err)
		}
		execResp := resp.GetExecute()
		if execResp == nil && resp.GetPull() != nil {
			execResp = resp.GetPull().GetResponse()
		}
		if execResp == nil {
			c.markConnFailed()
			return nil, fmt.Errorf("expected Execute response, got %s", describeServerMessage(resp))
		}
		if execResp.GetError() != nil {
			return nil, &DriverError{
				Code:    execResp.GetError().GetCode(),
				Message: execResp.GetError().GetMessage(),
			}
		}

		done, err := c.absorbPlanFrame(ctx, execResp, kind, out, &pullID)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
	}
	c.markConnFailed()
	return nil, fmt.Errorf("geode: %s: server sent more than %d frames without completing the plan",
		kind.keyword(), maxPlanFrames)
}

// absorbPlanFrame folds one response frame into out and reports whether the
// plan is complete. Callers must hold c.mu.
func (c *Conn) absorbPlanFrame(
	ctx context.Context, execResp *proto.ExecutionResponse, kind planKind,
	out *planFrames, pullID *uint64,
) (bool, error) {
	switch {
	case execResp.GetExplain() != nil:
		out.explain = execResp.GetExplain()
		return true, nil
	case execResp.GetProfile() != nil:
		out.profile = execResp.GetProfile()
		return true, nil
	case execResp.GetSchema() != nil:
		for _, col := range execResp.GetSchema().GetColumns() {
			out.columns = append(out.columns, col.GetName())
		}
		return false, nil
	case execResp.GetPage() != nil:
		if err := appendPlanRows(out, execResp.GetPage()); err != nil {
			return false, err
		}
		if execResp.GetPage().GetFinal() {
			// Mirror releaseRows: a trailing metrics frame may already be in
			// the read buffer, and leaving it there desynchronises the next
			// statement on this connection.
			c.drainBufferedFrames()
			return true, nil
		}
		*pullID++
		pull := proto.NewPullMessage(&proto.PullRequest{
			RequestId: *pullID,
			PageSize:  uint32(c.cfg.PageSize), //nolint:gosec // G115: page size fits uint32
		})
		if err := c.transport.SendProto(ctx, pull); err != nil {
			c.markConnFailed()
			return false, transportErrBadConn(err)
		}
		return false, nil
	case execResp.GetMetrics() != nil, execResp.GetHeartbeat() != nil:
		return false, nil
	default:
		c.markConnFailed()
		return false, fmt.Errorf("geode: %s: unexpected empty execution response", kind.keyword())
	}
}

// appendPlanRows stringifies one data page into the fallback text rows.
func appendPlanRows(out *planFrames, page *proto.DataPage) error {
	for _, row := range page.GetRows() {
		if len(out.rows) >= maxPlanRows {
			return fmt.Errorf("geode: plan text exceeded %d rows", maxPlanRows)
		}
		vals := row.GetValues()
		cells := make([]string, 0, len(vals))
		for _, v := range vals {
			cells = append(cells, planCellString(v))
		}
		out.rows = append(out.rows, cells)
	}
	return nil
}

// planCellString renders one plan cell as the string the text-row forms carry.
// The engine emits only strings and integers in these columns.
func planCellString(v *proto.Value) string {
	switch x := convertProtoValue(v).(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// flattenPlanRows takes the first cell of each text row: the EXPLAIN text form
// has a single `plan` column.
func flattenPlanRows(rows [][]string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if len(r) == 0 {
			continue
		}
		out = append(out, r[0])
	}
	return out
}

// pairPlanRows takes the first two cells of each text row: the PROFILE text
// form has `metric` and `value` columns. A row with fewer cells is dropped
// rather than padded — a half-row is not a metric.
func pairPlanRows(rows [][]string) [][2]string {
	out := make([][2]string, 0, len(rows))
	for _, r := range rows {
		if len(r) < profileMetricColumns {
			continue
		}
		out = append(out, [2]string{r[0], r[1]})
	}
	return out
}

// Explain returns the plan for a statement without executing it.
//
// Arguments are bound as parameters, so the plan reflects the parameterised
// statement the application would actually run — not a copy with values
// pasted into the text. A retrieval path can therefore assert that its real
// seed query reaches an index (ExplainPlan.UsesIndex) rather than asserting
// about a hand-written approximation of it.
//
// The call is gated on the engine advertising `explain` in SHOW CAPABILITIES
// and FAILS CLOSED: an engine that does not advertise it, or that cannot be
// probed, returns an error matching ErrCapabilityUnsupported instead of an
// empty plan.
//
// One connection is taken from the pool for the call and returned afterwards.
func (p *Pool) Explain(ctx context.Context, query string, args ...any) (*ExplainPlan, error) {
	var plan *ExplainPlan
	err := p.withPlanConn(ctx, "Pool.Explain", func(c *Conn) error {
		var err error
		plan, err = c.Explain(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// Profile executes a statement and returns its execution profile.
//
// PROFILE RUNS the statement — it is not a dry run — and geode 0.8.15 answers
// with the profile alone, discarding the result rows. Gated on the engine
// advertising `profile`, fail-closed, exactly as Explain is.
func (p *Pool) Profile(ctx context.Context, query string, args ...any) (*ProfileReport, error) {
	var rep *ProfileReport
	err := p.withPlanConn(ctx, "Pool.Profile", func(c *Conn) error {
		var err error
		rep, err = c.Profile(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// withPlanConn checks out one pooled connection and runs fn against the driver
// connection behind it. Raw is the only way to reach that connection, and it
// is what makes the whole plan round trip — capability probe included — ride a
// single connection instead of racing across two.
func (p *Pool) withPlanConn(ctx context.Context, op string, fn func(*Conn) error) error {
	if p == nil || p.db == nil {
		return fmt.Errorf("geode: %s: %w", op, ErrPoolNotInitialized)
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("geode: %s: %w", op, err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			return fmt.Errorf("%w: pooled connection is %T, not *geode.Conn", ErrInvalidState, dc)
		}
		return fn(c)
	}); err != nil {
		return fmt.Errorf("geode: %s: %w", op, err)
	}
	return nil
}

// Explain returns the plan for a statement on this Session's pinned
// connection, without executing it. See (*Pool).Explain for the contract.
func (s *Session) Explain(ctx context.Context, query string, args ...any) (*ExplainPlan, error) {
	var plan *ExplainPlan
	err := s.withPlanConn("Explain", func(c *Conn) error {
		var err error
		plan, err = c.Explain(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// Profile executes a statement on this Session's pinned connection and returns
// its execution profile. See (*Pool).Profile for the contract.
func (s *Session) Profile(ctx context.Context, query string, args ...any) (*ProfileReport, error) {
	var rep *ProfileReport
	err := s.withPlanConn("Profile", func(c *Conn) error {
		var err error
		rep, err = c.Profile(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// withPlanConn runs fn against the Session's pinned driver connection, under
// the same lifecycle gate every other Session operation uses: a closed,
// retired or busy Session is refused, and a connection that failed during the
// call retires the Session rather than being handed back to the pool.
func (s *Session) withPlanConn(op string, fn func(*Conn) error) error {
	if err := s.acquire(op); err != nil {
		return err
	}
	err := s.conn.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			return fmt.Errorf("%w: pinned connection is %T, not *geode.Conn", ErrInvalidState, dc)
		}
		return fn(c)
	})
	s.observe(err)
	if err != nil {
		return fmt.Errorf("geode: Session.%s: %w", op, err)
	}
	return nil
}

// millisToNanos converts the text-row fallback's time_ms to nanoseconds,
// saturating instead of wrapping. A server row of 9223372036854775807 ms would
// otherwise produce a NEGATIVE duration, and "the query took minus four
// seconds" is the one answer that is certainly wrong.
func millisToNanos(ms int64) int64 {
	if ms > math.MaxInt64/nanosPerMilli {
		return math.MaxInt64
	}
	if ms < math.MinInt64/nanosPerMilli {
		return math.MinInt64
	}
	return ms * nanosPerMilli
}

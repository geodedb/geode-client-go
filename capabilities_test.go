package geode

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// capabilityGoldenPath is the committed snapshot of the pinned engine's
// `SHOW CAPABILITIES` output: one row per line, `feature\tsupported`, in the
// exact order the engine emits. TestIntegration_ServerInfo regenerates it
// from a live engine (GEODE_UPDATE_GOLDEN=1) and fails when it drifts, so the
// unit lane can pin the Cap* constants against real engine row names without
// needing Docker.
const capabilityGoldenPath = "testdata/capabilities_0.8.18.golden"

func TestCapabilities_Has(t *testing.T) {
	c := Capabilities{"vector_knn": true, "tenant_ddl": false}
	if !c.Has("vector_knn") {
		t.Error(`Has("vector_knn") = false, want true`)
	}
	if c.Has("tenant_ddl") {
		t.Error(`Has("tenant_ddl") = true, want false`)
	}
	if c.Has("does_not_exist") {
		t.Error(`Has("does_not_exist") = true, want false (unknown features are unsupported)`)
	}
	if Capabilities(nil).Has("vector_knn") {
		t.Error("nil Capabilities reported a feature as supported")
	}
}

func TestCapabilities_Names(t *testing.T) {
	c := Capabilities{"b": true, "a": false}
	got := c.Names()
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("Names() = %v, want [a b]", got)
	}
}

// TestParseSupportedFlag pins the strict conversion the Sonny capability
// profile contract requires ("Pure validation": do not trim or coerce values).
// Only the exact strings "true" and "false" are booleans. Everything else —
// including "1", "TRUE" and " true" — is NOT a boolean, and the caller must
// route it to metadata or reject it rather than silently reading it as false.
func TestParseSupportedFlag(t *testing.T) {
	cases := []struct {
		in      string
		want    bool
		wantOK  bool
		comment string
	}{
		{"true", true, true, "canonical true"},
		{"false", false, true, "canonical false"},
		{"TRUE", false, false, "case variants are not booleans"},
		{"True", false, false, "case variants are not booleans"},
		{"1", false, false, "sonny_profile_version is 1 and must never coerce to true"},
		{"0", false, false, "numeric is not a boolean"},
		{"yes", false, false, "yes is not a boolean"},
		{"no", false, false, "no is not a boolean"},
		{"on", false, false, "on is not a boolean"},
		{" true", false, false, "values are not trimmed"},
		{"true ", false, false, "values are not trimmed"},
		{"", false, false, "empty is not a boolean"},
	}
	for _, c := range cases {
		got, ok := parseSupportedFlag(c.in)
		if got != c.want || ok != c.wantOK {
			t.Errorf("parseSupportedFlag(%q) = (%v, %v), want (%v, %v) — %s",
				c.in, got, ok, c.want, c.wantOK, c.comment)
		}
	}
}

// TestParseCapabilityRows_Valid proves the happy path keeps raw rows in order,
// builds the boolean map only from exactly-true/false rows, and lifts the
// version-valued metadata row into Metadata instead of the boolean map.
func TestParseCapabilityRows_Valid(t *testing.T) {
	rows := []CapabilityRow{
		{Feature: CapVectorKNN, Supported: "true"},
		{Feature: CapTenantDDL, Supported: "false"},
		{Feature: CapSonnyProfileVersion, Supported: "1"},
	}
	caps, meta, err := parseCapabilityRows(rows)
	if err != nil {
		t.Fatalf("parseCapabilityRows: %v", err)
	}
	if !caps.Has(CapVectorKNN) {
		t.Errorf("Capabilities[%s] = false, want true", CapVectorKNN)
	}
	if caps.Has(CapTenantDDL) {
		t.Errorf("Capabilities[%s] = true, want false", CapTenantDDL)
	}
	if _, ok := caps[CapSonnyProfileVersion]; ok {
		t.Errorf("%s leaked into the boolean map; it is version-valued metadata", CapSonnyProfileVersion)
	}
	if meta[CapSonnyProfileVersion] != "1" {
		t.Errorf("Metadata[%s] = %q, want \"1\"", CapSonnyProfileVersion, meta[CapSonnyProfileVersion])
	}
	if len(meta) != 1 {
		t.Errorf("Metadata = %v, want exactly one entry", meta)
	}
}

// TestParseCapabilityRows_Rejects pins the fail-closed rules: duplicate or
// empty names are malformed, and a non-boolean value for a feature that is not
// on the known-metadata allow-list is an error rather than a silent false.
func TestParseCapabilityRows_Rejects(t *testing.T) {
	cases := []struct {
		name string
		rows []CapabilityRow
	}{
		{"duplicate feature", []CapabilityRow{
			{Feature: CapExplain, Supported: "true"},
			{Feature: CapExplain, Supported: "false"},
		}},
		{"empty feature name", []CapabilityRow{
			{Feature: "", Supported: "true"},
		}},
		{"unknown non-boolean value", []CapabilityRow{
			{Feature: "some_future_row", Supported: "2"},
		}},
		{"unknown empty value", []CapabilityRow{
			{Feature: "some_future_row", Supported: ""},
		}},
		{"coercible-looking value", []CapabilityRow{
			{Feature: CapExplain, Supported: "TRUE"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := parseCapabilityRows(c.rows); !errors.Is(err, ErrMalformedCapabilityRows) {
				t.Fatalf("parseCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
			}
		})
	}
}

// TestParseCapabilityRows_UnknownBooleanRowAllowed: the contract permits extra
// well-formed boolean discovery rows from a future engine. Only non-boolean
// values from unknown features fail closed.
func TestParseCapabilityRows_UnknownBooleanRowAllowed(t *testing.T) {
	caps, meta, err := parseCapabilityRows([]CapabilityRow{
		{Feature: "some_future_row", Supported: "true"},
	})
	if err != nil {
		t.Fatalf("parseCapabilityRows: %v", err)
	}
	if !caps.Has("some_future_row") {
		t.Error("well-formed unknown boolean row was dropped")
	}
	if len(meta) != 0 {
		t.Errorf("Metadata = %v, want empty", meta)
	}
}

// TestCapabilityConstantsAreUnique guards against two constants sharing a row
// name, which would make a capability gate silently track the wrong feature.
func TestCapabilityConstantsAreUnique(t *testing.T) {
	seen := make(map[string]bool, len(allCapabilityNames))
	for _, name := range allCapabilityNames {
		if seen[name] {
			t.Errorf("capability name %q is declared by more than one constant", name)
		}
		seen[name] = true
	}
}

// TestCapabilityConstantsMatchEngine pins every Cap* constant to a row name the
// pinned engine actually emits, and every engine row to a constant. Renaming a
// constant without renaming the engine row silently disables a capability gate;
// an engine row with no constant is a capability Lane D cannot name.
func TestCapabilityConstantsMatchEngine(t *testing.T) {
	golden := readCapabilityGolden(t)

	if len(golden) != 59 {
		t.Errorf("golden has %d rows, want 59 (58 boolean rows + sonny_profile_version)", len(golden))
	}

	declared := make(map[string]bool, len(allCapabilityNames))
	for _, name := range allCapabilityNames {
		declared[name] = true
	}

	for _, row := range golden {
		if !declared[row.Feature] {
			t.Errorf("engine row %q has no Cap* constant", row.Feature)
		}
	}
	inGolden := make(map[string]bool, len(golden))
	for _, row := range golden {
		inGolden[row.Feature] = true
	}
	for _, name := range allCapabilityNames {
		if !inGolden[name] {
			t.Errorf("constant names %q, which the pinned engine does not emit", name)
		}
	}
}

// TestCapabilityGoldenParses proves the committed engine snapshot satisfies the
// strict parser: 58 booleans plus exactly one metadata row whose value is "1".
func TestCapabilityGoldenParses(t *testing.T) {
	golden := readCapabilityGolden(t)

	caps, meta, err := parseCapabilityRows(golden)
	if err != nil {
		t.Fatalf("parseCapabilityRows(golden): %v", err)
	}
	if len(caps) != 58 {
		t.Errorf("boolean capabilities = %d, want 58", len(caps))
	}
	if len(meta) != 1 || meta[CapSonnyProfileVersion] != "1" {
		t.Errorf("Metadata = %v, want {%s: 1}", meta, CapSonnyProfileVersion)
	}

	// The nine capabilities the Sonny profile requires of an existing engine
	// must be advertised true by the pinned engine.
	for _, feat := range []string{
		CapExplain, CapProfile, CapExplainPolicy, CapReadOnlyExecute,
		CapFTSSearch, CapFTSRowVisibility, CapFTSRebuild,
		CapVectorKNN, CapKNNMetricValidation,
	} {
		if !caps.Has(feat) {
			t.Errorf("required capability %q is not true in the pinned engine golden", feat)
		}
	}
}

// TestShowStatementsClassifyReadOnly proves Connector.ServerInfo works on a
// read-only connection: both statements it issues must pass the client-side
// read-only classifier, or a read-only Config could not discover capabilities.
func TestShowStatementsClassifyReadOnly(t *testing.T) {
	for _, stmt := range []string{showVersionStatement, showCapabilitiesStatement} {
		if IsMutatingStatement(stmt) {
			t.Errorf("IsMutatingStatement(%q) = true, want false", stmt)
		}
	}
}

// readCapabilityGolden parses the committed engine snapshot.
func readCapabilityGolden(t *testing.T) []CapabilityRow {
	t.Helper()

	data, err := os.ReadFile(capabilityGoldenPath)
	if err != nil {
		t.Fatalf("read %s: %v", capabilityGoldenPath, err)
	}
	var rows []CapabilityRow
	for i, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			t.Fatalf("%s:%d: %q is not feature<TAB>supported", capabilityGoldenPath, i+1, line)
		}
		rows = append(rows, CapabilityRow{Feature: parts[0], Supported: parts[1]})
	}
	return rows
}

// TestPooledHelloStateIsNeverAliased pins the copy semantics of the HELLO
// metadata a connection reports.
//
// The pooled connection records the handshake it last completed
// (markAuthedHello) and a Conn records its own (hello); both hand the
// capability slice out to callers, and neither may hand out the slice it holds.
// The strings come from a decoded protobuf message whose lifetime ends with the
// handshake, so an aliased slice is both a lifetime bug and a way for one
// caller to mutate what another sees.
//
// Note what is NOT asserted here any more: a request stream does not inherit
// another stream's HELLO. Since GAP-SONNY806-08 every stream performs its own
// handshake, because HELLO carries the tenant and graph binding and the engine
// applies both per stream.
func TestPooledHelloStateIsNeverAliased(t *testing.T) {
	pc := &pooledConn{}
	pc.markAuthedHello("g1", "sess-1", []string{"tx", "vectors"})

	if !pc.isAuthed() {
		t.Error("pooled connection is not authed after markAuthedHello")
	}
	if !pc.isGraphBound() {
		t.Error("pooled connection is not graph-bound after a non-empty HELLO graph")
	}

	// A Conn records the handshake IT performed; here it is populated the way
	// hello() populates it, from a copy of the recorded state.
	later := &Conn{}
	later.boundGraph, later.sessionID, later.helloCaps = pc.helloState()

	if later.BoundGraph() != "g1" {
		t.Errorf("BoundGraph() = %q, want %q", later.BoundGraph(), "g1")
	}
	if later.SessionID() != "sess-1" {
		t.Errorf("SessionID() = %q, want %q", later.SessionID(), "sess-1")
	}
	if got := later.HelloCapabilities(); !reflect.DeepEqual(got, []string{"tx", "vectors"}) {
		t.Errorf("HelloCapabilities() = %v, want [tx vectors]", got)
	}

	// The accessor hands out a copy: mutating it must not reach the pool.
	got := later.HelloCapabilities()
	got[0] = "tampered"
	if again := later.HelloCapabilities(); again[0] != "tx" {
		t.Errorf("HelloCapabilities() returned an aliased slice: %v", again)
	}
	if _, _, caps := pc.helloState(); caps[0] != "tx" {
		t.Errorf("pooledConn capabilities were aliased into a Conn: %v", caps)
	}
}

// ---------------------------------------------------------------------------
// scanVersionRow / scanCapabilityRows
//
// These exercise the scan-and-validate loop that showVersion/showCapabilities
// delegate to, via a fake driver.Rows. A compliant live engine never sends the
// malformed shapes below, so this is the only way to cover the wrong column
// count/order, non-string column value, maxCapabilityRows bound and bool
// wire-value rejection without a live server.
// ---------------------------------------------------------------------------

// fakeCapabilityRows is a minimal driver.Rows for driving
// scanVersionRow/scanCapabilityRows without a live engine or transport.
type fakeCapabilityRows struct {
	cols   []string
	data   [][]driver.Value
	idx    int
	closed bool
}

func (r *fakeCapabilityRows) Columns() []string { return r.cols }

func (r *fakeCapabilityRows) Close() error {
	r.closed = true
	return nil
}

func (r *fakeCapabilityRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.idx])
	r.idx++
	return nil
}

// TestScanVersionRow_HappyPath proves the single-column, single-row SHOW
// VERSION result is read verbatim, with no trimming or normalisation.
func TestScanVersionRow_HappyPath(t *testing.T) {
	rows := &fakeCapabilityRows{
		cols: []string{"version"},
		data: [][]driver.Value{{"v0.8.15"}},
	}
	got, err := scanVersionRow(rows)
	if err != nil {
		t.Fatalf("scanVersionRow: %v", err)
	}
	if got != "v0.8.15" {
		t.Errorf("scanVersionRow = %q, want %q", got, "v0.8.15")
	}
}

// TestScanVersionRow_Empty proves an empty SHOW VERSION result is a
// dedicated error rather than a zero-value success.
func TestScanVersionRow_Empty(t *testing.T) {
	rows := &fakeCapabilityRows{cols: []string{"version"}}
	_, err := scanVersionRow(rows)
	if err == nil {
		t.Fatal("scanVersionRow: got nil error, want an error for zero rows")
	}
	if !strings.Contains(err.Error(), "returned no rows") {
		t.Errorf("scanVersionRow error = %q, want substring %q", err.Error(), "returned no rows")
	}
}

// TestScanVersionRow_TwoRows characterizes existing behaviour: only the first
// row is read and returned; scanVersionRow does not currently reject a server
// that answers SHOW VERSION with more than one row.
func TestScanVersionRow_TwoRows(t *testing.T) {
	rows := &fakeCapabilityRows{
		cols: []string{"version"},
		data: [][]driver.Value{{"v0.8.15"}, {"v0.8.16"}},
	}
	got, err := scanVersionRow(rows)
	if err != nil {
		t.Fatalf("scanVersionRow: %v", err)
	}
	if got != "v0.8.15" {
		t.Errorf("scanVersionRow = %q, want the first row's value %q", got, "v0.8.15")
	}
	if rows.idx != 1 {
		t.Errorf("scanVersionRow consumed %d rows, want exactly 1", rows.idx)
	}
}

// TestScanVersionRow_NonStringValue proves a non-string (e.g. bool) version
// column is rejected rather than silently stringified.
func TestScanVersionRow_NonStringValue(t *testing.T) {
	rows := &fakeCapabilityRows{
		cols: []string{"version"},
		data: [][]driver.Value{{true}},
	}
	_, err := scanVersionRow(rows)
	if err == nil {
		t.Fatal("scanVersionRow: got nil error, want an error for a bool version column")
	}
	if !strings.Contains(err.Error(), "version column is bool, want a string") {
		t.Errorf("scanVersionRow error = %q, want substring %q", err.Error(), "version column is bool, want a string")
	}
}

// TestScanVersionRow_WrongColumns proves an unexpected column set is rejected.
func TestScanVersionRow_WrongColumns(t *testing.T) {
	rows := &fakeCapabilityRows{cols: []string{"ver"}}
	_, err := scanVersionRow(rows)
	if err == nil {
		t.Fatal("scanVersionRow: got nil error, want an error for wrong columns")
	}
	if !strings.Contains(err.Error(), "unexpected columns") {
		t.Errorf("scanVersionRow error = %q, want substring %q", err.Error(), "unexpected columns")
	}
}

// TestScanCapabilityRows_HappyPath proves well-formed rows are read verbatim
// and in order.
func TestScanCapabilityRows_HappyPath(t *testing.T) {
	rows := &fakeCapabilityRows{
		cols: []string{"feature", "supported"},
		data: [][]driver.Value{
			{CapVectorKNN, "true"},
			{CapTenantDDL, "false"},
			{CapSonnyProfileVersion, "1"},
		},
	}
	got, err := scanCapabilityRows(rows)
	if err != nil {
		t.Fatalf("scanCapabilityRows: %v", err)
	}
	want := []CapabilityRow{
		{Feature: CapVectorKNN, Supported: "true"},
		{Feature: CapTenantDDL, Supported: "false"},
		{Feature: CapSonnyProfileVersion, Supported: "1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("scanCapabilityRows = %+v, want %+v", got, want)
	}
}

// TestScanCapabilityRows_WrongColumnCount proves a SHOW CAPABILITIES result
// with an extra column is rejected rather than silently read positionally.
func TestScanCapabilityRows_WrongColumnCount(t *testing.T) {
	rows := &fakeCapabilityRows{cols: []string{"feature", "supported", "extra"}}
	_, err := scanCapabilityRows(rows)
	if !errors.Is(err, ErrMalformedCapabilityRows) {
		t.Fatalf("scanCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
	}
	if !strings.Contains(err.Error(), "columns are") {
		t.Errorf("scanCapabilityRows error = %q, want substring %q", err.Error(), "columns are")
	}
}

// TestScanCapabilityRows_SwappedColumnOrder proves the columns must be named
// feature, supported IN THAT ORDER, not merely contain those two names.
func TestScanCapabilityRows_SwappedColumnOrder(t *testing.T) {
	rows := &fakeCapabilityRows{cols: []string{"supported", "feature"}}
	_, err := scanCapabilityRows(rows)
	if !errors.Is(err, ErrMalformedCapabilityRows) {
		t.Fatalf("scanCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
	}
	if !strings.Contains(err.Error(), "columns are") {
		t.Errorf("scanCapabilityRows error = %q, want substring %q", err.Error(), "columns are")
	}
}

// TestScanCapabilityRows_NonStringValue proves a non-string wire value (e.g. a
// Go bool) is refused for either column rather than accepted "for forward
// compatibility" — the contract fixes both columns as strings.
func TestScanCapabilityRows_NonStringValue(t *testing.T) {
	t.Run("supported column is bool", func(t *testing.T) {
		rows := &fakeCapabilityRows{
			cols: []string{"feature", "supported"},
			data: [][]driver.Value{{CapVectorKNN, true}},
		}
		_, err := scanCapabilityRows(rows)
		if !errors.Is(err, ErrMalformedCapabilityRows) {
			t.Fatalf("scanCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
		}
		if !strings.Contains(err.Error(), "supported column of row") || !strings.Contains(err.Error(), "bool") {
			t.Errorf("scanCapabilityRows error = %q, want substrings %q and %q",
				err.Error(), "supported column of row", "bool")
		}
	})
	t.Run("feature column is bool", func(t *testing.T) {
		rows := &fakeCapabilityRows{
			cols: []string{"feature", "supported"},
			data: [][]driver.Value{{true, "true"}},
		}
		_, err := scanCapabilityRows(rows)
		if !errors.Is(err, ErrMalformedCapabilityRows) {
			t.Fatalf("scanCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
		}
		if !strings.Contains(err.Error(), "feature column of row") || !strings.Contains(err.Error(), "bool") {
			t.Errorf("scanCapabilityRows error = %q, want substrings %q and %q",
				err.Error(), "feature column of row", "bool")
		}
	})
}

// TestScanCapabilityRows_TooManyRows proves the maxCapabilityRows bound is
// enforced against a hostile or broken server, independent of parseCapabilityRows'
// row-level checks.
func TestScanCapabilityRows_TooManyRows(t *testing.T) {
	data := make([][]driver.Value, maxCapabilityRows+1)
	for i := range data {
		data[i] = []driver.Value{CapVectorKNN, "true"}
	}
	rows := &fakeCapabilityRows{
		cols: []string{"feature", "supported"},
		data: data,
	}
	_, err := scanCapabilityRows(rows)
	if !errors.Is(err, ErrMalformedCapabilityRows) {
		t.Fatalf("scanCapabilityRows err = %v, want ErrMalformedCapabilityRows", err)
	}
	wantSubstr := fmt.Sprintf("more than %d rows", maxCapabilityRows)
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("scanCapabilityRows error = %q, want substring %q", err.Error(), wantSubstr)
	}
}

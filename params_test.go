package geode

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

func TestBindParameters_RewritesPositionalPlaceholders(t *testing.T) {
	q, params, err := bindParameters(
		"MERGE (n:X {a: ?}) SET n.b = ?",
		[]driver.NamedValue{
			{Ordinal: 1, Value: "hello"},
			{Ordinal: 2, Value: int64(7)},
		},
	)
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	want := "MERGE (n:X {a: $_gq1}) SET n.b = $_gq2"
	if q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
	if len(params) != 2 {
		t.Fatalf("len(params) = %d, want 2", len(params))
	}
	if params[0].GetName() != "_gq1" || params[0].GetValue().GetStringVal().GetValue() != "hello" {
		t.Errorf("params[0] = %v, want name=_gq1 value=hello", params[0])
	}
	if params[1].GetName() != "_gq2" || params[1].GetValue().GetIntVal().GetValue() != 7 {
		t.Errorf("params[1] = %v, want name=_gq2 value=7", params[1])
	}
}

func TestBindParameters_NeverEmitsValueIntoQueryText(t *testing.T) {
	const hostile = "' OR 1=1 -- \x00\n"
	q, _, err := bindParameters("MATCH (n:X {k: ?}) RETURN n", []driver.NamedValue{
		{Ordinal: 1, Value: hostile},
	})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if strings.ContainsAny(q, "'\x00") || strings.Contains(q, "OR 1=1") {
		t.Fatalf("query text contains argument data: %q", q)
	}
}

func TestBindParameters_NamedArgsPassThrough(t *testing.T) {
	q, params, err := bindParameters("MATCH (n:X {k: $tenant}) RETURN n", []driver.NamedValue{
		{Ordinal: 1, Name: "tenant", Value: "t-1"},
	})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if q != "MATCH (n:X {k: $tenant}) RETURN n" {
		t.Errorf("query = %q, want unchanged", q)
	}
	if len(params) != 1 || params[0].GetName() != "tenant" {
		t.Fatalf("params = %v, want one param named tenant", params)
	}
}

func TestBindParameters_CountMismatch(t *testing.T) {
	_, _, err := bindParameters("MATCH (n:X {a: ?, b: ?}) RETURN n", []driver.NamedValue{
		{Ordinal: 1, Value: "only-one"},
	})
	if err == nil {
		t.Fatal("bindParameters: err = nil, want ErrParamCount")
	}
	if !strings.Contains(err.Error(), "placeholder count") {
		t.Errorf("err = %v, want a placeholder-count error", err)
	}
}

func TestBindParameters_IgnoresPlaceholdersInsideStringLiterals(t *testing.T) {
	// A '?' inside a GQL single-quoted literal in the *template* is data,
	// not a placeholder.
	q, params, err := bindParameters("MATCH (n:X) WHERE n.q = 'why?' AND n.k = ? RETURN n",
		[]driver.NamedValue{{Ordinal: 1, Value: "k"}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if q != "MATCH (n:X) WHERE n.q = 'why?' AND n.k = $_gq1 RETURN n" {
		t.Errorf("query = %q", q)
	}
	if len(params) != 1 {
		t.Fatalf("len(params) = %d, want 1", len(params))
	}
}

func TestBindParameters_RejectsReservedNamedArg(t *testing.T) {
	// A caller name in the driver's generated namespace could shadow a
	// positional argument, so the binder rejects it as well as
	// Conn.CheckNamedValue.
	_, _, err := bindParameters("MATCH (n:X {k: $_gq1}) RETURN n", []driver.NamedValue{
		{Ordinal: 1, Name: "_gq1", Value: "shadow"},
	})
	if !errors.Is(err, ErrReservedParamName) {
		t.Fatalf("err = %v, want ErrReservedParamName", err)
	}
}

func TestBindParameters_RejectsUnsupportedArgType(t *testing.T) {
	// Unsupported types must be a loud error, never a silent fmt
	// stringification into a wire value.
	_, _, err := bindParameters("MATCH (n:X {k: ?}) RETURN n", []driver.NamedValue{
		{Ordinal: 1, Value: struct{ A int }{A: 1}},
	})
	if err == nil {
		t.Fatal("bindParameters: err = nil, want unsupported-type error")
	}
	if !strings.Contains(err.Error(), "unsupported parameter type") {
		t.Errorf("err = %v, want an unsupported-parameter-type error", err)
	}
}

func TestBindParameters_EncodesEveryDriverValueKind(t *testing.T) {
	q, params, err := bindParameters("CREATE (n:X {s: ?, b: ?, i: ?, f: ?, y: ?, z: ?})",
		[]driver.NamedValue{
			{Ordinal: 1, Value: "s"},
			{Ordinal: 2, Value: true},
			{Ordinal: 3, Value: int64(-9)},
			{Ordinal: 4, Value: 2.25},
			{Ordinal: 5, Value: []byte{0x00, 0xff}},
			{Ordinal: 6, Value: nil},
		})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if q != "CREATE (n:X {s: $_gq1, b: $_gq2, i: $_gq3, f: $_gq4, y: $_gq5, z: $_gq6})" {
		t.Errorf("query = %q", q)
	}
	if params[1].GetValue().GetBoolVal() != true {
		t.Errorf("params[1] = %v, want bool true", params[1])
	}
	if params[2].GetValue().GetIntVal().GetValue() != -9 {
		t.Errorf("params[2] = %v, want int -9", params[2])
	}
	if params[3].GetValue().GetDoubleVal().GetValue() != 2.25 {
		t.Errorf("params[3] = %v, want double 2.25", params[3])
	}
	if string(params[4].GetValue().GetBytesVal().GetValue()) != "\x00\xff" {
		t.Errorf("params[4] = %v, want the raw bytes", params[4])
	}
	if params[5].GetValue().GetNullVal() == nil {
		t.Errorf("params[5] = %v, want null_val", params[5])
	}
}

func TestCountPlaceholders_SkipsStringLiterals(t *testing.T) {
	// Stmt.NumInput must agree with what bindParameters will replace.
	got := countPlaceholders("MATCH (n:X) WHERE n.q = 'why?' AND n.k = ? AND n.j = ? RETURN n")
	if got != 2 {
		t.Errorf("countPlaceholders = %d, want 2", got)
	}
}

func TestCheckNamedValue_RejectsGeneratedPrefix(t *testing.T) {
	c := &Conn{}
	for _, name := range []string{"_gq1", "_gq", "_gqtenant"} {
		nv := driver.NamedValue{Ordinal: 1, Name: name, Value: "x"}
		if err := c.CheckNamedValue(&nv); !errors.Is(err, ErrReservedParamName) {
			t.Errorf("CheckNamedValue(%q) = %v, want ErrReservedParamName", name, err)
		}
	}
}

func TestCheckNamedValue_TypePolicy(t *testing.T) {
	c := &Conn{}
	passthrough := []any{nil, "s", []byte("b"), true, int64(1), 1.5}
	for _, v := range passthrough {
		nv := driver.NamedValue{Ordinal: 1, Value: v}
		if err := c.CheckNamedValue(&nv); err != nil {
			t.Errorf("CheckNamedValue(%T) = %v, want nil", v, err)
		}
	}
	// Everything else defers to database/sql's default converter, which
	// narrows what it can (int -> int64) and rejects what it cannot.
	deferred := []any{1, float32(1.5), struct{}{}}
	for _, v := range deferred {
		nv := driver.NamedValue{Ordinal: 1, Value: v}
		if err := c.CheckNamedValue(&nv); !errors.Is(err, driver.ErrSkip) {
			t.Errorf("CheckNamedValue(%T) = %v, want driver.ErrSkip", v, err)
		}
	}
}

// TestBindParameters_ListParamStaysStructural guards the ENG-3910 fix: a
// list-valued parameter must reach the wire as list_val, not as a fmt-
// stringified scalar (which made `x IN $list` match nothing and `NOT IN`
// fail open). Rejecting containers outright would regress the same feature.
func TestBindParameters_ListParamStaysStructural(t *testing.T) {
	_, params, err := bindParameters("MATCH (n:X) WHERE n.f IN $files RETURN n",
		[]driver.NamedValue{{Ordinal: 1, Name: "files", Value: []string{"a.md", "b.md"}}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if len(params) != 1 {
		t.Fatalf("len(params) = %d, want 1", len(params))
	}
	vals := listValues(t, params[0])
	if len(vals) != 2 {
		t.Fatalf("len(list) = %d, want 2", len(vals))
	}
	wantStringValue(t, vals[0], "a.md")
	wantStringValue(t, vals[1], "b.md")
}

func TestBindParameters_MapParamStaysStructural(t *testing.T) {
	_, params, err := bindParameters("CREATE (n:X $props)",
		[]driver.NamedValue{{Ordinal: 1, Name: "props", Value: map[string]any{"k": "v"}}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	mv, ok := params[0].GetValue().GetKind().(*proto.Value_MapVal)
	if !ok {
		t.Fatalf("expected map_val, got %T", params[0].GetValue().GetKind())
	}
	if len(mv.MapVal.GetEntries()) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(mv.MapVal.GetEntries()))
	}
}

func TestCheckNamedValue_AllowsContainers(t *testing.T) {
	c := &Conn{}
	for _, v := range []any{[]string{"a"}, []int{1}, map[string]any{"k": 1}} {
		nv := driver.NamedValue{Ordinal: 1, Name: "p", Value: v}
		if err := c.CheckNamedValue(&nv); err != nil {
			t.Errorf("CheckNamedValue(%T) = %v, want nil", v, err)
		}
	}
}

// TestBindParameters_IntKindIsEngineCompatible pins the int_val kind the
// engine can actually store as a numeric property.
//
// engine 0.8.15 src/server/proto_util.zig:58-63 maps IntKind BIGINT to the
// distinct value_types .BigInt variant, which graph_store.zig:1438 persists as
// an ExtValue rather than a plain Int property: `WHERE n.c = 5` then matches
// nothing and `sum(n.v)` returns NULL (seen as TestLoad_LargeDataset failures).
// IntKind INT maps to .Int, which is what proto.MakeParam has always sent.
func TestBindParameters_IntKindIsEngineCompatible(t *testing.T) {
	_, params, err := bindParameters("CREATE (n:X {i: ?})",
		[]driver.NamedValue{{Ordinal: 1, Value: int64(5)}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if got := params[0].GetValue().GetIntVal().GetKind(); got != proto.IntKind_INT {
		t.Errorf("int kind = %v, want %v", got, proto.IntKind_INT)
	}
}

// TestBindParameters_TimeValuePositional pins the wire encoding for a
// positional time.Time argument (I1): it must be the historical
// fmt.Sprintf("%v", t) string — the same bytes the old inliner's default
// case and proto.MakeParam's containerValue fallback have always produced —
// and it must round-trip through ScanTime (time.go), which previously had no
// timeFormats layout that could parse the zone-name suffix fmt.Sprintf always
// appends.
func TestBindParameters_TimeValuePositional(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	arg := time.Date(2024, 3, 15, 10, 30, 45, 123456789, loc)
	want := fmt.Sprintf("%v", arg)

	_, params, err := bindParameters("CREATE (n:X {t: ?})",
		[]driver.NamedValue{{Ordinal: 1, Value: arg}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if len(params) != 1 {
		t.Fatalf("len(params) = %d, want 1", len(params))
	}
	wantStringValue(t, params[0].GetValue(), want)

	var dest time.Time
	if err := ScanTime(&dest).Scan(want); err != nil {
		t.Fatalf("ScanTime.Scan(%q): %v", want, err)
	}
	if !dest.Equal(arg) {
		t.Errorf("ScanTime round-trip = %v, want %v", dest, arg)
	}
}

// TestBindParameters_TimeValueNamed is the named-argument counterpart of
// TestBindParameters_TimeValuePositional: named args are encoded by the same
// encodeParamValue case, but through a different call site in
// bindParameters, so both paths are pinned independently.
func TestBindParameters_TimeValueNamed(t *testing.T) {
	arg := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	want := fmt.Sprintf("%v", arg)

	_, params, err := bindParameters("MATCH (n:X {t: $ts}) RETURN n",
		[]driver.NamedValue{{Ordinal: 1, Name: "ts", Value: arg}})
	if err != nil {
		t.Fatalf("bindParameters: %v", err)
	}
	if len(params) != 1 || params[0].GetName() != "ts" {
		t.Fatalf("params = %v, want one param named ts", params)
	}
	wantStringValue(t, params[0].GetValue(), want)

	var dest time.Time
	if err := ScanTime(&dest).Scan(want); err != nil {
		t.Fatalf("ScanTime.Scan(%q): %v", want, err)
	}
	if !dest.Equal(arg) {
		t.Errorf("ScanTime round-trip = %v, want %v", dest, arg)
	}
}

// TestCheckNamedValue_AllowsTime pins CheckNamedValue's passthrough for
// time.Time (I1): database/sql's default converter treats time.Time as
// already a valid driver.Value and passes it through unmodified, so without
// this case it would reach encodeParamValue at all — CheckNamedValue never
// gets the chance to defer via driver.ErrSkip.
func TestCheckNamedValue_AllowsTime(t *testing.T) {
	c := &Conn{}
	nv := driver.NamedValue{Ordinal: 1, Value: time.Now()}
	if err := c.CheckNamedValue(&nv); err != nil {
		t.Errorf("CheckNamedValue(time.Time) = %v, want nil", err)
	}
}

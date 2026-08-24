package geode

import (
	"reflect"
	"testing"

	"geodedb.com/geode/proto"
)

// ENG-3910: list- and map-valued parameters were stringified with fmt "%v"
// instead of being encoded as list_val/map_val, so `WHERE x IN $list` matched
// nothing and `NOT IN` failed open. These tests pin the structural encoding.

// listValues returns the list_val elements of a param, failing the test if the
// param is not a list_val.
func listValues(t *testing.T, p *proto.Param) []*proto.Value {
	t.Helper()
	kind := p.GetValue().GetKind()
	lv, ok := kind.(*proto.Value_ListVal)
	if !ok {
		t.Fatalf("expected list_val, got %T (value %v)", kind, p.GetValue())
	}
	return lv.ListVal.GetValues()
}

// wantStringValue asserts that v is a string_val holding want.
func wantStringValue(t *testing.T, v *proto.Value, want string) {
	t.Helper()
	sv, ok := v.GetKind().(*proto.Value_StringVal)
	if !ok {
		t.Fatalf("expected string_val, got %T", v.GetKind())
	}
	if sv.StringVal.GetValue() != want {
		t.Errorf("expected string_val %q, got %q", want, sv.StringVal.GetValue())
	}
}

// wantIntValue asserts that v is an int_val holding want.
func wantIntValue(t *testing.T, v *proto.Value, want int64) {
	t.Helper()
	iv, ok := v.GetKind().(*proto.Value_IntVal)
	if !ok {
		t.Fatalf("expected int_val, got %T", v.GetKind())
	}
	if iv.IntVal.GetValue() != want {
		t.Errorf("expected int_val %d, got %d", want, iv.IntVal.GetValue())
	}
}

func TestMakeParam_StringSliceIsListValue(t *testing.T) {
	p := proto.MakeParam("files", []string{"a.md", "b.md"})

	if p.GetName() != "files" {
		t.Errorf("expected name=files, got %q", p.GetName())
	}
	// Guard against the ENG-3910 regression: a stringified Go slice.
	if sv, ok := p.GetValue().GetKind().(*proto.Value_StringVal); ok {
		t.Fatalf("[]string was stringified as string_val %q; expected list_val", sv.StringVal.GetValue())
	}

	vals := listValues(t, p)
	if len(vals) != 2 {
		t.Fatalf("expected 2 list elements, got %d", len(vals))
	}
	wantStringValue(t, vals[0], "a.md")
	wantStringValue(t, vals[1], "b.md")
}

func TestMakeParam_IntSliceIsListValue(t *testing.T) {
	p := proto.MakeParam("ids", []int{1, 2, 3})

	vals := listValues(t, p)
	if len(vals) != 3 {
		t.Fatalf("expected 3 list elements, got %d", len(vals))
	}
	for i, want := range []int64{1, 2, 3} {
		wantIntValue(t, vals[i], want)
	}
}

func TestMakeParam_MixedAnySliceIsListValue(t *testing.T) {
	p := proto.MakeParam("mixed", []any{"a.md", 7, true, nil, 1.5})

	vals := listValues(t, p)
	if len(vals) != 5 {
		t.Fatalf("expected 5 list elements, got %d", len(vals))
	}
	wantStringValue(t, vals[0], "a.md")
	wantIntValue(t, vals[1], 7)
	if bv, ok := vals[2].GetKind().(*proto.Value_BoolVal); !ok || !bv.BoolVal {
		t.Errorf("expected bool_val true, got %T", vals[2].GetKind())
	}
	if _, ok := vals[3].GetKind().(*proto.Value_NullVal); !ok {
		t.Errorf("expected null_val, got %T", vals[3].GetKind())
	}
	dv, ok := vals[4].GetKind().(*proto.Value_DoubleVal)
	if !ok {
		t.Fatalf("expected double_val, got %T", vals[4].GetKind())
	}
	if dv.DoubleVal.GetValue() != 1.5 {
		t.Errorf("expected double_val 1.5, got %v", dv.DoubleVal.GetValue())
	}
}

func TestMakeParam_NestedListIsListValue(t *testing.T) {
	p := proto.MakeParam("nested", [][]string{{"a"}, {"b", "c"}})

	outer := listValues(t, p)
	if len(outer) != 2 {
		t.Fatalf("expected 2 outer elements, got %d", len(outer))
	}
	inner0, ok := outer[0].GetKind().(*proto.Value_ListVal)
	if !ok {
		t.Fatalf("expected nested list_val, got %T", outer[0].GetKind())
	}
	if len(inner0.ListVal.GetValues()) != 1 {
		t.Fatalf("expected 1 element in first nested list, got %d", len(inner0.ListVal.GetValues()))
	}
	wantStringValue(t, inner0.ListVal.GetValues()[0], "a")

	inner1, ok := outer[1].GetKind().(*proto.Value_ListVal)
	if !ok {
		t.Fatalf("expected nested list_val, got %T", outer[1].GetKind())
	}
	if len(inner1.ListVal.GetValues()) != 2 {
		t.Fatalf("expected 2 elements in second nested list, got %d", len(inner1.ListVal.GetValues()))
	}
	wantStringValue(t, inner1.ListVal.GetValues()[0], "b")
	wantStringValue(t, inner1.ListVal.GetValues()[1], "c")
}

func TestMakeParam_MapIsMapValue(t *testing.T) {
	p := proto.MakeParam("props", map[string]any{
		"name": "Alice",
		"age":  30,
		"tags": []string{"x", "y"},
	})

	mv, ok := p.GetValue().GetKind().(*proto.Value_MapVal)
	if !ok {
		t.Fatalf("expected map_val, got %T (value %v)", p.GetValue().GetKind(), p.GetValue())
	}
	entries := mv.MapVal.GetEntries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 map entries, got %d", len(entries))
	}

	byKey := make(map[string]*proto.Value, len(entries))
	for _, e := range entries {
		byKey[e.GetKey()] = e.GetValue()
	}
	wantStringValue(t, byKey["name"], "Alice")
	wantIntValue(t, byKey["age"], 30)
	tags, ok := byKey["tags"].GetKind().(*proto.Value_ListVal)
	if !ok {
		t.Fatalf("expected tags list_val, got %T", byKey["tags"].GetKind())
	}
	if len(tags.ListVal.GetValues()) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags.ListVal.GetValues()))
	}
}

func TestMakeParam_MapEntriesAreKeySorted(t *testing.T) {
	p := proto.MakeParam("props", map[string]int{"c": 3, "a": 1, "b": 2})

	mv, ok := p.GetValue().GetKind().(*proto.Value_MapVal)
	if !ok {
		t.Fatalf("expected map_val, got %T", p.GetValue().GetKind())
	}
	got := make([]string, 0, len(mv.MapVal.GetEntries()))
	for _, e := range mv.MapVal.GetEntries() {
		got = append(got, e.GetKey())
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected key order %v, got %v", want, got)
	}
}

func TestMakeParam_ScalarGuards(t *testing.T) {
	// A string must stay a scalar string_val, not become a list of runes/bytes.
	strParam := proto.MakeParam("s", "a.md")
	wantStringValue(t, strParam.GetValue(), "a.md")

	// []byte must stay bytes, not become a list of integers.
	bytesParam := proto.MakeParam("b", []byte{0x01, 0x02, 0xff})
	bv, ok := bytesParam.GetValue().GetKind().(*proto.Value_BytesVal)
	if !ok {
		t.Fatalf("expected bytes_val, got %T", bytesParam.GetValue().GetKind())
	}
	if !reflect.DeepEqual(bv.BytesVal.GetValue(), []byte{0x01, 0x02, 0xff}) {
		t.Errorf("expected bytes 01 02 ff, got %v", bv.BytesVal.GetValue())
	}

	// nil must stay null_val.
	nilParam := proto.MakeParam("n", nil)
	if _, ok := nilParam.GetValue().GetKind().(*proto.Value_NullVal); !ok {
		t.Errorf("expected null_val, got %T", nilParam.GetValue().GetKind())
	}
}

func TestMakeParam_EmptyListIsListValue(t *testing.T) {
	vals := listValues(t, proto.MakeParam("empty", []string{}))
	if len(vals) != 0 {
		t.Errorf("expected 0 elements, got %d", len(vals))
	}
}

// TestMakeParam_DepthLimit checks that over-nested containers terminate as
// null instead of recursing without bound (matches the server's 64-level cap
// and the Python client's PARAM_MAX_DEPTH).
func TestMakeParam_DepthLimit(t *testing.T) {
	const nesting = 70
	var val any = "leaf"
	for i := 0; i < nesting; i++ {
		val = []any{val}
	}

	v := proto.MakeParam("deep", val).GetValue()
	levels := 0
	for {
		lv, ok := v.GetKind().(*proto.Value_ListVal)
		if !ok {
			break
		}
		vals := lv.ListVal.GetValues()
		if len(vals) != 1 {
			t.Fatalf("expected 1 element at level %d, got %d", levels, len(vals))
		}
		levels++
		v = vals[0]
	}

	if levels != 64 {
		t.Errorf("expected encoding to stop after 64 list levels, got %d", levels)
	}
	if _, ok := v.GetKind().(*proto.Value_NullVal); !ok {
		t.Errorf("expected null_val past the depth limit, got %T", v.GetKind())
	}
}

// TestMakeParam_RoundTripsThroughDecode checks encode/decode symmetry with the
// decode path used for result values (convertProtoValue in rows.go).
func TestMakeParam_RoundTripsThroughDecode(t *testing.T) {
	p := proto.MakeParam("files", []any{"a.md", 2, map[string]any{"k": "v"}})

	decoded := convertProtoValue(p.GetValue())
	list, ok := decoded.([]any)
	if !ok {
		t.Fatalf("expected []any after decode, got %T (%v)", decoded, decoded)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 decoded elements, got %d", len(list))
	}
	if list[0] != "a.md" {
		t.Errorf("expected first=a.md, got %v", list[0])
	}
	if list[1] != int64(2) {
		t.Errorf("expected second=2, got %v", list[1])
	}
	m, ok := list[2].(map[string]any)
	if !ok {
		t.Fatalf("expected third to decode as map[string]any, got %T", list[2])
	}
	if m["k"] != "v" {
		t.Errorf("expected k=v, got %v", m["k"])
	}
}

package geode

import (
	"database/sql/driver"
	"errors"
	"math"
	"testing"
)

func TestVector_EncodesAsListOfDoubles(t *testing.T) {
	val, err := encodeParamValue(Vector{0.5, -1.25, 0})
	if err != nil {
		t.Fatalf("encodeParamValue: %v", err)
	}
	list := val.GetListVal()
	if list == nil {
		t.Fatalf("value kind = %T, want ListValue", val.GetKind())
	}
	got := list.GetValues()
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	want := []float64{0.5, -1.25, 0}
	for i, w := range want {
		d := got[i].GetDoubleVal()
		if d == nil {
			t.Fatalf("element %d kind = %T, want DoubleValue", i, got[i].GetKind())
		}
		if d.GetValue() != w {
			t.Errorf("element %d = %v, want %v", i, d.GetValue(), w)
		}
	}
}

func TestVector_RejectsOversize(t *testing.T) {
	big := make(Vector, MaxVectorDimensions+1)
	_, err := encodeParamValue(big)
	if !errors.Is(err, ErrVectorTooLarge) {
		t.Fatalf("err = %v, want ErrVectorTooLarge", err)
	}
}

func TestVector_RejectsEmpty(t *testing.T) {
	_, err := encodeParamValue(Vector{})
	if err == nil {
		t.Fatal("err = nil, want an empty-vector error")
	}
}

func TestVector_RejectsNonFinite(t *testing.T) {
	inf := Vector{1, float32(mustInf())}
	if _, err := encodeParamValue(inf); err == nil {
		t.Fatal("err = nil, want a non-finite-component error")
	}
}

// TestVector_RejectsNaN pins the NaN case separately from Inf: NaN fails
// both IsNaN comparisons that a naive `c != c` finite check could get
// backwards, and it is a distinct bit pattern from Inf that a component-wise
// IsInf-only check would miss. Requested by the Task 4 review
// (task-4-report.md) and closed here in Task 5.
func TestVector_RejectsNaN(t *testing.T) {
	nan := Vector{1, float32(math.NaN())}
	_, err := encodeParamValue(nan)
	if !errors.Is(err, ErrVectorNotFinite) {
		t.Fatalf("err = %v, want ErrVectorNotFinite", err)
	}
}

// TestVector_AcceptsSubnormal pins that a subnormal float32 component (finite,
// just below the normal range) is accepted and round-trips exactly through
// the DoubleValue encoding rather than being flushed to zero or rejected as
// non-finite. Requested by the Task 4 review (task-4-report.md) and closed
// here in Task 5.
func TestVector_AcceptsSubnormal(t *testing.T) {
	sub := Vector{math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32}
	val, err := encodeParamValue(sub)
	if err != nil {
		t.Fatalf("encodeParamValue(subnormal): %v, want no error", err)
	}
	list := val.GetListVal()
	if list == nil {
		t.Fatalf("value kind = %T, want ListValue", val.GetKind())
	}
	got := list.GetValues()
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	want := []float32{math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32}
	for i, w := range want {
		d := got[i].GetDoubleVal()
		if d == nil {
			t.Fatalf("element %d kind = %T, want DoubleValue", i, got[i].GetKind())
		}
		if float32(d.GetValue()) != w {
			t.Errorf("element %d = %v, want %v", i, float32(d.GetValue()), w)
		}
	}
}

func TestVector_ImplementsValuer(_ *testing.T) {
	var _ driver.Valuer = Vector{1, 2, 3}
}

func mustInf() float64 { return 1e400 / 1e300 } // +Inf without importing math in the test

// TestVector_DimensionsReportsComponentCount pins the accessor Task 24
// (llmindex KNN) uses to check an embedding against an index's dimension
// before binding it.
func TestVector_DimensionsReportsComponentCount(t *testing.T) {
	if got := (Vector{1, 2, 3}).Dimensions(); got != 3 {
		t.Errorf("Dimensions() = %d, want 3", got)
	}
	if got := (Vector(nil)).Dimensions(); got != 0 {
		t.Errorf("Dimensions() on nil = %d, want 0", got)
	}
}

// TestVector_ValueRejectsInvalid proves driver.Valuer fails closed: a Vector
// that encodeParamValue would refuse is refused by Value() too, so a caller
// that goes through database/sql's default converter cannot smuggle an
// oversize or non-finite embedding onto the wire.
func TestVector_ValueRejectsInvalid(t *testing.T) {
	if _, err := (Vector{}).Value(); err == nil {
		t.Error("Value() on empty vector = nil error, want an error")
	}
	if _, err := (Vector{1, float32(mustInf())}).Value(); err == nil {
		t.Error("Value() on non-finite vector = nil error, want an error")
	}
	v := Vector{1, 2}
	got, err := v.Value()
	if err != nil {
		t.Fatalf("Value() on a valid vector: %v", err)
	}
	if _, ok := got.(Vector); !ok {
		t.Errorf("Value() = %T, want geode.Vector (type information was lost)", got)
	}
}

// TestVector_CheckNamedValuePassesThrough proves database/sql's default
// converter never sees a Vector: Conn.CheckNamedValue accepts it as-is, so
// the concrete type reaches bindParameters and encodeParamValue.
func TestVector_CheckNamedValuePassesThrough(t *testing.T) {
	c := &Conn{}
	nv := &driver.NamedValue{Ordinal: 1, Value: Vector{1, 2, 3}}
	if err := c.CheckNamedValue(nv); err != nil {
		t.Fatalf("CheckNamedValue: %v", err)
	}
	if _, ok := nv.Value.(Vector); !ok {
		t.Fatalf("value = %T, want geode.Vector", nv.Value)
	}
}

// TestVector_UntypedFloat32SliceStillEncodesAsPlainList pins the Task 3
// container behaviour: a plain []float32 is NOT a Vector and keeps its
// unbounded structural list encoding (ENG-3910), including the empty case
// that `x IN ?` depends on.
func TestVector_UntypedFloat32SliceStillEncodesAsPlainList(t *testing.T) {
	val, err := encodeParamValue([]float32{0.5, -1.25})
	if err != nil {
		t.Fatalf("encodeParamValue([]float32): %v", err)
	}
	list := val.GetListVal()
	if list == nil {
		t.Fatalf("value kind = %T, want ListValue", val.GetKind())
	}
	if n := len(list.GetValues()); n != 2 {
		t.Fatalf("len = %d, want 2", n)
	}
	if d := list.GetValues()[0].GetDoubleVal(); d == nil || d.GetValue() != 0.5 {
		t.Errorf("element 0 = %v, want DoubleValue 0.5", list.GetValues()[0].GetKind())
	}
	empty, err := encodeParamValue([]float32{})
	if err != nil {
		t.Fatalf("encodeParamValue(empty []float32): %v, want it to stay a plain empty list", err)
	}
	if empty.GetListVal() == nil {
		t.Errorf("empty []float32 kind = %T, want an empty ListValue", empty.GetKind())
	}
}

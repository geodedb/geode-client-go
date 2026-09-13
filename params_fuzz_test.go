package geode

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

// FuzzBindParameters asserts the load-bearing security property of the
// driver: no byte of any argument may appear in the query text handed to the
// server. Any counterexample is a query-injection defect.
//
// The real, exact form of that property is that the returned query text is a
// pure function of the TEMPLATE, never of the argument value:
// rewritePlaceholderRefs only ever rewrites the caller's own template text,
// so binding two different values against the same template must produce
// byte-identical query text. That is what this target checks
// (assertQueryTextArgIndependent), and it holds for every argument length,
// including the 1-byte payloads (', ", ?) that the previous containment-based
// check (`len(arg) >= 3 && strings.Contains(q, arg)`) could never see a leak
// in — a 1-byte argument is too short to rule out a coincidental substring
// match, so that check skipped it entirely, silently never exercising the
// deliberately-added `"?"` seed below.
//
// That containment check is not restored here even for len(arg) >= 3,
// because it turns out to be unsound rather than merely weak: once query
// text is proven argument-independent, any occurrence of arg inside q that
// is NOT also an occurrence inside tmpl can only be a coincidence of q's own
// fixed, template-derived structure — most concretely, a generated
// reference's digit butting up against the template's next literal
// characters (tmpl "?0000", arg "100" -> q "$_gq10000", which contains "100"
// though nothing leaked; found by 15s of fuzzing FuzzBindParametersArity
// during this fix, see task-5-report.md's Fix report). It can never be a
// genuine leak, because q provably does not depend on arg's bytes at all —
// so the check could only ever fire on noise, never on a real defect.
// assertQueryTextArgIndependent is both stronger (exact, for every length)
// and immune to that class of false positive, so it stands alone.
//
// It also asserts count consistency: bindParameters either rejects the
// (template, args) pair with ErrParamCount, or it returns a query where the
// argument's generated `$_gqN` reference appears and exactly one parameter
// carries the reserved-prefix name.
//
// The seed corpus includes the scanner's documented blind spots
// (params.go, rewritePlaceholderRefs doc comment): double-quoted strings,
// backslash-escaped quotes, and `//` / `/* */` comments are not tracked, so
// a '?' inside one of those is still treated as a live placeholder. See
// task-5-report.md for what each blind-spot seed actually does (false
// ErrParamCount vs. a $_gqN reference landing inside what was meant to be
// static template text) — this fuzz target's job is only to prove that,
// whichever happens, no argument byte ever reaches the query text.
func FuzzBindParameters(f *testing.F) {
	f.Add("MATCH (n:X {k: ?}) RETURN n", "plain")
	f.Add("MERGE (n:X {k: ?})", "' OR 1=1 --")
	f.Add("MATCH (n) WHERE n.a = 'lit?eral' AND n.b = ? RETURN n", "?")
	f.Add("RETURN ?", "")
	f.Add("MATCH (n:X {k: $named, j: ?}) RETURN n", "\x00\n\t'\\")

	// Blind-spot seeds (params.go's documented, not-fixed-here gaps).
	f.Add(`MATCH (n) WHERE n.a = "lit?eral" RETURN n`, "x")       // '?' inside "..."
	f.Add(`MATCH (n) WHERE n.a = 'it\'s ? here' RETURN n`, "x")   // backslash escape
	f.Add("MATCH (n) RETURN n // is this a ? \n", "x")            // line comment
	f.Add("MATCH (n) /* a ? in a block comment */ RETURN n", "x") // block comment

	f.Fuzz(func(t *testing.T, tmpl, arg string) {
		args := []driver.NamedValue{{Ordinal: 1, Value: arg}}
		q, params, err := bindParameters(tmpl, args)
		if err != nil {
			return // count mismatch / invalid name: rejected, which is correct
		}

		// Property 1 (query text is argument-independent): exact for every
		// argument length, including the 1-byte payloads a containment check
		// cannot safely cover. See the doc comment above for why a
		// containment-based check is not also kept here.
		assertQueryTextArgIndependent(t, tmpl, q, arg)

		// Property 2: count consistency. There is exactly one positional
		// argument here, so exactly one generated reference must appear,
		// exactly one parameter must carry it, and no unbound '?' may
		// survive outside a (single-quote) literal in the returned query.
		if strings.Contains(stripStringLiterals(q), "?") {
			t.Fatalf("unbound placeholder survived: %q", q)
		}
		if len(params) != 1 {
			t.Fatalf("len(params) = %d, want 1 (query %q)", len(params), q)
		}
		name := params[0].GetName()
		if !strings.HasPrefix(name, generatedParamPrefix) {
			t.Fatalf("param name %q does not carry reserved prefix %q", name, generatedParamPrefix)
		}
		wantRef := "$" + generatedParamPrefix + "1"
		if strings.Count(q, wantRef) < 1 {
			t.Fatalf("generated reference %q missing from query %q", wantRef, q)
		}
		if got := params[0].GetValue().GetStringVal().GetValue(); got != arg {
			t.Fatalf("param value = %q, want %q", got, arg)
		}
	})
}

// fuzzControlValue is a sentinel argument value fuzz targets bind in place
// of the real fuzzed value to prove the returned query text does not depend
// on argument bytes. It is deliberately unlikely to appear verbatim in any
// template or ever be generated by the fuzzer itself.
const fuzzControlValue = "\x00GQ_FUZZ_CONTROL_VALUE_ABSENT_FROM_ANY_TEMPLATE\x00"

// assertQueryTextArgIndependent re-binds tmpl with fuzzControlValue in place
// of arg and asserts the rewritten query text is byte-identical to q. Since
// rewritePlaceholderRefs only ever rewrites the caller's own template text
// (params.go), this must hold for every argument, of every length,
// regardless of what bytes it contains — it is the exact property that
// "no argument byte reaches query text" is really standing in for. When arg
// already equals the control value the check is a no-op for that input; the
// fuzzer explores plenty of other inputs where it is not.
func assertQueryTextArgIndependent(t *testing.T, tmpl, q, arg string) {
	t.Helper()
	if arg == fuzzControlValue {
		return
	}
	q2, _, err2 := bindParameters(tmpl, []driver.NamedValue{{Ordinal: 1, Value: fuzzControlValue}})
	if err2 != nil {
		t.Fatalf("control argument rejected where fuzz argument was accepted: %v (template %q)", err2, tmpl)
	}
	if q != q2 {
		t.Fatalf("query text depends on argument bytes\n  template: %q\n  arg %q -> %q\n  control  -> %q",
			tmpl, arg, q, q2)
	}
}

// stripStringLiterals removes single-quoted GQL literals so placeholder
// counting ignores '?' characters that are template data.
func stripStringLiterals(s string) string {
	var b strings.Builder
	in := false
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			if in && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			in = !in
			continue
		}
		if !in {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// namedArgKind classifies the named argument's own NAME (not its value)
// that a fuzz input selects for FuzzBindParametersArity, per Task 5 review
// correction #1: the original target hardcoded Name: "n1" for every input,
// so bindParameters' reserved-prefix rejection (params.go ~91-93,
// ErrReservedParamName) was never reachable by the fuzz target at all.
type namedArgKind int

const (
	namedArgValid namedArgKind = iota
	namedArgReserved
	namedArgInvalid
)

// classifyNamedArg maps a fuzzed byte onto the three name shapes that
// exercise every branch of bindParameters' named-argument handling
// (params.go ~87-99): a name validate.ParamName and the reserved-prefix
// check both accept, a name that carries generatedParamPrefix (rejected by
// the reserved-prefix check), and a name validate.ParamName itself rejects.
func classifyNamedArg(nameKind uint8) namedArgKind {
	switch nameKind % 3 {
	case 1:
		return namedArgReserved
	case 2:
		return namedArgInvalid
	default:
		return namedArgValid
	}
}

// namedArgName returns the fixed representative name for kind. The exact
// text does not matter to the property under test — only which of
// validate.ParamName / the reserved-prefix check it trips, if any — so a
// fixed name per kind keeps the target deterministic per input.
func namedArgName(kind namedArgKind) string {
	switch kind {
	case namedArgReserved:
		return generatedParamPrefix + "1" // valid syntax, but reserved
	case namedArgInvalid:
		return "0invalid" // leading digit: validate.ParamName rejects it
	case namedArgValid:
		return "n1"
	}
	return "n1"
}

// FuzzBindParametersArity extends the count-consistency property across a
// varying number of positional and named arguments (FuzzBindParameters
// above fixes exactly one positional argument), per Task 5 correction #2:
// for any template and arg list, bindParameters either returns
// ErrParamCount, or the number of positional $_gqN references it emits
// equals the number of positional params returned, every emitted name is
// unique, and every generated name carries the reserved prefix.
//
// Per correction #1, the named argument's own name is also fuzzed (via
// classifyNamedArg/namedArgName): when the name carries generatedParamPrefix,
// bindParameters must reject it with ErrReservedParamName; when the name is
// otherwise invalid, it must be rejected with a validate.ParamName error;
// only a valid, non-reserved name reaches the existing consistency property.
func FuzzBindParametersArity(f *testing.F) {
	f.Add("MATCH (n:X {a: ?, b: ?, c: ?}) RETURN n", "v1", "v2", "v3", uint8(0b111), "n1", true, uint8(0))
	f.Add("RETURN ?", "a", "b", "c", uint8(0b001), "", false, uint8(0))
	f.Add("RETURN 1", "a", "b", "c", uint8(0b000), "n1", true, uint8(0))
	f.Add("MATCH (n:X {a: ?}) RETURN n, $n1", "'; DROP", "b", "c", uint8(0b001), "'; DROP", true, uint8(0))
	// Seeds that reach the two branches correction #1 says were previously
	// unreachable: a reserved-prefix name and a syntactically invalid name.
	f.Add("RETURN 1", "a", "b", "c", uint8(0b000), "x", true, uint8(1))
	f.Add("RETURN 1", "a", "b", "c", uint8(0b000), "x", true, uint8(2))

	f.Fuzz(func(t *testing.T, tmpl, a, b, c string, mask uint8, namedVal string, includeNamed bool, nameKind uint8) {
		kind := classifyNamedArg(nameKind)
		namedName := namedArgName(kind)
		args, positionalVals := buildArityArgs([3]string{a, b, c}, mask, includeNamed, namedName, namedVal)

		q, params, err := bindParameters(tmpl, args)

		if countPlaceholders(tmpl) != len(positionalVals) {
			if err == nil {
				t.Fatalf("placeholder/argument count mismatch accepted (tmpl %q, query %q)", tmpl, q)
			}
			return // ErrParamCount: checked before any named-arg validation
		}

		if includeNamed && kind != namedArgValid {
			assertArityNameRejected(t, err, namedName, kind)
			return
		}

		if err != nil {
			t.Fatalf("valid input rejected: err = %v (tmpl %q)", err, tmpl)
		}

		assertArityQueryTextArgIndependent(t, tmpl, q, mask, includeNamed, namedName)
		assertArityCountConsistency(t, q, params, positionalVals, includeNamed, namedName)
	})
}

// assertArityNameRejected checks the two new branches from correction #1:
// a reserved-prefix name must produce ErrReservedParamName, and any other
// invalid name must produce a (non-reserved-prefix) validation error.
func assertArityNameRejected(t *testing.T, err error, namedName string, kind namedArgKind) {
	t.Helper()
	switch kind {
	case namedArgReserved:
		if !errors.Is(err, ErrReservedParamName) {
			t.Fatalf("reserved-prefix named arg %q: err = %v, want ErrReservedParamName", namedName, err)
		}
	case namedArgInvalid:
		var verr *validate.ValidationError
		if err == nil || !errors.As(err, &verr) {
			t.Fatalf("invalid named arg %q: err = %v, want a *validate.ValidationError", namedName, err)
		}
	case namedArgValid:
		// unreachable: callers only invoke this for kind != namedArgValid.
	}
}

// buildArityArgs turns the fuzzed (up to 3 positional + 1 optional named)
// inputs into a driver.NamedValue list, selecting positional args by mask
// bit i for vals[i], and returns the positional values actually included in
// argument order (for the leak and count-consistency assertions). The named
// argument's own name comes from namedName (see namedArgName) rather than a
// hardcoded literal, per correction #1.
func buildArityArgs(vals [3]string, mask uint8, includeNamed bool, namedName, namedVal string) ([]driver.NamedValue, []string) {
	var args []driver.NamedValue
	var positionalVals []string
	ordinal := 1
	for i, v := range vals {
		if mask&(1<<uint(i)) == 0 {
			continue
		}
		args = append(args, driver.NamedValue{Ordinal: ordinal, Value: v})
		positionalVals = append(positionalVals, v)
		ordinal++
	}
	if includeNamed {
		args = append(args, driver.NamedValue{Ordinal: ordinal, Name: namedName, Value: namedVal})
	}
	return args, positionalVals
}

// assertArityQueryTextArgIndependent is assertQueryTextArgIndependent's
// arity-target counterpart, per correction #2: it rebinds the same
// (mask, includeNamed, namedName) shape with every value replaced by
// fuzzControlValue and asserts the query text is unchanged. This is exact
// for every argument length — including the sub-3-byte values a
// containment check would have to skip — which is also why no separate
// containment-based leak check is kept alongside it here: as
// FuzzBindParameters' doc comment explains, once this exact property holds,
// a containment check on the untouched positional/named values can only
// ever fire on a coincidence of q's own template-derived structure (a
// generated reference's digit combining with adjacent template text), never
// a real leak — 15s of fuzzing this exact target during this fix produced
// that counterexample (tmpl "?0000?", positional args "0"/"100", mask
// selecting both: q "$_gq10000$_gq2" spuriously "contains" "100").
func assertArityQueryTextArgIndependent(t *testing.T, tmpl, q string, mask uint8, includeNamed bool, namedName string) {
	t.Helper()
	controlArgs, _ := buildArityArgs(
		[3]string{fuzzControlValue, fuzzControlValue, fuzzControlValue},
		mask, includeNamed, namedName, fuzzControlValue,
	)
	q2, _, err2 := bindParameters(tmpl, controlArgs)
	if err2 != nil {
		t.Fatalf("control arguments rejected where fuzz arguments were accepted: %v (tmpl %q)", err2, tmpl)
	}
	if q != q2 {
		t.Fatalf("query text depends on argument bytes\n  template: %q\n  query: %q\n  control:  %q", tmpl, q, q2)
	}
}

// assertArityCountConsistency checks property 2: every returned parameter
// name is unique, the count of reserved-prefix names equals the positional
// argument count, each expected $_gqN reference appears in the query, and
// (when present) the named parameter survives under its own name (namedName,
// per correction #1 — no longer hardcoded to "n1").
func assertArityCountConsistency(t *testing.T, q string, params []*proto.Param, positionalVals []string, includeNamed bool, namedName string) {
	t.Helper()
	seenNames := make(map[string]bool, len(params))
	generatedCount := 0
	for _, p := range params {
		name := p.GetName()
		if seenNames[name] {
			t.Fatalf("duplicate parameter name %q (query %q)", name, q)
		}
		seenNames[name] = true
		if strings.HasPrefix(name, generatedParamPrefix) {
			generatedCount++
		}
	}
	if generatedCount != len(positionalVals) {
		t.Fatalf("generated param count = %d, want %d positional args (query %q, params %d)",
			generatedCount, len(positionalVals), q, len(params))
	}
	for i := 1; i <= len(positionalVals); i++ {
		ref := "$" + generatedParamPrefix + strconv.Itoa(i)
		if strings.Count(q, ref) < 1 {
			t.Fatalf("generated reference %q missing from query %q", ref, q)
		}
	}
	if includeNamed && !seenNames[namedName] {
		t.Fatalf("named parameter %q missing from returned params (query %q)", namedName, q)
	}
}

// FuzzEncodeParamValue asserts every accepted argument type encodes without
// panicking and every rejected one returns an error rather than a stringified
// fallback.
func FuzzEncodeParamValue(f *testing.F) {
	f.Add("s", int64(1), 1.5, true)
	f.Fuzz(func(t *testing.T, s string, i int64, fl float64, b bool) {
		for _, v := range []any{s, i, fl, b, []byte(s), nil} {
			if _, err := encodeParamValue(v); err != nil {
				t.Fatalf("encodeParamValue(%T) = %v, want nil", v, err)
			}
		}
		if _, err := encodeParamValue(struct{ X int }{1}); err == nil {
			t.Fatal("unsupported type accepted — values must never be stringified")
		}
	})
}

// maxFuzzVectorFloats caps the number of float32 components decoded from a
// fuzzed byte slice. Coverage-guided mutation can grow a seed corpus entry
// well past the boundary lengths the seeds below already exercise
// (MaxVectorDimensions ± 1); this keeps a single fuzz iteration bounded
// without narrowing the property under test — every length up to and past
// MaxVectorDimensions is still reachable via the explicit boundary seeds.
const maxFuzzVectorFloats = 3 * MaxVectorDimensions

// FuzzVectorParams fuzzes Vector arguments end-to-end through
// bindParameters: random []float32 components (including NaN, +/-Inf,
// subnormals and -0) and lengths at and around MaxVectorDimensions. Per
// Task 5 correction #2, every input must produce either a typed ErrVector*
// error or a ListValue of exactly len(v) DoubleValues, with no query-text
// leak (the same $_gqN-reference property FuzzBindParameters checks for
// string arguments).
func FuzzVectorParams(f *testing.F) {
	f.Add(floatsToBytes())                              // empty
	f.Add(floatsToBytes(1.5))                           // single component
	f.Add(floatsToBytes(0.5, -1.25, 0))                 // ordinary
	f.Add(floatsToBytes(float32(math.NaN())))           // NaN
	f.Add(floatsToBytes(float32(math.Inf(1))))          // +Inf
	f.Add(floatsToBytes(float32(math.Inf(-1))))         // -Inf
	f.Add(floatsToBytes(math.SmallestNonzeroFloat32))   // subnormal
	f.Add(floatsToBytes(float32(math.Copysign(0, -1)))) // -0
	f.Add(repeatFloatBytes(1, MaxVectorDimensions-1))   // one under the limit
	f.Add(repeatFloatBytes(1, MaxVectorDimensions))     // exactly at the limit
	f.Add(repeatFloatBytes(1, MaxVectorDimensions+1))   // one over the limit
	f.Add(append(floatsToBytes(1, 2, 3), 0x00, 0x01))   // trailing partial float, ignored

	f.Fuzz(func(t *testing.T, data []byte) {
		v := bytesToVector(data)

		q, params, err := bindParameters("MATCH (n) WHERE n.e = ? RETURN n",
			[]driver.NamedValue{{Ordinal: 1, Value: v}})

		if ok, wantErr := expectedVectorErr(v); ok {
			if !errors.Is(err, wantErr) {
				t.Fatalf("vector (%d components): err = %v, want %v", len(v), err, wantErr)
			}
			return
		}
		if err != nil {
			t.Fatalf("valid vector (%d finite components): unexpected error %v", len(v), err)
		}

		assertVectorNoLeak(t, q)
		assertVectorEncoding(t, params, v)
	})
}

// expectedVectorErr reports the typed ErrVector* the driver must return for
// an invalid Vector v, or ok=false if v is valid and encoding must succeed.
func expectedVectorErr(v Vector) (ok bool, err error) {
	switch {
	case len(v) == 0:
		return true, ErrVectorEmpty
	case len(v) > MaxVectorDimensions:
		return true, ErrVectorTooLarge
	case hasNonFiniteComponent(v):
		return true, ErrVectorNotFinite
	default:
		return false, nil
	}
}

// assertVectorNoLeak checks the same no-leftover-placeholder property
// FuzzBindParameters checks for string arguments: exactly one generated
// reference, no surviving unbound '?'.
func assertVectorNoLeak(t *testing.T, q string) {
	t.Helper()
	if strings.Contains(stripStringLiterals(q), "?") {
		t.Fatalf("unbound placeholder survived: %q", q)
	}
	wantRef := "$" + generatedParamPrefix + "1"
	if strings.Count(q, wantRef) != 1 {
		t.Fatalf("generated reference %q count != 1 in query %q", wantRef, q)
	}
}

// assertVectorEncoding checks the single returned param is a ListValue of
// exactly len(v) DoubleValues matching v component-for-component.
func assertVectorEncoding(t *testing.T, params []*proto.Param, v Vector) {
	t.Helper()
	if len(params) != 1 {
		t.Fatalf("len(params) = %d, want 1", len(params))
	}
	list := params[0].GetValue().GetListVal()
	if list == nil {
		t.Fatalf("param value kind = %T, want ListValue", params[0].GetValue().GetKind())
	}
	got := list.GetValues()
	if len(got) != len(v) {
		t.Fatalf("len(list) = %d, want %d", len(got), len(v))
	}
	for i, want := range v {
		d := got[i].GetDoubleVal()
		if d == nil {
			t.Fatalf("element %d kind = %T, want DoubleValue", i, got[i].GetKind())
		}
		if got32 := float32(d.GetValue()); got32 != want {
			t.Fatalf("element %d = %v, want %v", i, got32, want)
		}
	}
}

// floatsToBytes little-endian-encodes fs as the raw bytes FuzzVectorParams'
// corpus stores, so bytesToVector decodes each seed back to exactly fs.
func floatsToBytes(fs ...float32) []byte {
	buf := make([]byte, 0, 4*len(fs))
	for _, f := range fs {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
		buf = append(buf, b[:]...)
	}
	return buf
}

// repeatFloatBytes encodes n copies of f, used to build boundary-length
// seeds around MaxVectorDimensions without a giant literal.
func repeatFloatBytes(f float32, n int) []byte {
	fs := make([]float32, n)
	for i := range fs {
		fs[i] = f
	}
	return floatsToBytes(fs...)
}

// bytesToVector decodes data 4 bytes at a time into a Vector, discarding any
// trailing partial float32 (mirrors how a real caller could never produce a
// misaligned byte count — this is purely how the fuzzer turns arbitrary
// bytes into component values). Decoded length is capped at
// maxFuzzVectorFloats; see its doc comment.
func bytesToVector(data []byte) Vector {
	n := len(data) / 4
	if n > maxFuzzVectorFloats {
		n = maxFuzzVectorFloats
	}
	v := make(Vector, n)
	for i := range v {
		bits := binary.LittleEndian.Uint32(data[4*i : 4*i+4])
		v[i] = math.Float32frombits(bits)
	}
	return v
}

// hasNonFiniteComponent reports whether v has a NaN or +/-Inf component.
func hasNonFiniteComponent(v Vector) bool {
	for _, c := range v {
		f := float64(c)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return true
		}
	}
	return false
}

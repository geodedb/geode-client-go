package geode

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

// ErrParamCount reports a mismatch between `?` placeholders in the query
// template and the arguments supplied by database/sql.
var ErrParamCount = errors.New("geode: placeholder count does not match argument count")

// ErrReservedParamName reports a caller-supplied parameter name inside the
// driver's generated namespace, where it could shadow a positional argument.
var ErrReservedParamName = errors.New("geode: parameter name is reserved by the driver")

// generatedParamPrefix namespaces driver-generated parameter names so they
// can never collide with a caller's own $name references. The GQL lexer
// accepts leading underscores in parameter names
// (geode/src/unicode/unicode_tables.zig:84), and no application-supplied name
// may start with "_gq" (enforced by Conn.CheckNamedValue and bindParameters).
const generatedParamPrefix = "_gq"

// paramRefGrowth is the extra capacity reserved per query when rewriting
// placeholders: a `?` becomes `$_gqN`, four characters longer for the first
// nine placeholders.
const paramRefGrowth = 8

// bindParameters converts a database/sql query template into a GQL query with
// named parameter references, plus the typed protobuf parameters that carry
// the values.
//
// Each positional `?` becomes `$_gqN` (1-based, in template order) and the
// matching driver.NamedValue becomes a proto.Param. Explicitly named
// arguments keep their names and are appended; the template's own $name
// references are left untouched.
//
// NOTHING from an argument is ever written into the returned query text.
// That property is the whole point of this function (SONNY-806 /
// GEO-GO-UP-001: no string interpolation of source text, paths, identifiers
// or vectors into GQL) and is asserted by
// TestBindParameters_NeverEmitsValueIntoQueryText.
//
// The server resolves both $name and positional $N parameters everywhere an
// expression may appear, including CREATE/MERGE property maps and MATCH
// pattern properties (engine 0.8.15: src/eval.zig:886,
// src/execution/match_operations.zig:517), which is what makes inlining
// unnecessary. TestIntegration_ParamsBindInPropertyMaps is the live proof.
func bindParameters(query string, args []driver.NamedValue) (string, []*proto.Param, error) {
	positional := make([]driver.NamedValue, 0, len(args))
	named := make([]driver.NamedValue, 0, len(args))
	for _, nv := range args {
		if nv.Name != "" {
			named = append(named, nv)
			continue
		}
		positional = append(positional, nv)
	}

	newQuery, seen := rewritePlaceholderRefs(query)
	if seen != len(positional) {
		return "", nil, fmt.Errorf("%w: %d placeholders, %d positional arguments",
			ErrParamCount, seen, len(positional))
	}

	params := make([]*proto.Param, 0, len(args))
	// Numbered by slice position (idx+1), not nv.Ordinal: database/sql
	// already supplies positional args in the caller's template order via
	// the "positional" filter above, and Ordinal is 1-based over the
	// original, unfiltered args slice (it would skip numbers here once named
	// args are interleaved), so it is intentionally not consulted.
	for idx, nv := range positional {
		name := generatedParamPrefix + strconv.Itoa(idx+1)
		val, err := encodeParamValue(nv.Value)
		if err != nil {
			return "", nil, fmt.Errorf("geode: argument %d: %w", idx+1, err)
		}
		params = append(params, &proto.Param{Name: name, Value: val})
	}
	for _, nv := range named {
		if err := validate.ParamName(nv.Name); err != nil {
			return "", nil, err
		}
		if strings.HasPrefix(nv.Name, generatedParamPrefix) {
			return "", nil, fmt.Errorf("%w: %q", ErrReservedParamName, nv.Name)
		}
		val, err := encodeParamValue(nv.Value)
		if err != nil {
			return "", nil, fmt.Errorf("geode: parameter %q: %w", nv.Name, err)
		}
		params = append(params, &proto.Param{Name: nv.Name, Value: val})
	}

	return newQuery, params, nil
}

// rewritePlaceholderRefs replaces every positional `?` placeholder in the
// query template with a driver-generated `$_gqN` reference and reports how
// many it replaced. Single-quoted GQL string literals in the TEMPLATE are
// skipped so a '?' inside one stays data.
//
// This is pure text-to-text rewriting of the caller's own template: no
// argument value ever reaches it.
//
// Known blind spots, not fixed here (behaviour unchanged): double-quoted
// strings, backslash-escaped characters, and `//`/`/* */` comments are not
// recognized, so a '?' inside one of those is still treated as a
// placeholder. Scoped as Task 5 fuzz-testing work.
func rewritePlaceholderRefs(query string) (string, int) {
	var b strings.Builder
	b.Grow(len(query) + paramRefGrowth)

	seen := 0
	inString := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch {
		case ch == '\'':
			// A doubled '' inside a literal is an escaped quote, not a
			// terminator, so consume both and stay in the literal.
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				b.WriteByte(ch)
				b.WriteByte(query[i+1])
				i++
				continue
			}
			inString = !inString
			b.WriteByte(ch)
		case ch == '?' && !inString:
			seen++
			b.WriteByte('$')
			b.WriteString(generatedParamPrefix)
			b.WriteString(strconv.Itoa(seen))
		default:
			b.WriteByte(ch)
		}
	}

	return b.String(), seen
}

// countPlaceholders counts the `?` placeholders in a query template, using
// the same literal-aware scan as bindParameters so Stmt.NumInput agrees with
// what the binder will actually replace.
func countPlaceholders(query string) int {
	_, n := rewritePlaceholderRefs(query)
	return n
}

// encodeParamValue converts a driver value into a typed protobuf Value:
// database/sql's driver.Value scalar set directly, time.Time via the
// historical string encoding, containers structurally (see
// encodeContainerParamValue). Anything else is rejected rather than
// stringified, so an unsupported type is a loud error instead of a silent
// lossy conversion.
func encodeParamValue(v any) (*proto.Value, error) {
	switch x := v.(type) {
	case nil:
		return &proto.Value{Kind: &proto.Value_NullVal{NullVal: &proto.NullValue{}}}, nil
	case string:
		return &proto.Value{Kind: &proto.Value_StringVal{
			StringVal: &proto.StringValue{Value: x, Kind: proto.StringKind_STRING},
		}}, nil
	case []byte:
		return &proto.Value{Kind: &proto.Value_BytesVal{
			BytesVal: &proto.BytesValue{Value: x, Kind: proto.BytesKind_BYTEA},
		}}, nil
	case bool:
		return &proto.Value{Kind: &proto.Value_BoolVal{BoolVal: x}}, nil
	case int64:
		// IntKind_INT, not BIGINT: the engine maps BIGINT to its distinct
		// .BigInt value (src/server/proto_util.zig:58-63), which is persisted
		// as an ExtValue property (src/graph_store.zig:1438) — `WHERE n.c = 5`
		// then matches nothing and sum() returns NULL. INT is also what
		// proto.MakeParam has always sent for the named-param path.
		return &proto.Value{Kind: &proto.Value_IntVal{
			IntVal: &proto.IntValue{Value: x, Kind: proto.IntKind_INT},
		}}, nil
	case float64:
		return &proto.Value{Kind: &proto.Value_DoubleVal{
			DoubleVal: &proto.DoubleValue{Value: x, Kind: proto.FloatKind_DOUBLE},
		}}, nil
	case time.Time:
		// database/sql's default converter passes time.Time through
		// untouched (it satisfies driver.Value), so it must be encoded here
		// rather than rejected. Delegating to proto.MakeParam reuses
		// containerValue's default case (proto/codec.go) — the same
		// stringValue(fmt.Sprintf("%v", val)) historically produced for both
		// the positional path (old conn.go inlineValue's default case,
		// deleted in 153a81c) and the named path (proto.MakeParam), so the
		// wire encoding stays byte-identical to that historical form.
		// ScanTime (time.go) documented this format but could not actually
		// parse it back (no layout in timeFormats matched the zone-name
		// suffix fmt.Sprintf("%v", t) always appends); that gap is closed in
		// the same change via goStringerLayout.
		return proto.MakeParam("", x).GetValue(), nil
	default:
		return encodeContainerParamValue(v)
	}
}

// encodeContainerParamValue encodes slices, arrays and maps structurally as
// list_val/map_val. Containers must stay structural or `x IN $list` matches
// nothing and `NOT IN` fails open (ENG-3910, pinned by proto_params_test.go),
// so they are supported here rather than rejected with the other non-scalar
// types. Anything that is not a container is rejected: an unsupported type is
// a loud error instead of a silent lossy stringification.
func encodeContainerParamValue(v any) (*proto.Value, error) {
	if !isContainerParamValue(v) {
		return nil, fmt.Errorf("geode: unsupported parameter type %T", v)
	}
	// MakeParam encodes containers structurally; the name is irrelevant here
	// because only the encoded Value is taken.
	return proto.MakeParam("", v).GetValue(), nil
}

// isContainerParamValue reports whether v is a slice, array or map that
// encodeContainerParamValue can encode structurally. []byte is excluded: it is
// a scalar bytes value, handled by encodeParamValue.
func isContainerParamValue(v any) bool {
	if v == nil {
		return false
	}
	if _, ok := v.([]byte); ok {
		return false
	}
	//nolint:exhaustive // only container kinds are encoded structurally
	switch reflect.ValueOf(v).Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return true
	default:
		return false
	}
}

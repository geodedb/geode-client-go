package geode

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"

	"geodedb.com/geode/proto"
)

// MaxVectorDimensions is the engine's hard ceiling on a vector's component
// count: VectorF32.dim / VectorI32.dim are u16, so anything past 65535
// elements is refused server-side as SQLSTATE 54000 VectorTooLarge
// (engine 0.8.15: src/server/proto_util.zig:318, src/types/vector.zig:53).
// The driver rejects it before the wire so the caller gets a typed Go error
// instead of a round trip.
const MaxVectorDimensions = 65535

// ErrVectorTooLarge reports a vector with more than MaxVectorDimensions
// components.
var ErrVectorTooLarge = errors.New("geode: vector exceeds maximum dimensions")

// ErrVectorEmpty reports a vector with no components. The engine refuses an
// empty list where a vector is expected (src/eval.zig:2211 returns
// error.InvalidArgs for `items.len == 0`), so it is refused here instead.
var ErrVectorEmpty = errors.New("geode: vector must have at least one component")

// ErrVectorNotFinite reports a NaN or ±Inf component. Nothing in the wire
// format or the engine's distance functions gives such a component a defined
// meaning, so the driver fails closed rather than letting it silently poison
// a similarity score.
var ErrVectorNotFinite = errors.New("geode: vector component is not finite")

// Vector is a dense float32 embedding bound as a query parameter.
//
// It exists so embeddings never have to be rendered into query text, and so
// the driver — not the engine, a round trip later — is what refuses an
// oversize or non-finite embedding. Pass it straight to database/sql:
//
//	rows, err := db.QueryContext(ctx,
//	    "MATCH (d:Chunk) RETURN knn('embedding', ?, ?, 'cosine')",
//	    geode.Vector(queryEmbedding), 10)
//
// On the wire it becomes a protobuf ListValue of DoubleValue elements, which
// the engine coerces to a VectorF32 for every vector function and for
// knn()/vector_knn() specifically (engine 0.8.15: src/eval.zig:2210
// valueToVectorF32, src/gql/language_metadata.zig:1087).
//
// Vector is deliberately distinct from a plain []float32: an untyped
// []float32 keeps its unbounded structural list encoding (ENG-3910, where a
// list parameter must stay a list for `x IN $p`), while a Vector is the
// typed, bounded, validated embedding path.
type Vector []float32

// Compile-time interface check.
var _ driver.Valuer = (Vector)(nil)

// Value implements driver.Valuer so a Vector survives database/sql's
// argument conversion untouched. Conn.CheckNamedValue keeps the concrete
// Vector type, and encodeParamValue does the real encoding; Value returns
// the receiver so the default converter — should it ever be reached — cannot
// lose type information, and validates first so an invalid embedding fails
// closed on either path.
func (v Vector) Value() (driver.Value, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return v, nil
}

// Dimensions returns the component count.
func (v Vector) Dimensions() int { return len(v) }

// validate enforces every property the engine would otherwise have to
// enforce across the wire: a non-empty vector, no more than
// MaxVectorDimensions components, and finite components only.
func (v Vector) validate() error {
	if len(v) == 0 {
		return ErrVectorEmpty
	}
	if len(v) > MaxVectorDimensions {
		return fmt.Errorf("%w: %d > %d", ErrVectorTooLarge, len(v), MaxVectorDimensions)
	}
	for i, c := range v {
		f := float64(c)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%w: component %d is %v", ErrVectorNotFinite, i, c)
		}
	}
	return nil
}

// encodeVector converts a validated Vector into a protobuf ListValue of
// DoubleValue elements.
//
// The elements are written explicitly rather than handed to
// proto.MakeParam's container path because that path routes float64 whole
// numbers to int_val (proto/codec.go makeValue), which would send a
// zero-or-one component of an embedding as an integer.
func encodeVector(v Vector) (*proto.Value, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	vals := make([]*proto.Value, len(v))
	for i, c := range v {
		vals[i] = &proto.Value{Kind: &proto.Value_DoubleVal{
			DoubleVal: &proto.DoubleValue{Value: float64(c), Kind: proto.FloatKind_DOUBLE},
		}}
	}
	return &proto.Value{Kind: &proto.Value_ListVal{
		ListVal: &proto.ListValue{Values: vals},
	}}, nil
}

//go:build integration

package geode

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestIntegration_KNNWithBoundVector proves an embedding crosses the wire as
// a parameter — never as query text — and that the engine's knn() accepts it.
//
// knn('embedding', $query, k, metric) is the documented form and takes a
// VECTOR in argument 2 (engine 0.8.15:
// src/gql/language_metadata.zig:1101-1112); a bound ListValue of doubles is
// coerced to VectorF32 by src/eval.zig:2210 valueToVectorF32.
//
// The seed here writes the bound vector straight into the property, which
// stores a LIST (only an ExtValue.VectorF32 is a stored vector —
// src/execution/write_precheck.zig:166), so this exercises knn()'s
// brute-force scan path. The HNSW index path is exercised by
// TestIntegration_VectorMetricMismatchIsTyped, which seeds through
// vectorf32(?).
func TestIntegration_KNNWithBoundVector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=vectest")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	seed := []struct {
		id  string
		vec Vector
	}{
		{"v-a", Vector{1, 0, 0}},
		{"v-b", Vector{0, 1, 0}},
		{"v-c", Vector{0, 0, 1}},
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx,
			"MERGE (n:VecProbe {vid: ?}) ON CREATE SET n.embedding = ?", s.id, s.vec); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}

	if _, err := db.ExecContext(ctx,
		"CREATE INDEX vecprobe_idx ON VecProbe (embedding) USING vector WITH (metric = 'cosine')",
	); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	var ids any
	if err := db.QueryRowContext(ctx,
		"RETURN knn('embedding', ?, ?, 'cosine')", Vector{0.99, 0.01, 0}, 1,
	).Scan(&ids); err != nil {
		t.Fatalf("knn with bound vector: %v", err)
	}
	if ids == nil {
		t.Fatal("knn returned NULL — the vector parameter did not reach the function")
	}
	t.Logf("knn(bound vector) = %v", ids)
}

// TestIntegration_VectorMetricMismatchIsTyped asserts that querying an HNSW
// index with a metric other than the one it was built with surfaces as a
// typed *DriverError carrying the engine's own status
// (src/server/proto_util.zig:160 maps error.VectorMetricMismatch to 42704),
// not as an opaque string.
//
// It also pins the storage form an embedding needs: `SET n.p = ?` stores a
// LIST property, which the HNSW index does not index, so knn() falls back to
// the brute-force scan and the metric is never compared. `SET n.p =
// vectorf32(?)` (src/gql/language_metadata.zig:967) stores a real VectorF32
// and is what makes the index authoritative. The bound geode.Vector is the
// argument to vectorf32() either way — binding is what this task changes,
// and it loses nothing.
//
// The property is named "metricvec", not "embedding", deliberately:
// findAdvancedVectorIndexForProperty resolves indexes by PROPERTY NAME
// alone, and an index is authoritative only when its label provably covers
// every row the scan would visit (src/eval.zig:2760-2790). Another test's
// `embedding` in another graph is enough to revoke that authority, at which
// point knn() silently answers from the scan and this test would pass
// vacuously — observed against 0.8.15.
func TestIntegration_VectorMetricMismatchIsTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=vecmetric")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Ordering matters at engine 0.8.15 and is part of what this test pins:
	// one row must exist before CREATE INDEX (the advanced vector index is
	// registered for a property the store already knows) and one must be
	// written after it (nothing backfills an existing row into a freshly
	// created HNSW index). Only then is the index authoritative for knn()
	// (src/eval.zig:2786-2800); otherwise knn() silently takes the
	// brute-force scan, which never compares metrics and this test would
	// pass vacuously.
	if _, err := db.ExecContext(ctx,
		"MERGE (n:MetricProbe {vid: ?}) ON CREATE SET n.metricvec = vectorf32(?)", "m-a", Vector{1, 0, 0},
	); err != nil {
		t.Fatalf("seed m-a: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"CREATE INDEX metricprobe_idx ON MetricProbe (metricvec) USING vector WITH (metric = 'cosine')",
	); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"MERGE (n:MetricProbe {vid: ?}) ON CREATE SET n.metricvec = vectorf32(?)", "m-b", Vector{0, 1, 0},
	); err != nil {
		t.Fatalf("seed m-b: %v", err)
	}

	// The index's own metric answers normally: proof the bound vector reached
	// the HNSW query, not just the scan.
	var ids any
	if err := db.QueryRowContext(ctx,
		"RETURN knn('metricvec', ?, ?, 'cosine')", Vector{0.99, 0.01, 0}, 1,
	).Scan(&ids); err != nil {
		t.Fatalf("knn with the index metric: %v", err)
	}
	if ids == nil {
		t.Fatal("knn returned NULL — the vector parameter did not reach the index")
	}

	var mismatched any
	err = db.QueryRowContext(ctx,
		"RETURN knn('metricvec', ?, ?, 'l2')", Vector{0.99, 0.01, 0}, 1,
	).Scan(&mismatched)
	if err == nil {
		t.Fatalf("err = nil (result %v), want a metric-mismatch error", mismatched)
	}
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v (%T), want a *geode.DriverError", err, err)
	}
	t.Logf("metric mismatch surfaced as class=%q code=%q message=%q", de.Class, de.Code, de.Message)
	if de.Code != "42704" {
		t.Errorf("code = %q, want 42704 (src/server/proto_util.zig:160)", de.Code)
	}
}

// TestIntegration_VectorDimensionMismatchIsTyped asserts the engine's
// VectorDimensionMismatch — SQLSTATE 22000 per
// src/server/proto_util.zig:189 — surfaces as a typed *DriverError, not a
// raw string, when two different dimensions are written to the same
// (label, property).
//
// The dimension pin is armed only by an ExtValue.VectorF32 write
// (src/execution/write_precheck.zig:166), so the seed goes through
// vectorf32(?) — see TestIntegration_VectorMetricMismatchIsTyped.
func TestIntegration_VectorDimensionMismatchIsTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=vecdim")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx,
		"MERGE (n:DimProbe {vid: ?}) ON CREATE SET n.embedding = vectorf32(?)", "d1", Vector{1, 2, 3}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// No index: the pin belongs to the (graph, label, property), not to an
	// index (ENG-3922(a), src/server/proto_util.zig:186-189).
	_, err = db.ExecContext(ctx,
		"MERGE (n:DimProbe {vid: ?}) ON CREATE SET n.embedding = vectorf32(?)", "d2", Vector{1, 2})
	if err == nil {
		t.Fatal("err = nil, want a dimension-mismatch error")
	}
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v (%T), want a *geode.DriverError", err, err)
	}
	t.Logf("dimension mismatch surfaced as class=%q code=%q message=%q", de.Class, de.Code, de.Message)
	if de.Code != "22000" {
		t.Errorf("code = %q, want 22000 (src/server/proto_util.zig:189)", de.Code)
	}
}

// TestIntegration_OversizeVectorNeverReachesTheWire proves the driver's own
// ceiling fires first: the caller gets ErrVectorTooLarge, not the engine's
// SQLSTATE 54000 a round trip later (src/server/proto_util.zig:318).
func TestIntegration_OversizeVectorNeverReachesTheWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=vecbig")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	big := make(Vector, MaxVectorDimensions+1)
	for i := range big {
		big[i] = 0.1
	}
	_, err = db.ExecContext(ctx,
		"MERGE (n:BigProbe {vid: ?}) ON CREATE SET n.embedding = ?", "b1", big)
	if !errors.Is(err, ErrVectorTooLarge) {
		t.Fatalf("err = %v, want ErrVectorTooLarge from the driver", err)
	}
}

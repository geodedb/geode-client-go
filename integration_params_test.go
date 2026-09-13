//go:build integration

package geode

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestIntegration_ParamsBindInPropertyMaps is the evidence that lets the
// driver stop inlining values into query text. The historic conn.go comment
// claimed the server could not resolve $param inside CREATE/MERGE property
// maps; at engine 0.8.15 it can (src/eval.zig:886,
// src/execution/match_operations.zig:517). If this test ever fails, the
// inliner may NOT be reinstated: fail closed and file an engine gap.
func TestIntegration_ParamsBindInPropertyMaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=paramtest")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// A value that would break out of a single-quoted GQL literal if the
	// driver were still interpolating it.
	const hostile = "a' } ) RETURN 1 // "

	if _, err := db.ExecContext(ctx,
		"MERGE (n:ParamProbe {k: ?}) ON CREATE SET n.v = ?, n.i = ?, n.f = ?, n.b = ?",
		"probe-1", hostile, int64(42), 1.5, true,
	); err != nil {
		t.Fatalf("MERGE with bound params: %v", err)
	}

	var v string
	var i int64
	var f float64
	var b bool
	err = db.QueryRowContext(ctx,
		"MATCH (n:ParamProbe {k: ?}) RETURN n.v, n.i, n.f, n.b", "probe-1",
	).Scan(&v, &i, &f, &b)
	if err != nil {
		t.Fatalf("MATCH with bound param: %v", err)
	}
	if v != hostile {
		t.Errorf("n.v = %q, want %q (value was mangled — driver is still interpolating)", v, hostile)
	}
	if i != 42 || f != 1.5 || !b {
		t.Errorf("scalars = (%d, %v, %v), want (42, 1.5, true)", i, f, b)
	}
}

// TestIntegration_ParamsAreNotInQueryText proves the hostile value never
// appears in the text the server parses: if it did, the injected
// `RETURN 1` tail would create a second result set or a syntax error.
func TestIntegration_ParamsAreNotInQueryText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=paramtest")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	const injection = "x'}) DETACH DELETE n RETURN 1 //"
	if _, err := db.ExecContext(ctx, "MERGE (n:ParamProbe {k: ?})", injection); err != nil {
		t.Fatalf("MERGE with injection payload: %v", err)
	}

	var n int64
	if err := db.QueryRowContext(ctx,
		"MATCH (m:ParamProbe {k: ?}) RETURN count(m)", injection).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1 — payload was treated as query text, not data", n)
	}
}

// TestIntegration_ParamsBindTimeValue is the round-trip proof for I1: a
// time.Time argument must bind (not be rejected by CheckNamedValue), the
// server must store it as the historical fmt.Sprintf("%v", t) string
// (params.go's encodeParamValue), and ScanTime (time.go) must parse that
// string back into an equal time.Time. Covers both the positional and named
// paths, since each goes through bindParameters independently.
func TestIntegration_ParamsBindTimeValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN+"&graph=paramtest")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	want := time.Date(2024, 3, 15, 10, 30, 45, 123456789, loc)

	// Positional path.
	if _, err := db.ExecContext(ctx,
		"MERGE (n:ParamProbe {k: ?}) ON CREATE SET n.ts = ?", "time-probe-positional", want,
	); err != nil {
		t.Fatalf("MERGE with positional time.Time param: %v", err)
	}
	var gotPositional time.Time
	if err := db.QueryRowContext(ctx,
		"MATCH (n:ParamProbe {k: ?}) RETURN n.ts", "time-probe-positional",
	).Scan(ScanTime(&gotPositional)); err != nil {
		t.Fatalf("MATCH positional time.Time param: %v", err)
	}
	if !gotPositional.Equal(want) {
		t.Errorf("positional round-trip = %v, want %v", gotPositional, want)
	}

	// Named path.
	if _, err := db.ExecContext(ctx,
		"MERGE (n:ParamProbe {k: $key}) ON CREATE SET n.ts = $ts",
		sql.Named("key", "time-probe-named"), sql.Named("ts", want),
	); err != nil {
		t.Fatalf("MERGE with named time.Time param: %v", err)
	}
	var gotNamed time.Time
	if err := db.QueryRowContext(ctx,
		"MATCH (n:ParamProbe {k: $key}) RETURN n.ts", sql.Named("key", "time-probe-named"),
	).Scan(ScanTime(&gotNamed)); err != nil {
		t.Fatalf("MATCH named time.Time param: %v", err)
	}
	if !gotNamed.Equal(want) {
		t.Errorf("named round-trip = %v, want %v", gotNamed, want)
	}
}

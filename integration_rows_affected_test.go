//go:build integration

// integration_rows_affected_test.go — SONNY-2822 contract test.
//
// sql.Result.RowsAffected() was 0 for every Exec, although the write was
// applied, so a conditional write (MATCH ... WHERE ... SET, the shape of every
// compare-and-swap in go/oidc) could not tell "matched and changed" from
// "matched nothing". The engine now reports what a statement changed in
// DataPage.update_counts on the final page (geode a6de87812); the driver must
// carry it to RowsAffected (rowsAffectedFromCounts in result.go).
//
// The contract, from the ticket: MATCH ... SET on N rows reports N, and on
// 0 rows reports 0. It runs against a real engine on both transports.

package geode

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// updateCountsFirstEngine is the first engine release cut after geode
// a6de87812, the commit that puts update_counts on the wire. No release before
// it sends them, so the pinned engine (pinnedEngineVersion) cannot satisfy this
// contract until the pin reaches it.
const updateCountsFirstEngine = "v0.8.26"

// updateCountsEngineEnv lets the operator of a manual-server run say the
// engine behind GEODE_TEST_DSN sends update counts — a build from geode main
// after a6de87812 reports the version of the last release (0.8.25), so the
// version alone cannot tell.
const updateCountsEngineEnv = "GEODE_TEST_ENGINE_UPDATE_COUNTS"

// rowsAffectedGraph keeps this test's nodes out of the default graph other
// tests bulk-delete.
const rowsAffectedGraph = "rowsaffected"

// requireUpdateCountsEngine skips only when the engine under test is the
// pinned release and that release predates update counts. Once the pin moves
// to updateCountsFirstEngine or later the test runs on every lane, with no
// change here.
func requireUpdateCountsEngine(t *testing.T) {
	t.Helper()
	if os.Getenv(updateCountsEngineEnv) == "1" {
		return
	}
	if engineVersionLess(pinnedEngineVersion, updateCountsFirstEngine) {
		t.Skipf("pinned engine %s predates update counts (first release: %s); "+
			"set %s=1 to run against an engine built from geode main after a6de87812",
			pinnedEngineVersion, updateCountsFirstEngine, updateCountsEngineEnv)
	}
}

// engineVersionLess reports whether release a ("v0.8.22") sorts before b.
func engineVersionLess(a, b string) bool {
	pa, pb := splitEngineVersion(a), splitEngineVersion(b)
	for i := range pa {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return false
}

func splitEngineVersion(v string) [3]int {
	var out [3]int
	for i, part := range strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3) {
		n, err := strconv.Atoi(part)
		if err != nil {
			panic(fmt.Sprintf("malformed engine version %q", v))
		}
		out[i] = n
	}
	return out
}

func TestIntegration_RowsAffectedReportsWhatTheStatementChanged(t *testing.T) {
	requireUpdateCountsEngine(t)

	t.Run("QUIC", func(t *testing.T) {
		db, err := sql.Open("geode", getTestDSN(t)+"&graph="+rowsAffectedGraph)
		if err != nil {
			t.Fatalf("sql.Open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		assertRowsAffectedContract(t, db)
	})

	t.Run("gRPC", func(t *testing.T) {
		assertRowsAffectedContract(t, grpcDB(t, "&graph="+rowsAffectedGraph))
	})
}

// assertRowsAffectedContract drives each write shape the contract names
// against db and checks the count the driver reports.
func assertRowsAffectedContract(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// More rows than one gRPC pushed page (grpcStreamPageSize), so the
	// statement's matched rows span several pages and the counts must be read
	// from the final one, not the first.
	const n = 2*grpcStreamPageSize + 3
	// A run id keeps repeated manual-server runs from matching each other's
	// nodes.
	run := strconv.FormatInt(time.Now().UnixNano(), 36)

	steps := []struct {
		name  string
		query string
		args  []any
		want  int64
	}{
		{
			name:  "CREATE of N nodes reports N",
			query: "FOR i IN range(0, ?) CREATE (n:RowsAffectedProbe {run: ?, i: i})",
			args:  []any{int64(n), run},
			want:  n,
		},
		{
			name:  "MATCH ... SET on N rows reports N",
			query: "MATCH (n:RowsAffectedProbe {run: ?}) SET n.flag = true",
			args:  []any{run},
			want:  n,
		},
		{
			name:  "MATCH ... SET on 0 rows reports 0",
			query: "MATCH (n:RowsAffectedProbe {run: ?, i: 999}) SET n.flag = true",
			args:  []any{run},
			want:  0,
		},
		{
			name: "conditional SET that matches reports 1",
			query: "MATCH (n:RowsAffectedProbe {run: ?, i: 0}) WHERE n.flag = true " +
				"SET n.flag = false",
			args: []any{run},
			want: 1,
		},
		{
			name: "the same conditional SET, now stale, reports 0",
			query: "MATCH (n:RowsAffectedProbe {run: ?, i: 0}) WHERE n.flag = true " +
				"SET n.flag = false",
			args: []any{run},
			want: 0,
		},
		{
			name:  "a read reports 0",
			query: "MATCH (n:RowsAffectedProbe {run: ?}) RETURN n.i",
			args:  []any{run},
			want:  0,
		},
		{
			name:  "DETACH DELETE of N nodes reports N",
			query: "MATCH (n:RowsAffectedProbe {run: ?}) DETACH DELETE n",
			args:  []any{run},
			want:  n,
		},
		{
			name:  "DELETE that matches nothing reports 0",
			query: "MATCH (n:RowsAffectedProbe {run: ?}) DETACH DELETE n",
			args:  []any{run},
			want:  0,
		},
	}

	for _, step := range steps {
		res, err := db.ExecContext(ctx, step.query, step.args...)
		if err != nil {
			t.Fatalf("%s: ExecContext(%q): %v", step.name, step.query, err)
		}
		got, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("%s: RowsAffected: %v", step.name, err)
		}
		if got != step.want {
			t.Errorf("%s: RowsAffected() = %d, want %d", step.name, got, step.want)
		}
	}
}

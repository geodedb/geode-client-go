//go:build integration

package geode

import (
	"database/sql"
	"testing"
)

// Run the real fixture as a child so its function defers and test cleanups
// have both finished before an independent connection observes the graph.
func TestIntegration_LoadFixtureDeletesRowsBeforeClosingPool(t *testing.T) {
	if !t.Run("write_fixture", TestLoad_HighParallelismWrites) {
		t.Fatal("load fixture failed before cleanup could be verified")
	}
	db, err := sql.Open("geode", sharedDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := loadCtx()
	defer cancel()
	var remaining int64
	if err := db.QueryRowContext(ctx, "MATCH (n:LoadWrite) RETURN count(n)").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("load fixture left %d rows after its cleanup completed, want 0", remaining)
	}
}

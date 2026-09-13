//go:build integration

package geode

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// pinnedEngineVersion lives in engine_pin_test.go alongside defaultGeodeImage,
// so the two can be guarded together by a test that needs no Docker.

// TestIntegration_PinnedEngineVersion fails when the integration lane is
// pointed at an engine other than the one this driver is qualified against.
// Mocks never satisfy a release gate, so the pin must be asserted, not assumed.
func TestIntegration_PinnedEngineVersion(t *testing.T) {
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("geode", sharedDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	var version string
	if err := db.QueryRowContext(ctx, "SHOW VERSION").Scan(&version); err != nil {
		t.Fatalf("SHOW VERSION: %v", err)
	}
	if version != pinnedEngineVersion {
		t.Fatalf("engine version = %q, want %q (update GEODE_IMAGE and pinnedEngineVersion together)", version, pinnedEngineVersion)
	}
}

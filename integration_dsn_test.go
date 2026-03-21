//go:build integration

package geode

import "testing"

// getTestDSN returns the shared DSN for integration tests.
// The container is started once by TestMain in integration_main_test.go.
func getTestDSN(t *testing.T) string {
	t.Helper()
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
	}
	return sharedDSN
}

//go:build !integration

package geode

import (
	"testing"
)

// getTestDSN returns the DSN from environment variable when not running
// with the integration build tag.
func getTestDSN(t *testing.T) string {
	return getTestDSNFromEnv(t)
}

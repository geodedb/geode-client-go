//go:build integration

// integration_main_test.go starts a single shared Geode container for the
// entire integration test suite and tears it down when the suite finishes.
//
// Having TestMain here means a single Docker container is created once,
// shared across all TestDocker_*, TestIntegration_*, and BenchmarkDocker_*
// test functions, then terminated after the last test has run.
//
// Manual-server mode: set GEODE_TEST_DSN to skip Docker entirely.
//
//	GEODE_TEST_DSN="localhost:3141?insecure=true&user=geode&pass=..." go test -v -tags=integration ./...
package geode

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedDSN is the connection string used by every integration test.
// Populated by TestMain before any test runs.
var sharedDSN string

// TestMain runs once for the entire integration test binary.
// It starts a Geode container (or uses GEODE_TEST_DSN), runs all tests,
// and then terminates the container.
func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}

func runTestMain(m *testing.M) int {
	// Manual-server fast path: no container needed.
	if dsn := os.Getenv("GEODE_TEST_DSN"); dsn != "" {
		sharedDSN = dsn
		return m.Run()
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        getGeodeImage(),
		ExposedPorts: []string{defaultGeodePort},
		Env: map[string]string{
			// Initialise the HMAC chain so the server auto-creates the admin
			// user on first boot.
			"GEODE_AUTH_HMAC_KEY": testHMACKey,
			// Seed the admin credential directly. Without this the engine
			// generates a one-time password and (as of 0.8.15) writes it to
			// ./.geode_admin_password inside the container rather than to the
			// log, which the log-scraping bootstrap in setupContainerAuth
			// cannot read. Seeding it removes the scrape and the reset round
			// trip entirely and gives every integration test a stable DSN.
			"GEODE_ADMIN_PASSWORD": testAdminPassphrase,
		},
		WaitingFor: wait.ForLog(`"status":"listening"`).WithStartupTimeout(startupTimeout),
		Cmd: []string{
			"serve",
			"--listen", "0.0.0.0:3141",
			"--log-level", "info",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("integration: failed to start Geode container: %v", err)
	}

	// Change the auto-generated one-time password to a known passphrase.
	if err := setupContainerAuth(ctx, container); err != nil {
		container.Terminate(ctx) //nolint:errcheck
		log.Fatalf("integration: failed to set up container auth: %v", err)
	}

	sharedDSN, err = containerDSN(ctx, container, testAdminUser, testAdminPassphrase)
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		log.Fatalf("integration: failed to build container DSN: %v", err)
	}

	// Run all tests.
	code := m.Run()

	// Tear down the container.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := container.Terminate(stopCtx); err != nil {
		log.Printf("integration: warning: failed to terminate container: %v", err)
	}

	return code
}

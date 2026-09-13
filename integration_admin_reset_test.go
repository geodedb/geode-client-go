//go:build integration

// integration_admin_reset_test.go covers the engine's first-login forced
// password reset. The shared suite container (integration_main_test.go) seeds
// GEODE_ADMIN_PASSWORD and therefore never takes that path, so it gets its own
// throwaway container here and never touches sharedDSN.

package geode

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// generatedAdminPasswordFile is where engines that no longer print the
	// generated admin password write it instead. The engine writes it under
	// --data-dir (see the SONNY-1218 workaround on this test's container
	// below, which sets --data-dir /data), which is what the startup line
	// "written to ./.geode_admin_password" refers to.
	generatedAdminPasswordFile = "/data/.geode_admin_password"

	// resetPassphrase is the credential the forced reset moves the admin
	// account to. Deliberately distinct from testAdminPassphrase so a mix-up
	// with the shared suite container cannot pass unnoticed. Must satisfy the
	// server policy: >=20 chars, >=3 words, >=1 uppercase letter.
	resetPassphrase = "Rotated horse battery staple"
)

// readGeneratedAdminPassword returns the one-time admin password the engine
// generates on first boot when GEODE_ADMIN_PASSWORD is unset.
//
// Current engines write it to a file inside the container; older ones printed
// it to the log, so the log scan remains as a fallback.
func readGeneratedAdminPassword(ctx context.Context, container testcontainers.Container) (string, error) {
	fileErr := fmt.Errorf("not attempted")
	reader, err := container.CopyFileFromContainer(ctx, generatedAdminPasswordFile)
	if err == nil {
		defer reader.Close()
		data, readErr := io.ReadAll(reader)
		switch {
		case readErr != nil:
			fileErr = fmt.Errorf("read %s: %w", generatedAdminPasswordFile, readErr)
		case strings.TrimSpace(string(data)) == "":
			fileErr = fmt.Errorf("%s is empty", generatedAdminPasswordFile)
		default:
			return strings.TrimSpace(string(data)), nil
		}
	} else {
		fileErr = fmt.Errorf("copy %s: %w", generatedAdminPasswordFile, err)
	}

	// Fall back to the log scan for engines that still print the credential.
	logs, logErr := readContainerLogs(ctx, container)
	if logErr != nil {
		return "", fmt.Errorf("generated admin password unavailable: %w; log fallback: %w", fileErr, logErr)
	}
	if pass := parseAdminPassword(logs); pass != "" {
		return pass, nil
	}
	return "", fmt.Errorf("generated admin password unavailable: %w; and no password line in the container log", fileErr)
}

// TestIntegration_AdminPasswordResetFlow exercises the engine's forced
// first-login password reset end to end with its own container:
//
//  1. start an engine WITHOUT GEODE_ADMIN_PASSWORD, so it generates one;
//  2. obtain that generated credential the way the engine provides it;
//  3. confirm a normal query is refused while the reset is outstanding;
//  4. perform ALTER USER ... SET PASSWORD over the driver;
//  5. prove the new credential works and the old one no longer does.
//
// The shared suite seeds GEODE_ADMIN_PASSWORD to get a stable DSN, which skips
// this path entirely — this test is what keeps it covered.
func TestIntegration_AdminPasswordResetFlow(t *testing.T) {
	if os.Getenv("GEODE_TEST_DSN") != "" {
		t.Skip("manual-server mode (GEODE_TEST_DSN set): this test needs to start its own container")
	}

	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        getGeodeImage(),
			ExposedPorts: []string{defaultGeodePort},
			Env: map[string]string{
				// HMAC key only: no GEODE_ADMIN_PASSWORD, so the engine
				// generates a one-time credential flagged for reset.
				"GEODE_AUTH_HMAC_KEY": testHMACKey,
			},
			WaitingFor: wait.ForLog(`"status":"listening"`).WithStartupTimeout(startupTimeout),
			Cmd: []string{
				"serve",
				"--listen", "0.0.0.0:3141",
				"--log-level", "info",
				// SONNY-1218 workaround: the image's default CMD passes
				// --data-dir /data, but this harness overrides Cmd and drops
				// it. Without a storage adapter the engine's node_scan_stream
				// fast path returns empty rows instead of declining, which
				// silently corrupts results. Required until the engine
				// declines the fast path when no adapter is configured.
				// Container-local filesystem only; nothing is mounted from
				// the host.
				"--data-dir", "/data",
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start reset-flow container: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := container.Terminate(stopCtx); err != nil {
			t.Logf("warning: failed to terminate reset-flow container: %v", err)
		}
	})

	genPass, err := readGeneratedAdminPassword(ctx, container)
	if err != nil {
		t.Fatalf("obtain generated admin password: %v", err)
	}
	if genPass == testAdminPassphrase || genPass == resetPassphrase {
		t.Fatalf("generated admin password unexpectedly equals a test constant: the engine did not generate one")
	}

	genDSN, err := containerDSN(ctx, container, testAdminUser, genPass)
	if err != nil {
		t.Fatalf("build DSN for generated credential: %v", err)
	}

	// (3) The generated credential authenticates but must refuse real work
	// until it is changed.
	if err := verifyCredential(ctx, genDSN); err == nil {
		t.Fatal("SHOW VERSION succeeded on the generated credential: the engine did not enforce a password reset")
	} else if !strings.Contains(strings.ToLower(err.Error()), "password reset") {
		t.Fatalf("want a password-reset-required error on the generated credential, got: %v", err)
	}

	// (4) Perform the reset over the driver.
	db, err := sql.Open("geode", genDSN)
	if err != nil {
		t.Fatalf("sql.Open with generated credential: %v", err)
	}
	defer db.Close()

	execCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if _, err := db.ExecContext(execCtx,
		fmt.Sprintf("ALTER USER %s SET PASSWORD '%s'", testAdminUser, resetPassphrase),
	); err != nil {
		t.Fatalf("ALTER USER ... SET PASSWORD: %v", err)
	}

	// (5) The new credential works...
	newDSN, err := containerDSN(ctx, container, testAdminUser, resetPassphrase)
	if err != nil {
		t.Fatalf("build DSN for reset credential: %v", err)
	}
	if err := verifyCredential(ctx, newDSN); err != nil {
		t.Fatalf("reset credential does not authenticate: %v", err)
	}

	// ...and the generated one no longer does.
	if err := verifyCredential(ctx, genDSN); err == nil {
		t.Fatal("the generated credential still authenticates after the reset")
	}
}

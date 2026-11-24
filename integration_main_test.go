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
//
// Manual-server mode and strict tenancy: the DSN is taken verbatim, so when the
// server runs with --strict-tenancy / GEODE_STRICT_TENANCY=1 (which every
// Makefile-managed container does — see the docker-start recipe) the caller
// MUST bind a concrete tenant in that DSN:
//
//	GEODE_TEST_DSN="localhost:3141?insecure=true&user=geode&pass=...&tenant=it-harness"
//
// Without it every data statement in the suite is refused with GQLSTATE 42501
// "Tenant scope required". The Makefile's manual lanes and the CI integration
// job already append &tenant=$(GEODE_TEST_TENANT); a hand-written DSN has to do
// the same. Binding a tenant against a permissive server is harmless — it is
// just a concrete scope — so the tenant can be bound unconditionally.
package geode

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// harnessTenant is declared in harness_tenant_test.go, which carries no build
// tag: that keeps it reachable from a plain `go test` (no Docker, no
// `-tags=integration`) so TestHarnessTenant_MatchesMakefileAndCI can pin it as
// the single source of truth against the Makefile and .gitlab-ci.yml defaults.

// sharedDSN is the connection string used by every integration test.
// Populated by TestMain before any test runs. In container mode it is bound to
// harnessTenant; a test that needs a different scope appends its own
// "&tenant=..." (the last occurrence of a repeated DSN key wins — see
// applyQueryParams in config.go), and a test that needs NO scope strips it with
// dsnWithoutTenant.
var sharedDSN string

// grpcDSN addresses the SAME engine over gRPC. It is the gRPC half of the
// Task 14 parity lane: integration_grpc_test.go asserts on it the properties
// the QUIC lane asserts on sharedDSN.
//
// Empty when the engine exposes no reachable gRPC listener — a manual server
// started QUIC-only, or GEODE_TEST_GRPC_DSN left unset. The gRPC tests then
// FAIL rather than skip when the QUIC lane is live (requireGRPCDSN in
// integration_grpc_test.go): "the parity lane did not run" is a result, not a
// non-event.
//
// Two engine facts shape the DSN this harness builds:
//
//   - The gRPC listener is TLS-only in a release build. geode 0.8.15 refuses a
//     plaintext fallback with error.TlsRequired unless the build is Debug AND
//     GEODE_GRPC_ALLOW_PLAINTEXT_FALLBACK is set
//     (geode src/transport/grpc_server.zig:690-733); the published image is a
//     release build, so `tls=0` cannot work against it. The listener serves
//     the engine's auto-generated self-signed certificate (CN=geode.local), so
//     the DSN carries insecure_tls_skip_verify=true, exactly as the QUIC half
//     carries insecure=true.
//   - The listener binds LOOPBACK unless told otherwise. `--listen-grpc
//     0.0.0.0:50051` parses the address and then throws it away — only the
//     port reaches GeodeGrpcServer.init (geode 0.8.15
//     src/server/main.zig:2122-2152) — and the vendored grpc-zig defaults to
//     GrpcServer.DEFAULT_BIND_ADDRESS = "127.0.0.1" (grpc-zig
//     grpc_zig-0.1.8-7FnvpKz7BQAM3rNTfY4gV_dBgDfEB4JScP6cE--HzBl9
//     src/server.zig:138 — the content hash geode's build.zig.zon:305-310
//     resolves ref=v0.1.9 to; cite the hash, not the mutable tag).
//     Inside a container that means the
//     published port reaches nothing. GRPC_ZIG_BIND_ADDR=0.0.0.0 (read at
//     grpc-zig src/server.zig:145-151) is the documented opt-in and is set in
//     the container Env below, in the Makefile docker-start recipe, and in the
//     .gitlab-ci.yml integration service.
var grpcDSN string

// testContainer is the testcontainers-managed engine, nil in manual-server
// mode (GEODE_TEST_DSN). Restart-recovery tests drive it directly: only the
// container-managed lane can stop and start the engine under a live pool.
var testContainer testcontainers.Container

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
		// Register the harness tenant here too, as the container path does.
		// Since geode 0.8.26 (SONNY-6612) the engine refuses a session whose
		// tenant is not registered (42501 TenantNotRegistered) as soon as the
		// registry holds any tenant other than the default, and the cancel and
		// tenancy tests register theirs. Unregistered, it-harness would work
		// only until the first of them ran. The CALL runs unscoped, since a
		// session bound to an unregistered tenant is refused even for it.
		if err := ensureHarnessTenant(context.Background(), dsnWithoutTenant(dsn)); err != nil {
			log.Fatalf("integration: failed to register harness tenant on the manual server: %v", err)
		}
		// A manual server may or may not carry a gRPC listener, and only the
		// caller knows where it is. Taken verbatim, like GEODE_TEST_DSN.
		grpcDSN = os.Getenv("GEODE_TEST_GRPC_DSN")
		return m.Run()
	}

	ctx := context.Background()

	// Pin the container's port to a free HOST port instead of letting Docker
	// choose an ephemeral one per start. A container started with an ephemeral
	// publish gets a DIFFERENT host port every time it is started, so a
	// restart would move the engine out from under any pool holding the
	// original DSN — and TestIntegration_RecoversFromServerRestart would be
	// measuring a wrong address rather than an orphaned connection.
	hostPort, err := reserveHostPort()
	if err != nil {
		log.Fatalf("integration: failed to reserve a host port: %v", err)
	}
	enginePort, err := network.ParsePort(defaultGeodePort)
	if err != nil {
		log.Fatalf("integration: failed to parse %q: %v", defaultGeodePort, err)
	}

	req := testcontainers.ContainerRequest{
		Image: getGeodeImage(),
		// defaultGeodeGRPCPort is published on an ephemeral host port: only the
		// QUIC port needs a fixed binding (the restart-recovery test), and the
		// gRPC lane re-reads the mapping after every start.
		ExposedPorts: []string{defaultGeodePort, defaultGeodeGRPCPort},
		// Publish the engine on a FIXED host port. testcontainers' ExposedPorts
		// does not accept a host:container spec, so the binding goes through
		// the Docker HostConfig directly.
		// The binding is on LOOPBACK, not 0.0.0.0: a test engine with a
		// seeded admin credential and strict tenancy disabled for the harness
		// tenant has no business being reachable from the rest of the network
		// while the suite runs.
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				enginePort: []network.PortBinding{
					{HostIP: netip.AddrFrom4([4]byte{127, 0, 0, 1}), HostPort: hostPort},
				},
			}
		},
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
			// Match the CI integration service (.gitlab-ci.yml). Strict
			// tenancy makes a tenant-scoped operation from a session with no
			// concrete tenant scope a 42501 refusal instead of a silent
			// all-tenants read, which is the only mode in which
			// integration_tenancy_test.go proves anything.
			"GEODE_STRICT_TENANCY":                  "1",
			"GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING": "1",
			// Bind the gRPC listener on every interface so the published port
			// reaches it. --listen-grpc's address argument is parsed and
			// discarded by the engine (geode 0.8.15 src/server/main.zig:2122-2152),
			// and grpc-zig binds 127.0.0.1 by default (src/server.zig:138 of
			// grpc_zig-0.1.8-7FnvpKz7BQAM3rNTfY4gV_dBgDfEB4JScP6cE--HzBl9,
			// the content hash build.zig.zon:305-310 pins ref=v0.1.9 to),
			// which inside a container is unreachable from the host. See the
			// grpcDSN doc comment.
			"GRPC_ZIG_BIND_ADDR": "0.0.0.0",
		},
		WaitingFor: wait.ForLog(`"status":"listening"`).WithStartupTimeout(startupTimeout),
		Cmd: []string{
			"serve",
			"--listen", "0.0.0.0:3141",
			// The gRPC half of the ENG-4658 gate ("QUIC/TLS 1.3 AND gRPC
			// integration/race tests"). One engine, two listeners: the parity
			// tests must observe the same engine state the QUIC tests do.
			"--listen-grpc", "0.0.0.0:50051",
			"--log-level", "info",
			// SONNY-1218 workaround: the image's default CMD passes --data-dir
			// /data, but this harness overrides Cmd and drops it. Without a
			// storage adapter the engine's node_scan_stream fast path returns
			// empty rows instead of declining, which silently corrupts several
			// integration tests' results. Required until the engine declines
			// the fast path when no adapter is configured. Container-local
			// filesystem only; nothing is mounted from the host.
			"--data-dir", "/data",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("integration: failed to start Geode container: %v", err)
	}
	testContainer = container

	// Change the auto-generated one-time password to a known passphrase.
	if err := setupContainerAuth(ctx, container); err != nil {
		container.Terminate(ctx) //nolint:errcheck
		log.Fatalf("integration: failed to set up container auth: %v", err)
	}

	adminDSN, err := containerDSN(ctx, container, testAdminUser, testAdminPassphrase)
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		log.Fatalf("integration: failed to build container DSN: %v", err)
	}

	// Register the harness tenant, then bind it. Registration goes through the
	// CALL-only tenant DDL surface (there is no CREATE TENANT statement; 0.8.15
	// reports tenant_ddl=false in SHOW CAPABILITIES) and is gated on the
	// admin_tenant GPS action, NOT on the session's tenant scope
	// (geode docs/security/MULTI_TENANT_HARDENING.md, "Tenant lifecycle
	// administration": "Every procedure in the family ... is gated on the
	// admin_tenant GPS action via gps.decide()"). That is why this CALL runs
	// fine from the still-unscoped adminDSN under strict tenancy — only
	// tenant-scoped data statements go through resolveTenantFilter.
	//
	// Registering it also keeps the deployment honest: a registered non-default
	// tenant is the GAP-0848 multi-tenant signal, so strict tenancy stays on
	// even if the env flag were ever dropped.
	if err := ensureHarnessTenant(ctx, adminDSN); err != nil {
		container.Terminate(ctx) //nolint:errcheck
		log.Fatalf("integration: failed to register harness tenant: %v", err)
	}

	sharedDSN = adminDSN + "&tenant=" + harnessTenant

	grpcDSN, err = containerGRPCDSN(ctx, container, testAdminUser, testAdminPassphrase)
	if err != nil {
		// Not fatal: the QUIC lane is the release gate and must still run.
		// requireGRPCDSN turns the empty value into a loud failure in the gRPC
		// lane itself rather than a silent skip.
		log.Printf("integration: gRPC DSN unavailable, gRPC parity tests will fail: %v", err)
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

// reserveHostPort returns the host UDP port the engine container publishes on.
//
// GEODE_TEST_HOST_PORT pins it, which is what a caller needs when the
// ephemeral range is not usable (a restricted sandbox, a firewalled host) or
// when two lanes must be kept off each other's port. Otherwise a free
// ephemeral port is reserved — see freeHostUDPPort for the race that implies.
func reserveHostPort() (string, error) {
	if port := os.Getenv("GEODE_TEST_HOST_PORT"); port != "" {
		return port, nil
	}
	return freeHostUDPPort()
}

// freeHostUDPPort reserves an unused UDP port on the host and returns it. The
// port is released before it is handed to Docker, which leaves a small race
// window; an ephemeral port is not handed out again immediately, so in
// practice the container claims it first.
func freeHostUDPPort() (string, error) {
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve udp port: %w", err)
	}
	defer c.Close() //nolint:errcheck // reservation probe only

	_, port, err := net.SplitHostPort(c.LocalAddr().String())
	if err != nil {
		return "", fmt.Errorf("split reserved addr: %w", err)
	}
	return port, nil
}

// hostPortFromDSN extracts "host:port" from the shared integration DSN so a
// test can build a variant DSN with different credentials. It tolerates both
// shapes the suite uses: the scheme-less "host:port?..." produced by
// containerDSN and the "scheme://user:pass@host:port?..." form accepted by
// ParseDSN.
func hostPortFromDSN(dsn string) string {
	s := dsn
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndexByte(s, '@'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// ensureHarnessTenant registers harnessTenant with the engine using an
// admin session on the unscoped dsn. Re-registering an existing tenant (the
// suite may run against a reused server in manual mode) is the ONLY benign
// outcome, recognised by isTenantAlreadyExists; every other failure —
// including a connection failure — is fatal, so TestMain does not silently
// proceed against a harness that never got its tenant.
func ensureHarnessTenant(ctx context.Context, dsn string) error {
	db, err := sql.Open("geode", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open admin: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping admin: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		"CALL geode.tenant.create('"+harnessTenant+"')"); err != nil {
		if !isTenantAlreadyExists(err) {
			return fmt.Errorf("CALL geode.tenant.create(%q): %w", harnessTenant, err)
		}
		log.Printf("integration: harness tenant %q already registered", harnessTenant)
	}
	return nil
}

// dsnWithoutTenant returns dsn with every tenant/tenant_id parameter removed,
// leaving the rest of the query string byte-for-byte intact (re-encoding it
// through url.Values would re-escape the credential). A test that has to prove
// something about an UNSCOPED session — the strict-tenancy mode probe in
// integration_tenancy_test.go is the only one — must build its DSN through
// here, because sharedDSN now binds harnessTenant.
func dsnWithoutTenant(dsn string) string {
	i := strings.IndexByte(dsn, '?')
	if i < 0 {
		return dsn
	}
	base, query := dsn[:i], dsn[i+1:]

	kept := make([]string, 0, strings.Count(query, "&")+1)
	for _, param := range strings.Split(query, "&") {
		key, _, _ := strings.Cut(param, "=")
		if key == "tenant" || key == "tenant_id" {
			continue
		}
		kept = append(kept, param)
	}
	if len(kept) == 0 {
		return base
	}
	return base + "?" + strings.Join(kept, "&")
}

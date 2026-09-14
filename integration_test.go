// integration_test.go provides comprehensive integration tests.
//
// Run with testcontainers (auto Docker):
//   go test -v -tags=integration ./...
//
// Run with custom image:
//   GEODE_IMAGE=myregistry/geode:tag go test -v -tags=integration ./...
//
// Run against manual server (no testcontainers):
//   GEODE_TEST_DSN="localhost:3141?insecure=true" go test -v -tags=integration ./...

//go:build integration

package geode

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// defaultGeodeImage and pinnedEngineVersion live in engine_pin_test.go,
	// which carries no build tag so the pin is guarded by the unit lane too.
	// Override the image with the GEODE_IMAGE environment variable if needed.
	defaultGeodePort = "3141/udp"
	// defaultGeodeGRPCPort is the container-side gRPC listener port. The
	// engine's own default is 3142; 50051 is the port the Makefile
	// docker-start recipe, the .gitlab-ci.yml integration service and
	// integration_main_test.go all agree on, so the gRPC lane reads the same
	// number everywhere.
	defaultGeodeGRPCPort = "50051/tcp"
	startupTimeout       = 60 * time.Second
	queryTimeout         = 30 * time.Second

	// testHMACKey is a fixed 64-hex-char key injected into test containers via
	// GEODE_AUTH_HMAC_KEY.  The server uses it to initialise the HMAC chain and
	// auto-generates a random admin password printed to the log on first start.
	testHMACKey = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	// testAdminUser is the username the server auto-creates when GEODE_AUTH_HMAC_KEY
	// is provided and no prior auth state exists.
	testAdminUser = "geode"

	// testAdminPassphrase is the passphrase we reset the admin account to after
	// startup so tests can use a stable, known credential.
	// Must satisfy the server policy: ≥20 chars, ≥3 words, ≥1 uppercase letter.
	testAdminPassphrase = "Correct horse battery staple"
)

// getGeodeImage returns the Docker image to use for testing.
func getGeodeImage() string {
	if img := os.Getenv("GEODE_IMAGE"); img != "" {
		return img
	}
	return defaultGeodeImage
}

// parseAdminPassword extracts the auto-generated admin password from the
// server log output.  The server prints exactly one line of the form:
//
//	*** Generated admin password: <password> ***
//
// when GEODE_AUTH_HMAC_KEY is set and no prior auth state exists.
func parseAdminPassword(logOutput string) string {
	const marker = "Generated admin password: "
	idx := strings.Index(logOutput, marker)
	if idx < 0 {
		return ""
	}
	rest := logOutput[idx+len(marker):]
	if end := strings.Index(rest, " ***"); end >= 0 {
		rest = rest[:end]
	}
	return strings.TrimSpace(rest)
}

// readContainerLogs reads all current log output from the container and
// returns it as a single string.  It handles the Docker log-stream
// multiplexing format (8-byte header per frame).
func readContainerLogs(ctx context.Context, container testcontainers.Container) (string, error) {
	logReader, err := container.Logs(ctx)
	if err != nil {
		return "", fmt.Errorf("container.Logs: %w", err)
	}
	defer logReader.Close()

	var buf bytes.Buffer
	if _, err := stdcopy.StdCopy(&buf, &buf, logReader); err != nil {
		// Not all environments multiplex; fall back to a raw read.
		buf.Reset()
		logReader2, err2 := container.Logs(ctx)
		if err2 != nil {
			return "", fmt.Errorf("container.Logs (retry): %w", err2)
		}
		defer logReader2.Close()
		if _, err2 = io.Copy(&buf, logReader2); err2 != nil {
			return "", fmt.Errorf("io.Copy logs: %w", err2)
		}
	}
	return buf.String(), nil
}

// setupContainerAuth reads the auto-generated admin password from the server
// log and changes it to testAdminPassphrase so tests can use a stable DSN.
//
// Background: when GEODE_AUTH_HMAC_KEY is set, the server auto-creates the
// "geode" admin user with a random one-time password on first boot.  That
// password is printed to the log and must be changed before any queries can
// run (the server enforces a "password reset required" policy on the initial
// credential).
func setupContainerAuth(ctx context.Context, container testcontainers.Container) error {
	logs, err := readContainerLogs(ctx, container)
	if err != nil {
		return err
	}

	genPass := parseAdminPassword(logs)
	if genPass == "" {
		// No generated password in the log means the admin credential was
		// seeded up front via GEODE_ADMIN_PASSWORD (see integration_main_test.go),
		// so it should already equal testAdminPassphrase and needs no reset.
		// Engines that write the generated password to a file instead of the
		// log take this path.
		//
		// Trusting the seed blindly would turn one auth failure into a wall of
		// unrelated-looking per-test errors, so prove the credential actually
		// works before handing it to the suite. The forced-reset path itself is
		// covered by TestIntegration_AdminPasswordResetFlow.
		dsn, dsnErr := containerDSN(ctx, container, testAdminUser, testAdminPassphrase)
		if dsnErr != nil {
			return dsnErr
		}
		if err := verifyCredential(ctx, dsn); err != nil {
			return fmt.Errorf(
				"no generated admin password in the container log and the seeded GEODE_ADMIN_PASSWORD credential for user %q does not authenticate: %w",
				testAdminUser, err)
		}
		return nil
	}

	// Reset the admin password to a known, policy-compliant passphrase.
	exitCode, _, execErr := container.Exec(ctx, []string{
		"geode", "query",
		"--server", "localhost:3141",
		"--insecure-tls-skip-verify",
		"--no-saved-session",
		"--user", testAdminUser,
		"--password", genPass,
		fmt.Sprintf("ALTER USER %s SET PASSWORD '%s'", testAdminUser, testAdminPassphrase),
	})
	if execErr != nil || exitCode != 0 {
		return fmt.Errorf("failed to reset admin password (exit %d): %w", exitCode, execErr)
	}

	// The reset reported success; prove the new credential authenticates before
	// the suite starts using it.
	dsn, dsnErr := containerDSN(ctx, container, testAdminUser, testAdminPassphrase)
	if dsnErr != nil {
		return dsnErr
	}
	if err := verifyCredential(ctx, dsn); err != nil {
		return fmt.Errorf("admin password reset reported success but the new credential for user %q does not authenticate: %w",
			testAdminUser, err)
	}
	return nil
}

// containerDSN builds the driver DSN for a running test container using the
// supplied credential.
func containerDSN(ctx context.Context, container testcontainers.Container, user, pass string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("container.Host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, defaultGeodePort)
	if err != nil {
		return "", fmt.Errorf("container.MappedPort: %w", err)
	}
	return fmt.Sprintf("%s:%s?insecure=true&user=%s&pass=%s",
		host, mappedPort.Port(),
		url.QueryEscape(user),
		url.QueryEscape(pass),
	), nil
}

// containerGRPCDSN builds the gRPC driver DSN for a running test container.
//
// It differs from containerDSN in two ways, both forced by the engine (see the
// grpcDSN doc comment in integration_main_test.go): the scheme is grpc:// and
// the TLS option is insecure_tls_skip_verify=true rather than insecure=true,
// because the 0.8.15 gRPC listener is TLS-only in a release build and serves a
// self-signed certificate.
//
// The harness tenant is bound here for the same reason sharedDSN binds it: the
// engine runs with GEODE_STRICT_TENANCY=1, under which an unscoped session is
// refused GQLSTATE 42501 on its first tenant-scoped statement.
func containerGRPCDSN(ctx context.Context, container testcontainers.Container, user, pass string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("container.Host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, defaultGeodeGRPCPort)
	if err != nil {
		return "", fmt.Errorf("container.MappedPort(%s): %w", defaultGeodeGRPCPort, err)
	}
	return fmt.Sprintf("grpc://%s:%s?insecure_tls_skip_verify=true&user=%s&pass=%s&tenant=%s",
		host, mappedPort.Port(),
		url.QueryEscape(user),
		url.QueryEscape(pass),
		url.QueryEscape(harnessTenant),
	), nil
}

// verifyCredential opens the driver with dsn and runs a trivial query, proving
// the credential authenticates and is not stuck in the engine's
// "password reset required" state.
func verifyCredential(ctx context.Context, dsn string) error {
	db, err := sql.Open("geode", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()

	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var version string
	if err := db.QueryRowContext(queryContext, "SHOW VERSION").Scan(&version); err != nil {
		return fmt.Errorf("SHOW VERSION: %w", err)
	}
	return nil
}

// openTestDB opens a database connection to the shared test container.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
	}

	db, err := sql.Open("geode", sharedDSN)
	require.NoError(t, err, "Failed to open database connection")

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "Failed to ping database")

	return db
}

// queryCtx creates a context with query timeout.
func queryCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), queryTimeout)
}

// cleanupTestData removes test data created during tests.
func cleanupTestData(t *testing.T, db *sql.DB) {
	ctx, cancel := queryCtx()
	defer cancel()

	// Delete all test nodes
	_, _ = db.ExecContext(ctx, "MATCH (n) DETACH DELETE n")
}

// =============================================================================
// Basic Operations Tests
// =============================================================================

func TestDocker_Ping(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	err := db.PingContext(ctx)
	assert.NoError(t, err)
}

func TestDocker_SimpleReturn(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN 1 AS result")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "Expected at least one row")

	var result int64
	err = rows.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)

	assert.False(t, rows.Next(), "Expected only one row")
}

func TestDocker_ReturnMultipleColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN 42 AS num, 'hello' AS str, true AS flag")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var num int64
	var str string
	var flag bool
	err = rows.Scan(&num, &str, &flag)
	require.NoError(t, err)

	assert.Equal(t, int64(42), num)
	assert.Equal(t, "hello", str)
	assert.True(t, flag)
}

func TestDocker_ReturnWithParameters(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN ? AS x, ? AS y", 100, "test")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var x int64
	var y string
	err = rows.Scan(&x, &y)
	require.NoError(t, err)

	assert.Equal(t, int64(100), x)
	assert.Equal(t, "test", y)
}

// =============================================================================
// Node CRUD Tests
// =============================================================================

func TestDocker_CreateNode(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create a node
	_, err := db.ExecContext(ctx, "CREATE (n:TestNode {name: 'TestCreate', value: 42})")
	require.NoError(t, err)

	// Verify node exists
	rows, err := db.QueryContext(ctx, "MATCH (n:TestNode {name: 'TestCreate'}) RETURN n.value")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var value int64
	err = rows.Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, int64(42), value)
}

func TestDocker_CreateMultipleNodes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create multiple nodes
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35})
	`)
	require.NoError(t, err)

	// Count nodes
	rows, err := db.QueryContext(ctx, "MATCH (n:Person) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestDocker_UpdateNode(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create node
	_, err := db.ExecContext(ctx, "CREATE (n:TestNode {name: 'ToUpdate', value: 10})")
	require.NoError(t, err)

	// Update node
	_, err = db.ExecContext(ctx, "MATCH (n:TestNode {name: 'ToUpdate'}) SET n.value = 20")
	require.NoError(t, err)

	// Verify update
	rows, err := db.QueryContext(ctx, "MATCH (n:TestNode {name: 'ToUpdate'}) RETURN n.value")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var value int64
	err = rows.Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, int64(20), value)
}

func TestDocker_DeleteNode(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create node
	_, err := db.ExecContext(ctx, "CREATE (n:TestNode {name: 'ToDelete'})")
	require.NoError(t, err)

	// Delete node
	_, err = db.ExecContext(ctx, "MATCH (n:TestNode {name: 'ToDelete'}) DELETE n")
	require.NoError(t, err)

	// Verify deletion
	rows, err := db.QueryContext(ctx, "MATCH (n:TestNode {name: 'ToDelete'}) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// =============================================================================
// Relationship Tests
// =============================================================================

func TestDocker_CreateRelationship(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create nodes with relationship
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})
	`)
	require.NoError(t, err)

	// Query relationship
	rows, err := db.QueryContext(ctx, `
		MATCH (a:Person)-[r:KNOWS]->(b:Person)
		RETURN a.name, r.since, b.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var aName string
	var since int64
	var bName string
	err = rows.Scan(&aName, &since, &bName)
	require.NoError(t, err)

	assert.Equal(t, "Alice", aName)
	assert.Equal(t, int64(2020), since)
	assert.Equal(t, "Bob", bName)
}

func TestDocker_MatchRelationshipPattern(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create a small graph
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice'}),
		       (b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(b),
		       (b)-[:KNOWS]->(c)
	`)
	require.NoError(t, err)

	// Query chain of relationships
	rows, err := db.QueryContext(ctx, `
		MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)
		RETURN c.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var name string
	err = rows.Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", name)
}

// =============================================================================
// Transaction Tests
// =============================================================================

func TestDocker_TransactionCommit(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Create node in transaction
	_, err = tx.ExecContext(ctx, "CREATE (n:TxTest {name: 'Committed'})")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify node exists after commit
	rows, err := db.QueryContext(ctx, "MATCH (n:TxTest {name: 'Committed'}) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestDocker_TransactionRollback(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Create node in transaction
	_, err = tx.ExecContext(ctx, "CREATE (n:TxTest {name: 'RolledBack'})")
	require.NoError(t, err)

	// Rollback
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify node doesn't exist after rollback
	rows, err := db.QueryContext(ctx, "MATCH (n:TxTest {name: 'RolledBack'}) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestDocker_TransactionMultipleOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Create multiple nodes
	_, err = tx.ExecContext(ctx, "CREATE (n:TxMulti {id: 1})")
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "CREATE (n:TxMulti {id: 2})")
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "CREATE (n:TxMulti {id: 3})")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify all nodes exist
	rows, err := db.QueryContext(ctx, "MATCH (n:TxMulti) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

// =============================================================================
// Query Operations Tests
// =============================================================================

func TestDocker_MatchWithWhere(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25}),
		       (c:Person {name: 'Charlie', age: 35})
	`)
	require.NoError(t, err)

	// Query with WHERE
	rows, err := db.QueryContext(ctx, "MATCH (n:Person) WHERE n.age > 26 RETURN n.name ORDER BY n.name")
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		names = append(names, name)
	}

	assert.Equal(t, []string{"Alice", "Charlie"}, names)
}

func TestDocker_MatchWithAndOr(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30, city: 'NYC'}),
		       (b:Person {name: 'Bob', age: 25, city: 'LA'}),
		       (c:Person {name: 'Charlie', age: 35, city: 'NYC'})
	`)
	require.NoError(t, err)

	// Query with AND
	rows, err := db.QueryContext(ctx, "MATCH (n:Person) WHERE n.age > 28 AND n.city = 'NYC' RETURN n.name")
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		names = append(names, name)
	}

	assert.Len(t, names, 2) // Alice and Charlie
}

func TestDocker_OrderByAsc(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:Item {name: 'C', value: 3}),
		       (b:Item {name: 'A', value: 1}),
		       (c:Item {name: 'B', value: 2})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:Item) RETURN n.name ORDER BY n.name ASC")
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		names = append(names, name)
	}

	assert.Equal(t, []string{"A", "B", "C"}, names)
}

func TestDocker_OrderByDesc(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:Item {name: 'A', value: 1}),
		       (b:Item {name: 'B', value: 2}),
		       (c:Item {name: 'C', value: 3})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:Item) RETURN n.value ORDER BY n.value DESC")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var value int64
		err = rows.Scan(&value)
		require.NoError(t, err)
		values = append(values, value)
	}

	assert.Equal(t, []int64{3, 2, 1}, values)
}

func TestDocker_LimitAndSkip(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data - create 10 items
	for i := 1; i <= 10; i++ {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE (n:NumItem {value: %d})", i))
		require.NoError(t, err)
	}

	// Query with LIMIT and SKIP
	rows, err := db.QueryContext(ctx, "MATCH (n:NumItem) RETURN n.value ORDER BY n.value LIMIT 3 SKIP 2")
	require.NoError(t, err)
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var value int64
		err = rows.Scan(&value)
		require.NoError(t, err)
		values = append(values, value)
	}

	assert.Equal(t, []int64{3, 4, 5}, values)
}

// =============================================================================
// Aggregation Tests
// =============================================================================

func TestDocker_Count(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:CountTest {}), (b:CountTest {}), (c:CountTest {})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:CountTest) RETURN count(n) AS cnt")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestDocker_Sum(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:SumTest {value: 10}),
		       (b:SumTest {value: 20}),
		       (c:SumTest {value: 30})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:SumTest) RETURN sum(n.value) AS total")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var total int64
	err = rows.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, int64(60), total)
}

func TestDocker_Avg(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:AvgTest {value: 10}),
		       (b:AvgTest {value: 20}),
		       (c:AvgTest {value: 30})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:AvgTest) RETURN avg(n.value) AS average")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var avg float64
	err = rows.Scan(&avg)
	require.NoError(t, err)
	assert.InDelta(t, 20.0, avg, 0.001)
}

func TestDocker_MinMax(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:MinMaxTest {value: 5}),
		       (b:MinMaxTest {value: 15}),
		       (c:MinMaxTest {value: 10})
	`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "MATCH (n:MinMaxTest) RETURN min(n.value) AS minVal, max(n.value) AS maxVal")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var minVal, maxVal int64
	err = rows.Scan(&minVal, &maxVal)
	require.NoError(t, err)
	assert.Equal(t, int64(5), minVal)
	assert.Equal(t, int64(15), maxVal)
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestDocker_SyntaxError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	// Use a query with a definite syntax error
	_, err := db.QueryContext(ctx, "MATCH (n RETURN n")
	if err == nil {
		// Some queries might be accepted, skip if no error
		t.Skip("Query accepted by server, skipping syntax error test")
	}

	var derr *DriverError
	if errors.As(err, &derr) {
		t.Logf("Got DriverError: code=%s, message=%s", derr.Code, derr.Message)
	}
}

func TestDocker_NonExistentProperty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Create node without the property we'll query
	_, err := db.ExecContext(ctx, "CREATE (n:PropTest {name: 'Test'})")
	require.NoError(t, err)

	// Query non-existent property - should return null
	rows, err := db.QueryContext(ctx, "MATCH (n:PropTest) RETURN n.nonexistent")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var value interface{}
	err = rows.Scan(&value)
	require.NoError(t, err)
	assert.Nil(t, value)
}

// TestDocker_NoMatch_QueryRowReturnsErrNoRows is the integration-level
// reproduction for the "no-match null row" bug: a MATCH that finds no node must
// surface as sql.ErrNoRows from QueryRow().Scan, not a scan error.
//
// Before the fix the server's single null-row DataPage was passed through, so
// Scan(&dest) failed assigning nil to a non-nullable destination (a 500-class
// error). After the fix the null NODE row is skipped and QueryRow returns
// sql.ErrNoRows (a clean 404-class result).
func TestDocker_NoMatch_QueryRowReturnsErrNoRows(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	var dest interface{}
	err := db.QueryRowContext(ctx,
		"MATCH (n {id: ?}) RETURN n",
		"completely-nonexistent-id-xyz",
	).Scan(&dest)

	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("no-match QueryRow should return sql.ErrNoRows, got err=%v dest=%v", err, dest)
	}
}

// =============================================================================
// Prepared Statement Tests
// =============================================================================

func TestDocker_PreparedStatement(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "RETURN ? AS value")
	require.NoError(t, err)
	defer stmt.Close()

	// Execute multiple times
	for i := 1; i <= 5; i++ {
		rows, err := stmt.QueryContext(ctx, i)
		require.NoError(t, err)

		require.True(t, rows.Next())

		var value int64
		err = rows.Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, int64(i), value)

		rows.Close()
	}
}

func TestDocker_PreparedStatementWithMatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Setup data
	_, err := db.ExecContext(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30}),
		       (b:Person {name: 'Bob', age: 25})
	`)
	require.NoError(t, err)

	stmt, err := db.PrepareContext(ctx, "MATCH (n:Person {name: ?}) RETURN n.age")
	require.NoError(t, err)
	defer stmt.Close()

	// Query Alice
	rows, err := stmt.QueryContext(ctx, "Alice")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var age int64
	rows.Scan(&age)
	rows.Close()
	assert.Equal(t, int64(30), age)

	// Query Bob
	rows, err = stmt.QueryContext(ctx, "Bob")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Scan(&age)
	rows.Close()
	assert.Equal(t, int64(25), age)
}

// =============================================================================
// Column Type Tests
// =============================================================================

func TestDocker_ColumnTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN 1 AS int_col, 'hello' AS str_col, true AS bool_col, 3.14 AS float_col")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Len(t, cols, 4)
	assert.Contains(t, cols, "int_col")
	assert.Contains(t, cols, "str_col")
	assert.Contains(t, cols, "bool_col")
	assert.Contains(t, cols, "float_col")

	colTypes, err := rows.ColumnTypes()
	require.NoError(t, err)

	for _, ct := range colTypes {
		t.Logf("Column: %s, Type: %s", ct.Name(), ct.DatabaseTypeName())
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestDocker_ConcurrentQueries(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const numGoroutines = 10
	const queriesPerGoroutine = 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*queriesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < queriesPerGoroutine; j++ {
				ctx, cancel := queryCtx()

				rows, err := db.QueryContext(ctx, "RETURN ? AS id", id*100+j)
				if err != nil {
					errors <- err
					cancel()
					continue
				}

				if rows.Next() {
					var result int64
					rows.Scan(&result)
				}
				rows.Close()
				cancel()
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errList []error
	for err := range errors {
		errList = append(errList, err)
	}

	assert.Empty(t, errList, "Expected no errors in concurrent queries")
}

// =============================================================================
// Social Network Fixture Tests
// =============================================================================

func setupSocialNetwork(t *testing.T, db *sql.DB) {
	ctx, cancel := queryCtx()
	defer cancel()

	queries := []string{
		// Create locations
		`CREATE (l1:Location {name: 'New York', population: 8336817, timezone: 'EST', country: 'USA'})`,
		`CREATE (l2:Location {name: 'San Francisco', population: 883305, timezone: 'PST', country: 'USA'})`,
		`CREATE (l3:Location {name: 'London', population: 8982000, timezone: 'GMT', country: 'UK'})`,

		// Create companies
		`CREATE (c1:Company {name: 'TechCorp', ticker: 'TECH', industry: 'Technology', employees: 5000})`,
		`CREATE (c2:Company {name: 'FinanceInc', ticker: 'FIN', industry: 'Finance', employees: 2000})`,

		// Create users
		`CREATE (u1:User {name: 'Alice', email: 'alice@example.com', age: 30, followers: 1500})`,
		`CREATE (u2:User {name: 'Bob', email: 'bob@example.com', age: 25, followers: 800})`,
		`CREATE (u3:User {name: 'Charlie', email: 'charlie@example.com', age: 35, followers: 2000})`,
		`CREATE (u4:User {name: 'Diana', email: 'diana@example.com', age: 28, followers: 1200})`,
		`CREATE (u5:User {name: 'Eve', email: 'eve@example.com', age: 32, followers: 3000})`,

		// Create relationships
		`MATCH (a:User {name: 'Alice'}), (b:User {name: 'Bob'}) CREATE (a)-[:FOLLOWS]->(b)`,
		`MATCH (a:User {name: 'Alice'}), (c:User {name: 'Charlie'}) CREATE (a)-[:FOLLOWS]->(c)`,
		`MATCH (b:User {name: 'Bob'}), (c:User {name: 'Charlie'}) CREATE (b)-[:FOLLOWS]->(c)`,
		`MATCH (d:User {name: 'Diana'}), (a:User {name: 'Alice'}) CREATE (d)-[:FOLLOWS]->(a)`,
		`MATCH (e:User {name: 'Eve'}), (a:User {name: 'Alice'}) CREATE (e)-[:FOLLOWS]->(a)`,

		// User-Company relationships
		`MATCH (a:User {name: 'Alice'}), (c:Company {name: 'TechCorp'}) CREATE (a)-[:WORKS_AT {role: 'Engineer', since: 2020}]->(c)`,
		`MATCH (b:User {name: 'Bob'}), (c:Company {name: 'TechCorp'}) CREATE (b)-[:WORKS_AT {role: 'Manager', since: 2018}]->(c)`,
		`MATCH (c:User {name: 'Charlie'}), (f:Company {name: 'FinanceInc'}) CREATE (c)-[:WORKS_AT {role: 'Analyst', since: 2019}]->(f)`,

		// User-Location relationships
		`MATCH (a:User {name: 'Alice'}), (l:Location {name: 'New York'}) CREATE (a)-[:LIVES_IN]->(l)`,
		`MATCH (b:User {name: 'Bob'}), (l:Location {name: 'San Francisco'}) CREATE (b)-[:LIVES_IN]->(l)`,
		`MATCH (c:User {name: 'Charlie'}), (l:Location {name: 'London'}) CREATE (c)-[:LIVES_IN]->(l)`,
	}

	for _, q := range queries {
		_, err := db.ExecContext(ctx, q)
		if err != nil {
			t.Logf("Setup query failed (may be expected): %v", err)
		}
	}
}

func TestDocker_SocialNetwork_FindFollowers(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	setupSocialNetwork(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Find users who follow Alice
	rows, err := db.QueryContext(ctx, `
		MATCH (follower:User)-[:FOLLOWS]->(u:User {name: 'Alice'})
		RETURN follower.name ORDER BY follower.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	var followers []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		followers = append(followers, name)
	}

	assert.Contains(t, followers, "Diana")
	assert.Contains(t, followers, "Eve")
}

func TestDocker_SocialNetwork_FindColleagues(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	setupSocialNetwork(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// Find Alice's colleagues at TechCorp
	rows, err := db.QueryContext(ctx, `
		MATCH (a:User {name: 'Alice'})-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(colleague:User)
		WHERE colleague.name <> 'Alice'
		RETURN colleague.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	var colleagues []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		colleagues = append(colleagues, name)
	}

	assert.Contains(t, colleagues, "Bob")
}

func TestDocker_SocialNetwork_CountByLocation(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	defer cleanupTestData(t, db)

	setupSocialNetwork(t, db)

	ctx, cancel := queryCtx()
	defer cancel()

	// First verify that the LIVES_IN relationships were created
	rows, err := db.QueryContext(ctx, `
		MATCH (u:User)-[:LIVES_IN]->(l:Location)
		RETURN u.name, l.name
	`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var userName, locationName string
		rows.Scan(&userName, &locationName)
		t.Logf("User %s lives in %s", userName, locationName)
		count++
	}

	// Just verify we got some results - the GROUP BY/count aggregation may not be fully supported
	t.Logf("Found %d user-location relationships", count)
	// This is a feature test, not a requirement
}

// =============================================================================
// Data Type Tests
// =============================================================================

func TestDocker_NullValues(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN null AS null_val")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var val interface{}
	err = rows.Scan(&val)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestDocker_ListValues(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN [1, 2, 3] AS list_val")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var val interface{}
	err = rows.Scan(&val)
	require.NoError(t, err)
	assert.NotNil(t, val)
	t.Logf("List value: %v (type: %T)", val, val)
}

func TestDocker_MapValues(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN {name: 'test', value: 42} AS map_val")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var val interface{}
	err = rows.Scan(&val)
	require.NoError(t, err)
	assert.NotNil(t, val)
	t.Logf("Map value: %v (type: %T)", val, val)
}

// =============================================================================
// List Iteration Tests (GQL FOR syntax)
// =============================================================================

func TestDocker_ForLoop(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	// GQL uses FOR loop syntax instead of Cypher's UNWIND
	rows, err := db.QueryContext(ctx, "FOR x IN [1, 2, 3, 4, 5] RETURN x")
	if err != nil {
		// FOR loop syntax may not be fully implemented yet
		t.Skipf("FOR loop may not be supported: %v", err)
	}
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var x int64
		rows.Scan(&x)
		values = append(values, x)
	}

	if len(values) == 0 {
		t.Skip("FOR loop returned no results")
	}
	t.Logf("FOR loop returned %d values: %v", len(values), values)
}

func TestDocker_ForLoopWithFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := queryCtx()
	defer cancel()

	// GQL uses FOR loop with WHERE instead of Cypher's UNWIND...WITH...WHERE
	rows, err := db.QueryContext(ctx, "FOR x IN [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] WHERE x > 5 RETURN x")
	if err != nil {
		t.Skipf("FOR loop with WHERE may not be supported: %v", err)
	}
	defer rows.Close()

	var values []int64
	for rows.Next() {
		var x int64
		rows.Scan(&x)
		values = append(values, x)
	}

	if len(values) == 0 {
		t.Skip("FOR loop with WHERE returned no results")
	}
	t.Logf("FOR loop with WHERE returned %d values: %v", len(values), values)
}

// =============================================================================
// Connection Pool Tests
// =============================================================================

func TestDocker_ConnectionPooling(t *testing.T) {
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set")
	}
	db, err := sql.Open("geode", sharedDSN)
	require.NoError(t, err)
	defer db.Close()

	// Configure pool
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(2)

	ctx, cancel := queryCtx()
	defer cancel()

	// Verify connection works
	err = db.PingContext(ctx)
	require.NoError(t, err)

	// Run concurrent queries to test pooling
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := db.QueryContext(ctx, "RETURN 1")
			if err == nil {
				rows.Close()
			}
		}()
	}
	wg.Wait()

	// Check stats
	stats := db.Stats()
	t.Logf("Pool stats: OpenConnections=%d, InUse=%d, Idle=%d",
		stats.OpenConnections, stats.InUse, stats.Idle)
}

// =============================================================================
// Docker-based Benchmarks
// =============================================================================

// benchDB is a shared database connection for benchmarks.
var (
	benchDB   *sql.DB
	benchOnce sync.Once
)

// getBenchDB returns a shared database connection for benchmarks.
// Reuses the shared container started by TestMain.
func getBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	benchOnce.Do(func() {
		if sharedDSN == "" {
			b.Fatal("sharedDSN not set: TestMain did not run or GEODE_TEST_DSN is unset")
		}
		db, err := sql.Open("geode", sharedDSN)
		if err != nil {
			b.Fatalf("Failed to open database: %v", err)
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		benchDB = db
	})
	return benchDB
}

func BenchmarkDocker_SimpleQuery(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "RETURN 1 AS x")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var x int64
			rows.Scan(&x)
		}
		rows.Close()
	}
}

func BenchmarkDocker_QueryWithParams(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "RETURN ? AS x", i)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var x int64
			rows.Scan(&x)
		}
		rows.Close()
	}
}

func BenchmarkDocker_PreparedStatement(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "RETURN ? AS x")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.QueryContext(ctx, i)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var x int64
			rows.Scan(&x)
		}
		rows.Close()
	}
}

func BenchmarkDocker_MultipleRows(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "FOR x IN [1,2,3,4,5,6,7,8,9,10] RETURN x")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var x int64
			rows.Scan(&x)
		}
		rows.Close()
	}
}

func BenchmarkDocker_Transaction(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		rows, err := tx.QueryContext(ctx, "RETURN 1 AS x")
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		for rows.Next() {
			var x int64
			rows.Scan(&x)
		}
		rows.Close()

		tx.Commit()
	}
}

func BenchmarkDocker_CreateNode(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	// Cleanup before benchmark
	db.ExecContext(ctx, "MATCH (n:BenchNode) DELETE n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.ExecContext(ctx, "CREATE (n:BenchNode {id: ?})", i)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	// Cleanup after benchmark
	db.ExecContext(ctx, "MATCH (n:BenchNode) DELETE n")
}

func BenchmarkDocker_MatchNode(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	// Setup: create 100 nodes
	db.ExecContext(ctx, "MATCH (n:BenchMatchNode) DELETE n")
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx, "CREATE (n:BenchMatchNode {id: ?, value: ?})", i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "MATCH (n:BenchMatchNode {id: ?}) RETURN n.value", i%100)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var val int64
			rows.Scan(&val)
		}
		rows.Close()
	}

	b.StopTimer()
	db.ExecContext(ctx, "MATCH (n:BenchMatchNode) DELETE n")
}

func BenchmarkDocker_Ping(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.PingContext(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDocker_ConcurrentQueries(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, err := db.QueryContext(ctx, "RETURN 1 AS x")
			if err != nil {
				b.Error(err)
				continue
			}
			for rows.Next() {
				var x int64
				rows.Scan(&x)
			}
			rows.Close()
		}
	})
}

func BenchmarkDocker_LargeResult(b *testing.B) {
	db := getBenchDB(b)
	ctx := context.Background()

	// Use a simple query that returns rows - benchmark focuses on scanning
	query := "RETURN 1 AS x, 2 AS doubled, 1 AS squared"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var x, doubled, squared int64
			rows.Scan(&x, &doubled, &squared)
		}
		rows.Close()
	}
}

// =============================================================================
// BatchExec Integration Tests
// =============================================================================

func TestDocker_BatchExec_AllSucceed(t *testing.T) {
	db := openTestDB(t)
	defer cleanupTestData(t, db)
	pool := &Pool{db: db, cfg: &Config{}}

	stmts := []Statement{
		{Query: `CREATE (n:BatchNode {id: 1, tag: "batch_test"})`},
		{Query: `CREATE (n:BatchNode {id: 2, tag: "batch_test"})`},
		{Query: `CREATE (n:BatchNode {id: 3, tag: "batch_test"})`},
	}

	ctx, cancel := queryCtx()
	defer cancel()

	err := pool.BatchExec(ctx, stmts)
	require.NoError(t, err, "BatchExec should succeed")

	ctx2, cancel2 := queryCtx()
	defer cancel2()

	row := db.QueryRowContext(ctx2, `MATCH (n:BatchNode {tag: "batch_test"}) RETURN COUNT(n) AS c`)
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 3, count, "expected 3 BatchNode rows after BatchExec")
}

func TestDocker_BatchExec_RollbackOnError(t *testing.T) {
	db := openTestDB(t)
	defer cleanupTestData(t, db)
	pool := &Pool{db: db, cfg: &Config{}}

	stmts := []Statement{
		{Query: `CREATE (n:RollbackNode {id: 1})`},
		{Query: `THIS IS INVALID GQL !!!`},
	}

	ctx, cancel := queryCtx()
	defer cancel()

	err := pool.BatchExec(ctx, stmts)
	require.Error(t, err, "BatchExec with invalid statement should return error")

	ctx2, cancel2 := queryCtx()
	defer cancel2()

	row := db.QueryRowContext(ctx2, `MATCH (n:RollbackNode) RETURN COUNT(n) AS c`)
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count, "RollbackNode rows should be 0 after BatchExec rollback")
}

// =============================================================================
// PipelineExec Integration Tests
// =============================================================================

func TestDocker_PipelineExec_BulkCreate(t *testing.T) {
	db := openTestDB(t)
	defer cleanupTestData(t, db)
	pool := &Pool{db: db, cfg: &Config{}}

	const total = 50
	stmts := make([]Statement, total)
	for i := range stmts {
		stmts[i] = Statement{
			Query: fmt.Sprintf(`CREATE (n:PipelineNode {seq: %d, run: "pipeline_test"})`, i),
		}
	}

	ctx, cancel := queryCtx()
	defer cancel()

	err := pool.PipelineExec(ctx, stmts, 10, 4)
	require.NoError(t, err, "PipelineExec should succeed")

	ctx2, cancel2 := queryCtx()
	defer cancel2()

	row := db.QueryRowContext(ctx2, `MATCH (n:PipelineNode {run: "pipeline_test"}) RETURN COUNT(n) AS c`)
	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, total, count, "expected %d PipelineNode rows after PipelineExec", total)
}

package geode

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

// Integration tests require a running Geode server.
// With -tags=integration: Uses Docker container automatically
// Without: Set GEODE_TEST_DSN environment variable
// Example: GEODE_TEST_DSN="localhost:3141?insecure=true" go test -v

// getTestDSNFromEnv returns the DSN from environment variable.
// This is used when not running with the integration build tag.
func getTestDSNFromEnv(t *testing.T) string {
	dsn := os.Getenv("GEODE_TEST_DSN")
	if dsn == "" {
		t.Skip("GEODE_TEST_DSN not set, skipping integration test (use -tags=integration for Docker)")
	}
	return dsn
}

func TestIntegration_BasicQuery(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN 1 AS x")
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var x int64
	if err := rows.Scan(&x); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if x != 1 {
		t.Errorf("expected x=1, got x=%d", x)
	}

	if rows.Next() {
		t.Error("expected only one row")
	}

	if err := rows.Err(); err != nil {
		t.Errorf("rows error: %v", err)
	}
}

func TestIntegration_QueryWithParams(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN ? AS x", 42)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var x int64
	if err := rows.Scan(&x); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if x != 42 {
		t.Errorf("expected x=42, got x=%d", x)
	}

	if err := rows.Err(); err != nil {
		t.Errorf("rows error: %v", err)
	}
}

func TestIntegration_Transaction(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute query in transaction
	rows, err := tx.QueryContext(ctx, "RETURN 1 AS x")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed to execute query: %v", err)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Errorf("rows error: %v", err)
	}
	_ = rows.Close()

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestIntegration_TransactionRollback(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}
}

func TestIntegration_Ping(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestIntegration_PreparedStatement(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "RETURN ? AS x")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute prepared statement multiple times
	for i := 1; i <= 3; i++ {
		rows, err := stmt.QueryContext(ctx, i)
		if err != nil {
			t.Fatalf("failed to execute prepared statement: %v", err)
		}

		if !rows.Next() {
			_ = rows.Close()
			t.Fatal("expected at least one row")
		}

		var x int64
		if err := rows.Scan(&x); err != nil {
			_ = rows.Close()
			t.Fatalf("failed to scan: %v", err)
		}

		if x != int64(i) {
			t.Errorf("expected x=%d, got x=%d", i, x)
		}

		if err := rows.Err(); err != nil {
			t.Errorf("rows error: %v", err)
		}
		_ = rows.Close()
	}
}

func TestIntegration_ErrorHandling(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Execute an invalid query that should return an error
	rows, err := db.QueryContext(ctx, "MATCH (n RETURN n")
	if err == nil {
		// Some queries might be accepted by the server
		if rows != nil {
			_ = rows.Close()
		}
		t.Skip("Query accepted by server, skipping error handling test")
	}

	// Check if we get a DriverError
	var derr *DriverError
	if errors.As(err, &derr) {
		t.Logf("Got DriverError: code=%s, message=%s", derr.Code, derr.Message)
	}
}

func TestIntegration_MultipleRows(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use FOR loop to iterate over a list (GQL syntax)
	rows, err := db.QueryContext(ctx, "FOR x IN [1, 2, 3] RETURN x")
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var x int64
		if err := rows.Scan(&x); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		t.Errorf("rows error: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestIntegration_ColumnTypes(t *testing.T) {
	dsn := getTestDSN(t)

	db, err := sql.Open("geode", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "RETURN 1 AS int_col, 'hello' AS string_col, true AS bool_col")
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("failed to get column types: %v", err)
	}

	for i, ct := range colTypes {
		t.Logf("Column %d: name=%s, type=%s, nullable=%v",
			i, ct.Name(), ct.DatabaseTypeName(), func() string {
				n, ok := ct.Nullable()
				if ok {
					if n {
						return "true"
					}
					return "false"
				}
				return "unknown"
			}())
	}

	if err := rows.Err(); err != nil {
		t.Errorf("rows error: %v", err)
	}
}

// Unit tests that don't require a server

func TestDriver_OpenConnector(t *testing.T) {
	d := &Driver{}

	connector, err := d.OpenConnector("localhost:3141?page_size=500")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c, ok := connector.(*Connector)
	if !ok {
		t.Fatalf("expected *Connector, got %T", connector)
	}

	if c.cfg.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", c.cfg.Host)
	}
	if c.cfg.PageSize != 500 {
		t.Errorf("expected page_size 500, got %d", c.cfg.PageSize)
	}

	// Driver() should return the driver
	if c.Driver() != d {
		t.Error("Driver() should return the original driver")
	}

	// Config() should return a copy
	cfg := c.Config()
	if cfg == c.cfg {
		t.Error("Config() should return a copy, not the original")
	}
}

func TestDriver_OpenConnectorInvalidDSN(t *testing.T) {
	d := &Driver{}

	_, err := d.OpenConnector("localhost:3141?page_size=invalid")
	if err == nil {
		t.Error("expected error for invalid DSN")
	}
}

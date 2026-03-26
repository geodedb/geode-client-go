package graph_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"geodedb.com/geode/graph"
)

func TestNewConn_NilDB(t *testing.T) {
	t.Parallel()

	_, err := graph.NewConn(nil)
	if err == nil {
		t.Error("NewConn(nil) = nil error, want non-nil error")
	}
}

func TestNewConn_WithOptions(t *testing.T) {
	t.Parallel()

	// sql.Open does not connect until first use, so no server is required.
	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := graph.NewConn(db, graph.WithRetry(5, 2*time.Second))
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	if conn.DB() != db {
		t.Error("DB() did not return the same *sql.DB pointer")
	}
}

func TestConn_Close(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}

	conn, err := graph.NewConn(db)
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestConn_ExecContext_NilDB(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := graph.NewConn(db)
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	// Server is not reachable; ExecContext should return an error, not panic.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, execErr := conn.ExecContext(ctx, "MATCH (n) RETURN n")
	if execErr == nil {
		t.Error("ExecContext() against unreachable server: expected error, got nil")
	}
}

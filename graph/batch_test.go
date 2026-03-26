package graph_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"geodedb.com/geode/graph"
)

func TestBatchResult_Fields(t *testing.T) {
	t.Parallel()

	r := graph.BatchResult{
		Total:     3,
		Succeeded: 2,
		Failed:    1,
		Err:       nil,
	}

	if r.Total != 3 {
		t.Errorf("Total = %d, want 3", r.Total)
	}

	if r.Succeeded != 2 {
		t.Errorf("Succeeded = %d, want 2", r.Succeeded)
	}

	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}

	if r.Err != nil {
		t.Errorf("Err = %v, want nil", r.Err)
	}
}

func TestStatement_Fields(t *testing.T) {
	t.Parallel()

	s := graph.Statement{
		Query: "MATCH (n) RETURN n",
		Args:  []any{"arg1", 42},
	}

	if s.Query != "MATCH (n) RETURN n" {
		t.Errorf("Query = %q, want %q", s.Query, "MATCH (n) RETURN n")
	}

	if len(s.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(s.Args))
	}
}

func TestBatchExec_EmptyStatements(t *testing.T) {
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

	result, execErr := conn.BatchExec(context.Background(), []graph.Statement{})
	if execErr != nil {
		t.Errorf("BatchExec(empty) error = %v, want nil", execErr)
	}

	var zero graph.BatchResult
	if result != zero {
		t.Errorf("BatchExec(empty) result = %+v, want zero value", result)
	}
}

func TestBatchExec_StopsOnError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Use a very short timeout and minimal retries to keep the test fast.
	conn, err := graph.NewConn(db, graph.WithRetry(1, 0))
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	stmts := []graph.Statement{
		{Query: "MATCH (n) RETURN n"},
		{Query: "MATCH (m) RETURN m"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, execErr := conn.BatchExec(ctx, stmts)

	// The server is unreachable, so execution should stop after the first failure.
	if result.Failed < 1 {
		t.Errorf("Failed = %d, want >= 1", result.Failed)
	}

	if result.Err == nil {
		t.Error("Err = nil, want non-nil error")
	}

	// execErr is returned for caller convenience; it should equal result.Err.
	if execErr == nil {
		t.Error("BatchExec returned nil error, want non-nil")
	}
}

func TestBatchExec_RespectsContext(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := graph.NewConn(db, graph.WithRetry(1, 0))
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	stmts := []graph.Statement{
		{Query: "MATCH (n) RETURN n"},
	}

	// Cancel the context before calling BatchExec.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, execErr := conn.BatchExec(ctx, stmts)

	if execErr == nil {
		t.Error("BatchExec with cancelled context returned nil error, want context error")
	}

	// With a pre-cancelled context the first statement should be counted as failed.
	if result.Failed < 1 {
		t.Errorf("Failed = %d, want >= 1", result.Failed)
	}
}

package graph_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"geodedb.com/geode/graph"
)

func TestPipelineResult_Fields(t *testing.T) {
	t.Parallel()

	r := graph.PipelineResult{
		Total:     5,
		Succeeded: 4,
		Failed:    1,
		Err:       nil,
	}

	if r.Total != 5 {
		t.Errorf("Total = %d, want 5", r.Total)
	}

	if r.Succeeded != 4 {
		t.Errorf("Succeeded = %d, want 4", r.Succeeded)
	}

	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}

	if r.Err != nil {
		t.Errorf("Err = %v, want nil", r.Err)
	}
}

func TestPipelineExec_EmptyStatements(t *testing.T) {
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

	result, execErr := conn.PipelineExec(context.Background(), []graph.Statement{})
	if execErr != nil {
		t.Errorf("PipelineExec(empty) error = %v, want nil", execErr)
	}

	var zero graph.PipelineResult
	if result != zero {
		t.Errorf("PipelineExec(empty) result = %+v, want zero value", result)
	}
}

func TestPipelineExec_RespectsContext(t *testing.T) {
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
		{Query: "MATCH (m) RETURN m"},
		{Query: "MATCH (o) RETURN o"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, execErr := conn.PipelineExec(ctx, stmts)

	// Server is unreachable; not all statements should succeed.
	if execErr == nil {
		t.Error("PipelineExec with unreachable server returned nil error, want non-nil")
	}

	if result.Total != len(stmts) {
		t.Errorf("Total = %d, want %d", result.Total, len(stmts))
	}

	if result.Failed < 1 {
		t.Errorf("Failed = %d, want >= 1", result.Failed)
	}

	if result.Succeeded+result.Failed != result.Total {
		t.Errorf("Succeeded(%d) + Failed(%d) != Total(%d)", result.Succeeded, result.Failed, result.Total)
	}
}

func TestPipelineExec_Progress(t *testing.T) {
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

	// Verify that WithProgress compiles and does not panic when called.
	var progressCalled bool
	progressOpt := graph.WithProgress(func(completed, total int) {
		progressCalled = true
		if completed < 0 || completed > total {
			t.Errorf("progress callback: completed=%d out of range [0, %d]", completed, total)
		}
	})

	stmts := []graph.Statement{
		{Query: "MATCH (n) RETURN n"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// The call may error (server unreachable) but must not panic.
	result, _ := conn.PipelineExec(ctx, stmts, progressOpt)

	if result.Total != 1 {
		t.Errorf("Total = %d, want 1", result.Total)
	}

	// progressCalled may or may not be true depending on whether any statement
	// succeeded before the timeout. We just verify the type works end-to-end.
	_ = progressCalled
}

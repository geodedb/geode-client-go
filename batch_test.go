// batch_test.go
package geode

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// ── Statement ────────────────────────────────────────────────────────────────

func TestStatement_ZeroValue(t *testing.T) {
	var s Statement
	if s.Query != "" {
		t.Errorf("zero Statement.Query = %q, want empty", s.Query)
	}
	if s.Args != nil {
		t.Errorf("zero Statement.Args should be nil")
	}
}

func TestStatement_WithArgs(t *testing.T) {
	s := Statement{Query: "MATCH (n) RETURN n.id", Args: []any{"x", 42}}
	if s.Query != "MATCH (n) RETURN n.id" {
		t.Errorf("unexpected Query: %q", s.Query)
	}
	if len(s.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(s.Args))
	}
}

// ── BatchExec ────────────────────────────────────────────────────────────────

func TestBatchExec_EmptySlice(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	if err := p.BatchExec(context.Background(), nil); err != nil {
		t.Fatalf("BatchExec(nil): unexpected error: %v", err)
	}
	if err := p.BatchExec(context.Background(), []Statement{}); err != nil {
		t.Fatalf("BatchExec([]): unexpected error: %v", err)
	}
	if md.commits.Load() != 0 {
		t.Errorf("commits = %d, want 0 for empty input", md.commits.Load())
	}
}

func TestBatchExec_AllSucceed(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	stmts := []Statement{
		{Query: "CREATE (n:A)"},
		{Query: "CREATE (n:B)"},
		{Query: "CREATE (n:C)"},
	}
	if err := p.BatchExec(context.Background(), stmts); err != nil {
		t.Fatalf("BatchExec: unexpected error: %v", err)
	}
	if md.commits.Load() != 1 {
		t.Errorf("commits = %d, want 1 (all stmts in single tx)", md.commits.Load())
	}
}

func TestBatchExec_NonRetryableErrorStopsImmediately(t *testing.T) {
	t.Parallel()
	execCount := 0
	syntaxErr := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}
	md := &mockDriver{
		onExec: func(_ string) error {
			execCount++
			if execCount == 2 {
				return syntaxErr
			}
			return nil
		},
	}
	p := newMockPool(t, md)

	stmts := []Statement{
		{Query: "CREATE (n:A)"},
		{Query: "INVALID STATEMENT"},
		{Query: "CREATE (n:C)"},
	}
	err := p.BatchExec(context.Background(), stmts)
	if err == nil {
		t.Fatal("expected error from BatchExec, got nil")
	}
	if !errors.Is(err, syntaxErr) {
		t.Errorf("error = %v, want syntaxErr wrapped", err)
	}
}

func TestBatchExec_ContextCancelled(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.BatchExec(ctx, []Statement{{Query: "CREATE (n:A)"}})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestBatchExec_SingleStatement(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	if err := p.BatchExec(context.Background(), []Statement{{Query: "CREATE (n:X)"}}); err != nil {
		t.Fatalf("BatchExec: %v", err)
	}
	if md.commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", md.commits.Load())
	}
}

// ── PipelineExec ─────────────────────────────────────────────────────────────

func TestPipelineExec_EmptySlice(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	if err := p.PipelineExec(context.Background(), nil, 0, 0); err != nil {
		t.Fatalf("PipelineExec(nil): %v", err)
	}
	if err := p.PipelineExec(context.Background(), []Statement{}, 0, 0); err != nil {
		t.Fatalf("PipelineExec([]): %v", err)
	}
	if md.commits.Load() != 0 {
		t.Errorf("commits = %d, want 0 for empty input", md.commits.Load())
	}
}

func TestPipelineExec_AllSucceed(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	// 10 statements, chunk=3 → chunks: [3,3,3,1] → 4 commits
	stmts := make([]Statement, 10)
	for i := range stmts {
		stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:N%d)", i)}
	}
	if err := p.PipelineExec(context.Background(), stmts, 3, 2); err != nil {
		t.Fatalf("PipelineExec: %v", err)
	}
	if md.commits.Load() != 4 {
		t.Errorf("commits = %d, want 4", md.commits.Load())
	}
}

func TestPipelineExec_DefaultChunkAndWorkers(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	// 50 statements, default chunk=100 → 1 chunk → 1 commit
	stmts := make([]Statement, 50)
	for i := range stmts {
		stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:X%d)", i)}
	}
	if err := p.PipelineExec(context.Background(), stmts, 0, 0); err != nil {
		t.Fatalf("PipelineExec: %v", err)
	}
	if md.commits.Load() != 1 {
		t.Errorf("commits = %d, want 1 (fits in default chunk)", md.commits.Load())
	}
}

func TestPipelineExec_ErrorPropagates(t *testing.T) {
	t.Parallel()
	syntaxErr := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}
	md := &mockDriver{
		onExec: func(_ string) error {
			return syntaxErr
		},
	}
	p := newMockPool(t, md)

	if err := p.PipelineExec(context.Background(), []Statement{{Query: "BAD"}}, 1, 1); err == nil {
		t.Fatal("expected error from PipelineExec, got nil")
	}
}

func TestPipelineExec_ContextCancelled(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stmts := make([]Statement, 200)
	for i := range stmts {
		stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:C%d)", i)}
	}
	if err := p.PipelineExec(ctx, stmts, 10, 4); err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestPipelineExec_ChunkSizeOne(t *testing.T) {
	t.Parallel()
	md := &mockDriver{}
	p := newMockPool(t, md)

	stmts := make([]Statement, 5)
	for i := range stmts {
		stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:Y%d)", i)}
	}
	if err := p.PipelineExec(context.Background(), stmts, 1, 5); err != nil {
		t.Fatalf("PipelineExec: %v", err)
	}
	if md.commits.Load() != 5 {
		t.Errorf("commits = %d, want 5", md.commits.Load())
	}
}

func TestDefaultConstants(t *testing.T) {
	if DefaultChunkSize != 100 {
		t.Errorf("DefaultChunkSize = %d, want 100", DefaultChunkSize)
	}
	if DefaultMaxWorkers != 8 {
		t.Errorf("DefaultMaxWorkers = %d, want 8", DefaultMaxWorkers)
	}
}

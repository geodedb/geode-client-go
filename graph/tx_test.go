package graph_test

import (
	"context"
	"database/sql"
	"testing"

	"geodedb.com/geode/graph"
)

// TestTX_NilFn verifies that passing a nil function returns an error without
// attempting to begin a transaction.
func TestTX_NilFn(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	conn, err := graph.NewConn(db)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}

	err = conn.TX(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for nil fn, got nil")
	}

	const want = "graph: TX function must not be nil"
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

// TestTX_PropagatesFnError verifies that when BeginTx fails (no server running),
// the fn is NOT called and the error is propagated.
func TestTX_PropagatesFnError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	conn, err := graph.NewConn(db)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}

	fnCalled := false
	fn := func(_ *sql.Tx) error {
		fnCalled = true
		return nil
	}

	// BeginTx will fail because no server is running.
	err = conn.TX(context.Background(), nil, fn)
	if err == nil {
		t.Fatal("expected error when no server is running, got nil")
	}

	if fnCalled {
		t.Error("fn must not be called when BeginTx fails")
	}
}

// TestTXFunc_Signature verifies that TXFunc compiles with the expected
// function signature.
func TestTXFunc_Signature(t *testing.T) {
	t.Parallel()

	// This test is purely a compile-time check: if TXFunc does not match the
	// expected signature, the assignment below will not compile.
	var _ graph.TXFunc = func(_ *sql.Tx) error { return nil }
}

package geode

import (
	"context"
	"testing"
)

// TestConn_IsValid_EvictsServerClosedConn is the regression test for the
// stale-connection reuse bug: when a pooled connection's underlying QUIC
// connection has been closed by the server (or otherwise lost) but this
// driver.Conn has not run its own Close(), IsValid must report false so
// database/sql discards it instead of handing it back for a doomed query.
//
// Before the fix, IsValid only inspected local state and t.transport.IsClosed()
// (the local closed flag), never the underlying connection's liveness, so a
// server-killed connection reported valid and was reused.
func TestConn_IsValid_EvictsServerClosedConn(t *testing.T) {
	t.Parallel()

	transport := newMockTransport()
	conn := newTestConn(transport)

	// Sanity: a fresh idle connection on a live transport is valid.
	if !conn.IsValid() {
		t.Fatalf("precondition: fresh idle conn should be valid")
	}

	// The server tears down the underlying connection without our Close()
	// running (e.g. server restart, idle reap, transport error).
	transport.mu.Lock()
	transport.remoteClosed = true
	transport.mu.Unlock()

	if conn.IsValid() {
		t.Fatalf("IsValid returned true for a server-closed connection; " +
			"database/sql would reuse a dead conn (stale-connection reuse bug)")
	}
}

// TestConn_ResetSession_EvictsServerClosedConn verifies the same liveness gate
// on the reset path database/sql runs before reusing a pooled connection: a
// server-closed connection must surface ErrBadConn, not be silently reset and
// reused.
func TestConn_ResetSession_EvictsServerClosedConn(t *testing.T) {
	t.Parallel()

	transport := newMockTransport()
	conn := newTestConn(transport)

	transport.mu.Lock()
	transport.remoteClosed = true
	transport.mu.Unlock()

	if err := conn.ResetSession(context.Background()); err == nil {
		t.Fatalf("ResetSession returned nil for a server-closed connection; " +
			"expected ErrBadConn so database/sql evicts it")
	}
}

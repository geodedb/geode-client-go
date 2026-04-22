package geode

// Regression coverage for R4: "Go driver leaks inTx=true on transport
// failure". Before the fix, a transport failure during an in-flight
// Tx.Commit / Tx.Rollback / ExecContext-in-tx / QueryContext-in-tx
// would leave the Conn with c.inTx = true. The Conn would then be
// returned to the pool; the next BeginTx on the same conn failed with
// ErrTxInProgress even though the client never observed a successful
// BEGIN on that conn — the exact cascade that broke the msft
// msft-graph seed --parallel --workers 16 run on 2026-04-22.
//
// Every test here constructs a Conn backed by the shared mockTransport
// (see conn_stream_desync_test.go), forces a transport error at a
// specific lifecycle point, and asserts:
//
//   - c.inTx == false                    (client-side state cleared)
//   - c.state == StateInError            (eviction flag set)
//   - c.IsValid() == false               (pool will reject on reuse)
//   - c.ResetSession(ctx) == ErrBadConn  (pool will reject on return)
//
// Plus a whitebox check that calls the internal markConnFailed helper
// to lock in its minimal contract.

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"geodedb.com/geode/proto"
)

// newTxOnConn returns a Tx bound to c without actually issuing a BEGIN
// on the transport — the tests here need a tx object on a connection
// that is already flagged inTx=true so they can exercise just the
// finish() code path under transport failure.
func newTxOnConn(c *Conn) *Tx {
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()
	return &Tx{conn: c, ctx: context.Background()}
}

// assertConnFailed is the common invariant bundle.
func assertConnFailed(t *testing.T, c *Conn) {
	t.Helper()
	c.mu.Lock()
	inTx := c.inTx
	state := c.state
	active := c.activeRows
	c.mu.Unlock()
	if inTx {
		t.Error("inTx=true leaked after transport failure (R4)")
	}
	if state != StateInError {
		t.Errorf("state=%v, want StateInError", state)
	}
	if active != nil {
		t.Errorf("activeRows=%v, want nil", active)
	}
	if c.IsValid() {
		t.Error("IsValid()=true; a transport-failed conn must not be reused")
	}
	if got := c.ResetSession(context.Background()); !errors.Is(got, driver.ErrBadConn) {
		t.Errorf("ResetSession=%v, want driver.ErrBadConn", got)
	}
}

// ─── Tx.finish transport failures ─────────────────────────────────────────────

func TestR4_TxCommit_SendFailure_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	tx := newTxOnConn(c)

	// Close the transport so SendProto returns ErrClosed.
	_ = tr.Close()

	err := tx.Commit()
	if err == nil {
		t.Fatal("Commit returned nil; expected transport error")
	}
	assertConnFailed(t, c)
}

func TestR4_TxRollback_SendFailure_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	tx := newTxOnConn(c)

	_ = tr.Close()

	err := tx.Rollback()
	if err == nil {
		t.Fatal("Rollback returned nil; expected transport error")
	}
	assertConnFailed(t, c)
}

// TestR4_TxCommit_ReceiveFailure_ClearsInTx covers the case where the
// client successfully sends COMMIT but the response read fails —
// a common QUIC-graceful-close scenario where the server accepted
// the message and closed the stream before replying.
func TestR4_TxCommit_ReceiveFailure_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	tx := newTxOnConn(c)

	// Send succeeds because mockTransport isn't closed yet; then the
	// recv channel is empty and the context we pass is already
	// cancelled, simulating read-side failure.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx.ctx = ctx

	err := tx.Commit()
	if err == nil {
		t.Fatal("Commit returned nil; expected recv error")
	}
	assertConnFailed(t, c)
}

// TestR4_TxCommit_UnexpectedResponse_ClearsInTx covers the stream-
// desync case: send + recv succeed but the server replies with the
// wrong frame shape. The conn is in an indeterminate tx state; we
// must evict rather than reuse.
func TestR4_TxCommit_UnexpectedResponse_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	tx := newTxOnConn(c)

	// Prime the recv channel with a non-commit response.
	tr.push(mockSchemaMsg([]string{"x"}))

	err := tx.Commit()
	if err == nil {
		t.Fatal("Commit returned nil; expected response-shape error")
	}
	assertConnFailed(t, c)
}

// ─── ExecContext / QueryContext in-tx transport failures ──────────────────────

func TestR4_ExecContext_InTx_SendFailure_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)

	// Force the conn into a tx state (bypass BeginTx).
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()

	_ = tr.Close()

	_, err := c.ExecContext(context.Background(), "MERGE (n:Node {id: 1})", nil)
	if err == nil {
		t.Fatal("ExecContext returned nil; expected transport error")
	}
	assertConnFailed(t, c)
}

func TestR4_QueryContext_InTx_SendFailure_ClearsInTx(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)

	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()

	_ = tr.Close()

	_, err := c.QueryContext(context.Background(), "MATCH (n) RETURN n", nil)
	if err == nil {
		t.Fatal("QueryContext returned nil; expected transport error")
	}
	assertConnFailed(t, c)
}

// ─── ResetSession behavior on StateInError ────────────────────────────────────

// TestR4_ResetSession_StateInError_ReturnsBadConn is the counterpart
// to markConnFailed: once the conn lands in StateInError,
// database/sql must be told the conn is bad so it is evicted from
// the pool instead of being handed back out on the next Acquire.
func TestR4_ResetSession_StateInError_ReturnsBadConn(t *testing.T) {
	c := newTestConn(newMockTransport())
	c.mu.Lock()
	c.state = StateInError
	c.mu.Unlock()

	err := c.ResetSession(context.Background())
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("ResetSession on StateInError = %v; want driver.ErrBadConn", err)
	}
}

// TestR4_ResetSession_RollbackSendFails_ReturnsBadConn covers the
// path where the conn came back to the pool mid-tx and the implicit
// ROLLBACK hits a dead transport — the conn must be evicted, NOT
// allowed back with inTx=true.
func TestR4_ResetSession_RollbackSendFails_ReturnsBadConn(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()

	_ = tr.Close()

	err := c.ResetSession(context.Background())
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("ResetSession rollback-send-fail = %v; want driver.ErrBadConn", err)
	}
	assertConnFailed(t, c)
}

// TestR4_ResetSession_RollbackBadResponse_ReturnsBadConn covers the
// symmetric case for a malformed ROLLBACK reply.
func TestR4_ResetSession_RollbackBadResponse_ReturnsBadConn(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()

	// Prime recv with a wrong-shape response so the "expected
	// ROLLBACK response" branch fires.
	tr.push(mockSchemaMsg([]string{"x"}))

	err := c.ResetSession(context.Background())
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("ResetSession bad-rollback-response = %v; want driver.ErrBadConn", err)
	}
	assertConnFailed(t, c)
}

// TestR4_ResetSession_SuccessfulRollback_RestoresIdle is the positive
// counterpart: a clean rollback must leave the conn usable again.
func TestR4_ResetSession_SuccessfulRollback_RestoresIdle(t *testing.T) {
	tr := newMockTransport()
	c := newTestConn(tr)
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.mu.Unlock()

	// Server replies with a proper ROLLBACK response.
	tr.push(&proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Rollback{
			Rollback: &proto.RollbackResponse{},
		},
	})

	if err := c.ResetSession(context.Background()); err != nil {
		t.Fatalf("ResetSession=%v; want nil on successful rollback", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTx {
		t.Error("inTx stayed true after successful rollback")
	}
	if c.state != StateIdle {
		t.Errorf("state=%v, want StateIdle", c.state)
	}
}

// ─── markConnFailed contract ──────────────────────────────────────────────────

// TestR4_MarkConnFailed_Invariants is a whitebox guard on the
// helper itself. Any future refactor that drops one of these
// assignments reintroduces the R4 leak silently.
func TestR4_MarkConnFailed_Invariants(t *testing.T) {
	c := newTestConn(newMockTransport())
	c.mu.Lock()
	c.inTx = true
	c.state = StateInTransaction
	c.activeRows = &Rows{}
	c.mu.Unlock()

	c.mu.Lock()
	c.markConnFailed()
	inTx := c.inTx
	state := c.state
	rows := c.activeRows
	c.mu.Unlock()

	if inTx {
		t.Error("markConnFailed did not clear inTx")
	}
	if state != StateInError {
		t.Errorf("markConnFailed did not set state=StateInError, got %v", state)
	}
	if rows != nil {
		t.Error("markConnFailed did not clear activeRows")
	}
}

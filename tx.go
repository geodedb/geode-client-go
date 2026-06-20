package geode

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"geodedb.com/geode/proto"
)

// Tx represents a database transaction.
type Tx struct {
	conn *Conn
	ctx  context.Context

	mu   sync.Mutex
	done bool
}

// Compile-time interface check.
var _ driver.Tx = (*Tx)(nil)

// Commit commits the transaction.
func (t *Tx) Commit() error {
	return t.finish(proto.NewCommitMessage(&proto.CommitRequest{}), constOpCommit)
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error {
	return t.finish(proto.NewRollbackMessage(&proto.RollbackRequest{}), constOpRollback)
}

// finish executes either COMMIT or ROLLBACK and cleans up the transaction state.
//
// Any failure past the point where we've put bytes on the wire marks
// the transaction as done AND flags the connection for eviction via
// markConnFailed. This prevents the R4 leak where a failed COMMIT
// or ROLLBACK returned the conn to the pool with inTx=true, causing
// the next BeginTx to fail with ErrTxInProgress even though the
// client never observed a successful BEGIN on that conn.
func (t *Tx) finish(msg *proto.QuicClientMessage, opName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done {
		return ErrTxDone
	}

	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()

	if t.conn.state == StateClosed {
		return driver.ErrBadConn
	}

	// Send COMMIT or ROLLBACK
	if err := t.conn.transport.SendProto(t.ctx, msg); err != nil {
		t.done = true
		t.conn.markConnFailed()
		return transportErrBadConn(err)
	}

	// Read response
	resp, err := t.conn.transport.ReceiveProto(t.ctx)
	if err != nil {
		t.done = true
		t.conn.markConnFailed()
		return transportErrBadConn(err)
	}

	// Check for expected response
	switch opName {
	case constOpCommit:
		if resp.GetCommit() == nil {
			t.done = true
			t.conn.markConnFailed()
			return fmt.Errorf("expected COMMIT response")
		}
	case constOpRollback:
		if resp.GetRollback() == nil {
			t.done = true
			t.conn.markConnFailed()
			return fmt.Errorf("expected ROLLBACK response")
		}
	}

	t.done = true
	t.conn.inTx = false
	t.conn.state = StateIdle

	return nil
}

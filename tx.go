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
	return t.finish(proto.NewCommitMessage(&proto.CommitRequest{}), "commit")
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error {
	return t.finish(proto.NewRollbackMessage(&proto.RollbackRequest{}), "rollback")
}

// finish executes either COMMIT or ROLLBACK and cleans up the transaction state.
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
		return err
	}

	// Read response
	resp, err := t.conn.transport.ReceiveProto(t.ctx)
	if err != nil {
		return err
	}

	// Check for expected response
	switch opName {
	case "commit":
		if resp.GetCommit() == nil {
			return fmt.Errorf("expected COMMIT response")
		}
	case "rollback":
		if resp.GetRollback() == nil {
			return fmt.Errorf("expected ROLLBACK response")
		}
	}

	t.done = true
	t.conn.inTx = false
	t.conn.state = StateIdle
	t.conn.setGRPCTransactionStateLocked(false)

	return nil
}

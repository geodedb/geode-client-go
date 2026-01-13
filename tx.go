package geode

import (
	"context"
	"database/sql/driver"
	"sync"
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

	// Send COMMIT
	if err := t.conn.transport.Send(t.ctx, BuildCommitMessage()); err != nil {
		return err
	}

	// Read response
	data, err := t.conn.transport.Receive(t.ctx)
	if err != nil {
		return err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return err
	}

	if frame.IsError() {
		return frame.ToError()
	}

	t.done = true
	t.conn.inTx = false
	t.conn.state = StateIdle

	return nil
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error {
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

	// Send ROLLBACK
	if err := t.conn.transport.Send(t.ctx, BuildRollbackMessage()); err != nil {
		return err
	}

	// Read response
	data, err := t.conn.transport.Receive(t.ctx)
	if err != nil {
		return err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return err
	}

	if frame.IsError() {
		return frame.ToError()
	}

	t.done = true
	t.conn.inTx = false
	t.conn.state = StateIdle

	return nil
}

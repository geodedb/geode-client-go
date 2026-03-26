package graph

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// TXFunc is the function executed inside a managed transaction. If it returns
// an error the transaction is rolled back; if it returns nil the transaction is
// committed.
type TXFunc func(tx *sql.Tx) error

// TX begins a transaction, executes fn inside it, and commits on success. Any
// error returned by fn causes an immediate rollback and the fn error is
// returned to the caller. A panic inside fn also triggers a rollback before the
// panic is re-propagated. This ensures no transactions are ever leaked.
//
// opts may be nil; if so the database default isolation level is used.
func (c *Conn) TX(ctx context.Context, opts *sql.TxOptions, fn TXFunc) (err error) {
	if fn == nil {
		return errors.New("graph: TX function must not be nil")
	}

	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("graph: begin transaction: %w", err)
	}

	// Ensure the transaction is always resolved, even on panic.
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err = fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("graph: commit transaction: %w", err)
	}

	return nil
}

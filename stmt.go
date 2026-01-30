package geode

import (
	"context"
	"database/sql/driver"
	"sync"
)

// Stmt represents a prepared statement.
// Note: Geode doesn't have server-side prepared statements,
// so this is a client-side implementation that stores the query
// and validates parameters.
type Stmt struct {
	conn     *Conn
	query    string
	numInput int

	mu     sync.Mutex
	closed bool
}

// Compile-time interface checks.
var (
	_ driver.Stmt             = (*Stmt)(nil)
	_ driver.StmtExecContext  = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
)

// Close closes the statement.
func (s *Stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return s.numInput
}

// Exec executes a query that doesn't return rows.
//
// Deprecated: Use ExecContext instead for explicit timeout control.
// This method uses a default 30-second timeout.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeprecatedMethodTimeout)
	defer cancel()
	return s.ExecContext(ctx, valuesToNamedValues(args))
}

// ExecContext executes a query that doesn't return rows.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrStmtClosed
	}
	s.mu.Unlock()

	return s.conn.ExecContext(ctx, s.query, args)
}

// Query executes a query that returns rows.
//
// Deprecated: Use QueryContext instead for explicit timeout control.
// This method uses a default 30-second timeout.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeprecatedMethodTimeout)
	defer cancel()
	return s.QueryContext(ctx, valuesToNamedValues(args))
}

// QueryContext executes a query that returns rows.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrStmtClosed
	}
	s.mu.Unlock()

	return s.conn.QueryContext(ctx, s.query, args)
}

// valuesToNamedValues converts []driver.Value to []driver.NamedValue.
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return named
}

package graph

import (
	"context"
	"database/sql"
	"errors"

	_ "geodedb.com/geode" // register the geode driver with database/sql
)

// Conn wraps a *sql.DB with graph-specific configuration such as retry policy,
// batch size, and worker count. It serves as the foundation for higher-level
// graph operations (transactions, batch execution, pipeline processing, and
// schema management).
type Conn struct {
	db   *sql.DB
	opts Options
}

// NewConn creates a new Conn wrapping the provided *sql.DB. It returns an
// error if db is nil. Functional options are applied on top of the defaults
// provided by ApplyOptions.
func NewConn(db *sql.DB, options ...Option) (*Conn, error) {
	if db == nil {
		return nil, errors.New("graph: db must not be nil")
	}
	return &Conn{
		db:   db,
		opts: ApplyOptions(options...),
	}, nil
}

// DB returns the underlying *sql.DB.
func (c *Conn) DB() *sql.DB {
	return c.db
}

// Close closes the underlying *sql.DB, releasing all pooled connections.
func (c *Conn) Close() error {
	return c.db.Close()
}

// ExecContext executes a query that does not return rows. It delegates directly
// to the underlying *sql.DB.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns rows. It delegates directly to the
// underlying *sql.DB.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// PingContext verifies the connection to the database is still alive. It
// delegates directly to the underlying *sql.DB.
func (c *Conn) PingContext(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

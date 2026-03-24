package geode

import (
	"context"
	"database/sql"
	"time"
)

// Default pool settings tuned for Geode's QUIC transport.
const (
	// DefaultMaxOpenConns is the default maximum number of open connections.
	// Matches typical parallel worker counts (e.g. 8 chunk workers).
	DefaultMaxOpenConns = 16

	// DefaultMaxIdleConns is the default maximum number of idle connections.
	DefaultMaxIdleConns = 4

	// DefaultConnMaxLifetime is the default maximum lifetime of a connection.
	// Prevents connections from growing stale in long-running processes.
	DefaultConnMaxLifetime = 5 * time.Minute

	// DefaultConnMaxIdleTime is the default maximum idle time for a connection.
	// Set below QUIC's 30-second idle timeout to proactively recycle
	// connections before the server closes them.
	DefaultConnMaxIdleTime = 25 * time.Second

	// DefaultPingTimeout is the timeout for the initial ping when creating a pool.
	DefaultPingTimeout = 10 * time.Second
)

// Pool manages a pool of Geode database connections. It wraps a *sql.DB with
// Geode-appropriate defaults and provides convenience methods for executing
// GQL queries.
//
// Consumers should create a single Pool per DSN and share it across goroutines.
// The Pool handles connection lifecycle, health checking, and automatic
// reconnection through database/sql's built-in pooling.
//
// Usage:
//
//	pool, err := geode.NewPool("quic://localhost:3141?graph=mydb&user=admin&pass=secret")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pool.Close()
//
//	rows, err := pool.QueryContext(ctx, "MATCH (n:Person) RETURN n.name")
type Pool struct {
	db  *sql.DB
	cfg *Config
}

// poolConfig holds pool-specific configuration separate from the connection Config.
type poolConfig struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func defaultPoolConfig() poolConfig {
	return poolConfig{
		maxOpenConns:    DefaultMaxOpenConns,
		maxIdleConns:    DefaultMaxIdleConns,
		connMaxLifetime: DefaultConnMaxLifetime,
		connMaxIdleTime: DefaultConnMaxIdleTime,
	}
}

// PoolOption configures a Pool.
type PoolOption func(*poolConfig)

// WithMaxOpenConns sets the maximum number of open connections to the database.
// Zero means unlimited. Default: 16.
func WithMaxOpenConns(n int) PoolOption {
	return func(c *poolConfig) {
		c.maxOpenConns = n
	}
}

// WithMaxIdleConns sets the maximum number of idle connections in the pool.
// Default: 4.
func WithMaxIdleConns(n int) PoolOption {
	return func(c *poolConfig) {
		c.maxIdleConns = n
	}
}

// WithConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections are lazily closed before reuse. Default: 5 minutes.
func WithConnMaxLifetime(d time.Duration) PoolOption {
	return func(c *poolConfig) {
		c.connMaxLifetime = d
	}
}

// WithConnMaxIdleTime sets the maximum amount of time a connection may sit idle.
// Should be shorter than the server's idle timeout (QUIC default: 30s).
// Default: 25 seconds.
func WithConnMaxIdleTime(d time.Duration) PoolOption {
	return func(c *poolConfig) {
		c.connMaxIdleTime = d
	}
}

// NewPool creates a connection pool for the given DSN. The DSN format is the
// same as for sql.Open("geode", dsn), with an additional optional `graph`
// parameter that auto-selects a graph on each new connection.
//
// The pool is immediately validated with a ping. If the server is unreachable,
// NewPool returns an error.
//
// Example DSNs:
//
//	quic://localhost:3141?graph=mydb&user=admin&pass=secret
//	grpc://localhost:50051?graph=mydb&tls=0
func NewPool(dsn string, opts ...PoolOption) (*Pool, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	pc := defaultPoolConfig()
	for _, opt := range opts {
		opt(&pc)
	}

	db, err := sql.Open("geode", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(pc.maxOpenConns)
	db.SetMaxIdleConns(pc.maxIdleConns)
	db.SetConnMaxLifetime(pc.connMaxLifetime)
	db.SetConnMaxIdleTime(pc.connMaxIdleTime)

	// Verify the pool can establish a connection.
	ctx, cancel := context.WithTimeout(context.Background(), DefaultPingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return &Pool{db: db, cfg: cfg}, nil
}

// DB returns the underlying *sql.DB for advanced use cases such as
// running transactions or using sql.Conn for connection-pinned operations.
func (p *Pool) DB() *sql.DB {
	return p.db
}

// Config returns a copy of the pool's parsed configuration.
func (p *Pool) Config() *Config {
	return p.cfg.Clone()
}

// Close closes all connections in the pool.
func (p *Pool) Close() error {
	return p.db.Close()
}

// PingContext verifies a connection to the database is still alive.
func (p *Pool) PingContext(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

// ExecContext executes a GQL query that doesn't return rows.
func (p *Pool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a GQL query that returns rows.
func (p *Pool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a GQL query that returns at most one row.
func (p *Pool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, query, args...)
}

// BeginTx starts a transaction with the given options.
func (p *Pool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, opts)
}

// Stats returns pool statistics.
func (p *Pool) Stats() sql.DBStats {
	return p.db.Stats()
}

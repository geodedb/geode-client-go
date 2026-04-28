// Package geode provides a database/sql driver for the Geode graph database.
//
// The driver connects to Geode servers over QUIC with TLS 1.3 and implements
// the full GQL (Graph Query Language) specification.
//
// # Usage
//
// Import the driver anonymously to register it with database/sql:
//
//	import (
//	    "database/sql"
//	    _ "geodedb.com/geode"
//	)
//
//	db, err := sql.Open("geode", "localhost:3141")
//
// # DSN Format
//
// The driver supports multiple DSN formats:
//
//	quic://host:port?options (QUIC transport - recommended)
//	grpc://host:port?options (gRPC transport)
//	host:port?options (defaults to QUIC)
//	host:port
//	host
//
// Options:
//   - page_size: Results page size (default: 1000)
//   - hello_name: Client name (default: "geode-go")
//   - hello_ver: Client version (default: "0.1")
//   - conformance: GQL conformance level (default: "min")
//   - ca: Path to CA certificate
//   - cert: Path to client certificate (for mTLS)
//   - key: Path to client key (for mTLS)
//   - insecure: Skip TLS verification (testing only)
//   - connect_timeout: Connection timeout in seconds (default: 0 = no timeout)
//
// Environment variables:
//   - GEODE_HOST: Default host if DSN is empty
//   - GEODE_PORT: Default port if not specified
//   - GEODE_TLS_CA: Default CA certificate path
//
// # Example
//
//	db, err := sql.Open("geode", "quic://localhost:3141?page_size=500")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	rows, err := db.QueryContext(ctx, "MATCH (n:Person) RETURN n.name LIMIT 10")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
//
//	for rows.Next() {
//	    var name string
//	    if err := rows.Scan(&name); err != nil {
//	        log.Fatal(err)
//	    }
//	    fmt.Println(name)
//	}
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("geode", &Driver{})
}

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Compile-time interface checks.
var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// Open opens a new connection to the database.
// The name is a DSN (data source name) in the format described above.
//
// Deprecated: Use OpenConnector and Connector.Connect with an explicit context
// for timeout control. This method uses defaultDeprecatedMethodTimeout (30s).
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeprecatedMethodTimeout)
	defer cancel()
	return connector.Connect(ctx)
}

// OpenConnector returns a Connector for the given DSN.
// This allows for connection pooling with context support.
//
// For QUIC transport, a shared connection pool is created so that multiple
// database/sql connections can multiplex streams over a small number of
// underlying QUIC connections, avoiding per-query TLS handshake overhead.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}

	c := &Connector{
		driver: d,
		cfg:    cfg,
	}

	// Create a shared QUIC pool for stream multiplexing.
	// Honor cfg.PoolSize when set (via DSN ?pool_size=N); fall back to
	// DefaultQUICPoolSize when zero.  The previous hard-coded 4-connection
	// pool was a hidden ceiling on parallelism: callers using
	// `db.Conn(ctx)` to pin many sql.Conn handles all multiplexed onto
	// 4 underlying QUIC connections, which capped concurrent transaction
	// throughput at c=4 regardless of how many goroutines or workers
	// the application configured (e.g. msft-graph --workers 16 stalled
	// because workers 5-16 contended for the same 4 connections).
	if cfg.Transport == TransportQUIC {
		size := cfg.PoolSize
		if size <= 0 {
			size = DefaultQUICPoolSize
		}
		c.quicPool = NewQUICPool(cfg, size)
	}

	return c, nil
}

// Connector implements driver.Connector.
type Connector struct {
	driver   *Driver
	cfg      *Config
	quicPool *QUICPool // shared QUIC connection pool for stream multiplexing
}

// Compile-time interface check.
var _ driver.Connector = (*Connector)(nil)

// Connect returns a new connection to the database.
//
// For QUIC transport with a shared pool, this opens a new QUIC stream on a
// pooled connection rather than dialing a new QUIC connection. The HELLO
// handshake is performed once per underlying QUIC connection, and subsequent
// streams inherit the authenticated state.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.quicPool != nil {
		return connectPooled(ctx, c.cfg, c.quicPool)
	}
	return connect(ctx, c.cfg)
}

// Driver returns the underlying Driver.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Config returns a copy of the connector's configuration.
func (c *Connector) Config() *Config {
	return c.cfg.Clone()
}

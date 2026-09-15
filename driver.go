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
// A repeated option key resolves to its LAST occurrence, so a DSN can be
// extended by appending overrides, e.g. base+"&tenant=override" always wins
// over an earlier "&tenant=base" in the same string (SONNY-806).
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
	"errors"
)

// registeredDriver is the singleton driver instance used by sql.Register.
// It is shared across all connections opened via sql.Open/sql.OpenDB
// to ensure consistent driver reference semantics.
//
//nolint:gochecknoglobals // Driver registration requires a global singleton
var registeredDriver Driver

func init() {
	sql.Register("geode", &registeredDriver)
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

// NewConnector creates a Connector from a programmatic Config without
// requiring a DSN string. This is the preferred constructor when
// connection parameters are assembled from structured config rather
// than a DSN string.
//
// The returned Connector can be passed to sql.OpenDB:
//
//	c, err := geode.NewConnector(&geode.Config{
//	    Host: "localhost", Port: "3141", Transport: geode.TransportQUIC,
//	})
//	if err != nil {
//	    return err
//	}
//	db := sql.OpenDB(c)
func NewConnector(cfg *Config) (*Connector, error) {
	if cfg == nil {
		return nil, &ConfigError{Field: "config", Message: "config must not be nil"}
	}

	// Clone first so we never mutate the caller's Config, then backfill
	// unset optional fields with the same defaults applied by ParseDSN.
	// This keeps NewConnector ergonomic: a caller assembling a Config
	// programmatically only needs to set the fields they care about
	// (e.g. Host/Port/Transport) and still passes Validate.
	out := cfg.Clone()
	if out.Port == "" {
		out.Port = DefaultPort
	}
	if out.PageSize <= 0 {
		out.PageSize = DefaultPageSize
	}
	if out.HelloName == "" {
		out.HelloName = DefaultHelloName
	}
	if out.HelloVersion == "" {
		out.HelloVersion = DefaultHelloVersion
	}
	if out.Conformance == "" {
		out.Conformance = DefaultConformance
	}
	// HealthProbeInterval is deliberately NOT backfilled. Zero is a legitimate
	// setting for it — "do not probe" — and there is no way to tell an unset
	// field from a deliberate zero on a struct the caller filled in. ParseDSN
	// applies DefaultHealthProbeInterval because a DSN that omits
	// health_probe_ms genuinely has no opinion; a programmatic Config that
	// leaves the field at zero is taken at its word. Callers who want the
	// default set it explicitly.

	if err := out.Validate(); err != nil {
		return nil, err
	}

	c := &Connector{
		driver: &registeredDriver,
		cfg:    out,
	}
	if out.Transport == TransportQUIC {
		size := out.PoolSize
		if size <= 0 {
			size = DefaultQUICPoolSize
		}
		c.quicPool = NewQUICPool(out, size)
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
// A connection-level failure is reported as driver.ErrBadConn so database/sql
// transparently retries on a FRESH connection instead of handing the transport
// error to the caller — see markBadConn.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.quicPool != nil {
		conn, err := connectPooled(ctx, c.cfg, c.quicPool)
		if err != nil {
			return nil, markBadConn(err)
		}
		return conn, nil
	}
	conn, err := connect(ctx, c.cfg)
	if err != nil {
		return nil, markBadConn(err)
	}
	return conn, nil
}

// badConnError reports a connection-level failure to database/sql without
// throwing the driver's own classification away.
//
// database/sql re-dials only for driver.ErrBadConn: any other error from
// Connector.Connect is handed straight to the caller, which is why a burst
// whose connections were torn down under it saw 56 raw transport errors
// (SONNY-1219). Wrapping keeps both answers available — errors.Is(err,
// driver.ErrBadConn) for database/sql, errors.As(err, &te) for the caller and
// for Classify, which already maps *TransportError to RetryNewConn
// (retryclass.go). The message is the cause's own, so nothing a caller logs
// changes.
type badConnError struct{ cause error }

func (e *badConnError) Error() string { return e.cause.Error() }

// Unwrap returns both branches: the cause, so the concrete error type and every
// sentinel under it stay reachable, and driver.ErrBadConn, so database/sql
// retries. Go's errors package walks multi-error trees since 1.20.
func (e *badConnError) Unwrap() []error { return []error{e.cause, driver.ErrBadConn} }

// markBadConn marks a CONNECTION-LEVEL failure as driver.ErrBadConn.
//
// The condition is a *TransportError that is not a dial failure: the connection
// the driver was handed (or had just established) is gone or untrustworthy, and
// another one will work — exactly what database/sql's retry does. It is NOT
// classified as backoff-worthy: this is not resource pressure and waiting does
// not help (Classify maps it to RetryNewConn).
//
// A DIAL failure is deliberately excluded. There was no connection to be bad;
// the server is unreachable, and reporting ErrBadConn would make database/sql
// pay the connect timeout three times over before surfacing the same outage.
// Everything else — auth refusals, config errors, pool sentinels — is returned
// untouched, so their retry classes (RetryNever, RetryBackoff) stand.
func markBadConn(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, driver.ErrBadConn) {
		return err
	}
	var te *TransportError
	if !errors.As(err, &te) || te == nil || te.Op == constOpDial {
		return err
	}
	return &badConnError{cause: err}
}

// Driver returns the underlying Driver.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Config returns a copy of the connector's configuration.
func (c *Connector) Config() *Config {
	return c.cfg.Clone()
}

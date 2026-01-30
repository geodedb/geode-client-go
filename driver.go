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
//	geode://host:port?options
//	host:port?options
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
//	db, err := sql.Open("geode", "geode://localhost:3141?page_size=500")
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
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector returns a Connector for the given DSN.
// This allows for connection pooling with context support.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}
	return &Connector{
		driver: d,
		cfg:    cfg,
	}, nil
}

// Connector implements driver.Connector.
type Connector struct {
	driver *Driver
	cfg    *Config
}

// Compile-time interface check.
var _ driver.Connector = (*Connector)(nil)

// Connect returns a new connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
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

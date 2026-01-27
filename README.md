# Geode Go Client Library

A Go `database/sql` driver for the [Geode](https://github.com/devnw/geode) graph database.

## Features

- Full `database/sql` driver implementation
- QUIC transport with TLS 1.3
- Connection pooling via `database/sql`
- Prepared statements
- Transaction support
- Context cancellation and timeouts
- Rich error types with ISO/IEC 39075 status codes

## Installation

```bash
go get go.codepros.org/geode
```

## Usage

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    _ "go.codepros.org/geode"
)

func main() {
    // Connect to Geode server
    db, err := sql.Open("geode", "localhost:3141?ca=/path/to/ca.crt")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Configure connection pool
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(5)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // Execute a query
    rows, err := db.QueryContext(ctx, "MATCH (n:Person) RETURN n.name LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var name string
        if err := rows.Scan(&name); err != nil {
            log.Fatal(err)
        }
        log.Println(name)
    }
}
```

## DSN Format

```
geode://host:port?options
host:port?options
host:port
host
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `page_size` | Results page size | 1000 |
| `hello_name` | Client name for HELLO | geode-go |
| `hello_ver` | Client version for HELLO | 0.1 |
| `conformance` | GQL conformance level | min |
| `ca` | Path to CA certificate | |
| `cert` | Path to client certificate (mTLS) | |
| `key` | Path to client key (mTLS) | |
| `insecure` | Skip TLS verification (testing only) | false |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GEODE_HOST` | Default host |
| `GEODE_PORT` | Default port |
| `GEODE_TLS_CA` | Default CA certificate path |

## Transactions

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}

_, err = tx.ExecContext(ctx, "CREATE (n:Person {name: 'Alice'})")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

## Prepared Statements

```go
stmt, err := db.PrepareContext(ctx, "MATCH (n:Person {name: ?}) RETURN n")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

rows, err := stmt.QueryContext(ctx, "Alice")
// ...
```

## Error Handling

The driver provides rich error types:

```go
import "go.codepros.org/geode"

rows, err := db.QueryContext(ctx, "INVALID QUERY")
if err != nil {
    var derr *geode.DriverError
    if errors.As(err, &derr) {
        log.Printf("Code: %s, Message: %s", derr.Code, derr.Message)
        if derr.IsRetryable() {
            // Retry the operation
        }
    }
}
```

## Unicode Utilities

Helper functions for Unicode conversion:

```go
import "go.codepros.org/geode"

// UTF-8 to UTF-16
utf16 := geode.Utf8ToUtf16("Hello 🌍")

// UTF-16 to UTF-8
utf8 := geode.Utf16ToUtf8(utf16)

// WTF-8 lossy decode (replaces invalid sequences with U+FFFD)
safe := geode.Wtf8Lossy([]byte{0x61, 0x80, 0x62})
```

## Testing

```bash
# Unit tests (no server required)
go test -v -short ./...

# Integration tests with manual server
GEODE_TEST_DSN="localhost:3141?insecure=true" go test -v ./...

# Docker-based integration tests (recommended)
go test -v -tags=integration ./...

# Docker integration tests with custom image
GEODE_IMAGE="myregistry/geode:tag" go test -v -tags=integration ./...

# Unit benchmarks (no server required)
go test -bench=. -benchmem ./...

# Docker-based benchmarks
go test -tags=integration -bench=BenchmarkDocker -benchmem ./...

# Fuzzing
go test -fuzz=FuzzParseDSN -fuzztime=30s
```

### Docker Integration Tests

The Docker-based integration tests provide comprehensive coverage using `testcontainers-go`. They automatically:

- Pull and start a Geode container from `registry.gitlab.com/devnw/codepros/geode/geode:latest`
- Run a full test suite covering CRUD operations, transactions, queries, and more
- Clean up after tests complete

Test categories include:

- **Basic Operations**: Ping, simple queries, parameterized queries
- **Node CRUD**: Create, read, update, delete operations
- **Relationships**: Creating and traversing relationships
- **Transactions**: Commit, rollback, multi-operation transactions
- **Query Operations**: WHERE, ORDER BY, LIMIT, SKIP
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX
- **Error Handling**: Syntax errors, non-existent properties
- **Prepared Statements**: Parameterized query reuse
- **Concurrent Access**: Connection pooling under load
- **Data Types**: Null, lists, maps
- **Social Network Fixtures**: Complex relationship queries

## QUIC Performance Tuning

The Go client uses [quic-go](https://github.com/quic-go/quic-go) for QUIC transport. For optimal throughput on high-bandwidth connections, configure the following system-level optimizations.

### UDP Buffer Sizes

Increase the maximum UDP buffer size to ~7MB for high-throughput transfers:

**Linux:**
```bash
sudo sysctl -w net.core.rmem_max=7340032
sudo sysctl -w net.core.wmem_max=7340032

# Persist across reboots
echo "net.core.rmem_max=7340032" | sudo tee -a /etc/sysctl.d/99-geode-quic.conf
echo "net.core.wmem_max=7340032" | sudo tee -a /etc/sysctl.d/99-geode-quic.conf
```

**BSD/macOS:**
```bash
sudo sysctl -w kern.ipc.maxsockbuf=8441037
```

### Generic Segmentation Offload (GSO)

GSO batches UDP packets to reduce syscall overhead. It's automatically enabled on Linux 4.18+.

- **No configuration required** - enabled automatically
- **To disable** (not recommended): Set `QUIC_GO_DISABLE_GSO=true` environment variable
- **Requirements**: Linux kernel 4.18+

### Path MTU Discovery (DPLPMTUD)

DPLPMTUD probes for optimal MTU size, reducing per-packet overhead. It's enabled by default in the client.

- Sends fewer than 10 probe packets over a connection's lifetime
- Particularly beneficial on paths supporting MTUs larger than 1200 bytes
- Configured via `quic.Config.DisablePathMTUDiscovery` (default: false)

## License

See [LICENSE](LICENSE) for details.

# Geode Go Client Library

A Go `database/sql` driver for the [Geode](https://github.com/devnw/geode) graph database.

## Features

- Full `database/sql` driver implementation
- **Dual transport support**: QUIC (default) and gRPC
- Protobuf wire protocol
- TLS 1.3 security
- Connection pooling via `database/sql`
- Prepared statements
- Transaction support
- Context cancellation and timeouts
- Rich error types with ISO/IEC 39075 status codes

## Installation

```bash
go get geodedb.com/geode
```

## Usage

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    _ "geodedb.com/geode"
)

func main() {
    // Connect via QUIC (default)
    db, err := sql.Open("geode", "quic://localhost:3141?ca=/path/to/ca.crt")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Or connect via gRPC
    // db, err := sql.Open("geode", "grpc://localhost:50051?ca=/path/to/ca.crt")

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

> **Note**: See [`geode/docs/DSN.md`](../geode/docs/DSN.md) for the complete DSN specification.

The driver supports multiple DSN formats with scheme-based transport selection:

```
quic://host:port?options     # QUIC transport (recommended)
grpc://host:port?options     # gRPC transport
host:port?options            # QUIC transport (scheme-less)
host:port
host
```

### Transport Schemes

| Scheme | Transport | Default Port | Description |
|--------|-----------|--------------|-------------|
| `quic://` | QUIC | 3141 | High-performance UDP-based transport |
| `grpc://` | gRPC | 50051 | Standard gRPC over HTTP/2 |
| (none) | QUIC | 3141 | Scheme-less defaults to QUIC |

### IPv6 Support

IPv6 addresses are fully supported:

```
quic://[::1]:3141
grpc://[2001:db8::1]:50051
[::1]:3141
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `page_size` | Results page size | 1000 |
| `hello_name` | Client name for HELLO | geode-go |
| `hello_ver` | Client version for HELLO | 0.1 |
| `conformance` | GQL conformance level | min |
| `ca` | Path to CA certificate (must exist; no fallback) | |
| `cert` | Path to client certificate (mTLS) | |
| `key` | Path to client key (mTLS) | |
| `insecure_tls_skip_verify` | Skip TLS verification (testing only) | false |
| `tls` | Enable/disable TLS (`0` or `false` to disable) | enabled |
| `user`/`username` | Authentication username | |
| `pass`/`password` | Authentication password | |

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
import "geodedb.com/geode"

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

## Security Limits

- Inbound protocol frames are capped at 32MB to prevent memory exhaustion. Oversized frames are rejected.

## Unicode Utilities

Helper functions for Unicode conversion:

```go
import "geodedb.com/geode"

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

# Integration tests with manual server (QUIC)
GEODE_TEST_DSN="quic://localhost:3141?insecure_tls_skip_verify=true" go test -v ./...

# Integration tests with manual server (gRPC)
GEODE_TEST_DSN="grpc://localhost:50051?tls=0" go test -v ./...

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

- Pull and start a Geode container from `geodedb/geode:latest`
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

## Wire Protocol

The driver uses Protocol Buffers (protobuf) for wire encoding over both QUIC and gRPC transports.

### QUIC Transport

- Length-prefixed protobuf messages (4-byte big-endian length prefix)
- Single bidirectional stream per connection
- TLS 1.3 with ALPN protocol `geode/1`

### gRPC Transport

- Standard gRPC over HTTP/2
- Streaming responses for query execution
- TLS 1.3 by default (can be disabled with `tls=0` for testing)

## Go Module Configuration

This module uses a vanity import path (`geodedb.com/geode`) that redirects to the GitLab repository. The standard `go get` command works out of the box:

```bash
go get geodedb.com/geode
```

### Private/Self-Hosted GitLab

If you're using a private GitLab instance or need to configure authentication:

```bash
# Configure Git to use SSH for GitLab
git config --global url."git@gitlab.com:".insteadOf "https://gitlab.com/"

# Or use a personal access token
git config --global url."https://oauth2:${GITLAB_TOKEN}@gitlab.com/".insteadOf "https://gitlab.com/"
```

### GOPROXY Configuration

The module works with the default Go proxy (`proxy.golang.org`). For air-gapped environments or private proxies:

```bash
# Use direct fetching (bypasses proxy)
GOPROXY=direct go get geodedb.com/geode

# Use a private proxy
GOPROXY=https://your-proxy.example.com,direct go get geodedb.com/geode
```

### GOPRIVATE / GONOSUMDB

If you need to bypass the checksum database or proxy for this module:

```bash
# Add to your shell profile or CI environment
export GOPRIVATE=geodedb.com
export GONOSUMDB=geodedb.com

# Or configure globally
go env -w GOPRIVATE=geodedb.com
go env -w GONOSUMDB=geodedb.com
```

### GitLab Go Proxy (Self-Hosted)

GitLab Premium includes a built-in Go proxy. To use it:

1. Enable the Go proxy in GitLab Admin > Settings > CI/CD
2. Configure your Go environment:

```bash
export GOPROXY="https://gitlab.example.com/api/v4/projects/${PROJECT_ID}/packages/go,https://proxy.golang.org,direct"
export GONOSUMDB="gitlab.example.com/*"
```

### Verifying Installation

After installation, verify the module:

```bash
go list -m geodedb.com/geode
# Should output: geodedb.com/geode v0.x.x
```

## License

See [LICENSE](LICENSE) for details.

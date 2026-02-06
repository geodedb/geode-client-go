# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
go build ./...

# Run unit tests (no server required)
go test -v -short ./...

# Run Docker integration tests (recommended)
go test -v -tags=integration ./...

# Run Docker integration tests with custom image
GEODE_IMAGE="myregistry/geode:tag" go test -v -tags=integration ./...

# Run integration tests with manual server (QUIC)
GEODE_TEST_DSN="quic://localhost:3141?insecure_tls_skip_verify=true" go test -v ./...

# Run integration tests with manual server (gRPC)
GEODE_TEST_DSN="grpc://localhost:50051?tls=0" go test -v ./...

# Run a single test
go test -run TestParseDSN_Simple -v

# Run unit benchmarks
go test -bench=. -benchmem ./...

# Run Docker benchmarks
go test -tags=integration -bench=BenchmarkDocker -benchmem ./...

# Run fuzzing (30 seconds)
go test -fuzz=FuzzParseDSN -fuzztime=30s
go test -fuzz=FuzzProtoDecodeServerMessage -fuzztime=30s

# Lint
golangci-lint run
```

## Architecture

This is a Go `database/sql` driver for the Geode graph database with **dual transport support**: QUIC (default) and gRPC. Communication uses Protocol Buffers over TLS 1.3.

### Package Structure

```
geode-client-go/
├── driver.go          # Driver registration, Connector
├── conn.go            # Connection with state machine
├── stmt.go            # Prepared statements
├── rows.go            # Result set iterator
├── tx.go              # Transactions
├── result.go          # Exec result
├── errors.go          # Error types (DriverError, TransportError, etc.)
├── config.go          # DSN parsing, Config struct, TransportType
├── transport.go       # QUIC transport layer
├── grpc_transport.go  # gRPC transport layer
├── types.go           # GQL type definitions
├── security.go        # TLS configuration
├── unicode.go         # Unicode conversion utilities
├── proto/
│   ├── geode.pb.go       # Protobuf message types and encoding
│   ├── geode_grpc.pb.go  # gRPC client interface
│   └── codec.go          # gRPC codec for custom messages
└── internal/
    └── validate/
        └── input.go   # Query and parameter validation
```

### Key Interfaces Implemented

The driver implements all recommended `database/sql/driver` interfaces:

- `driver.Driver`, `driver.DriverContext`
- `driver.Connector`
- `driver.Conn`, `driver.ConnBeginTx`, `driver.ConnPrepareContext`
- `driver.ExecerContext`, `driver.QueryerContext`
- `driver.Pinger`, `driver.SessionResetter`, `driver.Validator`
- `driver.Stmt`, `driver.StmtExecContext`, `driver.StmtQueryContext`
- `driver.Rows`, `driver.RowsColumnTypeDatabaseTypeName`, `driver.RowsColumnTypeNullable`

### Connection State Machine

```
Connect() → Idle
Idle + Begin() → InTransaction
Idle + Query()/Exec() → Executing → Fetching/Idle
InTransaction + Query()/Exec() → Executing → Fetching → InTransaction
InTransaction + Commit()/Rollback() → Idle
Any + Close() → Closed
```

### DSN Format

The driver supports multiple DSN formats with scheme-based transport selection:

```
quic://host:port?options     # QUIC transport (recommended)
grpc://host:port?options     # gRPC transport
host:port?options            # QUIC transport (scheme-less)
host:port
host
```

#### Transport Schemes

| Scheme | Transport | Default Port | Description |
|--------|-----------|--------------|-------------|
| `quic://` | QUIC | 3141 | High-performance UDP-based transport |
| `grpc://` | gRPC | 50051 | Standard gRPC over HTTP/2 |
| (none) | QUIC | 3141 | Scheme-less defaults to QUIC |

#### IPv6 Support

IPv6 addresses are fully supported:

```
quic://[::1]:3141
grpc://[2001:db8::1]:50051
[::1]:3141
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `page_size` | Results page size | 1000 |
| `hello_name` | Client name for HELLO | geode-go |
| `hello_ver` | Client version for HELLO | 0.1 |
| `conformance` | GQL conformance level | min |
| `ca` | Path to CA certificate | |
| `cert` | Path to client certificate (mTLS) | |
| `key` | Path to client key (mTLS) | |
| `insecure_tls_skip_verify` | Skip TLS verification (testing only) | false |
| `tls` | Enable/disable TLS (`0` or `false` to disable) | enabled |
| `user`/`username` | Authentication username | |
| `pass`/`password` | Authentication password | |

Environment variables: `GEODE_HOST`, `GEODE_PORT`, `GEODE_TLS_CA`

### Wire Protocol

The driver uses Protocol Buffers (protobuf) for wire encoding over both transports.

#### QUIC Transport

- Length-prefixed protobuf messages (4-byte big-endian length prefix)
- Single bidirectional stream per connection
- TLS 1.3 with ALPN protocol `geode/1`

#### gRPC Transport

- Standard gRPC over HTTP/2
- Streaming responses for query execution
- TLS 1.3 by default (can be disabled with `tls=0` for testing)

### Error Types

- `DriverError` - Server errors with ISO/IEC 39075 status codes
- `TransportError` - Network/QUIC/gRPC failures
- `ConfigError` - DSN parsing errors
- `SecurityError` - TLS/validation failures
- `StateError` - Invalid connection state transitions

### Security

- TLS 1.3 required for all connections (can be disabled for gRPC testing)
- Certificate verification enabled by default (use `insecure_tls_skip_verify=true` only for testing)
- Query input validation (null bytes, control characters)
- Parameter name validation

### Integration Tests

Docker-based integration tests in `integration_test.go` use `testcontainers-go` to automatically:

- Pull and start a Geode container (`geodedb/geode:latest`)
- Run comprehensive test suite
- Clean up after tests

Test categories:

- Basic Operations (ping, queries, parameters)
- Node CRUD (create, update, delete)
- Relationships (create, traverse)
- Transactions (commit, rollback)
- Query Operations (WHERE, ORDER BY, LIMIT, SKIP)
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- Error Handling
- Prepared Statements
- Concurrent Access
- Data Types (null, list, map)
- Social Network Fixtures

Benchmarks (prefixed with `BenchmarkDocker_`) test real-world performance against a running Geode instance.

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

# Run integration tests with manual server
GEODE_TEST_DSN="localhost:3141?insecure=true" go test -v ./...

# Run a single test
go test -run TestParseDSN_Simple -v

# Run unit benchmarks
go test -bench=. -benchmem ./...

# Run Docker benchmarks
go test -tags=integration -bench=BenchmarkDocker -benchmem ./...

# Run fuzzing (30 seconds)
go test -fuzz=FuzzParseDSN -fuzztime=30s
go test -fuzz=FuzzParseFrame -fuzztime=30s

# Lint
golangci-lint run
```

## Architecture

This is a Go `database/sql` driver for the Geode graph database, communicating over QUIC with TLS 1.3.

### Package Structure

```
geode-client-go/
├── driver.go      # Driver registration, Connector
├── conn.go        # Connection with state machine
├── stmt.go        # Prepared statements
├── rows.go        # Result set iterator
├── tx.go          # Transactions
├── result.go      # Exec result
├── errors.go      # Error types (DriverError, TransportError, etc.)
├── config.go      # DSN parsing and Config struct
├── transport.go   # QUIC transport layer
├── protocol.go    # Wire protocol (frames, messages)
├── security.go    # TLS configuration
├── unicode.go     # Unicode conversion utilities
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

```
geode://host:port?options
host:port?options
host:port
host
```

Options: `page_size`, `hello_name`, `hello_ver`, `conformance`, `ca`, `cert`, `key`, `insecure`

Environment variables: `GEODE_HOST`, `GEODE_PORT`, `GEODE_TLS_CA`

### Wire Protocol

JSON line protocol over QUIC:

- Client messages: `HELLO`, `RUN_GQL`, `PULL`, `BEGIN`, `COMMIT`, `ROLLBACK`, `PING`
- Server responses: `SCHEMA`, `BINDINGS`, `ERROR`, `EXPLAIN`, `PROFILE`

### Error Types

- `DriverError` - Server errors with ISO/IEC 39075 status codes
- `TransportError` - Network/QUIC failures
- `ConfigError` - DSN parsing errors
- `SecurityError` - TLS/validation failures
- `StateError` - Invalid connection state transitions

### Security

- TLS 1.3 required for all connections
- Certificate verification enabled by default (use `insecure=true` only for testing)
- Query input validation (null bytes, control characters)
- Parameter name validation

### Integration Tests

Docker-based integration tests in `integration_test.go` use `testcontainers-go` to automatically:

- Pull and start a Geode container (`registry.gitlab.com/devnw/codepros/geode/geode:latest`)
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

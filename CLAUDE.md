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
├── transport.go       # Transport interface + QUIC transport layer
├── grpc_transport.go  # gRPC transport layer (server-pushed pages, per-Execute cancel)
├── params.go          # `?` -> typed proto.Param binding (no text interpolation)
├── vector.go          # geode.Vector (typed embedding parameter)
├── readonly.go        # Read-only classifier (fail-closed) + CALL allow-list
├── capabilities.go    # ServerInfo / Capabilities (SHOW VERSION, SHOW CAPABILITIES)
├── pool.go            # geode.Pool over *sql.DB
├── quic_pool.go       # Shared QUIC connection pool (stream multiplexing)
├── session.go         # Pool.Session: one pinned connection
├── health.go          # Pool.Health / Pool.Drain, probe stream
├── retryclass.go      # Classify + RetryClass
├── retry.go           # RetryPolicy, Pool.RetrySession
├── plan.go            # EXPLAIN / PROFILE typed payloads
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
| `insecure_tls_skip_verify` | Skip TLS verification (testing only; aliases: `insecure`, `skip_verify`) | false |
| `tls` | Enable/disable TLS (`0` or `false` to disable) | enabled |
| `user`/`username` | Authentication username | |
| `pass`/`password` | Authentication password | |
| `read_only` | Refuse mutating statements (aliases `readonly`, `read-only`) | false |
| `graph` | Graph bound at connect | |
| `tenant`/`tenant_id` | Tenant bound at HELLO | |
| `role` | Role bound at HELLO | |
| `strict_tenancy` | Refuse a config with no concrete tenant | false |
| `pool_size` | Max QUIC connections in the shared pool | 16 |
| `max_streams` | Max concurrent streams across the pool | 64 |
| `max_streams_per_conn` | Max streams per pooled connection | 8 |
| `health_probe_ms` | Probe interval in ms; `0` disables | 1000 |
| `connect_timeout` | Max seconds to wait for a connection; `0` = no timeout | 0 |

A repeated key resolves to its LAST occurrence, so a base DSN can be extended
by appending overrides.

Environment variables: `GEODE_HOST`, `GEODE_PORT`, `GEODE_TLS_CA`

### Wire Protocol

The driver uses Protocol Buffers (protobuf) for wire encoding over both transports.

#### QUIC Transport

- Length-prefixed protobuf messages (4-byte big-endian length prefix)
- Single bidirectional stream per connection
- TLS 1.3 with ALPN protocol `geode/1`

#### gRPC Transport

- Standard gRPC over HTTP/2
- `Execute` is a SERVER-STREAMING RPC: the engine pushes every page until one
  is final. The driver reads them; it never sends a `PullRequest` (the service
  has no `Pull` method).
- The release engine's gRPC listener is TLS-only, so a test DSN needs
  `insecure_tls_skip_verify=true` — `tls=0` cannot connect. The engine also
  discards `--listen-grpc`'s address and binds loopback, so the server needs
  `GRPC_ZIG_BIND_ADDR=0.0.0.0` (SONNY-1336 / GAP-SONNY806-03).
- No QUIC pool, therefore no probe stream and no pooled-connection census:
  `Pool.Health` degrades to a ping.

#### Who drives paging

`Transport.PagesArePushed()` is the predicate: `false` on QUIC (the driver
PULLs each page after the first), `true` on gRPC. `Rows.fetchNextPage` branches
on it. Two consequences worth knowing before touching `Rows` or `Conn`:

- A non-final `Rows.Close()` on a pushed-page transport must CANCEL the RPC
  (`streamCanceller` / `GRPCTransport.CancelActiveStream`). An abandoned stream
  parks geode's push loop on the session mutex and wedges the next statement on
  that session. Cancel, do not `markConnFailed`: the connection is still good.
- `Conn.drainTrailingFrames` runs on PULL-driven transports only. Only the
  context an `Execute` stream was created from binds that stream, so a
  background drain deadline cannot unblock a gRPC `Recv` — and on that wire the
  frame it would read is a page of the caller's result.

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

The gRPC parity lane lives in `integration_grpc_test.go`
(`TestIntegration_GRPC*`, `make test-integration-docker-grpc-race`): every QUIC
property asserted again on the gRPC wire, with a `GRPC` infix so the pairing is
greppable.

Engine pin: the lane runs `geodedb/geode:0.8.22` (manifest-list digest
`sha256:867d93375470fd18c60336036196a9ef9715a1922e063e8787dc05d8e798ddce`;
`v0.8.16`/`v0.8.17` are tags with no published image and are NOT pinnable), and
`engine_pin_test.go` asserts the running server reports it. One integration
test is known-RED and is NOT a regression:
`TestMultiplex_FindCreate_And_Merge_NoWedge` (throughput floor on a loaded
host). SONNY-1229 and SONNY-1560 are FIXED at 0.8.18 and verified, and
`TestIntegration_GRPCLargePushedResultFailsLoudly` now has an arm for 0.8.18's
prompt typed `ResourceExhausted` (its no-silent-truncation invariant is
unchanged).

**GAP-SONNY806-08 is FIXED (Task 16b).** The SONNY-1219 anchor stream
(`05b4653`) moved HELLO onto a dedicated stream and made request streams skip
it, so a scoped session read ZERO rows, its own writes included. HELLO carries
the tenant and the bound graph and the engine applies both PER STREAM, so
**every request stream now performs its own handshake** (`pooledHello`) and
`pooledGraphInit` reads the binding from that stream's own response. The anchor
is kept for what it is for: owning the QUIC connection's lifetime. Do not
reintroduce an `isAuthed` fast path on the request-stream path — an unbound
stream is a silent wrong answer, not an error. Regression:
`TestIntegration_TenantBoundSessionReadsItsOwnWritesOnEveryStream`, plus
`TestAnchor_RequestStreamCarriesTenantAndGraphBinding` /
`TestAnchor_EveryRequestStreamBindsItself` (no engine needed).

Host load is part of the result. Several integration tests seed their fixture
INSIDE the same context budget they then assert against —
`TestIntegration_CancelMidStream` seeds 2500 rows through `BatchExec` under a
120 s base context and then requires a post-cancel query to answer within what
is left. On a loaded host the seed alone can take most of that, and the test
fails at `query after cancellation: context deadline exceeded` with nothing
wrong in the driver. `-count=3` tells the two apart in one container: the seed
runs once (`cancelSeedOnce`), so if iterations 2 and 3 pass in under a second
the first failure was the seed competing for the budget, not a regression.

Running ONE integration test:
`make test-integration-docker-race GOFLAGS="-run=TestName -count=1"`. Do not
anchor the pattern with `$` — the shell eats it, `-run` then matches nothing
and the target exits 0 with no test having run. Confirm a
`--- PASS: <TestName>` line before believing a green exit code.

Benchmarks (prefixed with `BenchmarkDocker_`) test real-world performance against a running Geode instance.

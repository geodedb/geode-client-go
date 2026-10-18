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
| `insecure_tls_skip_verify` | Skip TLS verification (testing only; aliases: `insecure`, `skip_verify`) | false |
| `tls` | Enable/disable TLS (`0` or `false` to disable) | enabled |
| `user`/`username` | Authentication username | |
| `pass`/`password` | Authentication password | |
| `read_only` | Refuse mutating statements (aliases: `readonly`, `read-only`) | false |
| `graph` | Graph bound at connect (`USE GRAPH`, created first unless read-only) | |
| `tenant`/`tenant_id` | Tenant bound for the session at HELLO | |
| `role` | Role bound for the session at HELLO | |
| `strict_tenancy` | Refuse a config with no concrete tenant (`ErrTenantRequired`) | false |
| `pool_size` | Max QUIC connections in the shared pool (QUIC only) | 16 |
| `max_streams` | Max concurrent streams across the pool (QUIC only) | 64 |
| `max_streams_per_conn` | Max streams multiplexed on one pooled connection (QUIC only) | 8 |
| `health_probe_ms` | Pooled-connection liveness probe interval, ms; `0` disables (QUIC only) | 1000 |
| `connect_timeout` | Max seconds to wait for a connection; `0` = no timeout | 0 |

A repeated DSN key resolves to its **last** occurrence, so appending an option
to a DSN that already sets it overrides it.

### Read-only connections

`?read_only=true` makes a connection a reader. Enforcement is two independent
layers, and both are always on:

1. **Client-side, before the wire.** Every statement is classified by
   `IsMutatingStatement`; a mutating one is refused with `ErrReadOnly` and no
   bytes leave the process.
2. **Server-side, unconditionally.** Every `ExecuteRequest` carries
   `read_only = true` (proto field 6). The engine refuses any mutating program
   with GQLSTATE `42501` regardless of the connection's role, so a classifier
   miss still fails at the database.

```go
db, err := sql.Open("geode", "quic://localhost:3141?graph=docs&read_only=true")

_, err = db.ExecContext(ctx, "CREATE (n:Doc {id: ?})", 1)
// errors.Is(err, geode.ErrReadOnly) == true — nothing was sent

var n int64
err = db.QueryRowContext(ctx, "MATCH (d:Doc) RETURN count(d)").Scan(&n) // fine
```

The classifier **fails closed**: a statement is read-only only when its leading
keyword is on a closed allow-list (`MATCH`, `RETURN`, `SHOW`, `EXPLAIN`,
`PROFILE`, `FOR`, `LET`, `FILTER`, `ORDER`, `SKIP`, `LIMIT`, `OPTIONAL`) and it
contains no mutating keyword anywhere. Everything else — including empty,
unparseable, and unrecognised statements — is treated as mutating. Comments and
string literals are stripped first, so a write cannot hide behind
`/* MATCH */ CREATE ...` and a keyword inside data stays data. A `;`-separated
program (including a `graph.Batch` pipeline) is read-only only when *every*
statement in it is.

`CALL` is classified by target against a closed allow-list mirroring the
engine's own read-only procedure table: `geode.fts.search` is a read but
`geode.fts.rebuild` is not, `geode.ts.range` is a read but `geode.ts.insert` is
not, and any unrecognised procedure is a write.

Two behaviours worth knowing:

- A read-only connection does **not** run `CREATE GRAPH IF NOT EXISTS` for
  `?graph=`; it only runs `USE GRAPH`, which fails if the graph does not exist.
  Create the graph from a writable connection first.
- Transactions work normally: `BeginTx` succeeds (and pins
  `BeginRequest.read_only`), reads inside the transaction work, and a mutation
  inside it is refused with `ErrReadOnly`. Because that refusal emits nothing,
  the transaction stays open and usable for further reads — the caller still
  decides whether to `Commit` or `Rollback`.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GEODE_HOST` | Default host |
| `GEODE_PORT` | Default port |
| `GEODE_TLS_CA` | Default CA certificate path |

## Parameters

`?` placeholders are bound as **typed wire parameters**. The driver rewrites
each `?` into a generated name (`$_gq0`, `$_gq1`, …) and sends the value in
`ExecuteRequest.params` as a typed `proto.Param`. **No argument value is ever
written into query text** — there is no string interpolation path left in the
driver to escape badly.

```go
// The engine receives "MERGE (n:Chunk {chunk_id: $_gq0, score: $_gq1})"
// plus two typed params. "c-1" never appears in the statement.
_, err := db.ExecContext(ctx,
    "MERGE (n:Chunk {chunk_id: ?, score: ?})", "c-1", 0.91)
```

- Named `$name` references in your own statement text pass through untouched;
  bind them with `sql.Named("name", v)`.
- `_gq` is a **reserved** parameter-name prefix. A caller-supplied name in that
  namespace is refused with `ErrReservedParamName` rather than silently
  shadowing a positional argument.
- Scalars (`string`, `[]byte`, `bool`, integers, floats, `time.Time`) bind as
  typed values; slices, arrays and maps keep their structural `list_val` /
  `map_val` encoding.
- The property is fuzzed, not merely tested: `FuzzBindParameters` asserts that
  the statement text the engine receives is **independent of the argument
  values**, for arbitrary inputs (`params_fuzz_test.go`).

## Vectors

`geode.Vector` is a `[]float32` that binds as a typed vector parameter:

```go
var ids any
err := db.QueryRowContext(ctx,
    "RETURN knn('embedding', ?, ?, 'cosine')",
    geode.Vector{0.1, 0.2, 0.3}, 10,
).Scan(&ids)
```

- The call shape is `knn('property', ?, k, 'metric')` — the property name and
  the metric are literals, the query vector and `k` are bound.
- Dimensions are capped at 65535. Over that, the driver refuses before the
  wire.
- Engine facts at 0.8.15: a dimension mismatch answers GQLSTATE `22000` and a
  metric mismatch `42704` (both in `DriverError.Code`); only `vectorf32($v)`
  storage arms the vector index (SONNY-1224); and `knn` silently downgrades to
  a scan when another graph shares the property name (SONNY-1223).

## Tenancy

`?tenant=` (alias `tenant_id`) and `?role=` bind the session at HELLO — a
tenant is a property of the connection, not of a statement, so every statement
on that connection inherits it.

`?strict_tenancy=true` asserts the client intends to run tenant-scoped. A
config with no concrete tenant (empty or `default`) is refused by
`(*Config).Validate()` with `ErrTenantRequired` — **not** by `ParseDSN`, which
never validates and always returns a `*Config` for any DSN that merely parses.
`Validate` runs automatically on the first connection attempt
(`connect`/`connectPooled`) and inside `NewConnector`, so `sql.Open` plus a
query eventually reaches it — but a caller that wants to fail at startup,
before opening a connection, must call `cfg.Validate()` itself. The engine's
own refusal (GQLSTATE `42501`, "Tenant scope required") is surfaced as the
typed `ErrTenantScopeRequired`, so callers gate on `errors.Is` in both cases
instead of matching engine prose.

> **Known engine defect (SONNY-1229, GAP-SONNY806-02, Critical).** geode 0.8.15
> does **not** apply the session tenant filter on the `MATCH` read path. Two
> sessions bound to different tenants read each other's rows in the same graph,
> even with `GEODE_STRICT_TENANCY=1`. The driver cannot work around it. The
> reproducer `TestIntegration_CrossTenantReadIsDenied` is RED by design until
> the engine fixes it. **Do not rely on tenant binding as an isolation boundary
> at 0.8.15.**
>
> **Fixed at the pinned engine 0.8.18** (`0beb108b`, SONNY-1318/1326), verified
> 2026-09-16; `TestIntegration_CrossTenantReadIsDenied` now passes. Everything
> above describes 0.8.15 and is kept as the record of what was measured there.

## Cancellation

Cancelling a query's context stops the read promptly and reports the **context**
error, not the transport error the cancellation manifests as on the wire:

```go
ctx, cancel := context.WithCancel(parent)
rows, _ := db.QueryContext(ctx, "MATCH (n:Chunk) RETURN n.id")
rows.Next()
cancel()
// rows.Err() satisfies errors.Is(err, context.Canceled)
```

A `Rows` abandoned mid-result under a cancelled context **retires its
connection**: `Conn.IsValid()` becomes false and `ResetSession` returns
`driver.ErrBadConn`, so `database/sql` discards the connection instead of
handing a desynchronised stream to the next borrower. 0.8.15 has no
client-to-server cancel or discard message, so the stream itself is what gets
closed.

Closing a `Rows` **early** (before its final page) with a still-live context is
different and cheap: on gRPC the driver cancels the abandoned server-streaming
RPC and keeps the connection, because an uncancelled one would leave the
engine's push loop holding the session mutex and wedge the next statement on
that session.

## Capability discovery

Ask the server what it supports before using a feature; unknown features are
**unsupported**, never assumed:

```go
cfg, err := geode.ParseDSN("quic://localhost:3141")
if err != nil { return err } // malformed DSN only — ParseDSN never validates

// A caller that wants to fail at startup, before opening a connection, calls
// Validate explicitly: it is what catches a bad strict_tenancy config
// (ErrTenantRequired) and everything else NewConnector would otherwise only
// surface once a connection is attempted.
if err := cfg.Validate(); err != nil { return err }

connector, err := geode.NewConnector(cfg)
if err != nil { return err }

info, err := connector.ServerInfo(ctx)          // SHOW VERSION + SHOW CAPABILITIES
if err != nil { return err }
if !info.Capabilities.Has(geode.CapVectorKNN) {
    return fmt.Errorf("engine %s lacks vector_knn", info.Version)
}
```

`ServerInfo` uses a dedicated connection by design (see SONNY-1227), and the
0.8.15 capability contract is pinned as a golden set of **59 rows** —
`capabilities_test.go` fails when the engine's advertised set drifts.

## Pool health and drain

`geode.Pool` adds liveness the `database/sql` pool does not have:

```go
pool, _ := geode.NewPool("quic://localhost:3141?health_probe_ms=1000")
h := pool.Health(ctx)   // zero value is UNHEALTHY — a failed probe never reads as healthy
_ = pool.Drain(ctx)     // stop handing out connections, let in-flight work finish
```

Pooled QUIC connections are verified with **real protocol I/O** (a PING on a
long-lived per-connection probe stream, never opened or closed per probe), so a
connection whose server side went away is evicted instead of being handed to
the next query. **Consumers no longer need an external pool-recovery
watchdog.**

Over gRPC there is no QUIC pool, so there is no probe stream and no
pooled-connection census: `Health` degrades to a round trip through
`database/sql`.

## Retry classes

`Classify(err)` maps any error to one of four `RetryClass` values, so a bulk
loader can act on a failure instead of guessing:

| Class | Meaning |
|-------|---------|
| `RetryNever` | Deterministic. The same request fails the same way. |
| `RetrySameConn` | Transient; retry on the connection you have. |
| `RetryNewConn` | The connection or its server is wrong; take a new one. |
| `RetryBackoff` | Overloaded/exhausted; wait before retrying. |

`RetryPolicy.Classes` selects which classes a `Retry` attempt covers, and
`Pool.RetrySession` re-pins a session across attempts (honouring
`RetryNewConn` by closing and re-pinning).

Three classifications are pinned to engine behaviour at 0.8.15 and deviate from
the obvious reading of the GQLSTATE:

- `40001` with the engine's `ClusterNotLeader` / `ClusterWriteNotReplicable`
  message prefixes → `RetryNewConn` (another node can serve it), not
  `RetrySameConn`.
- `08006` → `RetryNever`, deliberately exempt from the rest of class `08`'s
  `RetryNewConn` answer. It is a generic engine TIMEOUT (`error.Timeout`, not
  something specific to clustered writes or the connection itself) reachable
  from any request path, so its outcome is **unknown** — the operation may
  have committed — and a blind retry can duplicate a row. Retryable only with
  an idempotency key, which is the caller's decision, not the driver's.
- `53200` (executor budget exceeded — read/write working-set limit, **not**
  out of memory) → `RetryNever`. Deterministic for the same statement: the
  working set does not shrink on a second attempt, so the fix is a narrower
  query or a larger budget.

> **`Pool.RetrySession` is not idempotent for you.** It re-runs your function.
> Only pass work that is safe to run twice (`MERGE`, not `CREATE`).

## Query plans

`Explain` and `Profile` return typed values, so a retrieval path can **record**
which access path a query used instead of guessing:

```go
plan, err := pool.Explain(ctx, "MATCH (n:Chunk) WHERE n.chunk_id = ? RETURN n", "c-1")
if err != nil { return err }
if !plan.UsesIndex() {
    log.Printf("seed query is a scan: %s", plan.Schema)
}
```

- Both are gated on the engine advertising `explain` / `profile` in
  `SHOW CAPABILITIES`, and the gate **fails closed** in both directions — an
  unlisted capability and a probe that could not be completed both refuse with
  `ErrCapabilityUnsupported` before anything reaches the wire. That matters
  most for `PROFILE`, which EXECUTES the statement.
- Arguments are bound through the same parameter path a real query uses, so the
  plan describes the parameterised statement.
- `ExplainPlan.UsesIndex()` fails closed: an empty, nil or unreadable plan
  reports `false`.
- `ErrPlanKindMismatch` refuses a response whose payload does not match the
  request, instead of handing back a zero-valued report with a nil error.

> **`ProfileReport.UsesIndex()` is gated on `Measured()` (SONNY-1335).** A
> tenant-bound or RLS-evaluating session is demoted off geode 0.8.15's planned
> read runtime, and the statement fallback that answers appends a
> `NodeIndexScan` for **any** equality predicate with no catalog lookup, naming
> it the literal `heuristic_index`. Timings from that path are synthetic too.
> `Measured()` reports whether the profile is real; ask **EXPLAIN** which index
> was used.

## Transaction pinning

`Pool.Session` pins one connection so a multi-statement unit of work cannot
straddle connections:

```go
s, err := pool.Session(ctx)
if err != nil { return err }
defer s.Close()

err = s.TX(ctx, nil, func(tx *sql.Tx) error {
    _, err := tx.ExecContext(ctx, "MERGE (n:Chunk {chunk_id: ?})", "c-1")
    return err
})
```

`Session.ConnID()` identifies the pinned connection, nesting a second operation
returns `ErrSessionBusy`, and a session whose connection was retired by a
cancellation returns `ErrSessionRetired` (re-pin with `Pool.RetrySession`).

## gRPC transport parity

The gRPC transport carries the same driver surface as QUIC — parameters,
vectors, read-only, tenancy, cancellation, plans, `Session` — and the parity is
asserted against a real listener (`TestIntegration_GRPC*`, run by
`make test-integration-docker-grpc-race`). One engine limit and four
deliberate differences separate the two wires.

> **gRPC is for SMALL results at engine 0.8.15 (GAP-SONNY806-07, High, engine
> ticket SONNY-1560).** A single `Execute` delivers roughly **64 KiB** of
> pushed payload — a few thousand narrow rows, or a few hundred wide ones —
> and then stalls until an engine-side timeout, whatever the client does. It
> is HTTP/2 flow control: the engine does not act on the `WINDOW_UPDATE`
> frames grpc-go emits as the driver consumes each page, and the driver has no
> lever. The same query over QUIC returns 50 000 rows in ~1.5 s. **QUIC is the
> release transport**; use gRPC where the wire is mandated and the results are
> small. A stalled result surfaces as an error, never as a short result set
> that looks complete (`TestIntegration_GRPCLargePushedResultFailsLoudly`).
>
> **The stall's blast radius crosses sessions.** The engine parks — not just
> the stalled stream — for roughly 30 s while it holds the flow-control
> window open and ignores the client's `RST_STREAM`. While parked, a
> **different session's** multi-statement transaction against the same engine
> process can fail mid-way with `driver.ErrBadConn`, not only the session that
> triggered the stall: `TestIntegration_GRPCBatchExecPastOnePage` (a fresh
> pool, a different connection) fails around statement 29 of 647 when it runs
> immediately after an oversized-result test, and passes cleanly on its own.
> A caller that triggers one oversized gRPC result should expect unrelated
> work on OTHER connections against the same engine to fail for roughly 30 s,
> not just the query that overflowed. See `docs/GAP_ANALYSIS.md` for the full
> measurement.
>
> **Fixed at the pinned engine 0.8.18** (`33a39499`, `3d3b1b60`, grpc-zig
> v0.1.10), verified 2026-09-16. An oversized result now ends in **0.62 s**
> with a typed gRPC `ResourceExhausted` ("stream window exhausted: peer
> stopped draining the pushed result") instead of parking ~30 s, and the
> cross-session blast radius is gone: `TestIntegration_GRPCBatchExecPastOnePage`
> passes immediately after the oversized-result test where it used to fail
> around statement 29. Everything above describes 0.8.15 and is kept as the
> record of what was measured there.

The four deliberate differences:

- **Pages are server-pushed.** geode serves `Execute` as a server-streaming RPC
  and pushes every page until one is final; the driver reads them. QUIC is
  PULL-driven (the driver asks for each page after the first). `Transport`
  answers which wire it is with `PagesArePushed()`.
- **No QUIC pool.** No connection pool, no probe stream, no `Drain` of pooled
  connections; `Pool.Health` degrades to a ping.
- **TLS-only listener.** A release engine's gRPC listener does not speak
  plaintext, so a test/dev DSN needs `insecure_tls_skip_verify=true` against
  the self-signed certificate — `tls=0` cannot connect.
- **`--listen-grpc <addr>` ignores the address (SONNY-1336,
  GAP-SONNY806-03).** The engine parses and discards it and the vendored
  grpc-zig binds `127.0.0.1`, so a published container port reaches nothing.
  Start the server with `GRPC_ZIG_BIND_ADDR=0.0.0.0`.

Five `QuicClientMessage` types have no `GeodeService` method — savepoint,
rollback_to, backup, restore, upload_backup — and are refused with the typed
`ErrUnsupportedOnTransport` (`RetryNever`).

## Engine support

This driver is developed and tested against **geode 0.8.22**
(`geodedb/geode:0.8.22`, manifest-list digest
`sha256:867d93375470fd18c60336036196a9ef9715a1922e063e8787dc05d8e798ddce`). The
integration lane pins that image, and `engine_pin_test.go` asserts the running
server reports it.

0.8.18 is the first published image carrying the 0.8.16 fix set: the `v0.8.16`
and `v0.8.17` tags exist in the engine repository but never produced a
container image, so neither can be pinned.

Known-RED tests, each tracking a defect rather than a flake. "Engine fix"
names the engine release that carries the server-side fix — it is NOT a claim
that this driver has been re-measured against it; every measurement in this
repository was taken against the pinned 0.8.15 image:

| Test | Tracking | Cause |
|------|----------|-------|
| `TestMultiplex_FindCreate_And_Merge_NoWedge` | — | Asserts a throughput floor; fails on a loaded host, not a correctness defect |

Fixed and verified on 2026-09-16: the cross-tenant read leak (SONNY-1229 /
GAP-SONNY806-02, Critical) and the gRPC ~64 KiB stall with its cross-session
blast radius (SONNY-1560 / GAP-SONNY806-07, High), both by the move to engine
0.8.18; and **GAP-SONNY806-08 (Critical, DRIVER)** — the SONNY-1219 anchor
stream had moved HELLO off the request streams, so a tenant-bound session
carried no scope and read zero rows including its own writes. Every request
stream now performs its own HELLO, which is what carries the tenant and graph
binding; the anchor is kept for the connection lifetime it exists to own.
`TestIntegration_GRPCLargePushedResultFailsLoudly` gained an arm for 0.8.18's
prompt typed `ResourceExhausted` and still fails hard on a silent truncation.

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

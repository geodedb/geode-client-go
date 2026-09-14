# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Pool health probing and graceful drain. `Pool.Health(ctx)` returns a
  `HealthStatus` (`Healthy`, `PooledConns`, `LiveConns`, `InFlightStreams`,
  `LastProbeErr`, `LastProbeAt`) whose zero value is unhealthy — health fails
  closed — and whose `LastProbeErr` is a static classification rather than the
  underlying error, so a readiness endpoint cannot leak a transport error's
  remote address. Behind it, `(*QUICPool).Probe(ctx)` verifies every pooled QUIC
  connection with a REAL round trip (a protocol-level PING on a long-lived probe
  stream) and evicts the ones that do not answer, so `acquire` can never hand
  out a connection the server has forgotten. A pool-owned goroutine runs the
  probe every `Config.HealthProbeInterval` (DSN `?health_probe_ms=N`, default
  1000 ms, 0 disables); it starts lazily on first use, never runs concurrently
  with itself, and is stopped and waited for by `Drain` and `Close`.
  `Pool.Drain(ctx)` is the graceful counterpart to `Close`: new work is refused
  with `ErrPoolDraining` (including `Pool.Session`, which reports it instead of
  blocking), in-flight statements are given until `ctx` expires, and only then
  is everything closed; it is idempotent and closes the pool even if `ctx`
  expires first.

  A probe never closes a stream on a connection that is still in the pool.
  A failed probe ABANDONS its stream — it takes `probeStrikes` (2) consecutive
  failures to evict a connection, so at strike 1 the connection is still
  serving, and geode 0.8.15 answers a stream closing on a pooled connection by
  tearing the whole connection down (SONNY-1227). The next probe opens a fresh
  stream; abandoned streams are closed with the connection itself, and a
  connection that has abandoned `probeStrikes` of them is condemned, which
  bounds them. Two budgets follow from that: a reused stream gets the
  steady-state budget, while a replacement stream gets it plus a stream-setup
  allowance, because the server materialises a probe stream when its first
  frame arrives (~70 ms measured) and charging that to a bare round trip was
  enough to evict a healthy connection after one slow moment. A probe that
  fails because the CALLER's context ended is reported but records no strike
  and does not disturb the stream, so a short or cancelled `Health` context can
  never condemn the pool. A connection no HELLO has run on yet is skipped by
  the probe and is counted neither live nor healthy.

  This replaces a local-only liveness check that could not see an orphaned
  connection: after an ungraceful server restart the pool kept handing out a
  pre-restart connection and every statement on it waited out its caller's full
  deadline (measured: 6.000 s of a 6 s budget against engine 0.8.15).
  Downstream watchdogs written to work around that are no longer needed
  (SONNY-806).

- Transaction pinning: `Pool.Session(ctx)` checks one connection out of the
  pool and returns a `*Session` whose `ExecContext`, `QueryContext` and
  `TX(ctx, opts, fn)` all ride that one connection — and therefore one QUIC
  stream — so a multi-statement ingest or a run of transactions cannot straddle
  connections. `Session.ConnID()` reports the pinned connection's identity
  (also exposed on the driver connection as `(*Conn).ConnID()`), and
  `Session.BoundGraph()` the graph it is bound to. `TX` runs the whole callback
  on the pinned connection, commits on a nil return, rolls back on an error,
  and rolls back then re-panics on a panic; a nested `TX` — or any statement
  sent to the `Session` instead of to the `*sql.Tx` — reports `ErrSessionBusy`
  instead of deadlocking. Once the pinned connection fails (a cancelled query
  retires it, a transport error marks it), the `Session` is retired rather than
  silently re-pinned to a different connection: every method then returns
  `ErrSessionRetired`, which matches both `ErrSessionClosed` and
  `driver.ErrBadConn`. `Close` is idempotent and releases the connection
  exactly once. Caller contract after a retirement (SONNY-1227): geode 0.8.15
  closes the whole pooled QUIC connection when the stream that carried its
  handshake goes away, so a `Session` pinned immediately after a retirement may
  fail on its first statement with an error matching `driver.ErrBadConn` —
  close that `Session` and pin again. `Pool.Session` deliberately does not hide
  this behind a validating round trip at pin time (SONNY-806).
- Capability discovery: `Connector.ServerInfo(ctx)` reports the engine's
  `SHOW VERSION` string verbatim, the raw ordered `SHOW CAPABILITIES` rows
  (`ServerInfo.Rows`), the boolean feature matrix built only from rows whose
  value is exactly `true`/`false` (`Capabilities`, with `Has`/`Names`; unknown
  features are unsupported), the version-valued metadata rows kept out of that
  map (`Metadata`, currently only `sonny_profile_version`), and the HELLO
  response metadata. Values are never coerced or trimmed, duplicate and empty
  feature names are rejected, and a non-boolean value from a feature that is
  not known metadata is an error rather than a silent `false`
  (`ErrMalformedCapabilityRows`). Feature-name constants (`CapVectorKNN`,
  `CapReadOnlyExecute`, ...) cover all 59 rows geode 0.8.15 emits and are
  pinned to `testdata/capabilities_0.8.15.golden` (SONNY-806).
- The server's HELLO capabilities were previously decoded and discarded. They
  are now captured per connection and exposed via `(*Conn).HelloCapabilities()`
  and `ServerInfo.HelloCapabilities`, alongside `(*Conn).BoundGraph()` and
  `(*Conn).SessionID()`. HELLO capabilities and `SHOW CAPABILITIES` are
  different vocabularies; neither is derived from the other (SONNY-806).

### Security

- Read-only connections: `?read_only=true` (aliases `readonly`, `read-only`)
  refuses mutating statements client-side with `ErrReadOnly` before any bytes
  reach the wire, and sets `ExecuteRequest.read_only` so the engine refuses
  them too with GQLSTATE `42501` regardless of role. Classification
  (`IsMutatingStatement`) fails closed: empty, unparseable and unrecognised
  statements, unknown `CALL` targets, and any `;`-separated program containing
  a write are all treated as mutating (SONNY-806).
- `?` placeholders are no longer inlined into the query text. Every argument
  now crosses the wire as a typed `proto.Param` (`$_gqN`), so no argument value
  can reach the GQL parser as source text (SONNY-806).

### Changed

- Query arguments are now bound as: `database/sql`'s `driver.Value` scalar set
  (`nil`, `string`, `[]byte`, `bool`, `int64`, `float64`); `time.Time`, encoded
  as the historical `fmt.Sprintf("%v", t)` string (round-trippable via
  `ScanTime`); other Go integer/float types and anything implementing
  `driver.Valuer`, narrowed by `database/sql`'s default converter (`int` ->
  `int64`, `float32` -> `float64`, etc.); and slices, arrays and maps, which
  keep their structural `list_val`/`map_val` encoding. Any other struct or
  type is rejected with an error instead of being silently formatted into a
  string.
- Parameter names beginning with `_gq` are reserved by the driver and rejected.
- `ParseDSN`: a DSN option repeated across the query string now resolves to
  its LAST occurrence for every option, not the first, so a DSN can be
  extended by appending overrides (e.g. a shared base DSN plus a per-call
  `&tenant=...&graph=...`) (SONNY-806).

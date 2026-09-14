package geode

// health.go is the pool's liveness surface: a probe that uses real network
// I/O, a cheap readout of its last result, and a graceful drain
// (SONNY-806 / GEO-GO-UP-001, Lane D Task 11).
//
// The defect it exists for: isQUICConnAlive (quic_pool.go) only reads the
// local quic.Conn context. After a Geode process restarts, the server no
// longer recognises the connection's ID and silently drops its packets — but
// nothing local went wrong, so the context stays live, the pool keeps handing
// the orphaned connection out, and every operation on it blocks for its
// caller's full deadline instead of failing. Only a round trip can tell the
// difference, so the pool performs one on a schedule and evicts what does not
// answer.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Health probe defaults.
const (
	// DefaultHealthProbeInterval is how often the driver verifies its pooled
	// QUIC connections with a real round trip, and therefore the ceiling on
	// how long an orphaned connection can keep being handed out.
	//
	// One second is the detection budget the downstream watchdog this replaces
	// settled on (mentats/internal/geodegraph carried ~100 lines to work
	// around the missing probe), for a restart that itself completes in well
	// under a second. Override via DSN ?health_probe_ms=N; 0 disables.
	DefaultHealthProbeInterval = time.Second

	// minProbeTimeout and maxProbeTimeout bound a STEADY-STATE probe: one PING
	// round trip on a probe stream the server has already materialised, which
	// costs well under a millisecond against geode 0.8.15. The timeout tracks
	// the configured interval between these bounds, which is what keeps the
	// detection budget at roughly two intervals — one to notice, one to fail.
	minProbeTimeout = 100 * time.Millisecond
	maxProbeTimeout = 2 * time.Second

	// probeWarmupTimeout bounds the FIRST round trip on a newly opened probe
	// stream. It is a much larger budget on purpose: the server materialises a
	// stream when its first frame arrives, and measured against geode 0.8.15
	// that costs ~70ms — within a whisker of the 100ms steady-state floor
	// above, and enough to fail every first probe if the two shared a budget
	// (measured: three consecutive first probes timed out at exactly 100ms,
	// the fourth succeeded in 68.6ms on a 5s budget, the fifth in 386µs).
	//
	// Paying it here rather than raising minProbeTimeout keeps the detection
	// loop fast: stream setup is a one-off cost, and an orphaned connection
	// always has its probe stream open already (the loop opened it long
	// before the server went away), so a restart is detected on the small
	// budget, not this one.
	probeWarmupTimeout = 5 * time.Second

	// probeStrikes is how many CONSECUTIVE failed probes condemn a connection.
	//
	// One failure is not evidence: a probe can lose its race with a saturated
	// engine, and evicting on that closes a healthy connection with live
	// streams on it. Measured — with eviction on a single failure,
	// TestLoad_PreparedStatementStorm (40 workers over a 10-connection pool)
	// failed 2 runs in 5 with a worker timing out, against 0 in 4 with probing
	// disabled. Two consecutive failures cost one extra interval of detection
	// and cannot be produced by a single slow moment.
	//
	// Reporting is not gated on this: ONE failed probe already makes
	// HealthStatus unhealthy. Only the destructive act waits for confirmation.
	probeStrikes = 2

	// probeRoundSlack is the headroom the probe loop adds when bounding a
	// whole round. Connections are probed concurrently, so a round is one
	// connection's budget deep plus scheduling.
	probeRoundSlack = 250 * time.Millisecond

	// drainPollInterval is how often a drain re-checks whether the statements
	// it is waiting for have finished.
	drainPollInterval = 10 * time.Millisecond

	// drainCloseGrace bounds the final close of the QUIC pool, after
	// database/sql has already released every driver connection. It should
	// complete immediately; the bound exists so a leaked stream turns into a
	// reported error rather than a Drain that never returns.
	drainCloseGrace = 5 * time.Second
)

// Probe failure classifications. These are the ONLY strings that ever reach
// HealthStatus.LastProbeErr: a readiness endpoint is a public-ish surface, and
// the underlying errors carry remote addresses (*TransportError.Addr) that
// have no business being published. They are static — no address, no tenant,
// no graph, no query.
const (
	probeErrTimeout        = "geode: health probe timed out"
	probeErrCanceled       = "geode: health probe cancelled"
	probeErrPeerClosed     = "geode: health probe failed: connection closed by peer"
	probeErrTransport      = "geode: health probe failed: transport error"
	probeErrProtocol       = "geode: health probe failed: unexpected protocol response"
	probeErrRefused        = "geode: health probe failed: server refused the probe"
	probeErrPoolClosed     = "geode: pool is closed"
	probeErrPoolDraining   = "geode: pool is draining"
	probeErrNoPooledConns  = "geode: no pooled connections to probe"
	probeErrNotInitialized = "geode: pool is not initialised"
	probeErrUnknown        = "geode: health probe failed"
)

// Probe-path sentinels.
var (
	// errNoPooledConns reports a probe that had nothing to verify. It is not a
	// failure of the server, but it is not evidence of health either — see
	// Pool.Health, which turns it into a real round trip.
	errNoPooledConns = errors.New("geode: no pooled connections to probe")

	// errProbeUnexpectedFrame reports a probe stream that answered PING with
	// something else, i.e. a desynchronised stream.
	errProbeUnexpectedFrame = errors.New("geode: probe stream returned a non-PING frame")

	// errProbeRefused reports a PING the server answered with ok=false.
	errProbeRefused = errors.New("geode: server refused the liveness probe")
)

// probeCount counts liveness probes performed by this process, so a test can
// assert the probe path actually ran rather than inferring it from timing.
//
//nolint:gochecknoglobals // process-wide diagnostic counter
var probeCount atomic.Uint64

// ProbeCount returns the number of network liveness probes this process has
// performed across all pools. It is a diagnostic, exported for tests and
// operators; it is not part of the connection contract.
func ProbeCount() uint64 { return probeCount.Load() }

// HealthStatus is a point-in-time view of a Pool's connection health.
//
// The zero value is unhealthy: health fails closed, so a caller that forgets
// to check an error, or reads a status nothing has written yet, gets "not
// ready" rather than a false all-clear.
type HealthStatus struct {
	// Healthy is true only when a real network round trip succeeded against
	// every connection the pool holds, at LastProbeAt.
	Healthy bool

	// PooledConns is the number of underlying QUIC connections the pool holds.
	PooledConns int

	// LiveConns is how many of them answered the last probe.
	LiveConns int

	// InFlightStreams is the number of QUIC streams checked out across those
	// connections when the probe ran.
	InFlightStreams int

	// LastProbeErr is a static classification of the probe failure, empty when
	// healthy. It is a classification rather than the error itself so a status
	// can be logged or serialised into a readiness endpoint without leaking a
	// wrapped transport error's remote address.
	LastProbeErr string

	// LastProbeAt is when the probe that produced this status ran.
	LastProbeAt time.Time
}

// classifyProbeErr maps a probe failure onto one of the fixed strings above.
// It never returns the error's own text.
func classifyProbeErr(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return probeErrTimeout
	case errors.Is(err, context.Canceled):
		return probeErrCanceled
	case errors.Is(err, errProbeUnexpectedFrame):
		return probeErrProtocol
	case errors.Is(err, errProbeRefused):
		return probeErrRefused
	case errors.Is(err, errNoPooledConns):
		return probeErrNoPooledConns
	case errors.Is(err, ErrPoolDraining):
		return probeErrPoolDraining
	case errors.Is(err, ErrClosed), errors.Is(err, sql.ErrConnDone):
		return probeErrPoolClosed
	}

	var te *TransportError
	if errors.As(err, &te) {
		return probeErrTransport
	}
	if isRemoteCloseErr(err) {
		return probeErrPeerClosed
	}
	return probeErrUnknown
}

// isRemoteCloseErr reports a raw (unwrapped) end-of-stream, which is how a
// peer that went away surfaces when no TransportError wrapped it.
func isRemoteCloseErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// Health reports whether the pool can serve work right now.
//
// It is cheap by design: it reads the result the pool's own probe loop
// recorded, and performs I/O only when there is no usable result to read —
// when probing is disabled (health_probe_ms=0), when nothing has probed yet,
// or when the last result is older than two probe intervals. In those cases it
// performs ONE bounded probe synchronously, and LastProbeAt is that probe's
// timestamp rather than a stale one, so a caller can always tell how fresh the
// answer is.
//
// A pool holding no connections has had nothing verified, so Health does not
// claim health from it: it performs one bounded round trip through
// database/sql instead, which dials, handshakes and pings. That is the only
// honest answer to "are you ready" from a cold pool.
//
// Health never mutates application data: the probe is a protocol-level PING.
func (p *Pool) Health(ctx context.Context) HealthStatus {
	if p == nil || p.db == nil {
		return HealthStatus{LastProbeErr: probeErrNotInitialized, LastProbeAt: time.Now()}
	}
	if p.draining.Load() {
		return HealthStatus{LastProbeErr: probeErrPoolDraining, LastProbeAt: time.Now()}
	}

	qp := p.quicPool()
	if qp == nil {
		// No QUIC pool to probe (gRPC transport, or a Pool built around a
		// connector this driver did not create). A round trip through
		// database/sql is the whole liveness signal available.
		return p.pingHealth(ctx, HealthStatus{})
	}
	if qp.isClosed() {
		// A closed pool can still be holding a healthy-looking status from
		// before it was closed. Fail closed rather than answer with it.
		return HealthStatus{LastProbeErr: probeErrPoolClosed, LastProbeAt: time.Now()}
	}

	h := qp.lastHealth()
	if p.healthIsStale(qp, h) {
		probeCtx, cancel := context.WithTimeout(ctx, probeWarmupTimeout+probeRoundSlack)
		_ = qp.Probe(probeCtx)
		cancel()
		h = qp.lastHealth()
	}

	if h.PooledConns == 0 {
		return p.pingHealth(ctx, h)
	}

	// InFlightStreams comes from the pool itself, not from the recorded
	// status: it moves constantly and the caller asked about now. PooledConns
	// and LiveConns stay as the probe recorded them, so the pair remains
	// internally consistent — LiveConns is a statement about the connections
	// that probe verified, not about whatever is in the pool a moment later.
	_, h.InFlightStreams = qp.counts()
	return h
}

// healthIsStale reports whether the recorded status is too old to answer with.
func (p *Pool) healthIsStale(qp *QUICPool, h HealthStatus) bool {
	if h.LastProbeAt.IsZero() {
		return true
	}
	if qp.probeInterval <= 0 {
		// Probing is disabled, so nothing will ever refresh the status: every
		// call has to probe for itself.
		return true
	}
	return time.Since(h.LastProbeAt) > 2*qp.probeInterval
}

// pingHealth answers with one bounded round trip through database/sql. It is
// used when there is no pooled QUIC connection to probe directly; the ping
// leaves its connection idle in the sql pool rather than closing it, which
// matters because closing it would be the SONNY-1227 teardown.
func (p *Pool) pingHealth(ctx context.Context, h HealthStatus) HealthStatus {
	pingCtx, cancel := context.WithTimeout(ctx, DefaultPingTimeout)
	defer cancel()

	h.LastProbeAt = time.Now()
	if err := p.db.PingContext(pingCtx); err != nil {
		h.Healthy = false
		h.LiveConns = 0
		h.LastProbeErr = classifyProbeErr(err)
		return h
	}

	h.Healthy = true
	h.LastProbeErr = ""
	if qp := p.quicPool(); qp != nil {
		h.PooledConns, h.InFlightStreams = qp.counts()
		h.LiveConns = h.PooledConns
	} else {
		stats := p.db.Stats()
		h.PooledConns = stats.OpenConnections
		h.LiveConns = stats.OpenConnections
		h.InFlightStreams = stats.InUse
	}
	return h
}

// Drain shuts the pool down gracefully: it stops handing out connections,
// waits for the statements already in flight to finish, and then closes
// everything. Close is the immediate counterpart — it does not wait.
//
// The ordering is the contract:
//
//  1. new work is refused with ErrPoolDraining (Pool.Session and every driver
//     connect, so a caller is told rather than left blocking);
//  2. the periodic probe goroutine is stopped and waited for;
//  3. in-flight statements and streams are given until ctx expires;
//  4. the sql pool and every pooled QUIC connection are closed.
//
// If ctx expires during step 3, Drain returns ctx.Err() and still performs
// step 4: a half-drained pool is not a state to leave anything in. Drain is
// idempotent, and safe on a nil or zero-value Pool.
func (p *Pool) Drain(ctx context.Context) error {
	if p == nil || p.db == nil {
		return nil
	}

	qp := p.quicPool()
	// Step 1. Set on the Pool and on the QUIC pool: the first refuses new
	// Sessions, the second refuses every driver-level connect underneath
	// database/sql, including the ones database/sql opens on its own.
	p.draining.Store(true)
	if qp != nil {
		qp.draining.Store(true)
		// Step 2.
		qp.stopProbeLoop()
	}

	// Step 3.
	var waitErr error
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()
	for !p.quiesced() {
		select {
		case <-ctx.Done():
			waitErr = ctx.Err()
		case <-ticker.C:
			continue
		}
		break
	}

	// Step 4. database/sql first: closing it releases the driver connections
	// that hold the QUIC streams the pool is about to close under them.
	errs := []error{waitErr}
	if err := p.db.Close(); err != nil {
		errs = append(errs, fmt.Errorf("geode: Pool.Drain: close sql pool: %w", err))
	}
	if qp != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), drainCloseGrace)
		err := qp.Drain(closeCtx)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("geode: Pool.Drain: close quic pool: %w", err))
		}
	}
	return errors.Join(errs...)
}

// quiesced reports whether any statement is still running.
//
// The measure is database/sql's InUse, deliberately NOT the QUIC pool's stream
// count. Every driver connection holds a QUIC stream for its whole life,
// including while it sits IDLE in the sql pool, so waiting for the stream
// count to reach zero would mean waiting out ConnMaxIdleTime — a 25-second
// drain with nothing actually in flight (measured). InUse counts the
// connections a caller is using right now, which is what "wait for in-flight
// work" means; the idle ones are released by db.Close moments later.
func (p *Pool) quiesced() bool {
	return p.db.Stats().InUse == 0
}

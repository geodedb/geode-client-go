package geode

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"geodedb.com/geode/proto"
)

// Default QUIC pool settings.
const (
	// DefaultQUICPoolSize is the maximum number of QUIC connections in the
	// shared pool that streams multiplex over.  Each `db.Conn(ctx)` returns
	// a stream-level handle on one of these QUIC connections, so the
	// effective parallelism of concurrent transactions is bounded by this
	// pool size when callers pin many sql.Conn handles.
	//
	// 2026-04-28: bumped from 4 to 16.  The previous value of 4 silently
	// capped concurrent transaction throughput at 4 regardless of how many
	// sql.Conn handles an application pinned via db.Conn(ctx); applications
	// using msft-graph-style parallel pipelines stalled at exactly 4
	// connections worth of forward progress no matter how high they set
	// `--workers`.  16 matches a typical CPU core count and is enough to
	// avoid the ceiling for almost all real workloads while keeping per-
	// process resource usage bounded.  Override via DSN `?pool_size=N`.
	DefaultQUICPoolSize = 16

	// DefaultMaxStreams is the maximum number of total in-flight QUIC
	// streams allowed across all pooled connections.  Stream creation
	// blocks (context-aware) when at capacity, providing driver-side
	// backpressure independent of the application's sql.DB pool tuning.
	//
	// Without this limit an application using sql.Open("geode", dsn)
	// directly inherits Go stdlib's MaxOpenConns=0 (unlimited) default —
	// it can pin arbitrary numbers of *sql.Conn handles, each opening a
	// new QUIC stream against the bounded connection pool.  Geode 0.5.x's
	// per-stream BufferedProtoReader spins on WouldBlock when the server's
	// single 50µs pollLoop is saturated; ~50 concurrent streams was the
	// empirical breaking point in divelife-qa.  64 leaves modest headroom
	// for short-lived spikes while preventing runaway accumulation.
	//
	// Override via DSN `?max_streams=N`.
	DefaultMaxStreams = 64

	// DefaultMaxStreamsPerConn is the maximum number of concurrent in-flight
	// QUIC streams the driver multiplexes over a SINGLE QUIC connection.  Each
	// request opens its own independent bidirectional stream (own request/
	// response pair, own bufio.Reader, own per-stream read deadline), so up to
	// this many requests can run concurrently on one connection before the pool
	// dials another.  The server multiplexes concurrent streams correctly when
	// started with GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1 (it services only the
	// first stream per connection otherwise — a deployment requirement).
	//
	// 8 balances throughput (multiplexing avoids the connection-count ceiling
	// of the old one-stream-per-conn workaround, BUG-0002) against per-stream
	// receive-window memory.  A value of 1 reduces the pool to the old
	// no-multiplex behaviour (one in-flight stream per connection).
	//
	// The connection's ANCHOR stream and its liveness probe's stream are NOT
	// counted here, nor against DefaultMaxStreams: both are opened directly on
	// the pooled connection rather than through acquire, so a pooled connection
	// carries up to this many REQUEST streams plus its anchor (see
	// pooledConn.anchor) and its probe stream (see pooledConn.probe).
	//
	// Override via DSN `?max_streams_per_conn=N` (minimum 1).
	DefaultMaxStreamsPerConn = 8
)

// anchorHandshakeTimeout bounds the anchor handshake when the caller set no
// deadline of its own.
//
// dialLocked runs the handshake with the POOL LOCK held (the dial already was),
// so a server that accepts a stream and then never answers the HELLO would
// otherwise hold every other caller out of the pool forever. A DSN that sets
// connect_timeout still wins: connectPooled applies it to the context before
// acquire, and a context that already carries a deadline is left alone.
//
// This is the DEFAULT bound, not a fallback for an unusual DSN: Config's
// ConnectTimeout is 0 — no timeout — unless `connect_timeout` is set
// explicitly (config.go), so for an ordinary DSN nothing else bounds the
// handshake and this value alone decides how long a silent server can freeze
// p.mu. 10s is chosen against that: long enough to survive a loaded engine's
// slow HELLO, short enough that a silent server costs roughly the QUIC dial
// plus ten seconds rather than half a minute. The real fix is to dial and
// handshake OFF the pool lock; that remains a follow-up (SONNY-1219 review,
// F3).
const anchorHandshakeTimeout = 10 * time.Second

// QUICPool manages a small pool of persistent QUIC connections.
// Each database/sql Conn gets a new QUIC stream on a pooled connection,
// amortizing the TLS handshake cost across many logical connections.
//
// QUICPool enforces two independent bounds:
//   - maxSize: the number of underlying QUIC connections that can be dialed.
//   - maxStreams: the total number of in-flight streams across all pooled
//     connections.  Stream creation blocks (context-aware) when at this
//     limit, providing driver-side backpressure.  See DefaultMaxStreams
//     for the rationale.
type QUICPool struct {
	mu                sync.Mutex
	conns             []*pooledConn
	cfg               *Config
	maxSize           int
	maxStreams        int
	maxStreamsPerConn int
	closed            bool

	// probeInterval is how often the probe loop verifies every pooled
	// connection with a real round trip. Zero disables probing.
	probeInterval time.Duration

	// draining is set by Drain. It is read on the acquire fast path, before
	// the admission semaphore, so a caller offered work during a drain is
	// refused with ErrPoolDraining instead of blocking for a slot that the
	// drain is trying to reclaim.
	draining atomic.Bool

	// probeMu serialises Probe with itself: two concurrent probes would open
	// two probe streams on one pooled connection and double the server-side
	// stream count for no extra information.
	probeMu sync.Mutex

	// health is the last recorded probe result. Pool.Health reads it instead
	// of performing I/O, which is what makes a readiness endpoint cheap.
	health atomic.Pointer[HealthStatus]

	// Probe-loop lifecycle. probeStop is closed by stopProbeLoop, probeDone by
	// the loop itself, so a Drain or Close can WAIT for the goroutine to be
	// gone rather than merely asking it to leave.
	probeOnce     sync.Once
	probeStopOnce sync.Once
	probeStop     chan struct{}
	probeDone     chan struct{}
	probeStarted  atomic.Bool

	// probeTripCount counts this pool's own probe round trips. The exported
	// ProbeCount is process-wide and therefore useless for asserting anything
	// about ONE pool while other pools in the same process are probing on
	// their own schedules.
	probeTripCount atomic.Uint64

	// probeHook, when set, runs inside the probe critical section. Tests use
	// it to observe the serialisation; it is nil in production.
	probeHook func()

	// probeBudgetOverride, when non-zero, replaces the per-probe deadline (in
	// nanoseconds). Tests use it to force a probe failure against a live
	// engine without hurting the engine; it is zero in production.
	probeBudgetOverride atomic.Int64

	// anchorWatchDone, when non-nil, receives once for every watchAnchor
	// goroutine that returns. It exists so a test can JOIN the watcher instead
	// of sleeping and hoping: the interesting assertion after a local close is
	// that the watcher decided NOT to condemn the connection, and that decision
	// is only final once the goroutine is gone. The send is non-blocking, so a
	// full or unread channel never holds a watcher up. Nil in production.
	anchorWatchDone chan struct{}

	// slotsCh is a counting semaphore bounding in-flight streams.  Capacity
	// equals maxStreams; tokens are consumed by acquireSlot and returned
	// by releaseSlot.  Pre-populated at construction so the channel acts
	// as a "tokens available" pool.
	slotsCh chan struct{}

	// closedCh is closed by Close so blocked acquireSlot callers unblock
	// with ErrClosed instead of leaking goroutines.
	closedCh chan struct{}
}

// pooledConn wraps a single QUIC connection with metadata for stream management.
type pooledConn struct {
	qConn      *quic.Conn
	mu         sync.Mutex
	active     int32  // atomic: number of active streams
	authed     bool   // HELLO handshake completed on this connection
	graphBound bool   // server bound graph during HELLO (DSN path)
	addr       string // remote address for diagnostics

	// HELLO response metadata, recorded by the one stream that performed the
	// handshake so later streams — which skip it — can still report what the
	// server said. Guarded by mu.
	graph     string
	sessionID string
	helloCaps []string

	// dead is set when this connection has been condemned by consecutive
	// failed liveness probes. It is the half of the liveness answer that
	// isQUICConnAlive cannot give: an orphaned connection's quic.Conn context
	// is still live locally, because nothing local went wrong.
	dead atomic.Bool

	// strikes counts CONSECUTIVE failed probes; a success resets it. See
	// probeStrikes: one failure is reported but not acted on, because
	// evicting on a single slow round trip closes healthy connections with
	// live streams on them.
	strikes atomic.Int32

	// warmed records that the PEER has answered a probe on this connection at
	// least once. It says nothing about any particular stream — stream setup
	// is paid per stream (see probeStreamSetup) — only that this connection's
	// round trip is known to work, which is what lets a replacement stream be
	// budgeted as "one known round trip plus one known stream setup" instead
	// of the open-ended probeWarmupTimeout a never-verified connection needs.
	warmed atomic.Bool

	// probe is a long-lived stream used only for liveness PINGs, opened
	// lazily on the first probe and reused by every later one. It is NOT
	// opened and closed per probe: geode 0.8.15 tears the whole pooled QUIC
	// connection down when a stream that carried a handshake goes away
	// (SONNY-1227), so a probe stream is closed only when the connection
	// itself is going away. Guarded by mu.
	//
	// The probe stream is not a REQUEST stream: it is opened directly on the
	// pooled connection rather than through acquire, so it is counted neither
	// against maxStreams (HealthStatus.InFlightStreams) nor against
	// maxStreamsPerConn (see DefaultMaxStreamsPerConn). A pooled connection
	// therefore carries up to maxStreamsPerConn request streams PLUS one probe
	// stream, plus at most probeStrikes abandoned streams (see abandoned: the
	// connection is condemned the moment the list reaches that many).
	probe Transport

	// abandoned holds probe streams a failed probe walked away from.
	//
	// A failed probe must NOT close its stream. At strike 1 the connection is
	// not evicted — it stays in the pool and keeps serving live request
	// streams — and geode 0.8.15 answers a stream closing on a pooled
	// connection by tearing the WHOLE connection down (SONNY-1227), which is
	// the outage the probe exists to detect. So the stream is dropped instead:
	// the next probe opens a fresh one, and every abandoned stream is closed
	// together with the connection itself in discard / QUICPool.Close.
	//
	// The list is bounded by probeStrikes, and the bound is enforced WHERE THE
	// LIST GROWS: abandonProbe condemns the connection the moment appending
	// reaches probeStrikes, so the list never holds more than probeStrikes
	// streams and every one of them is closed with the connection itself.
	//
	// The bound is enforced here rather than only in recordProbeFailure
	// because recordProbeFailure is not reached on every path that abandons:
	// probeAll skips it when the ROUND's context ended after a legitimate
	// failure had already abandoned a stream (probeCallerCancelled), and a
	// success between failures clears the strike count. Either way the strike
	// count can stay below the threshold while the list keeps growing.
	//
	// What the bound protects is the PEER's stream credit, not local memory.
	// An abandoned stream is never closed and never FINished, so the server
	// keeps its per-stream state and — under QUIC's MAX_STREAMS flow control —
	// keeps the credit it issued for it charged against this connection. A
	// connection that leaked one such stream per failure would eventually be
	// unable to open the request streams it exists to carry, and the probe
	// would have caused the outage it exists to detect. Guarded by mu.
	abandoned []Transport

	// probeOpener, when set, replaces the "open a stream on this connection"
	// step of probeTransport. Tests use it to drive the probe state machine
	// without a live QUIC connection; it is nil in production.
	probeOpener func(ctx context.Context) (Transport, error)

	// anchor is the stream that OWNS this QUIC connection (SONNY-1219).
	//
	// geode ties a connection's lifetime to the first stream opened on it:
	// when the HELLO-bearing stream closes, the engine shuts the per-connection
	// stream acceptor down and destroys the connection with CONNECTION_CLOSE
	// code 0, killing every sibling multiplexed stream — including ones with a
	// request in flight (geode src/server/quic_connection_handler.zig:3548-3589,
	// triage 2026-09-15). A burst of 64 requests over a pool of 8 connections
	// therefore resolved ok=8 err=56: exactly one stream per connection lived.
	//
	// So the pool opens its own stream FIRST, at dial time, performs the HELLO
	// on it, and holds it open for the connection's whole life. Every request
	// stream is then a sibling that may come and go freely, and no request's
	// Close is ever the connection's death. The anchor is closed only when the
	// connection itself goes away (discard / QUICPool.Close).
	//
	// Like the probe stream it is opened DIRECTLY on the pooled connection, not
	// through acquire, so it consumes no admission slot and counts against
	// neither maxStreams nor maxStreamsPerConn. It carries exactly one exchange
	// — the HELLO — and after that nothing is ever written on it; the only
	// reader is watchAnchor. Guarded by mu.
	anchor Transport

	// anchorClosed marks the anchor as closed BY US (discard, pool close), so
	// the watcher can tell a local teardown from the peer dropping the stream.
	anchorClosed atomic.Bool

	// retiring is set when the connection has been condemned and taken out of
	// the pool but may still be draining request streams. The last release
	// closes it. See QUICPool.retire.
	retiring atomic.Bool

	// discardOnce makes discard idempotent: a retire and a drain-completing
	// release can both reach it, and so can an eviction that raced them.
	discardOnce sync.Once

	// anchorOpener, when set, replaces the "open a stream on this connection"
	// step of the anchor handshake. Tests use it to drive the anchor state
	// machine without a live QUIC connection; it is nil in production.
	anchorOpener func(ctx context.Context) (Transport, error)
}

// NewQUICPool creates a new QUIC connection pool for the given config.
// The pool starts empty and creates connections on demand, up to maxSize.
//
// The total in-flight stream limit comes from cfg.MaxStreams (default
// DefaultMaxStreams when zero or negative).  See QUICPool docs for details.
func NewQUICPool(cfg *Config, maxSize int) *QUICPool {
	if maxSize <= 0 {
		maxSize = DefaultQUICPoolSize
	}

	maxStreams := DefaultMaxStreams
	if cfg != nil && cfg.MaxStreams > 0 {
		maxStreams = cfg.MaxStreams
	}

	// maxStreamsPerConn caps concurrent multiplexed streams per QUIC connection.
	// Default to 8 when unset; clamp to a minimum of 1 (a conn must always carry
	// at least one stream). cfg may be constructed directly (not via ParseDSN),
	// so the clamp lives here too, not only in the DSN parser.
	maxStreamsPerConn := DefaultMaxStreamsPerConn
	if cfg != nil && cfg.MaxStreamsPerConn != 0 {
		maxStreamsPerConn = cfg.MaxStreamsPerConn
	}
	if maxStreamsPerConn < 1 {
		maxStreamsPerConn = 1
	}

	// Admission ceiling for total in-flight streams across the whole pool. With
	// multiplexing, each of maxSize connections carries up to maxStreamsPerConn
	// concurrent streams, so the structural ceiling is maxSize*maxStreamsPerConn;
	// the configured maxStreams caps it when smaller. Sizing the admission
	// semaphore to min(maxStreams, maxSize*maxStreamsPerConn) makes callers
	// beyond the ceiling block (clean, context-aware backpressure) instead of
	// being admitted with nowhere to run. (With maxStreamsPerConn=1 this reduces
	// to the legacy min(maxStreams, maxSize) one-stream-per-conn ceiling.)
	admitBound := maxStreams
	if streamCap := maxSize * maxStreamsPerConn; streamCap < admitBound {
		admitBound = streamCap
	}

	// Health probing follows the Config exactly: ParseDSN defaults it to
	// DefaultHealthProbeInterval, a programmatic Config keeps whatever the
	// caller set (zero disables). A nil cfg — only reachable from tests that
	// never dial — disables it too.
	var probeInterval time.Duration
	if cfg != nil {
		probeInterval = cfg.HealthProbeInterval
	}

	p := &QUICPool{
		cfg:               cfg,
		maxSize:           maxSize,
		maxStreams:        maxStreams,
		maxStreamsPerConn: maxStreamsPerConn,
		probeInterval:     probeInterval,
		slotsCh:           make(chan struct{}, admitBound),
		closedCh:          make(chan struct{}),
		probeStop:         make(chan struct{}),
		probeDone:         make(chan struct{}),
	}
	// Pre-populate the semaphore: every slot starts available.
	for i := 0; i < admitBound; i++ {
		p.slotsCh <- struct{}{}
	}
	return p
}

// acquireSlot reserves one stream-admission slot, blocking until a slot is
// available, ctx is cancelled, or the pool is closed.  Each successful call
// must be paired with releaseSlot.
//
// This is the driver-side backpressure mechanism described on QUICPool.
// Without it, an unbounded *sql.DB pool can pin arbitrarily many *sql.Conn
// handles, each producing a new QUIC stream, until the server's pollLoop
// saturates and queries deadlock waiting for bytes that never arrive.
func (p *QUICPool) acquireSlot(ctx context.Context) error {
	// Fast-path: pool already closed.
	select {
	case <-p.closedCh:
		return ErrClosed
	default:
	}

	select {
	case <-p.slotsCh:
		// Re-check closed in case Close raced us after we passed the
		// fast-path but before the channel receive.
		select {
		case <-p.closedCh:
			p.releaseSlot()
			return ErrClosed
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closedCh:
		return ErrClosed
	}
}

// releaseSlot returns one stream-admission slot to the semaphore.  Safe to
// call after Close (slots returned to a closed pool are silently dropped).
func (p *QUICPool) releaseSlot() {
	select {
	case p.slotsCh <- struct{}{}:
	default:
		// Channel is full — either the pool was closed (slots no longer
		// matter) or release/acquire became unbalanced.  Drop silently
		// rather than blocking; an unbalanced count manifests as a
		// downstream acquire timeout, which is the right symptom.
	}
}

// acquire returns a pooled connection with the lowest active stream count.
// If all connections are busy and the pool hasn't reached maxSize, a new
// QUIC connection is dialed. The caller must call release when done.
//
// acquire first reserves a stream-admission slot via acquireSlot — if the
// pool's per-process stream budget is exhausted, this call blocks until a
// slot is released or ctx is cancelled.  See QUICPool docs for the
// rationale behind admission control.
func (p *QUICPool) acquire(ctx context.Context) (*pooledConn, error) {
	// Checked before the semaphore: a drain is trying to reclaim slots, so a
	// new caller must be turned away rather than queued behind it.
	if p.draining.Load() {
		return nil, ErrPoolDraining
	}

	if err := p.acquireSlot(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		p.releaseSlot()
		return nil, ErrClosed
	}
	if p.draining.Load() {
		p.releaseSlot()
		return nil, ErrPoolDraining
	}

	// Remove dead connections
	p.pruneDeadLocked()

	// Multiplex policy (supersedes the BUG-0002 no-multiplex workaround): each
	// request opens its OWN independent QUIC stream, and the server services up
	// to N concurrent streams per connection correctly when started with
	// GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1 (the de-risk gate proved this).
	// So reuse the LEAST-loaded connection that is still under the per-conn cap
	// (active < maxStreamsPerConn), spreading load evenly. maxStreamsPerConn=1
	// reduces this to the legacy one-stream-per-conn behaviour. Only when every
	// connection is at capacity do we fall through to dial a new one.
	if idx := selectConnWithCapacity(p.conns, p.maxStreamsPerConn); idx >= 0 {
		pc := p.conns[idx]
		atomic.AddInt32(&pc.active, 1)
		p.startProbeLoop()
		return pc, nil
	}

	if len(p.conns) < p.maxSize {
		pc, err := p.dialLocked(ctx)
		if err != nil {
			p.releaseSlot()
			return nil, err
		}
		atomic.AddInt32(&pc.active, 1)
		p.startProbeLoop()
		return pc, nil
	}

	// Every connection is at its per-conn stream cap and the pool is at maxSize,
	// i.e. all maxSize*maxStreamsPerConn stream slots are in use. The admission
	// semaphore (sized to that ceiling) should have blocked this caller, so this
	// is a defensive backstop; fail fast rather than overloading a connection.
	p.releaseSlot()
	return nil, ErrPoolExhausted
}

// selectConnWithCapacity returns the index of the LEAST-loaded pooled connection
// whose in-flight stream count is below max (active < max), or -1 if every
// connection is at capacity. Preferring the least-loaded connection spreads
// concurrent streams evenly across the pool rather than piling them onto the
// first available one. With maxPerConn == 1 it is equivalent to
// selectFreeConnIndex (the legacy no-multiplex policy: only an idle connection
// qualifies; active < 1 ⇔ active == 0). Pure
// function of the slice so the multiplex selection policy is unit-testable
// without a live server.
func selectConnWithCapacity(conns []*pooledConn, maxPerConn int) int {
	best := -1
	var bestActive int32
	for i, pc := range conns {
		active := atomic.LoadInt32(&pc.active)
		if int(active) >= maxPerConn {
			continue
		}
		if best == -1 || active < bestActive {
			best = i
			bestActive = active
		}
	}
	return best
}

// selectFreeConnIndex returns the index of the first pooled connection with no
// in-flight stream (active == 0), or -1 if every connection is busy. Retained as
// the reference for the legacy no-multiplex policy (max_streams_per_conn=1);
// selectConnWithCapacity(conns, 1) is equivalent. Pure function of the slice so
// the selection policy is unit-testable without a live server.
func selectFreeConnIndex(conns []*pooledConn) int {
	for i, pc := range conns {
		if atomic.LoadInt32(&pc.active) == 0 {
			return i
		}
	}
	return -1
}

// release decrements the active stream count for the pooled connection and
// returns its admission slot to the semaphore.
func (p *QUICPool) release(pc *pooledConn) {
	if pc == nil {
		return
	}
	remaining := atomic.AddInt32(&pc.active, -1)
	p.releaseSlot()

	// A retired connection is out of the pool and waiting for its last stream:
	// this may be it. See QUICPool.retire for the ordering that guarantees one
	// of the two sides closes it.
	if remaining <= 0 && pc.retiring.Load() {
		pc.discard("drained after its anchor was lost")
	}
}

// remove removes a dead connection from the pool so it won't be reused.
func (p *QUICPool) remove(pc *pooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeLocked(pc)
}

// removeLocked drops pc from the pool without closing it.
// Must be called with p.mu held.
func (p *QUICPool) removeLocked(pc *pooledConn) {
	for i, c := range p.conns {
		if c == pc {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			return
		}
	}
}

// Close closes all pooled QUIC connections and unblocks any goroutines
// waiting on acquireSlot.
func (p *QUICPool) Close() error {
	// Outside the lock: the probe loop takes p.mu on every tick, so waiting
	// for it while holding p.mu would deadlock.
	p.stopProbeLoop()

	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	// closedCh is created in NewQUICPool, but defensively guard for pools
	// constructed by other means (e.g., test fixtures with zero-value
	// fields) so Close is always safe to call.
	if p.closedCh != nil {
		close(p.closedCh)
	}

	var firstErr error
	for _, pc := range p.conns {
		// The anchor is closed here and only here (and in discard): closing it
		// while the connection is still in service is the teardown it exists to
		// prevent. Marking it as OUR close keeps its watcher quiet.
		pc.closeAnchor()
		pc.closeProbeStreams()
		// nil-safe: a pool assembled by a test fixture may hold connections
		// with no quic.Conn behind them, and Close must still be callable.
		if pc.qConn == nil {
			continue
		}
		if err := pc.qConn.CloseWithError(0, "pool closing"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.mu.Unlock()
	return firstErr
}

// dialLocked creates a new QUIC connection and adds it to the pool.
// Must be called with p.mu held.
func (p *QUICPool) dialLocked(ctx context.Context) (*pooledConn, error) {
	tlsCfg, err := NewTLSConfig(p.cfg)
	if err != nil {
		return nil, err
	}

	addr := p.cfg.Addr()

	qConn, err := quic.DialAddr(ctx, addr, tlsCfg, &quic.Config{
		MaxIdleTimeout:                 quicMaxIdleTimeout,
		KeepAlivePeriod:                quicKeepAlivePeriod,
		InitialStreamReceiveWindow:     quicInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicMaxConnectionReceiveWindow,
		DisablePathMTUDiscovery:        false,
	})
	if err != nil {
		return nil, &TransportError{
			Op:    constOpDial,
			Addr:  addr,
			cause: err,
		}
	}

	pc := &pooledConn{
		qConn: qConn,
		addr:  addr,
	}

	// Anchor the connection BEFORE it joins the pool: a connection whose
	// handshake failed is not a pooled connection, and one that joined the pool
	// unanchored would hand its first request stream the ownership the anchor
	// exists to take away (SONNY-1219).
	if err := p.anchorConn(ctx, pc); err != nil {
		_ = qConn.CloseWithError(0, "anchor handshake failed")
		return nil, err
	}

	p.conns = append(p.conns, pc)
	return pc, nil
}

// anchorConn opens this connection's anchor stream, performs the HELLO on it,
// and leaves it open with a watcher reading it.
//
// After this returns the connection is authenticated for EVERY stream on it —
// geode authenticates the connection, not the stream — so pooledHello becomes a
// pure fast path for every request stream (see pooledConn.anchor).
func (p *QUICPool) anchorConn(ctx context.Context, pc *pooledConn) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, anchorHandshakeTimeout)
		defer cancel()
	}

	t, err := pc.openAnchor(ctx)
	if err != nil {
		return err
	}

	resp, err := sendHello(ctx, p.cfg, t)
	if err != nil {
		// Nothing was authenticated, and the caller is about to close the QUIC
		// connection; the anchor goes with it.
		pc.closeAnchor()
		return err
	}

	pc.markAuthedHello(resp.GetGraph(), resp.GetSessionId(), resp.GetCapabilities())

	// The watcher must OUTLIVE this call: the anchor is held for the pooled
	// connection's whole life, long after the dial context that created it is
	// done. Inheriting that context would cancel the watch the moment the
	// caller who happened to dial went away, and with it the only signal that
	// the peer dropped the anchor. watchAnchor uses a plain Background context
	// for the same reason, and because a cancellable one would arm a read
	// deadline on the stream (see watchAnchor).
	//nolint:gosec // G118: the anchor watch is connection-scoped, not request-scoped
	go p.watchAnchor(pc, t)
	return nil
}

// openAnchor opens the anchor stream and records it. It is called exactly once
// per connection, from dialLocked, before the connection is reachable by
// anything else.
func (pc *pooledConn) openAnchor(ctx context.Context) (Transport, error) {
	if opener := pc.anchorOpener; opener != nil {
		t, err := opener(ctx)
		if err != nil {
			return nil, err
		}
		pc.setAnchor(t)
		return t, nil
	}

	stream, err := pc.openStream(ctx)
	if err != nil {
		return nil, err
	}
	t := &QUICTransport{
		pc:       pc,
		stream:   stream,
		rdStream: stream,
		br:       bufio.NewReaderSize(stream, quicReadBufferSize),
		addr:     pc.addr,
	}
	pc.setAnchor(t)
	return t, nil
}

func (pc *pooledConn) setAnchor(t Transport) {
	pc.mu.Lock()
	pc.anchor = t
	pc.mu.Unlock()
}

// closeAnchor closes the anchor stream and records that WE closed it, so the
// watcher does not read its own teardown as a peer failure.
//
// It runs only when the connection itself is going away (discard,
// QUICPool.Close) or when the handshake on it failed: closing the anchor on a
// live pooled connection is exactly the teardown this whole mechanism exists to
// prevent.
func (pc *pooledConn) closeAnchor() {
	pc.anchorClosed.Store(true)
	pc.mu.Lock()
	t := pc.anchor
	pc.anchor = nil
	pc.mu.Unlock()
	if t != nil {
		_ = t.Close()
	}
}

// isRetiring reports whether this connection has been condemned and taken out
// of the pool, whether or not its last stream has drained yet.
func (pc *pooledConn) isRetiring() bool { return pc.retiring.Load() }

// watchAnchor is the anchor's only reader. It parks on the stream for the
// connection's whole life and never writes to it.
//
// Nothing is expected to arrive: the HELLO exchange is over and no request ever
// rides the anchor. So ANY return — a frame, a reset, an EOF — means the anchor
// is no longer holding this connection open, and the connection must not be
// handed out again. That is the signal the pool could not otherwise get: the
// engine tears a connection down with CONNECTION_CLOSE code 0 and no reason
// string, and a local liveness check (isQUICConnAlive) cannot see a stream the
// peer dropped while the connection itself still looks alive.
//
// The read uses a context with no deadline and no cancellation on purpose.
// ReceiveProto arms a read deadline on the stream only when ctx.Done() is
// non-nil (transport.go), so a plain Background context means the anchor arms
// no deadline, writes no frame, and keeps no timer alive: QUIC idle detection
// (quicMaxIdleTimeout with quicKeepAlivePeriod keep-alives) and the health
// probe remain exactly as they were.
//
// What unblocks the read is the CONNECTION going away, not the stream close
// that precedes it: closeAnchor calls Transport.Close, and for a QUICTransport
// that is quic.Stream.Close, which shuts the SEND side only and leaves a parked
// read parked. Both callers of closeAnchor follow it with
// qConn.CloseWithError — discard directly, QUICPool.Close in the same loop —
// and that is what returns this read. (A pool assembled without a quic.Conn,
// i.e. a test fixture, has its fake stream return on Close instead.)
func (p *QUICPool) watchAnchor(pc *pooledConn, t Transport) {
	// Deferred so the signal lands AFTER retire (or the decision not to
	// retire), which is what a joining test needs to observe.
	defer p.signalAnchorWatchDone()

	_, err := t.ReceiveProto(context.Background())

	if pc.anchorClosed.Load() {
		// We closed it: the connection is already on its way out.
		return
	}
	if p.isClosed() {
		return
	}
	reason := "anchor stream closed by peer"
	if err != nil {
		reason = "anchor stream lost: " + err.Error()
	}
	p.retire(pc, reason)
}

// signalAnchorWatchDone reports one watchAnchor goroutine's exit to a test that
// asked to be told. It never blocks: production leaves the channel nil, and a
// test that stopped listening must not wedge a watcher.
func (p *QUICPool) signalAnchorWatchDone() {
	if p.anchorWatchDone == nil {
		return
	}
	select {
	case p.anchorWatchDone <- struct{}{}:
	default:
	}
}

// retire condemns a pooled connection and drains it: it leaves the pool
// immediately, so acquire can never hand it out again, and it is closed as soon
// as its last in-flight request stream finishes.
//
// Draining rather than closing outright is the difference between this and a
// failed liveness probe. A probe failure says the peer is not answering at all,
// so the streams riding it are already lost and closing them early is a
// kindness. A lost ANCHOR says only that the connection has lost its owner: the
// requests already in flight may still complete, and killing them would turn a
// recoverable event into the very burst of failures this change exists to
// prevent.
func (p *QUICPool) retire(pc *pooledConn, reason string) {
	if pc == nil || !pc.retiring.CompareAndSwap(false, true) {
		return
	}
	pc.dead.Store(true)
	p.remove(pc)

	// Ordering with release: retiring is stored before active is read here,
	// while release decrements active before reading retiring. Go's atomics are
	// sequentially consistent, so at least one of the two sees the other and
	// the connection is always closed exactly once (discardOnce).
	if atomic.LoadInt32(&pc.active) <= 0 {
		pc.discard(reason)
	}
}

// pruneDeadLocked removes connections that are no longer usable — those whose
// local quic.Conn has gone away AND those a liveness probe found orphaned. The
// second class is the one acquire could not see before: an orphaned connection
// looks perfectly alive locally.
//
// Leaving the pool and being CLOSED are two different things here, and the
// difference is what keeps a retired connection's requests alive. retire
// condemns in two steps that are not atomic against this lock: it stores
// pc.dead and only then queues for p.mu in remove. A goroutine already holding
// p.mu — every acquire does — runs this prune inside that window and finds
// isDead() true for a connection that is merely DRAINING. Discarding it there
// would close qConn under the request streams still in flight: the yank retire
// exists to avoid (SONNY-1219 review, F1). Reordering retire would not help,
// because isDead() also turns true on its own the moment the peer cancels the
// QUIC connection's context.
//
// So a retiring connection with in-flight streams is dropped from the pool (it
// must never be handed out again) but NOT discarded. Its last release closes
// it — see release and retire for the ordering that guarantees exactly one
// side does.
// Must be called with p.mu held.
func (p *QUICPool) pruneDeadLocked() {
	alive := p.conns[:0]
	var dropped []*pooledConn
	for _, pc := range p.conns {
		if pc.isDead() {
			dropped = append(dropped, pc)
			continue
		}
		alive = append(alive, pc)
	}
	// Zero the tail so a pruned connection is not kept reachable through the
	// slice's spare capacity.
	for i := len(alive); i < len(p.conns); i++ {
		p.conns[i] = nil
	}
	p.conns = alive

	for _, pc := range dropped {
		if pc.isRetiring() {
			// Condemned by retire, not by a probe. Still draining: leave the
			// close to the release that finishes the last stream.
			if atomic.LoadInt32(&pc.active) > 0 {
				continue
			}
			pc.discard("retired after its anchor was lost")
			continue
		}
		pc.discard("failed liveness probe")
	}
}

// recordProbeFailure counts one failed probe and reports whether that condemns
// the connection.
//
// Report now, act on confirmation: the caller already counts this connection
// as not live (so the status is unhealthy immediately), but only probeStrikes
// CONSECUTIVE failures mark it dead. Evicting on a single failure closes a
// healthy connection — with live streams on it — every time a probe loses its
// race with a saturated engine. See probeStrikes for the measurement.
//
// A connection that has ABANDONED probeStrikes probe streams is condemned too,
// however its strike count fell. Every genuine failure leaves one stream
// behind that cannot be closed while the connection is still serving traffic
// (see pooledConn.abandoned), so without this bound a connection that failed
// and recovered over and over would leak one server-side stream per failure.
// This is the SECOND place that bound is applied, not the only one: it is
// enforced authoritatively in abandonProbe, where the list grows, because this
// function is not reached on every path that abandons a stream.
//
// A probe that failed because the CALLER's context ended is not counted at
// all: see probeAll.
func (pc *pooledConn) recordProbeFailure() bool {
	if pc.strikes.Add(1) < probeStrikes && pc.abandonedProbes() < probeStrikes {
		return false
	}
	pc.dead.Store(true)
	return true
}

// recordProbeSuccess clears the strike count: the strikes that condemn a
// connection must be consecutive.
func (pc *pooledConn) recordProbeSuccess() {
	pc.strikes.Store(0)
}

// isDead reports whether this connection must not be handed out again. It is
// the union of the two liveness signals: the local one (quic-go cancels the
// connection's context when the connection is closed or times out) and the
// probed one (a real round trip that the peer did not answer).
//
// A zero-value pooledConn — the shape the pool-policy unit tests use — has no
// quic.Conn and is not dead; only the probe flag can condemn it.
func (pc *pooledConn) isDead() bool {
	if pc.dead.Load() {
		return true
	}
	return pc.qConn != nil && !isQUICConnAlive(pc.qConn)
}

// discard closes this connection for good: its anchor and probe streams first,
// then the QUIC connection itself. Any stream still in flight on it dies with it, which
// is the point — after a failed probe those streams are riding a peer that did
// not answer, and leaving them alone means every one of their callers waits out
// its own full deadline. That is the defect this whole path exists to fix.
//
// It is idempotent: a retire, the release that drains it, and an eviction can
// all reach the same connection.
func (pc *pooledConn) discard(reason string) {
	pc.discardOnce.Do(func() {
		// The anchor first, and marked as OUR close, so its watcher does not
		// report this teardown as a peer failure (see watchAnchor).
		pc.closeAnchor()
		pc.closeProbeStreams()
		if pc.qConn != nil {
			_ = pc.qConn.CloseWithError(0, reason)
		}
	})
}

// isQUICConnAlive checks if a QUIC connection is still usable by inspecting
// its context. quic-go cancels the connection's context when the connection
// is closed or times out.
//
// It is a LOCAL check and performs no I/O, so it cannot see a connection the
// server has forgotten: after a restart the peer drops the connection's
// packets without answering, nothing cancels the context, and this keeps
// reporting true. QUICPool.Probe is the network-level answer.
func isQUICConnAlive(qConn *quic.Conn) bool {
	return qConn.Context().Err() == nil
}

// markAuthed marks a pooled connection as having completed the HELLO handshake
// without recording any further response metadata.
// If serverGraph is non-empty the server bound the graph during HELLO (DSN
// path) and subsequent USE GRAPH must be skipped.
// Thread-safe.
func (pc *pooledConn) markAuthed(serverGraph string) {
	pc.markAuthedHello(serverGraph, "", nil)
}

// markAuthedHello marks the connection authenticated and records the HELLO
// response metadata every later stream on it will inherit.
//
// The capability slice is copied: the caller's slice belongs to a decoded
// protobuf message whose lifetime ends with the handshake.
// Thread-safe.
func (pc *pooledConn) markAuthedHello(serverGraph, sessionID string, caps []string) {
	pc.mu.Lock()
	pc.authed = true
	pc.graphBound = serverGraph != ""
	pc.graph = serverGraph
	pc.sessionID = sessionID
	pc.helloCaps = append([]string(nil), caps...)
	pc.mu.Unlock()
}

// helloState returns the HELLO response metadata recorded for this connection.
// The capability slice is a copy, so the caller may retain it.
// Thread-safe.
func (pc *pooledConn) helloState() (graph, sessionID string, caps []string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.graph, pc.sessionID, append([]string(nil), pc.helloCaps...)
}

// isGraphBound returns whether the server bound the graph during HELLO.
// Thread-safe.
func (pc *pooledConn) isGraphBound() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.graphBound
}

// isAuthed returns whether the HELLO handshake has been completed.
// Thread-safe.
func (pc *pooledConn) isAuthed() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.authed
}

// openStream opens a new bidirectional QUIC stream on this connection.
func (pc *pooledConn) openStream(ctx context.Context) (*quic.Stream, error) {
	stream, err := pc.qConn.OpenStreamSync(ctx)
	if err != nil {
		return nil, &TransportError{
			Op:    "open_stream",
			Addr:  pc.addr,
			cause: fmt.Errorf("failed to open stream on pooled connection: %w", err),
		}
	}
	return stream, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Liveness probing (SONNY-806, Lane D Task 11)
// ─────────────────────────────────────────────────────────────────────────────

// Probe verifies every pooled QUIC connection with a real network round trip
// and evicts the ones that do not answer, then records the result for
// Pool.Health to read.
//
// The probe is a protocol-level PING on a long-lived probe stream — the
// cheapest round trip geode 0.8.15 answers, and one that mutates nothing. Each
// connection gets its own bounded deadline (probeTimeout) and they are probed
// concurrently, so one round costs about one timeout however many connections
// are orphaned.
//
// Probe never runs concurrently with itself, so a pooled connection never has
// more than one probe stream.
//
// It returns the first probe failure, errNoPooledConns when there is nothing
// to verify, or ErrClosed / ErrPoolDraining when the pool is on its way out.
func (p *QUICPool) Probe(ctx context.Context) error {
	if p == nil {
		return ErrClosed
	}

	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	if p.probeHook != nil {
		p.probeHook()
	}

	conns, err := p.probeTargets()
	if err != nil {
		return err
	}

	status, firstErr := p.probeAll(ctx, conns)
	p.storeHealth(status)
	return firstErr
}

// probeTargets snapshots the connections a probe should verify. When there are
// none to verify — the pool is closed, draining, or empty — it records the
// fail-closed status itself and returns the reason, because in every one of
// those cases nothing was verified and nothing may be claimed.
func (p *QUICPool) probeTargets() ([]*pooledConn, error) {
	p.mu.Lock()
	closed := p.closed
	conns := append([]*pooledConn(nil), p.conns...)
	p.mu.Unlock()

	var reason string
	var err error
	switch {
	case closed:
		reason, err = probeErrPoolClosed, ErrClosed
	case p.draining.Load():
		reason, err = probeErrPoolDraining, ErrPoolDraining
	case len(conns) == 0:
		reason, err = probeErrNoPooledConns, errNoPooledConns
	default:
		return conns, nil
	}
	p.storeHealth(HealthStatus{LastProbeAt: time.Now(), LastProbeErr: reason})
	return nil, err
}

// probeAll probes every connection concurrently, evicts the ones that did not
// answer, and returns the resulting status together with the first failure.
//
// Concurrency is what keeps a round cheap: a probe against an orphaned
// connection never returns early — the peer simply does not answer — so a
// serial round would cost one timeout per orphan. Probed together, a whole
// round costs about one timeout however many connections went away at once,
// which is exactly the case a server restart produces.
func (p *QUICPool) probeAll(ctx context.Context, conns []*pooledConn) (HealthStatus, error) {
	status := HealthStatus{LastProbeAt: time.Now(), PooledConns: len(conns)}
	for _, pc := range conns {
		status.InFlightStreams += int(atomic.LoadInt32(&pc.active))
	}

	errs := make([]error, len(conns))
	probed := make([]bool, len(conns))
	var wg sync.WaitGroup
	for i, pc := range conns {
		// A connection nothing has handshaken on yet has no session for the
		// server to have forgotten; probing it would only race the HELLO that
		// is about to run on it. It does not count as LIVE either: nothing
		// about it was verified, and health fails closed.
		if !pc.isAuthed() {
			continue
		}
		probed[i] = true
		wg.Add(1)
		go func(i int, pc *pooledConn) {
			defer wg.Done()
			errs[i] = p.probeConn(ctx, pc)
		}(i, pc)
	}
	wg.Wait()

	var firstErr error
	var dead []*pooledConn
	for i, pc := range conns {
		if !probed[i] {
			continue
		}
		if errs[i] == nil {
			pc.recordProbeSuccess()
			status.LiveConns++
			continue
		}
		if firstErr == nil {
			firstErr = errs[i]
		}
		// The caller's own cancellation is not evidence about the connection.
		// Reporting it is honest; striking a connection for it is not — a
		// caller that passes an already-cancelled or very short context to
		// Pool.Health would otherwise condemn every connection in the pool in
		// two calls, and take their probe streams with it.
		if probeCallerCancelled(ctx, errs[i]) {
			// Not a strike — but a connection this round already condemned
			// still has to leave. A legitimate failure early in the round
			// abandons a stream, and if some OTHER connection then burns the
			// round's budget, this branch is where that failure lands. The
			// abandoned-stream bound was applied where the list grew
			// (abandonProbe); honouring it here is what turns "condemned" into
			// "gone", so the streams it capped are actually closed.
			if pc.isDead() {
				dead = append(dead, pc)
			}
			continue
		}
		if pc.recordProbeFailure() {
			dead = append(dead, pc)
		}
	}
	p.evict(dead)

	// Healthy means a round trip answered on every connection this probe could
	// verify, AND that there was at least one. A pool whose connections were
	// all skipped verified nothing, and nothing verified is not health.
	status.Healthy = firstErr == nil && status.LiveConns > 0
	status.LastProbeErr = classifyProbeErr(firstErr)
	if firstErr == nil && !status.Healthy {
		status.LastProbeErr = probeErrUnverified
	}
	return status, firstErr
}

// probeCallerCancelled reports whether a probe failure is the CALLER's doing
// rather than the connection's.
//
// A probe's own budget only ever expires on a context the pool made, which
// leaves the caller's context untouched — so a caller context that is done is
// the tell. A Canceled error is another: no probe budget produces one.
func probeCallerCancelled(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// evict removes probe-failed connections from the pool and closes them, so
// acquire can never hand one out again.
func (p *QUICPool) evict(dead []*pooledConn) {
	if len(dead) == 0 {
		return
	}
	p.mu.Lock()
	for _, pc := range dead {
		p.removeLocked(pc)
	}
	p.mu.Unlock()
	for _, pc := range dead {
		pc.discard("failed liveness probe")
	}
}

// probeConn performs one bounded PING round trip on pc's probe stream.
//
// The budget depends on whether the STREAM is new, not merely on whether the
// connection has answered before: the server materialises a probe stream when
// its first frame arrives (~70ms against 0.8.15), while every later probe on
// that stream is a bare round trip (~0.4ms). A replacement stream — the one
// opened after a failure abandoned the last — therefore gets the round trip's
// budget PLUS that setup cost. Budgeting it as a bare round trip is how a
// healthy connection got evicted for one slow moment: the retry paid ~70ms of
// stream setup out of a 100ms budget, failed, and that second failure was the
// strike that condemned it. See probeBudget.
//
// A failure ABANDONS the stream instead of closing it — pc is still in the
// pool and may still be carrying live request streams, and closing a stream on
// a pooled connection is what SONNY-1227 answers with a full connection
// teardown. See probeStreamFailed.
func (p *QUICPool) probeConn(parent context.Context, pc *pooledConn) error {
	if err := parent.Err(); err != nil {
		// The caller is already gone. Opening a stream for it would only
		// produce a failure nobody asked about.
		return err
	}

	openCtx, openCancel := context.WithTimeout(parent, probeWarmupTimeout)
	t, fresh, err := pc.probeTransport(openCtx)
	openCancel()
	if err != nil {
		// Nothing was opened, so there is no stream to abandon.
		return err
	}

	ctx, cancel := context.WithTimeout(parent, p.probeBudget(pc, fresh))
	defer cancel()

	probeCount.Add(1)
	p.probeTripCount.Add(1)
	if err := t.SendProto(ctx, proto.NewPingMessage(&proto.PingRequest{})); err != nil {
		return pc.probeStreamFailed(parent, err)
	}
	resp, err := t.ReceiveProto(ctx)
	if err != nil {
		return pc.probeStreamFailed(parent, err)
	}
	ping := resp.GetPing()
	if ping == nil {
		// The probe stream is desynchronised; it can never be trusted again.
		return pc.probeStreamFailed(parent, errProbeUnexpectedFrame)
	}
	if !ping.GetOk() {
		// The server answered, so the connection is alive and the stream is
		// in step. Only the verdict is bad; the stream is kept.
		return errProbeRefused
	}
	pc.warmed.Store(true)
	return nil
}

// probeBudget bounds one probe round trip. There are three cases, and the
// middle one is the fix for a healthy connection being evicted after a single
// slow moment:
//
//   - a REUSED stream is a bare round trip (~0.4ms against 0.8.15), bounded by
//     probeTimeout, which tracks the configured interval;
//   - a REPLACEMENT stream on a connection the peer has already answered on
//     pays one known round trip plus one known stream setup, so it gets
//     probeTimeout plus probeStreamSetup. Stream setup is per STREAM, and a
//     replacement exists precisely because the last probe failed — budgeting
//     it as a bare round trip made the retry fail too, which is strike 2;
//   - the FIRST stream on a connection nothing has ever answered on has no
//     measured cost to lean on and is not yet part of any detection story, so
//     it gets the open-ended probeWarmupTimeout.
func (p *QUICPool) probeBudget(pc *pooledConn, fresh bool) time.Duration {
	if override := p.probeBudgetOverride.Load(); override > 0 {
		return time.Duration(override)
	}
	switch {
	case !fresh:
		return p.probeTimeout()
	case pc.warmed.Load():
		return p.probeTimeout() + probeStreamSetup
	default:
		return probeWarmupTimeout
	}
}

// probeStreamFailed records a probe failure that leaves pc's probe stream
// untrustworthy — a timed-out send may have written half a length-prefixed
// frame, and a timed-out receive leaves the answer to arrive later, where the
// NEXT probe would read it as its own.
//
// The stream is ABANDONED, never closed. pc remains in the pool until
// probeStrikes consecutive failures condemn it, so at strike 1 it is still
// serving live request streams, and geode 0.8.15 answers a stream closing on a
// pooled connection by tearing the whole connection down (SONNY-1227) — which
// would make a single slow probe cause the outage the probe exists to detect.
// Abandoned streams are closed with the connection itself (closeProbeStreams).
//
// A failure the CALLER caused is different and leaves the stream alone: the
// probe stream carries nothing but PING replies, so a late one makes at worst
// the next probe's answer one round trip stale, while abandoning a stream for
// something the connection did not do would let cancelled Health calls consume
// the connection's whole abandoned-stream budget and condemn it.
func (pc *pooledConn) probeStreamFailed(parent context.Context, err error) error {
	if parent.Err() != nil {
		return err
	}
	// The bound on the list this moves to is enforced inside abandonProbe, so
	// it holds even when the caller of this probe never reaches
	// recordProbeFailure. Whether this call condemned the connection is read
	// back off pc.isDead by probeAll, which owns the eviction.
	_ = pc.abandonProbe()
	return err
}

// probeTimeout bounds a single connection's probe. It tracks the configured
// interval so the detection budget stays roughly two intervals — the number
// the integration test asserts — while never collapsing to something a local
// round trip cannot make, nor stretching past the point where a probe round
// would outlive its own schedule.
func (p *QUICPool) probeTimeout() time.Duration {
	d := p.probeInterval
	switch {
	case d < minProbeTimeout:
		return minProbeTimeout
	case d > maxProbeTimeout:
		return maxProbeTimeout
	default:
		return d
	}
}

// probeTransport returns pc's probe stream, opening it on first use.
//
// The stream is long-lived on purpose: it is opened once and reused by every
// later probe, so a probe never closes a stream on a live pooled connection
// (SONNY-1227). It carries no HELLO — on a pooled connection the server
// authenticates the CONNECTION, which is why every stream after the first
// already skips the handshake (see pooledHello).
//
// The stream is opened DIRECTLY on the pooled connection, not through
// acquire, so it consumes no admission slot: it is counted neither against
// maxStreams (and therefore not in HealthStatus.InFlightStreams) nor against
// maxStreamsPerConn. A probe can therefore never be blocked by, or steal
// capacity from, the request streams it is checking on.
//
// fresh reports whether this call opened the stream, so the caller can budget
// the server-side stream setup that the first frame on it pays for.
func (pc *pooledConn) probeTransport(ctx context.Context) (t Transport, fresh bool, err error) {
	pc.mu.Lock()
	existing, opener := pc.probe, pc.probeOpener
	pc.mu.Unlock()
	if existing != nil {
		return existing, false, nil
	}

	if opener != nil {
		if t, err = opener(ctx); err != nil {
			return nil, false, err
		}
	} else {
		stream, serr := pc.openStream(ctx)
		if serr != nil {
			return nil, false, serr
		}
		t = &QUICTransport{
			pc:       pc,
			stream:   stream,
			rdStream: stream,
			br:       bufio.NewReaderSize(stream, quicReadBufferSize),
			addr:     pc.addr,
		}
	}

	pc.mu.Lock()
	if existing := pc.probe; existing != nil {
		// Unreachable while Probe is serialised by probeMu — which it is — so
		// this closes a stream that never carried a frame rather than one the
		// connection is using.
		pc.mu.Unlock()
		_ = t.Close()
		return existing, false, nil
	}
	pc.probe = t
	pc.mu.Unlock()
	return t, true, nil
}

// abandonProbe drops the probe stream WITHOUT closing it, so the next probe
// opens a fresh one, and condemns the connection if that reaches the
// abandoned-stream bound. It reports whether this call condemned it.
//
// The bound is enforced HERE, where the list grows, and not only in
// recordProbeFailure. recordProbeFailure is reached from one path — probeAll's
// result loop — and that path skips it whenever the round's context ended
// (probeCallerCancelled), which happens after a legitimate failure has already
// abandoned a stream whenever some OTHER connection in the same round burns
// the round's budget. A success between failures clears the strike count as
// well. Either way the strike count stays below the threshold while the list
// keeps growing, and the bound that makes "never close a live connection's
// probe stream" affordable is silently not a bound at all.
//
// Condemning is the whole action taken here: the connection is marked dead, so
// acquire's pruneDeadLocked and the next probe round both drop and discard it,
// which closes every stream on this list. Removing it from the pool needs the
// POOL's lock, which a pooledConn has no handle on — see QUICPool.probeAll and
// QUICPool.evict.
//
// See pooledConn.abandoned for why an abandoned stream is not closed and what
// the bound protects.
func (pc *pooledConn) abandonProbe() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.probe == nil {
		return false
	}
	pc.abandoned = append(pc.abandoned, pc.probe)
	pc.probe = nil
	if len(pc.abandoned) < probeStrikes {
		return false
	}
	return pc.dead.CompareAndSwap(false, true)
}

// abandonedProbes reports how many probe streams this connection has walked
// away from and not yet closed.
func (pc *pooledConn) abandonedProbes() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.abandoned)
}

// closeProbeStreams closes and forgets every probe stream this connection ever
// opened — the live one and every abandoned one. Safe to call repeatedly and
// on a connection that never had one.
//
// This is the ONLY place a probe stream is closed, and it runs in exactly two
// situations, both of which end with the connection gone: the pool is closing
// (QUICPool.Close) or the connection is being discarded (discard, which every
// eviction goes through). It NEVER runs while the connection is still in the
// pool — that is the SONNY-1227 rule. A failed probe does not close its
// stream; it abandons it (probeStreamFailed), because at strike 1 the
// connection stays in service.
func (pc *pooledConn) closeProbeStreams() {
	pc.mu.Lock()
	t := pc.probe
	pc.probe = nil
	abandoned := pc.abandoned
	pc.abandoned = nil
	pc.mu.Unlock()

	if t != nil {
		_ = t.Close()
	}
	for _, a := range abandoned {
		_ = a.Close()
	}
}

// lastHealth returns the last recorded probe result. The zero value — nothing
// probed yet — is unhealthy, so a caller that reads it before the first probe
// gets the fail-closed answer.
func (p *QUICPool) lastHealth() HealthStatus {
	if p == nil {
		return HealthStatus{}
	}
	if h := p.health.Load(); h != nil {
		return *h
	}
	return HealthStatus{}
}

// probeTrips reports how many probe round trips THIS pool has performed.
func (p *QUICPool) probeTrips() uint64 {
	if p == nil {
		return 0
	}
	return p.probeTripCount.Load()
}

// isClosed reports whether Close has run.
func (p *QUICPool) isClosed() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *QUICPool) storeHealth(h HealthStatus) {
	p.health.Store(&h)
}

// counts reports how many connections the pool holds and how many streams are
// in flight across them, without probing anything.
func (p *QUICPool) counts() (pooled, inFlight int) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.conns {
		pooled++
		inFlight += int(atomic.LoadInt32(&pc.active))
	}
	return pooled, inFlight
}

// ─────────────────────────────────────────────────────────────────────────────
// The probe loop
// ─────────────────────────────────────────────────────────────────────────────

// startProbeLoop starts the periodic probe, at most once for the life of the
// pool. It is called from acquire, so a pool that is never used never starts a
// goroutine, and a pool configured with health_probe_ms=0 never starts one at
// all.
func (p *QUICPool) startProbeLoop() {
	if p.probeInterval <= 0 {
		return
	}
	select {
	case <-p.probeStop:
		return // already stopped; do not resurrect the loop
	default:
	}
	p.probeOnce.Do(func() {
		p.probeStarted.Store(true)
		go p.probeLoop()
	})
}

// probeLoop is the pool's own goroutine. It runs one Probe per tick — never
// two at once, because it is a single goroutine and Probe serialises anyway —
// and exits on stopProbeLoop or Close, closing probeDone so a drain can wait
// for it.
func (p *QUICPool) probeLoop() {
	defer close(p.probeDone)

	ticker := time.NewTicker(p.probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.probeStop:
			return
		case <-p.closedCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), probeWarmupTimeout+probeRoundSlack)
			_ = p.Probe(ctx)
			cancel()
		}
	}
}

// stopProbeLoop asks the probe goroutine to exit and waits until it has. It is
// idempotent and safe on a pool whose loop never started.
func (p *QUICPool) stopProbeLoop() {
	p.probeStopOnce.Do(func() { close(p.probeStop) })
	if p.probeStarted.Load() {
		<-p.probeDone
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Drain
// ─────────────────────────────────────────────────────────────────────────────

// Drain stops the pool handing out connections, waits for the streams already
// in flight to finish, then closes every pooled QUIC connection.
//
// New acquires fail with ErrPoolDraining from the moment Drain is entered —
// before the wait, so the wait can actually finish. If ctx expires first, Drain
// returns ctx.Err() and closes the pool anyway: a half-drained pool is not a
// state anything should be left in. Drain is idempotent.
func (p *QUICPool) Drain(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.draining.Store(true)
	p.stopProbeLoop()

	var waitErr error
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()
	for {
		if _, inFlight := p.counts(); inFlight == 0 {
			break
		}
		select {
		case <-ctx.Done():
			waitErr = ctx.Err()
		case <-ticker.C:
			continue
		}
		break
	}

	if err := p.Close(); err != nil && waitErr == nil {
		waitErr = err
	}
	return waitErr
}

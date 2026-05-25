package geode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
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
)

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
	mu         sync.Mutex
	conns      []*pooledConn
	cfg        *Config
	maxSize    int
	maxStreams int
	closed     bool

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

	p := &QUICPool{
		cfg:        cfg,
		maxSize:    maxSize,
		maxStreams: maxStreams,
		slotsCh:    make(chan struct{}, maxStreams),
		closedCh:   make(chan struct{}),
	}
	// Pre-populate the semaphore: every slot starts available.
	for i := 0; i < maxStreams; i++ {
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
	if err := p.acquireSlot(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		p.releaseSlot()
		return nil, ErrClosed
	}

	// Remove dead connections
	p.pruneDeadLocked()

	// Find connection with lowest active stream count
	var best *pooledConn
	bestActive := int32(^uint32(0) >> 1) // max int32

	for _, pc := range p.conns {
		active := atomic.LoadInt32(&pc.active)
		if active < bestActive {
			best = pc
			bestActive = active
		}
	}

	// If we have capacity to create a new connection and either no connections
	// exist or all are moderately loaded, create a new one
	if len(p.conns) < p.maxSize && (best == nil || bestActive > 0) {
		pc, err := p.dialLocked(ctx)
		if err != nil {
			// If we have an existing connection, use it despite load
			if best != nil {
				atomic.AddInt32(&best.active, 1)
				return best, nil
			}
			p.releaseSlot()
			return nil, err
		}
		atomic.AddInt32(&pc.active, 1)
		return pc, nil
	}

	if best == nil {
		// All connections are dead and we failed to prune + redial
		pc, err := p.dialLocked(ctx)
		if err != nil {
			p.releaseSlot()
			return nil, err
		}
		atomic.AddInt32(&pc.active, 1)
		return pc, nil
	}

	atomic.AddInt32(&best.active, 1)
	return best, nil
}

// release decrements the active stream count for the pooled connection and
// returns its admission slot to the semaphore.
func (p *QUICPool) release(pc *pooledConn) {
	if pc == nil {
		return
	}
	atomic.AddInt32(&pc.active, -1)
	p.releaseSlot()
}

// remove removes a dead connection from the pool so it won't be reused.
func (p *QUICPool) remove(pc *pooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, c := range p.conns {
		if c == pc {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
}

// Close closes all pooled QUIC connections and unblocks any goroutines
// waiting on acquireSlot.
func (p *QUICPool) Close() error {
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
			Op:    "dial",
			Addr:  addr,
			cause: err,
		}
	}

	pc := &pooledConn{
		qConn: qConn,
		addr:  addr,
	}
	p.conns = append(p.conns, pc)
	return pc, nil
}

// pruneDeadLocked removes connections that are no longer usable.
// Must be called with p.mu held.
func (p *QUICPool) pruneDeadLocked() {
	alive := p.conns[:0]
	for _, pc := range p.conns {
		if isQUICConnAlive(pc.qConn) {
			alive = append(alive, pc)
		}
	}
	p.conns = alive
}

// isQUICConnAlive checks if a QUIC connection is still usable by inspecting
// its context. quic-go cancels the connection's context when the connection
// is closed or times out.
func isQUICConnAlive(qConn *quic.Conn) bool {
	return qConn.Context().Err() == nil
}

// markAuthed marks a pooled connection as having completed the HELLO handshake.
// If serverGraph is non-empty the server bound the graph during HELLO (DSN
// path) and subsequent USE GRAPH must be skipped.
// Thread-safe.
func (pc *pooledConn) markAuthed(serverGraph string) {
	pc.mu.Lock()
	pc.authed = true
	pc.graphBound = serverGraph != ""
	pc.mu.Unlock()
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

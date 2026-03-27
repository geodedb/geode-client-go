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
	// DefaultQUICPoolSize is the maximum number of QUIC connections in the pool.
	DefaultQUICPoolSize = 4
)

// QUICPool manages a small pool of persistent QUIC connections.
// Each database/sql Conn gets a new QUIC stream on a pooled connection,
// amortizing the TLS handshake cost across many logical connections.
type QUICPool struct {
	mu      sync.Mutex
	conns   []*pooledConn
	cfg     *Config
	maxSize int
	closed  bool
}

// pooledConn wraps a single QUIC connection with metadata for stream management.
type pooledConn struct {
	qConn  *quic.Conn
	mu     sync.Mutex
	active int32  // atomic: number of active streams
	authed bool   // HELLO handshake completed on this connection
	addr   string // remote address for diagnostics
}

// NewQUICPool creates a new QUIC connection pool for the given config.
// The pool starts empty and creates connections on demand, up to maxSize.
func NewQUICPool(cfg *Config, maxSize int) *QUICPool {
	if maxSize <= 0 {
		maxSize = DefaultQUICPoolSize
	}
	return &QUICPool{
		cfg:     cfg,
		maxSize: maxSize,
	}
}

// acquire returns a pooled connection with the lowest active stream count.
// If all connections are busy and the pool hasn't reached maxSize, a new
// QUIC connection is dialed. The caller must call release when done.
func (p *QUICPool) acquire(ctx context.Context) (*pooledConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
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
			return nil, err
		}
		atomic.AddInt32(&pc.active, 1)
		return pc, nil
	}

	if best == nil {
		// All connections are dead and we failed to prune + redial
		pc, err := p.dialLocked(ctx)
		if err != nil {
			return nil, err
		}
		atomic.AddInt32(&pc.active, 1)
		return pc, nil
	}

	atomic.AddInt32(&best.active, 1)
	return best, nil
}

// release decrements the active stream count for the pooled connection.
func (p *QUICPool) release(pc *pooledConn) {
	if pc == nil {
		return
	}
	atomic.AddInt32(&pc.active, -1)
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

// Close closes all pooled QUIC connections.
func (p *QUICPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for _, pc := range p.conns {
		if err := pc.qConn.CloseWithError(0, "pool closing"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
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
// Thread-safe.
func (pc *pooledConn) markAuthed() {
	pc.mu.Lock()
	pc.authed = true
	pc.mu.Unlock()
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

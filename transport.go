package geode

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"geodedb.com/geode/proto"
	"github.com/quic-go/quic-go"
)

// QUIC transport configuration constants.
const (
	quicMaxIdleTimeout                 = 120 * time.Second
	quicKeepAlivePeriod                = 10 * time.Second
	quicInitialStreamReceiveWindow     = 4 * 1024 * 1024  // 4MB initial stream window
	quicMaxStreamReceiveWindow         = 16 * 1024 * 1024 // 16MB max stream window
	quicInitialConnectionReceiveWindow = 8 * 1024 * 1024  // 8MB initial connection window
	quicMaxConnectionReceiveWindow     = 32 * 1024 * 1024 // 32MB max connection window
	quicReadBufferSize                 = 64 * 1024        // 64KB buffer
	lengthPrefixSize                   = 4                // 4-byte big-endian length prefix
)

// Transport operation name constants used in TransportError.Op fields.
const (
	constOpDial = "dial"
	constOpRead = "read"
)

// readDeadlineStream is the read side of a transport stream with deadline
// control. *quic.Stream satisfies it in production; tests inject a fake to
// exercise ReceiveProto's context-cancellation handling without a live QUIC
// connection.
type readDeadlineStream interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

// Transport defines the interface for communication with a Geode server.
type Transport interface {
	// SendProto writes a protobuf message to the server.
	SendProto(ctx context.Context, msg *proto.QuicClientMessage) error

	// ReceiveProto reads a protobuf message from the server.
	ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error)

	// TryReceiveProtoBuffered reads a message only if enough data is already buffered.
	// Returns (nil, false, nil) when no complete message is buffered.
	TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error)

	// Close closes the transport.
	Close() error

	// IsClosed returns true if the transport is closed.
	IsClosed() bool

	// PagesArePushed reports how a multi-page result set is delivered on this
	// wire, and therefore who drives paging.
	//
	// false — PULL-driven (QUIC). The driver asks for each page after the
	// first with a PullRequest and reads the answer.
	//
	// true — server-pushed (gRPC). The server sends every page of its own
	// accord and the driver's only job is to keep reading until a page is
	// marked final; sending a PULL would be both unanswerable and unnecessary.
	//
	// Rows.fetchNextPage branches on this instead of hard-coding the QUIC
	// protocol's PULL, so a transport whose engine pushes pages reads them
	// instead of stalling after the first one.
	PagesArePushed() bool

	// RemoteClosed returns true if the underlying connection has been closed
	// or lost — by the server, an idle timeout, or a transport error —
	// independent of whether this transport's own Close() has run. It lets the
	// driver evict a pooled connection whose server side has gone away instead
	// of reusing it for a query that would fail or hang.
	RemoteClosed() bool

	// Addr returns the remote address.
	Addr() string
}

// QUICTransport implements Transport over QUIC with TLS using length-prefixed protobuf.
//
// QUICTransport operates in one of two modes:
//   - Standalone mode: owns a dedicated QUIC connection (conn != nil, pool == nil).
//     Created via NewQUICTransport. Close() shuts down both stream and connection.
//   - Pooled mode: uses a stream on a shared pooled connection (pool != nil, pc != nil).
//     Created via NewPooledQUICTransport. Close() shuts down only the stream and
//     releases the pooled connection back to the pool.
type QUICTransport struct {
	// Standalone mode fields (legacy, one-connection-per-transport)
	conn *quic.Conn

	// Pooled mode fields (stream multiplexing)
	pool *QUICPool
	pc   *pooledConn

	// Shared fields
	stream *quic.Stream
	// rdStream is the read side with deadline control. In production it is the
	// same object as stream; it exists as an interface so ReceiveProto's
	// cancellation handling can be unit-tested with a fake stream.
	rdStream readDeadlineStream
	br       *bufio.Reader

	mu     sync.Mutex
	closed bool
	addr   string

	// lenBuf is reused for reading length prefixes
	lenBuf [4]byte
}

// NewQUICTransport creates a new QUIC transport to the Geode server.
func NewQUICTransport(ctx context.Context, cfg *Config) (*QUICTransport, error) {
	// Build TLS config
	tlsCfg, err := NewTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	addr := cfg.Addr()

	// Connect with context
	// Configure flow control windows to prevent server write blocking under high load
	// Default values (512KB initial, 6MB max stream, 15MB max connection) are too small
	// for high-throughput benchmarks with many rapid queries
	//
	// QUIC Performance Optimizations:
	// - DisablePathMTUDiscovery: false (default) enables DPLPMTUD which probes for larger MTUs
	//   to reduce per-packet overhead. Less than 10 probe packets sent over connection lifetime.
	// - GSO (Generic Segmentation Offload) is automatically enabled on Linux 4.18+ by quic-go.
	//   Set QUIC_GO_DISABLE_GSO=true to disable (not recommended).
	// - For optimal throughput, increase host UDP buffer sizes:
	//   sysctl -w net.core.rmem_max=7340032
	//   sysctl -w net.core.wmem_max=7340032
	qConn, err := quic.DialAddr(ctx, addr, tlsCfg, &quic.Config{
		MaxIdleTimeout:                 quicMaxIdleTimeout,
		KeepAlivePeriod:                quicKeepAlivePeriod,
		InitialStreamReceiveWindow:     quicInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicMaxConnectionReceiveWindow,
		DisablePathMTUDiscovery:        false, // Enable DPLPMTUD for optimal MTU detection
	})
	if err != nil {
		return nil, &TransportError{
			Op:    constOpDial,
			Addr:  addr,
			cause: err,
		}
	}

	// Open a bidirectional stream
	stream, err := qConn.OpenStreamSync(ctx)
	if err != nil {
		_ = qConn.CloseWithError(0, "failed to open stream")
		return nil, &TransportError{
			Op:    "open_stream",
			Addr:  addr,
			cause: err,
		}
	}

	t := &QUICTransport{
		conn:     qConn,
		stream:   stream,
		rdStream: stream,
		br:       bufio.NewReaderSize(stream, quicReadBufferSize),
		addr:     addr,
	}

	return t, nil
}

// NewPooledQUICTransport creates a new QUIC transport that uses a stream from a
// shared QUIC connection pool. This avoids the TLS handshake overhead for each
// new database/sql connection.
func NewPooledQUICTransport(ctx context.Context, pool *QUICPool) (*QUICTransport, error) {
	pc, err := pool.acquire(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := pc.openStream(ctx)
	if err != nil {
		pool.release(pc)
		return nil, err
	}

	return &QUICTransport{
		pool:     pool,
		pc:       pc,
		stream:   stream,
		rdStream: stream,
		br:       bufio.NewReaderSize(stream, quicReadBufferSize),
		addr:     pc.addr,
	}, nil
}

// SendProto writes a length-prefixed protobuf message to the server.
func (t *QUICTransport) SendProto(ctx context.Context, msg *proto.QuicClientMessage) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	t.mu.Unlock()

	// Encode the message
	data, err := msg.Encode()
	if err != nil {
		return &TransportError{
			Op:    "marshal",
			Addr:  t.addr,
			cause: err,
		}
	}

	// Set write deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		if err := t.stream.SetWriteDeadline(deadline); err != nil {
			return &TransportError{
				Op:    "set_deadline",
				Addr:  t.addr,
				cause: err,
			}
		}
		defer func() { _ = t.stream.SetWriteDeadline(time.Time{}) }()
	}

	// Check context before write
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Write 4-byte length prefix (big endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data))) //nolint:gosec // message sizes are bounded
	if _, err := t.stream.Write(lenBuf[:]); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &TransportError{
			Op:    "write_length",
			Addr:  t.addr,
			cause: err,
		}
	}

	// Write message body
	if _, err := t.stream.Write(data); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &TransportError{
			Op:    "write",
			Addr:  t.addr,
			cause: err,
		}
	}

	return nil
}

// ReceiveProto reads a length-prefixed protobuf message from the server.
// Note: This is a blocking call. Use context deadline for timeout control.
func (t *QUICTransport) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrClosed
	}
	t.mu.Unlock()

	// Honor the caller's context for the duration of the otherwise-blocking
	// read. A context can hit its deadline or be cancelled while io.ReadFull is
	// parked, and QUIC keep-alives stop the connection from ever reaching its
	// idle timeout — so without this watcher the read could block forever on a
	// half-open stream the server has already reaped (active_streams=0).
	//
	// Both deadline expiry and cancellation surface through ctx.Done(); the
	// watcher forces an immediate read deadline when ctx becomes Done, which
	// unblocks the read. Because Go guarantees ctx.Err() is non-nil once Done
	// is closed, the post-read checks below always surface the context error
	// (DeadlineExceeded or Canceled) rather than a raw i/o timeout. Driving
	// both cases through Done (instead of also setting the stream deadline from
	// ctx.Deadline directly) avoids a race where the stream's timeout could
	// fire a tick before ctx.Err() is observable.
	//
	// Lifecycle ordering is critical for pooled streams that are reused by the
	// next request. The previous implementation only `close(stop)`d the watcher
	// and reset the deadline in a separate defer — but close(stop) merely
	// *signals* the goroutine; it does not wait for it. If ctx became Done at
	// almost the same instant the read completed, the watcher could still take
	// its ctx.Done() branch and call SetReadDeadline(time.Now()) *after* the
	// reset defer had already cleared the deadline. That left a stale, in-the-
	// past read deadline on the reused stream, so the NEXT ReceiveProto failed
	// immediately with "deadline exceeded" even though its own context was not
	// done — surfacing as a spurious read_length TransportError -> ErrBadConn,
	// conn eviction, and (under sustained ingest) an eventual full wedge.
	//
	// The fix: the parent must JOIN the watcher (wait for it to exit) and only
	// then clear the deadline. We register the join+reset as a single deferred
	// step that runs after close(stop), guaranteeing no watcher write to the
	// deadline can be observed by a subsequent read on this stream.
	if ctx.Done() != nil {
		stop := make(chan struct{})
		watcherDone := make(chan struct{})
		go func() {
			defer close(watcherDone)
			select {
			case <-ctx.Done():
				_ = t.rdStream.SetReadDeadline(time.Now())
			case <-stop:
			}
		}()
		defer func() {
			// Signal the watcher to exit, wait until it has fully returned, then
			// clear any deadline it may have set. Joining before the reset closes
			// the race where a late watcher write would otherwise re-arm the
			// deadline after we cleared it.
			close(stop)
			<-watcherDone
			_ = t.rdStream.SetReadDeadline(time.Time{})
		}()
	}

	// Check context before read
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Read 4-byte length prefix (big endian)
	if _, err := io.ReadFull(t.br, t.lenBuf[:]); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{
			Op:    "read_length",
			Addr:  t.addr,
			cause: err,
		}
	}

	msgLen := binary.BigEndian.Uint32(t.lenBuf[:])

	// Sanity check length (16MB max)
	if msgLen > 16*1024*1024 {
		return nil, &TransportError{
			Op:    constOpRead,
			Addr:  t.addr,
			cause: proto.ErrInvalidMessage,
		}
	}

	// Read message body
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(t.br, msgBuf); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{
			Op:    constOpRead,
			Addr:  t.addr,
			cause: err,
		}
	}

	// Decode protobuf message
	msg, err := proto.DecodeQuicServerMessage(msgBuf)
	if err != nil {
		return nil, &TransportError{
			Op:    "decode",
			Addr:  t.addr,
			cause: err,
		}
	}

	return msg, nil
}

// TryReceiveProtoBuffered reads a complete protobuf message if enough data is buffered.
// This avoids blocking when probing for inline frames.
func (t *QUICTransport) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false, ErrClosed
	}
	t.mu.Unlock()

	buffered := t.br.Buffered()
	if buffered < lengthPrefixSize {
		return nil, false, nil
	}

	// Peek length prefix
	peeked, err := t.br.Peek(lengthPrefixSize)
	if err != nil {
		return nil, false, &TransportError{
			Op:    "peek",
			Addr:  t.addr,
			cause: err,
		}
	}

	msgLen := binary.BigEndian.Uint32(peeked)

	// Check if full message is buffered
	if buffered < lengthPrefixSize+int(msgLen) {
		return nil, false, nil
	}

	// Read length prefix (we already peeked it)
	if _, err := io.ReadFull(t.br, t.lenBuf[:]); err != nil {
		return nil, false, &TransportError{
			Op:    "read_length",
			Addr:  t.addr,
			cause: err,
		}
	}

	// Read message body
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(t.br, msgBuf); err != nil {
		return nil, false, &TransportError{
			Op:    "read_buffered",
			Addr:  t.addr,
			cause: err,
		}
	}

	// Decode protobuf message
	msg, err := proto.DecodeQuicServerMessage(msgBuf)
	if err != nil {
		return nil, false, &TransportError{
			Op:    "decode",
			Addr:  t.addr,
			cause: err,
		}
	}

	return msg, true, nil
}

// Close closes the transport.
//
// In standalone mode, both the stream and QUIC connection are closed.
// In pooled mode, only the stream is closed and the pooled connection is
// released back to the pool for reuse by other transports.
func (t *QUICTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	var firstErr error

	// Close the stream first
	if t.stream != nil {
		if err := t.stream.Close(); err != nil {
			firstErr = err
		}
	}

	if t.pool != nil {
		// Pooled mode: release the connection back to the pool (don't close it)
		t.pool.release(t.pc)
	} else if t.conn != nil {
		// Standalone mode: close the dedicated QUIC connection
		if err := t.conn.CloseWithError(0, "closing"); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// IsClosed returns true if the transport is closed.
func (t *QUICTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// PagesArePushed reports false: the QUIC surface is PULL-driven.
//
// geode 0.8.15 answers EXECUTE over QUIC with a schema frame and the first
// data page, and then waits. Every further page is fetched by a PullRequest
// the driver sends (geode src/server/quic_connection_handler.zig:7726-7743).
// The server does also push remaining pages inline on some paths and still
// answers a PULL on top of them (see Rows.abortOnCancel), so the PULL-driven
// reading below remains the correct and the only supported QUIC behaviour.
func (t *QUICTransport) PagesArePushed() bool { return false }

// RemoteClosed reports whether the underlying QUIC connection can no longer
// carry streams. quic-go cancels the connection's context when the connection
// goes away for any reason — server-initiated close, idle timeout, or a
// transport error — so a cancelled context means a server-side teardown this
// transport's own closed flag does not capture. The pool's acquire path already
// prunes such connections (isQUICConnAlive); this surfaces the same liveness to
// IsValid so a driver.Conn still holding a stream on a dead pooled connection
// is evicted by database/sql instead of being reused.
func (t *QUICTransport) RemoteClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return true
	}
	// Pooled mode borrows pc.qConn; standalone mode owns conn. pc.qConn is set
	// once at dial and never mutated, so reading it under t.mu is safe.
	qc := t.conn
	if t.pc != nil {
		qc = t.pc.qConn
	}
	if qc == nil {
		return false
	}
	return !isQUICConnAlive(qc)
}

// Addr returns the remote address.
func (t *QUICTransport) Addr() string {
	return t.addr
}

// HasBufferedData reports whether the read buffer holds any unconsumed
// bytes from a prior request.  At the start of a fresh request the buffer
// MUST be empty — non-zero buffered data after a request finishes
// indicates a stream-state desync (e.g., an undrained metrics or
// heartbeat frame).  Callers (BeginTx, ExecContext) use this as a guard
// to fail fast with ErrBadConn instead of letting the next protocol
// message land in the wrong place.
//
// Not part of the Transport interface — interface embedders that don't
// expose buffered state will simply skip the desync check.
func (t *QUICTransport) HasBufferedData() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.br != nil && t.br.Buffered() > 0
}

// BufferedLen returns the number of unconsumed bytes in the read buffer.
// Used in error messages alongside HasBufferedData to surface the size
// of any desync to the operator.
func (t *QUICTransport) BufferedLen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.br == nil {
		return 0
	}
	return t.br.Buffered()
}

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
	quicMaxIdleTimeout                 = 30 * time.Second
	quicKeepAlivePeriod                = 10 * time.Second
	quicInitialStreamReceiveWindow     = 4 * 1024 * 1024  // 4MB initial stream window
	quicMaxStreamReceiveWindow         = 16 * 1024 * 1024 // 16MB max stream window
	quicInitialConnectionReceiveWindow = 8 * 1024 * 1024  // 8MB initial connection window
	quicMaxConnectionReceiveWindow     = 32 * 1024 * 1024 // 32MB max connection window
	quicReadBufferSize                 = 64 * 1024        // 64KB buffer
	lengthPrefixSize                   = 4                // 4-byte big-endian length prefix
)

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
	br     *bufio.Reader

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
			Op:    "dial",
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
		conn:   qConn,
		stream: stream,
		br:     bufio.NewReaderSize(stream, quicReadBufferSize),
		addr:   addr,
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
		pool:   pool,
		pc:     pc,
		stream: stream,
		br:     bufio.NewReaderSize(stream, quicReadBufferSize),
		addr:   pc.addr,
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

	// Set read deadline from context if available
	if deadline, ok := ctx.Deadline(); ok {
		if err := t.stream.SetReadDeadline(deadline); err != nil {
			return nil, &TransportError{
				Op:    "set_deadline",
				Addr:  t.addr,
				cause: err,
			}
		}
		defer func() { _ = t.stream.SetReadDeadline(time.Time{}) }()
	}

	// Check context before read
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Read 4-byte length prefix (big endian)
	if _, err := io.ReadFull(t.br, t.lenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
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
			Op:    "read",
			Addr:  t.addr,
			cause: proto.ErrInvalidMessage,
		}
	}

	// Read message body
	msgBuf := make([]byte, msgLen)
	if _, err := io.ReadFull(t.br, msgBuf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{
			Op:    "read",
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

// Addr returns the remote address.
func (t *QUICTransport) Addr() string {
	return t.addr
}

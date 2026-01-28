package geode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Transport defines the interface for communication with a Geode server.
type Transport interface {
	// Send writes a message to the server.
	Send(ctx context.Context, msg map[string]any) error

	// Receive reads a message from the server.
	Receive(ctx context.Context) ([]byte, error)

	// TryReceiveBuffered reads a message only if a full line is already buffered.
	// Returns (nil, false, nil) when no complete line is buffered.
	TryReceiveBuffered() ([]byte, bool, error)

	// Close closes the transport.
	Close() error

	// IsClosed returns true if the transport is closed.
	IsClosed() bool

	// Addr returns the remote address.
	Addr() string
}

// QUICTransport implements Transport over QUIC with TLS.
type QUICTransport struct {
	conn   *quic.Conn
	stream *quic.Stream
	br     *bufio.Reader

	mu     sync.Mutex
	closed bool
	addr   string
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
		MaxIdleTimeout:                 30 * time.Second,
		KeepAlivePeriod:                10 * time.Second,
		InitialStreamReceiveWindow:     4 * 1024 * 1024,  // 4MB initial stream window (was 512KB)
		MaxStreamReceiveWindow:         16 * 1024 * 1024, // 16MB max stream window (was 6MB)
		InitialConnectionReceiveWindow: 8 * 1024 * 1024,  // 8MB initial connection window (was 512KB)
		MaxConnectionReceiveWindow:     32 * 1024 * 1024, // 32MB max connection window (was 15MB)
		DisablePathMTUDiscovery:        false,            // Enable DPLPMTUD for optimal MTU detection
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
		br:     bufio.NewReaderSize(stream, 64<<10), // 64KB buffer
		addr:   addr,
	}

	return t, nil
}

// Send writes a JSON message to the server.
func (t *QUICTransport) Send(ctx context.Context, msg map[string]any) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	t.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return &TransportError{
			Op:    "marshal",
			Addr:  t.addr,
			cause: err,
		}
	}
	data = append(data, '\n')

	// Set write deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		if err := t.stream.SetWriteDeadline(deadline); err != nil {
			return &TransportError{
				Op:    "set_deadline",
				Addr:  t.addr,
				cause: err,
			}
		}
		defer t.stream.SetWriteDeadline(time.Time{})
	}

	// Check context before write
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if _, err := t.stream.Write(data); err != nil {
		// Check if context was cancelled
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

// Receive reads a JSON line from the server.
// Note: This is a blocking call. Use context deadline for timeout control.
func (t *QUICTransport) Receive(ctx context.Context) ([]byte, error) {
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
		defer t.stream.SetReadDeadline(time.Time{})
	}

	// Check context before read
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Simple blocking read - no goroutines
	line, err := t.br.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		// Check if context was cancelled during read
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{
			Op:    "read",
			Addr:  t.addr,
			cause: err,
		}
	}

	return bytes.TrimSuffix(line, []byte{'\n'}), nil
}

// TryReceiveBuffered reads a complete buffered line without touching the network.
// This avoids partial reads/timeouts when probing for inline frames.
func (t *QUICTransport) TryReceiveBuffered() ([]byte, bool, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false, ErrClosed
	}
	t.mu.Unlock()

	buffered := t.br.Buffered()
	if buffered == 0 {
		return nil, false, nil
	}

	peeked, err := t.br.Peek(buffered)
	if err != nil {
		return nil, false, &TransportError{
			Op:    "peek",
			Addr:  t.addr,
			cause: err,
		}
	}

	idx := bytes.IndexByte(peeked, '\n')
	if idx == -1 {
		return nil, false, nil
	}

	line, err := t.br.ReadBytes('\n')
	if err != nil {
		return nil, false, &TransportError{
			Op:    "read_buffered",
			Addr:  t.addr,
			cause: err,
		}
	}

	return bytes.TrimSuffix(line, []byte{'\n'}), true, nil
}

// Close closes the transport.
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
		if err := t.stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close the connection
	if t.conn != nil {
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

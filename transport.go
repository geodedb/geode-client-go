package geode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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
	maxFrameSize                       = 32 * 1024 * 1024 // 32MB max frame size (JSON line)
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
		defer func() { _ = t.stream.SetWriteDeadline(time.Time{}) }()
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
		defer func() { _ = t.stream.SetReadDeadline(time.Time{}) }()
	}

	// Check context before read
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Simple blocking read with frame size limit
	line, err := readLineLimited(t.br, maxFrameSize)
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

	return line, nil
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

	line, err := readLineLimited(t.br, maxFrameSize)
	if err != nil {
		return nil, false, &TransportError{
			Op:    "read_buffered",
			Addr:  t.addr,
			cause: err,
		}
	}

	return line, true, nil
}

// readLineLimited reads a single newline-delimited frame with a hard size limit.
// The returned slice does not include the trailing newline.
func readLineLimited(br *bufio.Reader, limit int) ([]byte, error) {
	var buf bytes.Buffer
	total := 0

	for {
		frag, err := br.ReadSlice('\n')
		total += len(frag)
		if total > limit+1 {
			if errors.Is(err, bufio.ErrBufferFull) {
				if derr := discardUntilNewline(br); derr != nil {
					return nil, derr
				}
			}
			return nil, &SecurityError{
				Type:    "transport",
				Message: fmt.Sprintf("frame exceeds maximum size of %d bytes", limit),
			}
		}

		buf.Write(frag)

		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return nil, err
	}

	line := buf.Bytes()
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func discardUntilNewline(br *bufio.Reader) error {
	for {
		_, err := br.ReadSlice('\n')
		if err == nil {
			return nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return err
	}
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
		if err := t.stream.Close(); err != nil {
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

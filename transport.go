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
	qConn, err := quic.DialAddr(ctx, addr, tlsCfg, &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
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

// Receive reads a JSON line from the server with proper context cancellation.
func (t *QUICTransport) Receive(ctx context.Context) ([]byte, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrClosed
	}
	t.mu.Unlock()

	// Set read deadline from context
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

	// Use a channel to coordinate the read with context cancellation
	type result struct {
		line []byte
		err  error
	}
	ch := make(chan result, 1)

	go func() {
		line, err := t.br.ReadBytes('\n')
		select {
		case ch <- result{line: line, err: err}:
		case <-ctx.Done():
			// Context cancelled - goroutine will exit after sending to channel
			// or when channel is garbage collected
		}
	}()

	select {
	case <-ctx.Done():
		// Cancel the read by setting an immediate deadline
		// This will cause the ReadBytes to return with an error
		_ = t.stream.SetReadDeadline(time.Now())
		// Wait briefly for the goroutine to finish
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
		}
		return nil, ctx.Err()

	case r := <-ch:
		if r.err != nil {
			if r.err == io.EOF {
				return nil, io.EOF
			}
			// Check if context was cancelled during read
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &TransportError{
				Op:    "read",
				Addr:  t.addr,
				cause: r.err,
			}
		}
		return bytes.TrimSuffix(r.line, []byte{'\n'}), nil
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

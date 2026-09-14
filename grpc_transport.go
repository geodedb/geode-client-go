package geode

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"geodedb.com/geode/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements Transport over gRPC.
type GRPCTransport struct {
	conn   *grpc.ClientConn
	client proto.GeodeServiceClient

	// Session state
	sessionID string

	// Response buffering for streaming Execute
	execStream   proto.GeodeService_ExecuteClient
	lastResponse *proto.QuicServerMessage

	mu     sync.Mutex
	closed bool
	addr   string
}

// NewGRPCTransport creates a new gRPC transport to the Geode server.
func NewGRPCTransport(_ context.Context, cfg *Config) (*GRPCTransport, error) {
	addr := cfg.Addr()

	// Build dial options
	var opts []grpc.DialOption

	switch {
	case cfg.TLSDisabled:
		// Plaintext mode (tls=0) — no TLS at all
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case cfg.InsecureSkipVerify:
		// TLS with skip verification (for self-signed certs in testing)
		tlsCfg := &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // intentional for testing with self-signed certs
			MinVersion:         tls.VersionTLS13,
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	default:
		// Full TLS with CA verification
		tlsCfg, err := buildGRPCTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}

	// Connect using NewClient (lazy connection, established on first RPC)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, &TransportError{
			Op:    constOpDial,
			Addr:  addr,
			cause: err,
		}
	}

	t := &GRPCTransport{
		conn:   conn,
		client: proto.NewGeodeServiceClient(conn),
		addr:   addr,
	}

	return t, nil
}

// buildGRPCTLSConfig builds a TLS configuration for gRPC.
func buildGRPCTLSConfig(cfg *Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	// Load CA certificate if provided
	if cfg.TLSCA != "" {
		pool, err := loadCACertPool(cfg.TLSCA)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}

	// Load client certificate if provided (mTLS)
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, &SecurityError{
				Type:    paramTLS,
				Message: "failed to load client certificate",
				cause:   err,
			}
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// SendProto sends a protobuf message via gRPC.
//
//nolint:funlen // gRPC dispatch requires handling multiple message types
func (t *GRPCTransport) SendProto(ctx context.Context, msg *proto.QuicClientMessage) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	t.mu.Unlock()

	// Handle different message types via gRPC methods
	switch m := msg.GetMsg().(type) {
	case *proto.QuicClientMessage_Hello:
		resp, err := t.client.Handshake(ctx, m.Hello)
		if err != nil {
			return &TransportError{
				Op:    "handshake",
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.sessionID = resp.GetSessionId()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Hello{Hello: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Ping:
		resp, err := t.client.Ping(ctx, m.Ping)
		if err != nil {
			return &TransportError{
				Op:    constOpPing,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Ping{Ping: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Execute:
		// Set session ID for gRPC
		req := m.Execute
		req.SessionId = t.sessionID

		stream, err := t.client.Execute(ctx, req)
		if err != nil {
			return &TransportError{
				Op:    constOpExecute,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.execStream = stream
		t.lastResponse = nil
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Begin:
		req := m.Begin
		req.SessionId = t.sessionID

		resp, err := t.client.Begin(ctx, req)
		if err != nil {
			return &TransportError{
				Op:    constOpBegin,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Begin{Begin: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Commit:
		req := m.Commit
		req.SessionId = t.sessionID

		resp, err := t.client.Commit(ctx, req)
		if err != nil {
			return &TransportError{
				Op:    constOpCommit,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Commit{Commit: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Rollback:
		req := m.Rollback
		req.SessionId = t.sessionID

		resp, err := t.client.Rollback(ctx, req)
		if err != nil {
			return &TransportError{
				Op:    constOpRollback,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Rollback{Rollback: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_CdcDiag:
		resp, err := t.client.GetCdcDiagnostics(ctx, m.CdcDiag)
		if err != nil {
			return &TransportError{
				Op:    "cdc_diag",
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_CdcDiag{CdcDiag: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_CdcCtrl:
		resp, err := t.client.ControlCdc(ctx, m.CdcCtrl)
		if err != nil {
			return &TransportError{
				Op:    "cdc_ctrl",
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.lastResponse = &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_CdcCtrl{CdcCtrl: resp},
		}
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Pull:
		// Unreachable from the driver's own page loop, and kept as an
		// assertion rather than deleted.
		//
		// PULL is the QUIC protocol's client-driven paging request and has no
		// gRPC counterpart: geode 0.8.15 serves Execute as a SERVER-STREAMING
		// RPC that pushes every page itself and then ends the stream
		// (src/transport/grpc_server.zig:1009-1101 handleExecuteStreaming), and
		// GeodeService declares no Pull method (proto/geode_grpc.pb.go
		// GeodeService_ServiceDesc). Since Task 14b the driver consumes those
		// pushed pages instead of asking for them — Rows.fetchNextPage branches
		// on Transport.PagesArePushed, which is true here — so nothing in the
		// driver sends a PULL over gRPC any more.
		//
		// A PULL arriving here therefore means a caller reached past the page
		// loop. Failing loudly and typed is the right answer: a blind write
		// would be unanswerable and the following read would block until the
		// context expired.
		return &TransportError{
			Op:   "pull",
			Addr: t.addr,
			cause: fmt.Errorf("%w: gRPC Execute is server-streaming and carries no PULL; "+
				"pages arrive unasked (Transport.PagesArePushed)",
				ErrUnsupportedOnTransport),
		}

	default:
		// The QuicClientMessage oneof carries five message types this gRPC
		// surface genuinely cannot express — savepoint, rollback_to, backup,
		// restore and upload_backup — because GeodeService declares no method
		// for any of them (proto/geode_grpc.pb.go GeodeService_ServiceDesc).
		// This is the sentinel's live, unfixable cause: no retry and no other
		// connection grows the method, so Classify answers RetryNever.
		return &TransportError{
			Op:    "send",
			Addr:  t.addr,
			cause: fmt.Errorf("%w: %T", ErrUnsupportedOnTransport, msg.GetMsg()),
		}
	}
}

// PagesArePushed reports true: the gRPC surface is server-pushed.
//
// geode 0.8.15 serves Execute as a server-streaming RPC. handleExecuteStreaming
// (src/transport/grpc_server.zig:1009-1101) sends the schema/first response and
// then loops buildStreamDataPageLocked -> sendStreamResponse until the page it
// built is final, paging at grpc_stream_page_size (:915 ->
// src/client/quic_query_runner.zig:5 = 128 rows), and ends the stream. Every
// page is therefore already on the wire; Rows.fetchNextPage reads the next one
// off this stream instead of sending a PullRequest the service cannot answer.
func (t *GRPCTransport) PagesArePushed() bool { return true }

// ReceiveProto reads a protobuf message from the gRPC stream.
//
// Cancellation semantics match QUICTransport.ReceiveProto (Task 9): a context
// that is already done is reported before the read, and a read that fails while
// the context is done is reported as the CONTEXT error rather than as the
// transport error the cancellation manifests as on the wire — so callers keep
// matching errors.Is(err, context.Canceled).
//
// No deadline watcher is needed to unblock the read. The Execute stream is
// created from the same context in SendProto (t.client.Execute(ctx, req)), so
// grpc-go tears the RPC down the moment that context is cancelled or its
// deadline expires and the parked Recv returns immediately with a Canceled or
// DeadlineExceeded status. The checks below convert that into ctx.Err();
// Rows.fetchNextPage then wraps it and retires the connection.
func (t *GRPCTransport) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrClosed
	}

	// Check for stored response (from non-streaming RPCs)
	if t.lastResponse != nil {
		resp := t.lastResponse
		t.lastResponse = nil
		t.mu.Unlock()
		return resp, nil
	}

	// Check for streaming Execute response
	stream := t.execStream
	t.mu.Unlock()

	if stream != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := stream.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, &TransportError{
				Op:    "recv",
				Addr:  t.addr,
				cause: err,
			}
		}
		return &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Execute{Execute: resp},
		}, nil
	}

	return nil, fmt.Errorf("no response available")
}

// TryReceiveProtoBuffered attempts to read a buffered message without blocking.
func (t *GRPCTransport) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false, ErrClosed
	}

	// Check for stored response
	if t.lastResponse != nil {
		resp := t.lastResponse
		t.lastResponse = nil
		t.mu.Unlock()
		return resp, true, nil
	}
	t.mu.Unlock()

	// gRPC doesn't support non-blocking receive, so we return false
	return nil, false, nil
}

// Close closes the transport.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true

	// Close any active stream
	if t.execStream != nil {
		_ = t.execStream.CloseSend()
		t.execStream = nil
	}
	t.mu.Unlock()

	return t.conn.Close()
}

// IsClosed returns true if the transport is closed.
func (t *GRPCTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// RemoteClosed reports whether the underlying gRPC connection is terminally
// gone. gRPC transparently reconnects on transient failures, so only the
// Shutdown state (or a local Close) is non-recoverable; treating a merely
// transient failure as dead would needlessly evict connections gRPC would have
// healed. A Shutdown ClientConn must not be reused.
func (t *GRPCTransport) RemoteClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return true
	}
	return t.conn != nil && t.conn.GetState() == connectivity.Shutdown
}

// Addr returns the remote address.
func (t *GRPCTransport) Addr() string {
	return t.addr
}

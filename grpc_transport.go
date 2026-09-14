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
		// PULL has no gRPC counterpart. geode 0.8.15 serves Execute as a
		// server-streaming RPC and pushes every page itself
		// (src/transport/grpc_server.zig:1008-1101 handleExecuteStreaming),
		// and GeodeService declares no Pull method at all
		// (proto/geode_grpc.pb.go GeodeService_ServiceDesc). The driver's Rows
		// pages with PULL, so a result larger than the engine's gRPC stream
		// page size (128 rows) ends here.
		//
		// This is reported as a typed, documented refusal rather than an
		// opaque "unknown message type": a caller that gets a short result set
		// must be able to tell "the transport cannot page" from "the engine
		// errored", and Classify must answer RetryNever rather than churning
		// connections over a capability that no retry restores.
		return &TransportError{
			Op:   "pull",
			Addr: t.addr,
			cause: fmt.Errorf("%w: gRPC Execute is server-streaming and carries no PULL; "+
				"results longer than one gRPC page cannot be paged over this transport",
				ErrUnsupportedOnTransport),
		}

	default:
		return &TransportError{
			Op:    "send",
			Addr:  t.addr,
			cause: fmt.Errorf("%w: %T", ErrUnsupportedOnTransport, msg.GetMsg()),
		}
	}
}

// ReceiveProto reads a protobuf message from the gRPC stream.
func (t *GRPCTransport) ReceiveProto(_ context.Context) (*proto.QuicServerMessage, error) {
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
		resp, err := stream.Recv()
		if err != nil {
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

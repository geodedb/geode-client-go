package geode

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"geodedb.com/geode/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Long-lived grpc-go channels start returning malformed Execute requests after
// roughly a dozen RPCs in Geode integration runs. Recycle early, but defer the
// recycle while a transaction is active so session-bound work stays coherent.
const grpcMaxRPCsPerConn = 10
const grpcTxRPCBudget = 7

// GRPCTransport implements Transport over gRPC.
type GRPCTransport struct {
	conn        *grpc.ClientConn
	client      proto.GeodeServiceClient
	dialOptions []grpc.DialOption
	rpcCount    int

	// Session state
	sessionID string
	inTx      bool

	// Response buffering for streaming Execute
	execStream   proto.GeodeService_ExecuteClient
	execCancel   context.CancelFunc
	lastResponse *proto.QuicServerMessage
	recycleAfter bool

	mu     sync.Mutex
	closed bool
	addr   string
}

func grpcExecuteStreamComplete(resp *proto.ExecutionResponse) bool {
	if resp == nil {
		return false
	}
	if resp.GetError() != nil {
		return true
	}
	if page := resp.GetPage(); page != nil && page.GetFinal() {
		return true
	}
	if resp.GetHeartbeat() != nil {
		return true
	}
	return false
}

func (t *GRPCTransport) closeConnLocked() {
	if t.conn != nil {
		_ = t.conn.Close()
		t.conn = nil
		t.client = nil
	}
	t.rpcCount = 0
	t.recycleAfter = false
}

func (t *GRPCTransport) maybeRecycleLocked() {
	if t.client == nil || t.rpcCount < grpcMaxRPCsPerConn {
		return
	}
	if t.inTx || t.execStream != nil {
		t.recycleAfter = true
		return
	}
	t.closeConnLocked()
}

func (t *GRPCTransport) setTransactionState(inTx bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.inTx = inTx
	if !inTx && t.recycleAfter && t.execStream == nil {
		t.closeConnLocked()
	}
}

func (t *GRPCTransport) prepareForTransaction() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || t.execStream != nil {
		return
	}
	if t.client != nil && t.rpcCount >= grpcMaxRPCsPerConn-grpcTxRPCBudget {
		t.closeConnLocked()
	}
}

func (t *GRPCTransport) abortExecute() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.execCancel != nil {
		t.execCancel()
		t.execCancel = nil
	}
	if t.execStream != nil {
		_ = t.execStream.CloseSend()
		t.execStream = nil
	}
	t.closeConnLocked()
}

func (t *GRPCTransport) ensureClient(_ context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	if t.client != nil {
		t.mu.Unlock()
		return nil
	}
	opts := append([]grpc.DialOption(nil), t.dialOptions...)
	addr := t.addr
	t.mu.Unlock()

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return &TransportError{
			Op:    "dial",
			Addr:  addr,
			cause: err,
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = conn.Close()
		return ErrClosed
	}
	if t.client != nil {
		_ = conn.Close()
		return nil
	}
	t.conn = conn
	t.client = proto.NewGeodeServiceClient(conn)
	return nil
}

func (t *GRPCTransport) prepareClient(ctx context.Context) (proto.GeodeServiceClient, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrClosed
	}
	t.maybeRecycleLocked()
	t.mu.Unlock()

	if err := t.ensureClient(ctx); err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.client == nil {
		return nil, ErrClosed
	}
	t.rpcCount++
	return t.client, nil
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

	t := &GRPCTransport{
		addr:        addr,
		dialOptions: opts,
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
				Type:    "tls",
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		resp, err := client.Handshake(ctx, m.Hello)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		resp, err := client.Ping(ctx, m.Ping)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
			return &TransportError{
				Op:    "ping",
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

		t.mu.Lock()
		if t.execCancel != nil {
			t.execCancel()
			t.execCancel = nil
		}
		if t.execStream != nil {
			_ = t.execStream.CloseSend()
			t.execStream = nil
		}
		t.mu.Unlock()

		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}

		execCtx, execCancel := context.WithCancel(ctx)
		stream, err := client.Execute(execCtx, req)
		if err != nil {
			execCancel()
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
			return &TransportError{
				Op:    "execute",
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.execStream = stream
		t.execCancel = execCancel
		t.lastResponse = nil
		t.mu.Unlock()
		return nil

	case *proto.QuicClientMessage_Pull:
		// Execute is server-streaming over gRPC, so additional pages are read
		// from the active Execute stream rather than via a separate Pull RPC.
		return nil

	case *proto.QuicClientMessage_Begin:
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		req := m.Begin
		req.SessionId = t.sessionID

		resp, err := client.Begin(ctx, req)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
			return &TransportError{
				Op:    "begin",
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		req := m.Commit
		req.SessionId = t.sessionID

		resp, err := client.Commit(ctx, req)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
			return &TransportError{
				Op:    "commit",
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		req := m.Rollback
		req.SessionId = t.sessionID

		resp, err := client.Rollback(ctx, req)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
			return &TransportError{
				Op:    "rollback",
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		resp, err := client.GetCdcDiagnostics(ctx, m.CdcDiag)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
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
		client, err := t.prepareClient(ctx)
		if err != nil {
			return err
		}
		resp, err := client.ControlCdc(ctx, m.CdcCtrl)
		if err != nil {
			t.mu.Lock()
			t.closeConnLocked()
			t.mu.Unlock()
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

	default:
		return fmt.Errorf("unknown message type")
	}
}

// ReceiveProto reads a protobuf message from the gRPC stream.
//
//nolint:gocognit // stream receive needs explicit handling for cached, active, and terminal states
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
		type recvResult struct {
			resp *proto.ExecutionResponse
			err  error
		}

		done := make(chan recvResult, 1)
		go func() {
			resp, err := stream.Recv()
			done <- recvResult{resp: resp, err: err}
		}()

		select {
		case <-ctx.Done():
			t.mu.Lock()
			if t.execCancel != nil {
				t.execCancel()
				t.execCancel = nil
			}
			if t.execStream == stream {
				t.execStream = nil
			}
			t.closeConnLocked()
			t.mu.Unlock()
			return nil, ctx.Err()
		case result := <-done:
			if result.err != nil {
				t.mu.Lock()
				if t.execStream == stream {
					t.execStream = nil
				}
				if t.execCancel != nil {
					t.execCancel()
					t.execCancel = nil
				}
				t.closeConnLocked()
				t.mu.Unlock()
				return nil, &TransportError{
					Op:    "recv",
					Addr:  t.addr,
					cause: result.err,
				}
			}
			if grpcExecuteStreamComplete(result.resp) {
				t.mu.Lock()
				if t.execStream == stream {
					t.execStream = nil
				}
				if t.execCancel != nil {
					t.execCancel()
					t.execCancel = nil
				}
				t.mu.Unlock()
			}
			return &proto.QuicServerMessage{
				Msg: &proto.QuicServerMessage_Execute{Execute: result.resp},
			}, nil
		}
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
	if t.execCancel != nil {
		t.execCancel()
		t.execCancel = nil
	}
	t.closeConnLocked()
	t.mu.Unlock()

	return nil
}

// IsClosed returns true if the transport is closed.
func (t *GRPCTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Addr returns the remote address.
func (t *GRPCTransport) Addr() string {
	return t.addr
}

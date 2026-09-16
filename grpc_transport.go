package geode

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"geodedb.com/geode/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// finishExecStreamBudget bounds the graceful drain in FinishExecStream.
//
// The stream being drained has already delivered everything the driver asked
// for, so the only thing left on it is the server's end-of-stream; that arrives
// in microseconds on a healthy connection. The budget exists so a server that
// never ends it degrades to the old reset behaviour instead of blocking the
// caller — it is a backstop, not a wait anybody should ever observe.
const finishExecStreamBudget = 2 * time.Second

// GRPCTransport implements Transport over gRPC.
type GRPCTransport struct {
	conn   *grpc.ClientConn
	client proto.GeodeServiceClient

	// Session state
	sessionID string

	// Response buffering for streaming Execute
	execStream   proto.GeodeService_ExecuteClient
	lastResponse *proto.QuicServerMessage

	// execCancel cancels the context the CURRENT Execute RPC was created
	// from. It is the only way to tell geode to stop pushing pages: the
	// service is server-streaming and the QuicClientMessage oneof carries no
	// cancel/discard message (src/proto/geode.proto:36-53), so abandoning a
	// result without cancelling leaves the server writing pages nobody reads.
	//
	// Why that wedges the session rather than merely wasting bandwidth: the
	// engine's push loop holds the session mutex for the whole send
	// (src/transport/grpc_server.zig:1065-1090, entry.mutex.lockUncancelable)
	// and handleExecute (:1935) takes that same mutex. Once an abandoned
	// result fills the HTTP/2 flow-control window the push loop parks INSIDE
	// the critical section, and the next Execute on that session blocks behind
	// it until the abandoned reader's context finally dies. Cancelling the RPC
	// closes the stream, unblocks the parked write and releases the mutex.
	//
	// Fired in exactly three places: the next Execute (which would otherwise
	// overwrite execStream and orphan the old RPC) — BEFORE that Execute is
	// issued, see the ordering note at the call site — Close, and a non-final
	// Rows.Close (CancelActiveStream, via the streamCanceller assertion in
	// rows.go).
	execCancel context.CancelFunc

	// streamResets counts the Execute RPCs this transport tore down with
	// CancelActiveStream while they were still live — i.e. every RST_STREAM
	// the driver made the engine handle. The engine's vendored grpc-zig turns
	// ONE stream reset into a whole-connection teardown
	// (zig-pkg/grpc_zig/src/server.zig treats an inbound RST_STREAM exactly
	// like GOAWAY), so this is not a statistic: it is the count of times the
	// driver put a pooled connection at risk. Regression tests assert it stays
	// at zero for requests that completed (SONNY-806 Task 16e).
	streamResets atomic.Uint64

	mu     sync.Mutex
	closed bool
	addr   string
}

// ExecStreamResets reports how many live Execute RPCs this transport has torn
// down with a reset rather than finishing. Zero is the only correct value for
// a workload in which every request ran to completion.
func (t *GRPCTransport) ExecStreamResets() uint64 { return t.streamResets.Load() }

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
				Op:    constOpHandshake,
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

		// Whatever RPC this transport was on is now unreachable: execStream
		// is about to be replaced and nothing can ever Recv from the old one
		// again. Tear it down rather than orphan it, or its unread pages park
		// the engine's push loop on the session mutex and the Execute below
		// waits behind it.
		//
		// BEFORE issuing the new Execute, never after. Measured against
		// engine 0.8.15: cancelling the superseded RPC while the next Execute
		// is already in flight on the same session poisons the session — a
		// 647-statement Pool.BatchExec fails around statement 29 with
		// driver.ErrBadConn (TestIntegration_GRPCBatchExecPastOnePage).
		// Ordering the RST_STREAM ahead of the new request keeps the engine's
		// view of the session single-threaded and the batch green. It is the
		// same grpc-zig weakness GAP-SONNY806-07 asks the engine to fix.
		t.mu.Lock()
		prev := t.execCancel
		t.execCancel = nil
		t.execStream = nil
		t.mu.Unlock()
		if prev != nil {
			// A superseded RPC is reset, not finished: nothing has read it to
			// its end and the engine may still be pushing it. Counted for the
			// same reason CancelActiveStream is — every one of these is a
			// whole-connection teardown the engine has to survive.
			t.streamResets.Add(1)
			prev()
		}

		// Give this Execute its own cancellable child of the caller's context
		// so the RPC can be torn down independently of that context — the
		// caller's may well stay live long after the driver has stopped
		// reading (an early rows.Close on a still-valid query context).
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := t.client.Execute(streamCtx, req)
		if err != nil {
			cancel()
			// A stale lastResponse (e.g. a prior Hello/Ping response) must not
			// survive a failed Execute: ReceiveProto checks lastResponse
			// before it checks execStream, so leaving one set would hand the
			// caller of this failed Execute somebody else's cached response
			// instead of the "no response available" / stream-nil state a
			// failed Execute should leave behind.
			t.mu.Lock()
			t.lastResponse = nil
			t.mu.Unlock()
			return &TransportError{
				Op:    constOpExecute,
				Addr:  t.addr,
				cause: err,
			}
		}
		t.mu.Lock()
		t.execStream = stream
		t.execCancel = cancel
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
// No deadline watcher is needed to unblock the read — but ONLY for the context
// the stream was created from. PRECONDITION: ctx must be the context passed to
// the SendProto that opened this Execute (or a child of it). grpc-go binds a
// server-streaming RPC to the context it was created with, and nothing else:
// SendProto derives streamCtx from that context and t.client.Execute rides it,
// so cancelling it (or letting its deadline expire) tears the RPC down and the
// parked Recv returns at once with Canceled or DeadlineExceeded. The checks
// below convert that into ctx.Err(); Rows.fetchNextPage then wraps it and
// retires the connection.
//
// An UNRELATED ctx — a fresh context.WithTimeout over context.Background(),
// say — does not bind the stream at all. Its expiry is invisible to Recv,
// which stays parked on the wire until the server sends something, so a caller
// that passes one can block for far longer than the timeout it asked for.
// That is why Conn.drainTrailingFrames, whose drain context is exactly such a
// background context, does not run on a pushed-page transport.
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

// CancelActiveStream tears down the Execute RPC this transport is currently
// reading, if any, and forgets it. It is idempotent and safe to call on a
// transport that never issued an Execute.
//
// The driver calls it when a caller abandons a result set before its final
// page (Rows.Close on a pushed-page transport). Cancelling — rather than
// marking the connection failed — is deliberate: the RPC is per-Execute, so
// killing it leaves the gRPC ClientConn and the engine session perfectly
// usable, and the connection goes back to the pool instead of being evicted
// and re-dialled. Only a desynchronised connection (a cancelled query context,
// a transport error) is worth retiring.
func (t *GRPCTransport) CancelActiveStream() {
	t.mu.Lock()
	cancel := t.execCancel
	t.execCancel = nil
	// The stream is dead once cancel runs; drop it so a later ReceiveProto
	// reports "no response available" instead of reading a torn-down RPC.
	t.execStream = nil
	t.mu.Unlock()

	if cancel != nil {
		t.streamResets.Add(1)
		cancel()
	}
}

// FinishExecStream ends the Execute RPC this transport is reading the way the
// SERVER expects: half-closed and drained to its trailers, not reset.
//
// Every Execute opens its own server-streaming RPC and the driver stops reading
// as soon as it has what it asked for, so until this existed the RPC was left
// dangling and the NEXT Execute tore it down with an RST_STREAM (see the
// ordering note in SendProto). A statement-per-RPC workload therefore emitted
// one RST_STREAM per statement, and the engine's vendored grpc-zig races that
// reset against its own handler: it reports `gRPC streaming: failed to send
// initial response error=PeerGone` and then drops the WHOLE TCP connection
// rather than the one stream (`error: Handler error: error.PeerGone`), so a
// 647-statement Pool.BatchExec died mid-transaction with `rpc error: code =
// Unavailable desc = error reading from server: EOF` at a statement number that
// moved run to run. Draining instead of resetting keeps the driver off that
// race: a stream the server finished itself is never reset.
//
// It is called only where the driver KNOWS the response is complete. A result
// the engine is still pushing must still be cancelled, not drained — draining
// one of those is what GAP-SONNY806-07 parks on — so CancelActiveStream stays
// exactly as it was.
//
// The drain is bounded by finishExecStreamBudget and falls back to cancelling.
// A server that will not end a stream it has already answered gets the reset it
// used to get, on a timer, instead of hanging the caller.
func (t *GRPCTransport) FinishExecStream() {
	t.mu.Lock()
	stream := t.execStream
	cancel := t.execCancel
	t.execStream = nil
	t.execCancel = nil
	t.mu.Unlock()

	if stream == nil {
		if cancel != nil {
			cancel()
		}
		return
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	select {
	case <-drained:
	case <-time.After(finishExecStreamBudget):
	}

	// Always run cancel: after a clean drain the RPC is already over and this
	// only releases the context's resources, and after a timeout it is the
	// teardown that stops the goroutine above.
	if cancel != nil {
		cancel()
	}
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

	// Close any active stream. CloseSend only half-closes the client side of a
	// server-streaming RPC — it does not stop the server pushing — so cancel
	// the RPC's context as well, or a result abandoned at Close keeps the
	// engine's push loop on the session mutex.
	if t.execStream != nil {
		_ = t.execStream.CloseSend()
		t.execStream = nil
	}
	cancel := t.execCancel
	t.execCancel = nil
	t.mu.Unlock()

	if cancel != nil {
		cancel()
	}

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

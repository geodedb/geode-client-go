// grpc_transport_cancel_test.go covers the per-Execute cancellation contract of
// GRPCTransport (SONNY-806 / GEO-GO-UP-001, Lane D Task 14c).
//
// geode serves Execute as a server-streaming RPC and pushes every page of a
// result unasked. The only way to stop it is to cancel the RPC, and failing to
// do so is not merely wasteful: the engine's push loop holds the session mutex
// across the send (src/transport/grpc_server.zig:1065-1090,
// entry.mutex.lockUncancelable) and handleExecute (:1935) takes that same
// mutex, so an abandoned result that fills the HTTP/2 window parks the loop
// inside the critical section and the next Execute on that session wedges
// behind it.
//
// These tests need no server: grpc.NewClient is lazy and the client interface
// is replaced with a fake, so what is asserted is exactly the driver's own
// bookkeeping — which context each Execute rides, and who cancels it.
package geode

import (
	"context"
	"errors"
	"testing"

	"geodedb.com/geode/proto"
	"google.golang.org/grpc"
)

// fakeExecStream is a server-streaming Execute client whose Recv is supplied
// by the test. The embedded grpc.ClientStream (nil) satisfies the rest of the
// interface; any test that called into it would panic loudly rather than
// silently pass, which is the intent.
type fakeExecStream struct {
	grpc.ClientStream
	recv func() (*proto.ExecutionResponse, error)
}

func (f *fakeExecStream) Recv() (*proto.ExecutionResponse, error) {
	if f.recv == nil {
		return nil, errors.New("fakeExecStream: Recv called unexpectedly")
	}
	return f.recv()
}

func (f *fakeExecStream) CloseSend() error { return nil }

// errFakeUnused marks the GeodeServiceClient methods these tests never reach.
var errFakeUnused = errors.New("fakeGeodeClient: method not used by this test")

// fakeGeodeClient records the context of every Execute it is handed. That
// context is the whole point: SendProto must derive a cancellable child of the
// caller's context per Execute, keep its cancel, and fire it when the RPC is
// superseded or abandoned.
type fakeGeodeClient struct {
	t        *testing.T
	execCtxs []context.Context //nolint:containedctx // captured for assertion, never used to call
}

// Execute asserts the ordering guarantee at the point that matters: the
// moment the NEXT Execute is dispatched, not merely by the time SendProto
// eventually returns. SendProto fires the previous stream's cancel BEFORE
// issuing this one (the "RST_STREAM ahead of the new request" ordering that
// keeps engine 0.8.15's view of the session single-threaded,
// GAP-SONNY806-07), so by the time this method runs, any earlier Execute's
// context must already report done.
func (c *fakeGeodeClient) Execute(
	ctx context.Context, _ *proto.ExecuteRequest, _ ...grpc.CallOption,
) (grpc.ServerStreamingClient[proto.ExecutionResponse], error) {
	if len(c.execCtxs) > 0 {
		if prev := c.execCtxs[len(c.execCtxs)-1]; prev.Err() == nil {
			c.t.Fatalf("Execute called for a new stream while the previous Execute's " +
				"context is still live: the previous RPC must be cancelled BEFORE the " +
				"next Execute is issued, or the engine keeps pushing pages into a window " +
				"nobody drains and parks the push loop on the session mutex the new " +
				"Execute needs (GAP-SONNY806-07)")
		}
	}
	c.execCtxs = append(c.execCtxs, ctx)
	return &fakeExecStream{}, nil
}

func (*fakeGeodeClient) Handshake(
	context.Context, *proto.HelloRequest, ...grpc.CallOption,
) (*proto.HelloResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Ping(
	context.Context, *proto.PingRequest, ...grpc.CallOption,
) (*proto.PingResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Begin(
	context.Context, *proto.BeginRequest, ...grpc.CallOption,
) (*proto.BeginResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Commit(
	context.Context, *proto.CommitRequest, ...grpc.CallOption,
) (*proto.CommitResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Rollback(
	context.Context, *proto.RollbackRequest, ...grpc.CallOption,
) (*proto.RollbackResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) GetCdcDiagnostics(
	context.Context, *proto.CdcDiagnosticsRequest, ...grpc.CallOption,
) (*proto.CdcDiagnosticsResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) ControlCdc(
	context.Context, *proto.CdcControlRequest, ...grpc.CallOption,
) (*proto.CdcControlResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Savepoint(
	context.Context, *proto.SavepointRequest, ...grpc.CallOption,
) (*proto.SavepointResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) RollbackTo(
	context.Context, *proto.RollbackToRequest, ...grpc.CallOption,
) (*proto.RollbackToResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Backup(
	context.Context, *proto.BackupRequest, ...grpc.CallOption,
) (*proto.BackupResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) Restore(
	context.Context, *proto.RestoreRequest, ...grpc.CallOption,
) (*proto.RestoreResponse, error) {
	return nil, errFakeUnused
}

func (*fakeGeodeClient) UploadBackup(
	context.Context, *proto.UploadBackupRequest, ...grpc.CallOption,
) (*proto.UploadBackupResponse, error) {
	return nil, errFakeUnused
}

// newFakeGRPCTransport returns a GRPCTransport wired to a fake client. conn is
// left nil, so only Close (which dereferences it) is off limits; the Close
// test builds a real lazy transport instead.
func newFakeGRPCTransport(t *testing.T) (*GRPCTransport, *fakeGeodeClient) {
	fc := &fakeGeodeClient{t: t}
	return &GRPCTransport{client: fc, addr: "fake://grpc"}, fc
}

func execMsg() *proto.QuicClientMessage {
	return &proto.QuicClientMessage{
		Msg: &proto.QuicClientMessage_Execute{Execute: &proto.ExecuteRequest{Query: "RETURN 1"}},
	}
}

// TestGRPCTransport_NextExecuteCancelsThePreviousStream is the transport half
// of the wedge fix. Before it, the second Execute simply overwrote execStream
// and the first RPC was orphaned with its context still live — the engine went
// on pushing pages into a window nobody drained, holding the session mutex the
// new Execute needed.
func TestGRPCTransport_NextExecuteCancelsThePreviousStream(t *testing.T) {
	tr, fc := newFakeGRPCTransport(t)
	ctx := context.Background()

	if err := tr.SendProto(ctx, execMsg()); err != nil {
		t.Fatalf("first SendProto: %v", err)
	}
	if len(fc.execCtxs) != 1 {
		t.Fatalf("Execute called %d time(s), want 1", len(fc.execCtxs))
	}
	first := fc.execCtxs[0]
	if err := first.Err(); err != nil {
		t.Fatalf("first Execute's context is already done (%v): the RPC would never run", err)
	}

	if err := tr.SendProto(ctx, execMsg()); err != nil {
		t.Fatalf("second SendProto: %v", err)
	}

	if err := first.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Execute's context Err() = %v, want context.Canceled: the "+
			"superseded RPC was orphaned, so the engine keeps pushing its pages and "+
			"parks the push loop on the session mutex the new Execute needs", err)
	}
	if err := fc.execCtxs[1].Err(); err != nil {
		t.Fatalf("second Execute's context Err() = %v, want nil: the live RPC was "+
			"cancelled along with the old one", err)
	}
}

// TestGRPCTransport_CancelActiveStreamIsIdempotent pins the call Rows.Close
// makes on a non-final close, including the repeat calls a double Close or a
// Close-after-Close-on-a-fresh-transport produces.
func TestGRPCTransport_CancelActiveStreamIsIdempotent(t *testing.T) {
	tr, fc := newFakeGRPCTransport(t)

	// Safe before any Execute.
	tr.CancelActiveStream()

	if err := tr.SendProto(context.Background(), execMsg()); err != nil {
		t.Fatalf("SendProto: %v", err)
	}
	tr.CancelActiveStream()
	tr.CancelActiveStream()

	if err := fc.execCtxs[0].Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute context Err() = %v, want context.Canceled", err)
	}

	tr.mu.Lock()
	stream, cancel := tr.execStream, tr.execCancel
	tr.mu.Unlock()
	if stream != nil {
		t.Error("execStream survived CancelActiveStream: a later ReceiveProto would " +
			"read a torn-down RPC")
	}
	if cancel != nil {
		t.Error("execCancel survived CancelActiveStream")
	}
}

// TestGRPCTransport_CloseCancelsTheActiveStream covers the third firing site.
// CloseSend only half-closes the client side of a server-streaming RPC; on its
// own it does not stop the server pushing.
func TestGRPCTransport_CloseCancelsTheActiveStream(t *testing.T) {
	// A real (lazy, unconnected) transport so Close can close its ClientConn,
	// with the client swapped for the fake.
	tr, err := NewGRPCTransport(context.Background(), &Config{
		Host: "127.0.0.1", Port: "50051", Transport: TransportGRPC, TLSDisabled: true,
	})
	if err != nil {
		t.Fatalf("NewGRPCTransport: %v", err)
	}
	fc := &fakeGeodeClient{t: t}
	tr.client = fc

	if err := tr.SendProto(context.Background(), execMsg()); err != nil {
		t.Fatalf("SendProto: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := fc.execCtxs[0].Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute context Err() after Close = %v, want context.Canceled", err)
	}
}

// TestGRPCTransport_ReceiveProtoRejectsADoneContextBeforeReading pins the
// pre-read guard. A context that is already done must be reported as its own
// error without ever touching the stream: Recv on a torn-down RPC yields a
// gRPC status, and surfacing that instead would break
// errors.Is(err, context.Canceled) for every caller — the Task 9 contract that
// Rows.abortOnCancel and rows.Err() depend on.
func TestGRPCTransport_ReceiveProtoRejectsADoneContextBeforeReading(t *testing.T) {
	tr, _ := newFakeGRPCTransport(t)

	recvCalls := 0
	tr.execStream = &fakeExecStream{recv: func() (*proto.ExecutionResponse, error) {
		recvCalls++
		return &proto.ExecutionResponse{}, nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg, err := tr.ReceiveProto(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReceiveProto with a cancelled context = (%v, %v), want context.Canceled",
			msg, err)
	}
	if msg != nil {
		t.Fatalf("ReceiveProto returned a message (%v) alongside the context error", msg)
	}
	if recvCalls != 0 {
		t.Fatalf("Recv called %d time(s) on an already-done context, want 0", recvCalls)
	}
}

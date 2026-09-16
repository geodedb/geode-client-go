//go:build integration

// integration_grpc_stream_finish_test.go — SONNY-806, Lane D Task 16d.
//
// Every gRPC Execute rides its own server-streaming RPC. The driver stops
// reading as soon as it has the answer, so an RPC it does not finish is left
// dangling — and the NEXT Execute tears that one down with an RST_STREAM before
// issuing itself (GRPCTransport.SendProto). A statement-per-RPC workload
// therefore emitted one RST_STREAM per statement.
//
// The engine's vendored grpc-zig races that reset against its own handler. It
// logs `gRPC streaming: failed to send initial response error=PeerGone` and
// then drops the WHOLE TCP connection rather than the single stream (`error:
// Handler error: error.PeerGone`), so TestIntegration_GRPCBatchExecPastOnePage
// died mid-transaction with `rpc error: code = Unavailable desc = error reading
// from server: EOF` at a statement number that moved run to run — 3 in CI, 5,
// 84 and 138 across local reproductions. Against the testcontainers harness
// each test gets its own engine, which is why it only showed up on CI's one
// shared service engine.
//
// The engine should not lose a connection over one reset stream, and that is
// its own ticket. This test pins the driver's side of it: a request the driver
// has finished with leaves NOTHING for the next one to reset.

package geode

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

// TestIntegration_GRPCCompletedExecLeavesNoStreamToReset is deliberately
// white-box. The black-box symptom is a race — it needs hundreds of statements
// and still moves around — whereas the state that causes it is exact and can be
// asserted after a single statement.
func TestIntegration_GRPCCompletedExecLeavesNoStreamToReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := grpcDriverConn(t, ctx, grpcBatchGraph)

	transport, ok := conn.transport.(*GRPCTransport)
	if !ok {
		t.Fatalf("gRPC conn carries a %T, want *GRPCTransport", conn.transport)
	}

	for i := range 5 {
		args := []driver.NamedValue{{Ordinal: 1, Value: int64(i)}}
		if _, err := conn.ExecContext(ctx,
			"MERGE (n:GRPCFinishProbe {i: ?})", args); err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}

		transport.mu.Lock()
		stream, streamCancel := transport.execStream, transport.execCancel
		transport.mu.Unlock()

		if stream != nil {
			t.Fatalf("statement %d left its Execute RPC dangling: the next statement "+
				"will reset it, and the engine drops the whole connection on that race", i)
		}
		if streamCancel != nil {
			t.Fatalf("statement %d left an exec cancel func behind", i)
		}
	}
}

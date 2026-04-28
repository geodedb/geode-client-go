// Regression tests for the BeginTx desync guard + descriptive error message
// added 2026-04-28.
//
// The msft-graph parallel seed surfaced a chunk-failure pattern of the form
//
//   batch begin tx: expected BEGIN response
//   batch begin tx: sql: connection is already closed
//
// at the start of edge files when the pinned conns were reused across files.
// Two failure modes were possible:
//
//   (1) The previous request on the same QUIC stream left buffered bytes
//       (e.g., undrained metrics or heartbeat frame).  The next BeginTx's
//       ReceiveProto would consume those stale bytes as if they were the
//       BEGIN response and either fail to decode or surface a non-`.begin`
//       variant.
//   (2) The server sent something other than `.begin` for some other reason
//       (e.g., a stream-level error path).
//
// The fixes pinned by this file:
//   * BeginTx now checks `transport.HasBufferedData()` BEFORE sending and
//     short-circuits with ErrBadConn if any unconsumed bytes are present —
//     so the conn gets evicted instead of poisoning the stream further.
//   * On a non-`.begin` response, BeginTx now reports `expected BEGIN
//     response, got <variant>` so incident analysis can see what the
//     server actually sent.

package geode

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"geodedb.com/geode/proto"
)

// ─────────────────────────────────────────────────────────────────────────────
// describeServerMessage: pin one tag per oneof variant.
// ─────────────────────────────────────────────────────────────────────────────

func TestDescribeServerMessage_Nil(t *testing.T) {
	if got := describeServerMessage(nil); got != "nil" {
		t.Fatalf("describeServerMessage(nil) = %q, want %q", got, "nil")
	}
}

func TestDescribeServerMessage_Begin(t *testing.T) {
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Begin{
			Begin: &proto.BeginResponse{TxId: "abc"},
		},
	}
	if got := describeServerMessage(msg); got != "begin" {
		t.Fatalf("got %q, want %q", got, "begin")
	}
}

func TestDescribeServerMessage_Hello(t *testing.T) {
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Hello{
			Hello: &proto.HelloResponse{Success: true},
		},
	}
	if got := describeServerMessage(msg); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestDescribeServerMessage_Commit(t *testing.T) {
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Commit{
			Commit: &proto.CommitResponse{Success: true},
		},
	}
	if got := describeServerMessage(msg); got != "commit" {
		t.Fatalf("got %q, want %q", got, "commit")
	}
}

func TestDescribeServerMessage_Rollback(t *testing.T) {
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Rollback{
			Rollback: &proto.RollbackResponse{Success: true},
		},
	}
	if got := describeServerMessage(msg); got != "rollback" {
		t.Fatalf("got %q, want %q", got, "rollback")
	}
}

func TestDescribeServerMessage_ExecuteWithError(t *testing.T) {
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Error{
					Error: &proto.Error{
						Code:    "42P01",
						Message: "relation does not exist",
					},
				},
			},
		},
	}
	got := describeServerMessage(msg)
	// The execute(error=...) form must include both the marker and the
	// embedded message so an operator can search the seed log for the
	// specific server-side condition that triggered the desync.
	if !strings.HasPrefix(got, "execute(error=") {
		t.Fatalf("expected execute(error=...) prefix, got %q", got)
	}
	if !strings.Contains(got, "relation does not exist") {
		t.Fatalf("expected embedded server message, got %q", got)
	}
}

func TestDescribeServerMessage_ExecutePlain(t *testing.T) {
	// Schema-only execute payload (no error) → just "execute".
	msg := &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Schema{
					Schema: &proto.SchemaDefinition{},
				},
			},
		},
	}
	if got := describeServerMessage(msg); got != "execute" {
		t.Fatalf("got %q, want %q", got, "execute")
	}
}

func TestDescribeServerMessage_UnknownEmpty(t *testing.T) {
	// An empty oneof shouldn't crash; fall through to "unknown".
	msg := &proto.QuicServerMessage{}
	if got := describeServerMessage(msg); got != "unknown" {
		t.Fatalf("got %q, want %q", got, "unknown")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QUICTransport.HasBufferedData / BufferedLen: pin the bufio-reader peek.
// ─────────────────────────────────────────────────────────────────────────────

// constructTransportWithBufferedData builds a QUICTransport whose internal
// bufio.Reader is populated with `n` bytes of synthetic data.  Used to
// simulate the "previous request left undrained bytes" condition without
// needing a real QUIC connection.
func constructTransportWithBufferedData(t *testing.T, n int) *QUICTransport {
	t.Helper()
	tr := &QUICTransport{addr: "test://memory"}
	if n == 0 {
		// Empty buffer state — bufio.Reader over an empty source.
		tr.br = bufio.NewReaderSize(bytes.NewReader(nil), 64*1024)
		return tr
	}
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i)
	}
	tr.br = bufio.NewReaderSize(bytes.NewReader(payload), 64*1024)
	// Force the bufio reader to actually buffer the bytes by peeking
	// at one byte (Peek primes the underlying buffer).
	if _, err := tr.br.Peek(n); err != nil {
		t.Fatalf("priming peek failed: %v", err)
	}
	return tr
}

func TestQUICTransport_HasBufferedData_EmptyBuffer(t *testing.T) {
	tr := constructTransportWithBufferedData(t, 0)
	if tr.HasBufferedData() {
		t.Fatalf("HasBufferedData = true on empty buffer; want false")
	}
	if got := tr.BufferedLen(); got != 0 {
		t.Fatalf("BufferedLen = %d on empty buffer; want 0", got)
	}
}

func TestQUICTransport_HasBufferedData_NonEmptyBuffer(t *testing.T) {
	tr := constructTransportWithBufferedData(t, 17)
	if !tr.HasBufferedData() {
		t.Fatalf("HasBufferedData = false on 17-byte buffer; want true")
	}
	if got := tr.BufferedLen(); got != 17 {
		t.Fatalf("BufferedLen = %d on 17-byte buffer; want 17", got)
	}
}

func TestQUICTransport_HasBufferedData_NilReader(t *testing.T) {
	// A QUICTransport that hasn't been fully initialized (br=nil) must
	// not panic when interrogated — the desync guard runs from BeginTx
	// even on partly-constructed conns during error paths.
	tr := &QUICTransport{addr: "test://memory"}
	if tr.HasBufferedData() {
		t.Fatalf("HasBufferedData on nil reader = true; want false")
	}
	if got := tr.BufferedLen(); got != 0 {
		t.Fatalf("BufferedLen on nil reader = %d; want 0", got)
	}
}

// rows_pushed_test.go covers the server-pushed paging contract of Rows
// (SONNY-806 / GEO-GO-UP-001, Lane D Task 14b): on a transport that reports
// Transport.PagesArePushed() == true — geode's gRPC surface, which serves
// Execute as a server-streaming RPC and pushes every page until one is final
// (src/transport/grpc_server.zig:1009-1101 handleExecuteStreaming) — the page
// loop must READ the next page rather than ask for it with a PullRequest the
// service has no method to answer.
//
// The whole of GAP-SONNY806-04 lived in one unconditional PULL, so these tests
// assert the absence of that PULL as hard as they assert the rows.
package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

// pushTransport is a mockTransport that pages the way gRPC does: the frames
// queued with push() arrive unasked. Everything else — the recv channel, the
// context handling, the sent-message log — is the QUIC mock's, so a PULL that
// escapes the page loop is still recorded and still countable.
type pushTransport struct {
	*mockTransport
}

func (t *pushTransport) PagesArePushed() bool { return true }

// mockErrorMsg builds an ExecutionResponse carrying an engine error, the frame
// a mid-stream failure arrives as on either wire.
func mockErrorMsg(code, message string) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Error{
					Error: &proto.Error{Code: code, Message: message},
				},
			},
		},
	}
}

// openPushedRows returns a Conn on a push-paging transport and a Rows holding
// first as its (non-final) opening page — the position conn.QueryContext leaves
// a gRPC reader in after the schema frame and the engine's first pushed page.
func openPushedRows(
	ctx context.Context, t *testing.T, first []string,
) (*Conn, driver.Rows, *pushTransport) {
	t.Helper()
	pt := &pushTransport{mockTransport: newMockTransport()}
	c := newConn(&Config{PageSize: 100}, pt)
	pt.push(mockSchemaMsg([]string{"i"}))
	pt.push(mockPageMsgFinal(first, false))

	rows, err := c.QueryContext(ctx, "MATCH (n:X) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	return c, rows, pt
}

// drainStrings reads rows to EOF and returns the string column.
func drainStrings(t *testing.T, rows driver.Rows) []string {
	t.Helper()
	dest := make([]driver.Value, 1)
	got := make([]string, 0, 8)
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		s, ok := dest[0].(string)
		if !ok {
			t.Fatalf("dest[0] = %T, want string", dest[0])
		}
		got = append(got, s)
	}
}

// TestRows_PushedPages_ConsumedWithoutPull is the Task 14b regression test: a
// three-page pushed result must arrive whole, in order, with NO PullRequest on
// the wire. The page sizes cover the edges the engine really produces — an
// empty non-final page, a one-row page and a multi-row final page — because
// each one takes a different branch of Next's buffered/final/fetch loop.
func TestRows_PushedPages_ConsumedWithoutPull(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, pt := openPushedRows(ctx, t, []string{"a", "b"})
	pt.push(mockPageMsgFinal(nil, false))               // 0 rows, more to come
	pt.push(mockPageMsgFinal([]string{"c"}, false))     // 1 row
	pt.push(mockPageMsgFinal([]string{"d", "e"}, true)) // 2 rows, final

	got := drainStrings(t, rows)
	if strings.Join(got, ",") != "a,b,c,d,e" {
		t.Fatalf("rows = %v, want [a b c d e]: a pushed page was dropped or reordered", got)
	}
	if n := countPullMessages(pt.mockTransport); n != 0 {
		t.Fatalf("%d PULL(s) sent on a push-paging transport, want 0: "+
			"gRPC has no Pull method to answer them (GAP-SONNY806-04)", n)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after a fully-read pushed result")
	}
}

// TestRows_PushedPages_FinalFlagStopsTheRead proves the reader stops on
// DataPage.final rather than on the stream running dry. A frame queued after
// the final page must still be sitting there afterwards: reading it would be a
// desync that hands the NEXT statement this result's rows.
func TestRows_PushedPages_FinalFlagStopsTheRead(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, pt := openPushedRows(ctx, t, []string{"a"})
	pt.push(mockPageMsgFinal([]string{"b"}, true))
	// Queued AFTER the final page: nothing may consume it.
	pt.push(mockPageMsgFinal([]string{"ghost"}, true))

	got := drainStrings(t, rows)
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("rows = %v, want [a b]: the reader ran past the final page", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after a fully-read pushed result")
	}
}

// TestRows_PushedPages_ErrorMidStream proves an engine error arriving as a
// pushed frame is reported as a typed DriverError, not swallowed as an end of
// results. handleExecuteStreaming can answer a page request with an error
// response (grpc_server.zig buildStreamDataPageLocked: "02000 No active result
// set", "28000 Unknown session"), and a reader that treated that as EOF would
// hand the caller a short result set as if it were complete.
func TestRows_PushedPages_ErrorMidStream(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, pt := openPushedRows(ctx, t, []string{"a"})
	pt.push(mockErrorMsg("02000", "No active result set"))

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on the buffered row: %v", err)
	}

	err := rows.Next(dest)
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("Next after a pushed error frame = %v (%T), want *DriverError", err, err)
	}
	if de.Code != "02000" {
		t.Fatalf("DriverError.Code = %q, want %q", de.Code, "02000")
	}
	if errors.Is(err, io.EOF) {
		t.Fatal("a mid-stream error frame was reported as EOF: the caller would " +
			"read a truncated result set as a complete one")
	}
	if n := countPullMessages(pt.mockTransport); n != 0 {
		t.Fatalf("%d PULL(s) sent on a push-paging transport, want 0", n)
	}
	_ = rows.Close()
	_ = c.Close()
}

// TestRows_PushedPages_CancelMidStreamRetiresConn carries the Task 9 contract
// onto the pushed path: a cancellation that lands while Next is parked waiting
// for the next pushed page must surface as the wrapped context error (never a
// transport error) and must retire the connection, because the server-streaming
// RPC is abandoned with pages still owed.
func TestRows_PushedPages_CancelMidStreamRetiresConn(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, rows, pt := openPushedRows(ctx, t, []string{"a"})

	// Nothing further is queued, so the read for the next pushed page blocks
	// until this fires.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on the buffered row: %v", err)
	}

	start := time.Now()
	err := rows.Next(dest)
	elapsed := time.Since(start)

	assertCancelError(t, err, context.Canceled)
	if elapsed > 5*time.Second {
		t.Fatalf("Next took %v to observe cancellation while blocked, want prompt", elapsed)
	}
	if n := countPullMessages(pt.mockTransport); n != 0 {
		t.Fatalf("%d PULL(s) sent on a push-paging transport, want 0", n)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.IsValid() {
		t.Fatal("IsValid() is true after cancelling a blocked pushed-page read; " +
			"database/sql would recycle a connection whose Execute RPC was abandoned")
	}
}

// TestTransportsAgreeOnWhoDrivesPaging pins the two real answers Rows depends
// on. Getting either backwards is silent and expensive: a false here caps gRPC
// results at the engine's first page (the GAP-SONNY806-04 defect), and a true
// on QUIC would stop the driver sending the PULL that wire requires.
func TestTransportsAgreeOnWhoDrivesPaging(t *testing.T) {
	if (&QUICTransport{}).PagesArePushed() {
		t.Error("QUICTransport.PagesArePushed() = true, want false: QUIC paging is PULL-driven")
	}

	// grpc.NewClient is lazy — no connection is made here, so this needs no
	// server and no network.
	gt, err := NewGRPCTransport(context.Background(), &Config{
		Host:        "127.0.0.1",
		Port:        "50051",
		Transport:   TransportGRPC,
		TLSDisabled: true,
	})
	if err != nil {
		t.Fatalf("NewGRPCTransport: %v", err)
	}
	defer func() { _ = gt.Close() }()

	if !gt.PagesArePushed() {
		t.Error("GRPCTransport.PagesArePushed() = false, want true: geode 0.8.15 " +
			"serves Execute as a server-streaming RPC and pushes every page")
	}
}

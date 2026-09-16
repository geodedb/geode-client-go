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
	"sync"
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

	cancelMu sync.Mutex
	cancels  int
	finishes int
}

func (t *pushTransport) PagesArePushed() bool { return true }

// CancelActiveStream makes pushTransport a streamCanceller, the way
// GRPCTransport is, and counts the calls. On the real transport this tears
// down the server-streaming Execute RPC with an RST_STREAM the engine
// escalates into a whole-connection teardown; here it only has to be
// observable.
func (t *pushTransport) CancelActiveStream() {
	t.cancelMu.Lock()
	t.cancels++
	t.cancelMu.Unlock()
}

// FinishExecStream makes pushTransport an execStreamFinisher, the way
// GRPCTransport is. On the real transport this half-closes an ANSWERED request
// and drains it to its trailers — no reset, nothing for the engine to
// escalate. Which of the two a Close picks is the whole point of
// TestRows_PushedPages_FullReadFinishesInsteadOfResetting.
func (t *pushTransport) FinishExecStream() {
	t.cancelMu.Lock()
	t.finishes++
	t.cancelMu.Unlock()
}

func (t *pushTransport) cancelCount() int {
	t.cancelMu.Lock()
	defer t.cancelMu.Unlock()
	return t.cancels
}

func (t *pushTransport) finishCount() int {
	t.cancelMu.Lock()
	defer t.cancelMu.Unlock()
	return t.finishes
}

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
	// The ghost frame must still be sitting on the wire. Close runs
	// Conn.releaseRows, whose blocking drain would have swallowed it on a
	// PULL-driven transport; on a pushed-page transport that drain is skipped
	// precisely because anything it read would be a page of somebody's result
	// set, not a stray trailing frame (Conn.drainTrailingFrames).
	if n := len(pt.recv); n != 1 {
		t.Fatalf("%d frame(s) left queued after Close, want 1: the trailing-frame "+
			"drain consumed a pushed page", n)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after a fully-read pushed result")
	}
}

// TestRows_PushedPages_EarlyCloseCancelsTheStream is the unit half of the
// wedge fix. A caller that stops reading a large pushed result before its
// final page — `rows.Close()` after one page, with a perfectly live context —
// leaves the engine's server-streaming Execute pushing pages nobody will read.
// Rows.Close must cancel that RPC.
//
// The connection must nonetheless stay valid: early Close is ordinary caller
// behaviour and the cancel already makes the connection safe to reuse, so
// retiring it would cost an eviction and a re-dial for nothing.
func TestRows_PushedPages_EarlyCloseCancelsTheStream(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, pt := openPushedRows(ctx, t, []string{"a", "b"})
	// Pages the engine is still pushing and that nobody will now read.
	pt.push(mockPageMsgFinal([]string{"c"}, false))
	pt.push(mockPageMsgFinal([]string{"d"}, true))

	// Read one row only, then abandon the result.
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on the first buffered row: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := pt.cancelCount(); n != 1 {
		t.Fatalf("CancelActiveStream called %d time(s) on a non-final Close, want 1: "+
			"the abandoned Execute keeps pushing and parks the engine's push loop on "+
			"the session mutex (grpc_server.zig:1065-1090), wedging the next Execute", n)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after an early Close on a live context: cancelling " +
			"the RPC makes the connection reusable, so it must not be retired")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if n := pt.cancelCount(); n != 1 {
		t.Fatalf("CancelActiveStream called %d time(s) after a second Close, want 1", n)
	}
	if n := pt.finishCount(); n != 0 {
		t.Fatalf("FinishExecStream called %d time(s) on a NON-final Close, want 0: "+
			"draining a result the engine is still pushing means reading pages "+
			"nobody wants for as long as the engine cares to send them — that one "+
			"has to be cancelled (SONNY-806 Task 16e)", n)
	}
}

// TestRows_PushedPages_FullReadFinishesInsteadOfResetting is the other half: a
// result read to its final page must still have its Execute retired on Close —
// but FINISHED, not reset.
//
// Retiring it at all is not redundant, even though the RPC has finished
// sending. Before Task 14b, a final Close left
// GRPCTransport.execCancel/execStream referencing a spent RPC's context with
// nothing scheduled to retire it: the only remaining triggers were the next
// Execute on THIS SAME connection and the transport's own Close. Between this
// Close returning the connection to database/sql's pool and either of those
// firing, that child context sat alive, its fate tied to whatever unrelated
// deadline its parent eventually hit — unordered against whichever statement
// next borrowed the connection (GAP-SONNY806-07, SONNY-1560).
//
// WHICH retirement it gets is Task 16e, and it is not cosmetic. Cancelling
// emits an RST_STREAM, and the engine's vendored grpc-zig turns one stream
// reset into a whole-connection teardown — the next RPC on that connection
// dies with `Unavailable: error reading from server: EOF`, which is exactly
// what failed pipeline 2855806396's gRPC lane. A result that reached its final
// page has nothing left on its stream but the server's end-of-stream, so it is
// finished the way ExecContext finishes a completed statement. Cancelling stays
// for the case that genuinely needs it: a result the engine may still be
// pushing (the test above).
func TestRows_PushedPages_FullReadFinishesInsteadOfResetting(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, pt := openPushedRows(ctx, t, []string{"a"})
	pt.push(mockPageMsgFinal([]string{"b"}, true))

	if got := strings.Join(drainStrings(t, rows), ","); got != "a,b" {
		t.Fatalf("rows = %q, want \"a,b\"", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := pt.finishCount(); n != 1 {
		t.Fatalf("FinishExecStream called %d time(s) after a fully-read result, want 1: "+
			"the finished Execute must be retired on Close, not left to outlive the "+
			"connection's return to the pool (GAP-SONNY806-07, SONNY-1560)", n)
	}
	if n := pt.cancelCount(); n != 0 {
		t.Fatalf("CancelActiveStream called %d time(s) after a fully-read result, want 0: "+
			"a reset on an answered stream is an RST_STREAM the engine escalates into a "+
			"whole-connection teardown (SONNY-806 Task 16e)", n)
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

// TestConn_PushedPages_RefusedExecFinishesItsStream is the unit half of
// TestIntegration_GRPCRefusedExecLeavesNoStreamToReset (SONNY-806 Task 16e,
// F-16d-01).
//
// ExecContext's engine-refusal branch returned the DriverError without
// retiring the request stream at all, so a refused statement — a duplicate
// CREATE INDEX, a constraint violation, a syntax error inside a batch — handed
// a pooled connection back with a live Execute RPC on it, and the statement
// after it reset that RPC. A refusal is a COMPLETE answer: the engine planned
// the statement, rejected it, and will send nothing else, so the stream is
// finished, not cancelled.
func TestConn_PushedPages_RefusedExecFinishesItsStream(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	pt := &pushTransport{mockTransport: newMockTransport()}
	c := newConn(&Config{PageSize: 100}, pt)
	pt.push(mockErrorMsg("42P07", "Duplicate index name"))

	_, err := c.ExecContext(ctx, "CREATE INDEX x_idx ON X(i) USING btree", nil)
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("ExecContext returned %v, want a DriverError refusal", err)
	}
	if de.Code != "42P07" {
		t.Fatalf("DriverError code = %q, want 42P07", de.Code)
	}
	if n := pt.finishCount(); n != 1 {
		t.Fatalf("FinishExecStream called %d time(s) after a refused statement, want 1: "+
			"the refusal is the engine's complete answer, and a stream left live is one "+
			"the next statement resets — which the engine escalates into a "+
			"whole-connection teardown", n)
	}
	if n := pt.cancelCount(); n != 0 {
		t.Fatalf("CancelActiveStream called %d time(s) after a refused statement, want 0", n)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after an ENGINE refusal: the statement was answered, " +
			"the stream is in sync, and the connection must stay poolable")
	}
}

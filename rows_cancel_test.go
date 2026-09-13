// rows_cancel_test.go covers the streaming-cancellation contract of Rows
// (SONNY-806 / GEO-GO-UP-001, Lane D Task 9) against the in-process
// mockTransport, so the two timing-sensitive halves — cancelling before a PULL
// and cancelling while blocked receiving a page — are exercised
// deterministically rather than raced against a real server.
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

// mockPageMsgFinal builds a single-column DataPage with an explicit final flag
// (mockPageMsg always marks the page final, which would end paging).
func mockPageMsgFinal(values []string, final bool) *proto.QuicServerMessage {
	rows := make([]*proto.Row, len(values))
	for i, v := range values {
		rows[i] = &proto.Row{Values: []*proto.Value{{
			Kind: &proto.Value_StringVal{StringVal: &proto.StringValue{Value: v}},
		}}}
	}
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Page{
					Page: &proto.DataPage{Rows: rows, Final: final},
				},
			},
		},
	}
}

// openMidStreamRows returns a Conn and a Rows positioned mid-result: the first
// page is buffered and final is false, so the next exhausted read issues a PULL.
func openMidStreamRows(ctx context.Context, t *testing.T, first []string) (*Conn, driver.Rows, *mockTransport) {
	t.Helper()
	m := newMockTransport()
	c := newTestConn(m)
	m.push(mockSchemaMsg([]string{"i"}))
	m.push(mockPageMsgFinal(first, false))

	rows, err := c.QueryContext(ctx, "MATCH (n:X) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	return c, rows, m
}

// assertCancelError checks the driver reported the context error, wrapped in a
// geode-prefixed message rather than replaced by a transport error.
func assertCancelError(t *testing.T, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %v, got nil", want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v (%T), want errors.Is(..., %v)", err, err, want)
	}
	var te *TransportError
	if errors.As(err, &te) {
		t.Fatalf("err = %v, want the context error, not a transport error", err)
	}
	if !strings.Contains(err.Error(), "geode:") {
		t.Fatalf("err = %q, want a geode-prefixed message", err.Error())
	}
}

// countPullMessages returns how many PULL requests reached the transport.
func countPullMessages(m *mockTransport) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, msg := range m.sent {
		if msg.GetPull() != nil {
			n++
		}
	}
	return n
}

// TestRows_Next_CancelledBeforePull proves Next observes cancellation before
// putting another PULL on the wire, even while it still holds buffered rows.
func TestRows_Next_CancelledBeforePull(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithCancel(context.Background())

	c, rows, m := openMidStreamRows(ctx, t, []string{"a", "b", "c"})

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next before cancel: %v", err)
	}

	cancel()

	err := rows.Next(dest)
	assertCancelError(t, err, context.Canceled)

	if n := countPullMessages(m); n != 0 {
		t.Fatalf("%d PULL(s) sent after cancellation, want 0", n)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.IsValid() {
		t.Fatal("IsValid() is true after a cancelled mid-stream Rows; " +
			"database/sql would recycle a desynchronised connection")
	}
}

// TestRows_Next_CancelledWhileBlockedOnPage proves a cancellation that lands
// while Next is parked in ReceiveProto is reported as the context error, not as
// the transport read error the cancellation manifests as on the wire.
func TestRows_Next_CancelledWhileBlockedOnPage(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, rows, m := openMidStreamRows(ctx, t, []string{"a"})

	// Nothing is queued for the next page, so the PULL's ReceiveProto blocks
	// until this fires.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on buffered row: %v", err)
	}

	start := time.Now()
	err := rows.Next(dest)
	elapsed := time.Since(start)

	assertCancelError(t, err, context.Canceled)
	if elapsed > 5*time.Second {
		t.Fatalf("Next took %v to observe cancellation while blocked, want prompt", elapsed)
	}
	if n := countPullMessages(m); n != 1 {
		t.Fatalf("%d PULL(s) sent, want exactly 1 (the one that blocked)", n)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.IsValid() {
		t.Fatal("IsValid() is true after cancelling a blocked page read")
	}
}

// TestRows_Next_CancelledByDeadline proves the same handling for a deadline,
// which surfaces context.DeadlineExceeded rather than context.Canceled.
func TestRows_Next_CancelledByDeadline(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	c, rows, _ := openMidStreamRows(ctx, t, []string{"a"})

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("Next on buffered row: %v", err)
	}

	assertCancelError(t, rows.Next(dest), context.DeadlineExceeded)

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.IsValid() {
		t.Fatal("IsValid() is true after a deadline expired mid-stream")
	}
}

// TestRows_Close_CancelledMidStreamRetiresConn covers the path where the caller
// never calls Next again after cancelling — database/sql's own context watcher
// closes the Rows directly. The abandoned stream must still retire the conn.
func TestRows_Close_CancelledMidStreamRetiresConn(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithCancel(context.Background())

	c, rows, _ := openMidStreamRows(ctx, t, []string{"a", "b"})
	cancel()

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.IsValid() {
		t.Fatal("IsValid() is true after closing an abandoned mid-stream Rows")
	}
}

// TestRows_Close_CancelledAfterFinalPageKeepsConn is the other half of the
// contract: a result whose final page already arrived owes the stream nothing,
// so a context cancelled afterwards must not cost the pool a connection.
func TestRows_Close_CancelledAfterFinalPageKeepsConn(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithCancel(context.Background())

	m := newMockTransport()
	c := newTestConn(m)
	m.push(mockSchemaMsg([]string{"i"}))
	m.push(mockPageMsgFinal([]string{"a", "b"}, true))

	rows, err := c.QueryContext(ctx, "MATCH (n:X) RETURN n.i", nil)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}

	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Next: %v", err)
		}
	}

	cancel()

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after a fully-read result; " +
			"cancelling a finished Rows must not retire a clean connection")
	}
}

// TestRows_Next_UncancelledPagingUnaffected is the regression guard: the cancel
// checks must not disturb ordinary multi-page reads.
func TestRows_Next_UncancelledPagingUnaffected(t *testing.T) {
	fastDrainTimeout(t)
	ctx := context.Background()

	c, rows, m := openMidStreamRows(ctx, t, []string{"a", "b"})
	m.push(mockPageMsgFinal([]string{"c", "d"}, true))

	dest := make([]driver.Value, 1)
	got := make([]string, 0, 4)
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			break
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

	if strings.Join(got, ",") != "a,b,c,d" {
		t.Fatalf("rows = %v, want [a b c d]", got)
	}
	if n := countPullMessages(m); n != 1 {
		t.Fatalf("%d PULL(s) sent, want 1", n)
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("IsValid() is false after an ordinary fully-paged read")
	}
}

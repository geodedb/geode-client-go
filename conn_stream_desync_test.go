package geode

// Tests for GAP-0046 / BUG-0001: QUIC stream desynchronization on sequential queries.
//
// Root cause: releaseRows() used TryReceiveProtoBuffered() (non-blocking) to drain
// trailing server frames (metrics, heartbeat). Frames still in the QUIC socket but
// not yet in the 64KB bufio.Reader buffer were invisible to that drain. The next
// query's ReceiveProto call then read the stale trailing frame as its response,
// causing wrong columns or a transport error.
//
// Fix: after the non-blocking drain, perform a short blocking drain with a
// deadline of trailingFrameDrainTimeout (default 50 ms) to consume any
// in-flight trailing frames before releasing the connection lock.

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

// ─────────────────────────────────────────────────────────────────────────────
// mockTransport
// ─────────────────────────────────────────────────────────────────────────────

// mockTransport is a controllable in-process Transport for unit tests.
//
// Messages are delivered via two distinct channels:
//   - recv   → ReceiveProto() (blocking reads, simulates QUIC socket)
//   - buf    → TryReceiveProtoBuffered() (non-blocking, simulates bufio.Reader)
//
// This lets each test precisely control which frames are "already buffered"
// vs. still "in flight" at the moment releaseRows() runs its drain.
type mockTransport struct {
	mu     sync.Mutex
	closed bool
	addr   string

	// recv feeds ReceiveProto. Push here to simulate arriving frames that are
	// NOT yet in the bufio.Reader (i.e., still in the QUIC socket).
	recv chan *proto.QuicServerMessage

	// buf feeds TryReceiveProtoBuffered. Push here to simulate frames that
	// are already held in the 64 KB bufio.Reader buffer.
	buf chan *proto.QuicServerMessage

	// sent records every message passed to SendProto.
	sent []*proto.QuicClientMessage
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		recv: make(chan *proto.QuicServerMessage, 64),
		buf:  make(chan *proto.QuicServerMessage, 64),
		addr: "mock://test",
	}
}

// push enqueues msg for the next ReceiveProto (blocking) call.
func (m *mockTransport) push(msg *proto.QuicServerMessage) { m.recv <- msg }

// pushBuffered enqueues msg for the next TryReceiveProtoBuffered (non-blocking) call.
func (m *mockTransport) pushBuffered(msg *proto.QuicServerMessage) { m.buf <- msg }

func (m *mockTransport) SendProto(_ context.Context, msg *proto.QuicClientMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockTransport) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-m.recv:
		return msg, nil
	}
}

func (m *mockTransport) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, false, ErrClosed
	}
	m.mu.Unlock()

	select {
	case msg := <-m.buf:
		return msg, true, nil
	default:
		return nil, false, nil
	}
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	// Drain channels so blocked receivers unblock.
	for len(m.recv) > 0 {
		<-m.recv
	}
	for len(m.buf) > 0 {
		<-m.buf
	}
	return nil
}

func (m *mockTransport) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockTransport) Addr() string { return m.addr }

// ─────────────────────────────────────────────────────────────────────────────
// Proto message builders
// ─────────────────────────────────────────────────────────────────────────────

func mockSchemaMsg(cols []string) *proto.QuicServerMessage {
	defs := make([]*proto.ColumnDefinition, len(cols))
	for i, name := range cols {
		defs[i] = &proto.ColumnDefinition{Name: name}
	}
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Schema{
					Schema: &proto.SchemaDefinition{Columns: defs},
				},
			},
		},
	}
}

func mockPageMsg(rows [][]string) *proto.QuicServerMessage {
	protoRows := make([]*proto.Row, len(rows))
	for i, row := range rows {
		values := make([]*proto.Value, len(row))
		for j, v := range row {
			values[j] = &proto.Value{
				Kind: &proto.Value_StringVal{
					StringVal: &proto.StringValue{Value: v},
				},
			}
		}
		protoRows[i] = &proto.Row{Values: values}
	}
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Page{
					Page: &proto.DataPage{Rows: protoRows, Final: true},
				},
			},
		},
	}
}

func mockMetricsMsg() *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Metrics{
					Metrics: &proto.ExecutionMetrics{},
				},
			},
		},
	}
}

func mockHeartbeatMsg() *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Heartbeat{
					Heartbeat: &proto.Heartbeat{},
				},
			},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// newTestConn returns a Conn backed by a fresh mockTransport.
func newTestConn(transport *mockTransport) *Conn {
	return newConn(&Config{PageSize: 100}, transport)
}

// fastDrainTimeout overrides trailingFrameDrainTimeout for the duration of a
// test so the blocking drain's deadline fires quickly in the "no trailing
// frame" case, keeping the test suite fast.
func fastDrainTimeout(t *testing.T) {
	t.Helper()
	orig := trailingFrameDrainTimeout
	trailingFrameDrainTimeout = 2 * time.Millisecond
	t.Cleanup(func() { trailingFrameDrainTimeout = orig })
}

// consumeAllRows calls Next until EOF or error.
func consumeAllRows(t *testing.T, rows driver.Rows, ncols int) int {
	t.Helper()
	dest := make([]driver.Value, ncols)
	count := 0
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next() error: %v", err)
		}
		count++
	}
	return count
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// TestConn_ReleaseRows_DrainsMissedTrailingMetrics is the primary reproduction
// for GAP-0046 / BUG-0001.
//
// Sequence:
//  1. Query 1: schema + final page arrive via blocking read (recv).
//  2. All rows consumed; r.final == true.
//  3. A metrics frame is pushed to recv — simulating it arriving after the
//     final page but before the next query, i.e., still in the QUIC socket
//     when TryReceiveProtoBuffered runs its non-blocking drain.
//  4. rows.Close() is called.
//     Bug:  TryReceiveProtoBuffered sees nothing in buf → exits immediately.
//     Metrics frame stays in recv.
//     Fix:  Blocking drain reads and discards the metrics frame.
//  5. Query 2 responses are pushed to recv.
//  6. QueryContext for query 2 must receive its own schema (not the stale
//     metrics frame) and return the correct column names.
func TestConn_ReleaseRows_DrainsMissedTrailingMetrics(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	// ── Query 1 ───────────────────────────────────────────────────────────
	transport.push(mockSchemaMsg([]string{"id"}))
	transport.push(mockPageMsg([][]string{{"row1"}}))

	rows1, err := conn.QueryContext(ctx, "RETURN 1 AS id", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}

	got1 := consumeAllRows(t, rows1, 1)
	if got1 != 1 {
		t.Fatalf("query 1: expected 1 row, got %d", got1)
	}

	// Push trailing metrics AFTER final page — this simulates the frame
	// arriving in the QUIC socket but not yet in the bufio.Reader buffer.
	// TryReceiveProtoBuffered (non-blocking) will miss it; only a blocking
	// ReceiveProto call in the fix will catch it.
	transport.push(mockMetricsMsg())

	// Close rows. With the fix, this must drain the metrics frame.
	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close(): %v", err)
	}

	// ── Query 2 ───────────────────────────────────────────────────────────
	// Push after Close() so the blocking drain cannot accidentally consume
	// query 2's schema (the server only sends schema after EXECUTE).
	transport.push(mockSchemaMsg([]string{"id"}))
	transport.push(mockPageMsg([][]string{{"row2"}}))

	rows2, err := conn.QueryContext(ctx, "RETURN 2 AS id", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	// With the bug: rows2 received the metrics frame as its first response.
	// execResp.GetSchema() == nil → no columns → Columns() returns [].
	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("stream desync: query 2 columns = %v, want [id]", cols)
	}
}

// TestConn_ReleaseRows_DrainsMissedTrailingHeartbeat mirrors the metrics test
// but with a heartbeat frame — both must be drained by the fix.
func TestConn_ReleaseRows_DrainsMissedTrailingHeartbeat(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	transport.push(mockSchemaMsg([]string{"val"}))
	transport.push(mockPageMsg([][]string{{"a"}}))

	rows1, err := conn.QueryContext(ctx, "RETURN 'a' AS val", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	consumeAllRows(t, rows1, 1)

	transport.push(mockHeartbeatMsg()) // trailing heartbeat, not yet buffered

	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close(): %v", err)
	}

	transport.push(mockSchemaMsg([]string{"val"}))
	transport.push(mockPageMsg([][]string{{"b"}}))

	rows2, err := conn.QueryContext(ctx, "RETURN 'b' AS val", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "val" {
		t.Errorf("stream desync (heartbeat): query 2 columns = %v, want [val]", cols)
	}
}

// TestConn_ReleaseRows_MultipleTrailingFrames ensures that multiple trailing
// frames (e.g., metrics followed by heartbeat) are all consumed.
func TestConn_ReleaseRows_MultipleTrailingFrames(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	transport.push(mockSchemaMsg([]string{"x"}))
	transport.push(mockPageMsg([][]string{{"1"}}))

	rows1, err := conn.QueryContext(ctx, "RETURN 1 AS x", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	consumeAllRows(t, rows1, 1)

	// Two trailing frames in-flight.
	transport.push(mockMetricsMsg())
	transport.push(mockHeartbeatMsg())

	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close(): %v", err)
	}

	transport.push(mockSchemaMsg([]string{"x"}))
	transport.push(mockPageMsg([][]string{{"2"}}))

	rows2, err := conn.QueryContext(ctx, "RETURN 2 AS x", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "x" {
		t.Errorf("stream desync (multiple frames): query 2 columns = %v, want [x]", cols)
	}
}

// TestConn_ReleaseRows_TrailingFrameAlreadyBuffered verifies the happy path:
// when the trailing metrics frame IS already in the bufio buffer, the existing
// TryReceiveProtoBuffered drain handles it without needing the blocking drain.
func TestConn_ReleaseRows_TrailingFrameAlreadyBuffered(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	transport.push(mockSchemaMsg([]string{"n"}))
	transport.push(mockPageMsg([][]string{{"42"}}))

	rows1, err := conn.QueryContext(ctx, "RETURN 42 AS n", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	consumeAllRows(t, rows1, 1)

	// Push metrics to the BUFFERED channel — it is already "in the bufio.Reader".
	transport.pushBuffered(mockMetricsMsg())

	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close(): %v", err)
	}

	transport.push(mockSchemaMsg([]string{"n"}))
	transport.push(mockPageMsg([][]string{{"99"}}))

	rows2, err := conn.QueryContext(ctx, "RETURN 99 AS n", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "n" {
		t.Errorf("buffered drain broken: query 2 columns = %v, want [n]", cols)
	}
}

// TestConn_SequentialQueries_NoDesync runs 20 sequential queries on a single
// connection, each followed by a trailing metrics frame that arrives
// "out of the bufio buffer". All queries must succeed with correct columns.
//
// This is the driver-level analogue of the reproduction script in
// docs/BUG_STREAM_DESYNC.md.
func TestConn_SequentialQueries_NoDesync(t *testing.T) {
	fastDrainTimeout(t)

	const iterations = 20

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	for i := range iterations {
		// Push this query's responses.
		transport.push(mockSchemaMsg([]string{"id", "label"}))
		transport.push(mockPageMsg([][]string{{"id1", "labelA"}, {"id2", "labelB"}}))

		rows, err := conn.QueryContext(ctx, "MATCH (n) RETURN n.id, n.label", nil)
		if err != nil {
			t.Fatalf("iteration %d: QueryContext failed: %v", i, err)
		}

		got := consumeAllRows(t, rows, 2)
		if got != 2 {
			t.Errorf("iteration %d: expected 2 rows, got %d", i, got)
		}

		// Trailing metrics arrive after the final page — simulating in-flight delivery.
		transport.push(mockMetricsMsg())

		// Close must drain the metrics frame so the next QueryContext is clean.
		if err := rows.Close(); err != nil {
			t.Fatalf("iteration %d: rows.Close() failed: %v", i, err)
		}
	}
}

// TestConn_QueryRowThenQuery_NoDesync reproduces the GetFollowers() pattern
// from the divelife-api: a single-row query (count) immediately followed by a
// multi-row query on the same connection. With the bug, the second query
// receives the count query's trailing metrics frame as its schema response.
func TestConn_QueryRowThenQuery_NoDesync(t *testing.T) {
	fastDrainTimeout(t)

	const iterations = 15

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	for i := range iterations {
		// ── Query A: "SELECT count(*)" — single scalar result ─────────────
		transport.push(mockSchemaMsg([]string{"total"}))
		transport.push(mockPageMsg([][]string{{"42"}}))

		rowsA, err := conn.QueryContext(ctx, "MATCH (n) RETURN count(n)", nil)
		if err != nil {
			t.Fatalf("iteration %d: count query failed: %v", i, err)
		}

		dest := make([]driver.Value, 1)
		if err := rowsA.Next(dest); err != nil {
			t.Fatalf("iteration %d: count Next() failed: %v", i, err)
		}
		// Consume to EOF.
		_ = rowsA.Next(dest)

		// Trailing metrics for the count query — arrives after final page.
		transport.push(mockMetricsMsg())

		if err := rowsA.Close(); err != nil {
			t.Fatalf("iteration %d: rowsA.Close() failed: %v", i, err)
		}

		// ── Query B: "SELECT users" — multi-column result ─────────────────
		transport.push(mockSchemaMsg([]string{"id", "name", "handle"}))
		transport.push(mockPageMsg([][]string{
			{"u1", "Alice", "@alice"},
			{"u2", "Bob", "@bob"},
		}))

		rowsB, err := conn.QueryContext(ctx, "MATCH (u:User) RETURN u.id, u.name, u.handle", nil)
		if err != nil {
			t.Fatalf("iteration %d: fetch query failed: %v", i, err)
		}

		cols := rowsB.Columns()
		if len(cols) != 3 || cols[0] != "id" || cols[1] != "name" || cols[2] != "handle" {
			t.Errorf("iteration %d: stream desync — query B columns = %v, want [id name handle]", i, cols)
		}

		gotB := consumeAllRows(t, rowsB, 3)
		if gotB != 2 {
			t.Errorf("iteration %d: expected 2 rows from query B, got %d", i, gotB)
		}

		if err := rowsB.Close(); err != nil {
			t.Fatalf("iteration %d: rowsB.Close() failed: %v", i, err)
		}
	}
}

// TestConn_ReleaseRows_NoTrailingFrames verifies baseline behavior when the
// server sends no trailing frames — the drain timeout fires quickly and the
// connection returns to idle without error.
func TestConn_ReleaseRows_NoTrailingFrames(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	for range 5 {
		transport.push(mockSchemaMsg([]string{"v"}))
		transport.push(mockPageMsg([][]string{{"hello"}}))

		rows, err := conn.QueryContext(ctx, "RETURN 'hello' AS v", nil)
		if err != nil {
			t.Fatalf("QueryContext failed: %v", err)
		}
		consumeAllRows(t, rows, 1)

		// No trailing frames — blocking drain fires its deadline and exits.
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close() failed: %v", err)
		}
	}
}

// TestConn_ReleaseRows_EmptyResultSet verifies that a query returning zero
// rows also drains correctly and doesn't leave the stream in an inconsistent
// state.
func TestConn_ReleaseRows_EmptyResultSet(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	// Query 1 returns an empty page (0 rows, final=true).
	transport.push(mockSchemaMsg([]string{"id"}))
	transport.push(mockPageMsg(nil))

	rows1, err := conn.QueryContext(ctx, "MATCH (n:Missing) RETURN n.id", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	got1 := consumeAllRows(t, rows1, 1)
	if got1 != 0 {
		t.Errorf("expected 0 rows, got %d", got1)
	}

	transport.push(mockMetricsMsg()) // trailing metric after empty result

	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close(): %v", err)
	}

	// Query 2 must still work correctly.
	transport.push(mockSchemaMsg([]string{"id"}))
	transport.push(mockPageMsg([][]string{{"found"}}))

	rows2, err := conn.QueryContext(ctx, "MATCH (n) RETURN n.id", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("stream desync after empty result: columns = %v, want [id]", cols)
	}
}

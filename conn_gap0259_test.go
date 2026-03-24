package geode

// Tests for GAP-0259: Go driver connection corruption under rapid sequential queries.
//
// Three sub-bugs:
//   Bug B/C: ResetSession() and IsValid() don't detect poisoned connections
//            (state=StateFetching, activeRows!=nil). database/sql returns these
//            connections to the pool and reuses them, causing stream desync.
//   Bug D:   releaseRows() only drains metrics/heartbeat frames; un-consumed
//            data page frames are left on the wire, poisoning the next query.
//
// All tests are designed to FAIL before the fix and PASS after.

import (
	"context"
	"database/sql/driver"
	"testing"

	"geodedb.com/geode/proto"
)

// ---------------------------------------------------------------------------
// Bug B/C: ResetSession does not detect poisoned connections
// ---------------------------------------------------------------------------

// TestGAP0259_ResetSession_DetectsFetchingState verifies that ResetSession
// returns driver.ErrBadConn when the connection state is StateFetching.
//
// Before fix: ResetSession only checks StateClosed; it ignores StateFetching
// and blindly resets to StateIdle, allowing database/sql to reuse a poisoned
// connection.
//
// After fix: ResetSession must detect StateFetching and return driver.ErrBadConn
// so database/sql discards the connection.
func TestGAP0259_ResetSession_DetectsFetchingState(t *testing.T) {
	transport := newMockTransport()
	conn := newTestConn(transport)

	// Simulate a connection that is mid-fetch (e.g., rows were opened but
	// the caller lost the reference without calling Close).
	conn.mu.Lock()
	conn.state = StateFetching
	conn.mu.Unlock()

	err := conn.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("GAP-0259 Bug B/C regression: ResetSession() with state=StateFetching "+
			"returned %v, want driver.ErrBadConn", err)
	}
}

// TestGAP0259_ResetSession_DetectsActiveRows verifies that ResetSession
// returns driver.ErrBadConn when activeRows is non-nil, even if state is
// StateIdle (which can happen after a partial state reset).
//
// Before fix: ResetSession does not check activeRows at all.
// After fix: ResetSession must detect activeRows != nil and return driver.ErrBadConn.
func TestGAP0259_ResetSession_DetectsActiveRows(t *testing.T) {
	transport := newMockTransport()
	conn := newTestConn(transport)

	// Simulate a connection with an active rows object that was never closed.
	// Use a minimal Rows struct -- we only need it to be non-nil.
	conn.mu.Lock()
	conn.activeRows = &Rows{conn: conn}
	conn.mu.Unlock()

	err := conn.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Errorf("GAP-0259 Bug B/C regression: ResetSession() with activeRows!=nil "+
			"returned %v, want driver.ErrBadConn", err)
	}
}

// ---------------------------------------------------------------------------
// Bug B/C: IsValid does not detect poisoned connections
// ---------------------------------------------------------------------------

// TestGAP0259_IsValid_ReturnsFalseForFetchingState verifies that IsValid
// returns false when the connection state is StateFetching.
//
// Before fix: IsValid only checks StateClosed and StateInError; it returns
// true for StateFetching, telling database/sql the connection is reusable.
// After fix: IsValid must return false for StateFetching.
func TestGAP0259_IsValid_ReturnsFalseForFetchingState(t *testing.T) {
	transport := newMockTransport()
	conn := newTestConn(transport)

	conn.mu.Lock()
	conn.state = StateFetching
	conn.mu.Unlock()

	if conn.IsValid() {
		t.Error("GAP-0259 Bug B/C regression: IsValid() returned true for " +
			"state=StateFetching, want false")
	}
}

// TestGAP0259_IsValid_ReturnsFalseForActiveRows verifies that IsValid returns
// false when activeRows is non-nil, regardless of the nominal state.
//
// Before fix: IsValid does not check activeRows at all.
// After fix: IsValid must return false when activeRows != nil.
func TestGAP0259_IsValid_ReturnsFalseForActiveRows(t *testing.T) {
	transport := newMockTransport()
	conn := newTestConn(transport)

	conn.mu.Lock()
	conn.activeRows = &Rows{conn: conn}
	// Leave state as StateIdle to show that state alone is insufficient.
	conn.mu.Unlock()

	if conn.IsValid() {
		t.Error("GAP-0259 Bug B/C regression: IsValid() returned true with " +
			"activeRows!=nil, want false")
	}
}

// ---------------------------------------------------------------------------
// Bug D: releaseRows does not drain un-consumed data page frames
// ---------------------------------------------------------------------------

// TestGAP0259_ReleaseRows_DrainsDataPages verifies that releaseRows drains
// remaining data page frames from the transport when the caller closes rows
// before consuming all pages (non-final pages still on the wire).
//
// Before fix: releaseRows only drains metrics and heartbeat frames. If there
// are unconsumed data page frames, they remain on the transport and are read
// as the response to the next query, causing stream desynchronization.
//
// After fix: releaseRows must also drain data page frames (continuing until
// it sees a final page or the drain timeout expires).
func TestGAP0259_ReleaseRows_DrainsDataPages(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	// Query 1: server sends schema + a non-final data page.
	// We will close rows WITHOUT consuming all pages, leaving a second
	// data page (final) and a trailing metrics frame on the wire.
	transport.push(mockSchemaMsg([]string{"id"}))
	transport.push(mockNonFinalPageMsg([][]string{{"row1"}}))

	rows1, err := conn.QueryContext(ctx, "MATCH (n) RETURN n.id", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}

	// Consume the first page.
	dest := make([]driver.Value, 1)
	if err := rows1.Next(dest); err != nil {
		t.Fatalf("rows1.Next() failed: %v", err)
	}

	// Server sends remaining frames: another data page (final) + metrics.
	// These are "in the QUIC socket" (push to recv, not buf) so
	// TryReceiveProtoBuffered won't see them -- only the blocking drain can.
	transport.push(mockPageMsg([][]string{{"row2"}})) // final=true data page
	transport.push(mockMetricsMsg())                  // trailing metrics

	// Close rows early -- releaseRows must drain the final page AND metrics.
	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close() failed: %v", err)
	}

	// Query 2: push its own responses AFTER Close so they can't be
	// accidentally consumed by the drain.
	transport.push(mockSchemaMsg([]string{"name"}))
	transport.push(mockPageMsg([][]string{{"Alice"}}))

	rows2, err := conn.QueryContext(ctx, "MATCH (u:User) RETURN u.name", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	// With the bug: query 2 receives the leftover data page as its schema
	// response, causing a desync. The columns will be wrong or empty.
	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "name" {
		t.Errorf("GAP-0259 Bug D regression: query 2 columns = %v, want [name]", cols)
	}

	// Verify query 2 returns the correct data.
	dest2 := make([]driver.Value, 1)
	if err := rows2.Next(dest2); err != nil {
		t.Fatalf("rows2.Next() failed: %v", err)
	}
	if dest2[0] != "Alice" {
		t.Errorf("GAP-0259 Bug D regression: query 2 row value = %v, want Alice", dest2[0])
	}
}

// TestGAP0259_ReleaseRows_DrainsMultipleDataPages verifies that releaseRows
// drains multiple unconsumed data pages (not just one) when rows are closed
// early in a multi-page result set.
func TestGAP0259_ReleaseRows_DrainsMultipleDataPages(t *testing.T) {
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	// Query 1: schema + non-final page (consumed), then 2 more pages + metrics
	// on the wire that must all be drained.
	transport.push(mockSchemaMsg([]string{"x"}))
	transport.push(mockNonFinalPageMsg([][]string{{"1"}}))

	rows1, err := conn.QueryContext(ctx, "MATCH (n) RETURN n.x", nil)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}

	// Consume the first page.
	dest := make([]driver.Value, 1)
	if err := rows1.Next(dest); err != nil {
		t.Fatalf("rows1.Next() failed: %v", err)
	}

	// Server sends: non-final page, final page, metrics -- all unconsumed.
	transport.push(mockNonFinalPageMsg([][]string{{"2"}}))
	transport.push(mockPageMsg([][]string{{"3"}})) // final=true
	transport.push(mockMetricsMsg())

	if err := rows1.Close(); err != nil {
		t.Fatalf("rows1.Close() failed: %v", err)
	}

	// Query 2 must work cleanly.
	transport.push(mockSchemaMsg([]string{"y"}))
	transport.push(mockPageMsg([][]string{{"ok"}}))

	rows2, err := conn.QueryContext(ctx, "RETURN 'ok' AS y", nil)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	defer rows2.Close()

	cols := rows2.Columns()
	if len(cols) != 1 || cols[0] != "y" {
		t.Errorf("GAP-0259 Bug D regression: query 2 columns = %v, want [y]", cols)
	}
}

// ---------------------------------------------------------------------------
// Helpers specific to GAP-0259
// ---------------------------------------------------------------------------

// mockNonFinalPageMsg creates a DataPage message with Final=false, simulating
// a server that has more pages to send.
func mockNonFinalPageMsg(rows [][]string) *proto.QuicServerMessage {
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
					Page: &proto.DataPage{Rows: protoRows, Final: false},
				},
			},
		},
	}
}

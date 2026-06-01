package geode

// Regression tests for the "no-match null row" bug.
//
// Bug: When a Geode MATCH query finds no node/edge/path with the given ID, the
// server emits a DataPage containing a single row whose typed reference column
// (NODE/EDGE/PATH) holds a NullValue, instead of an empty page. The driver
// passed this null-sentinel row through to the caller as a real row. Callers
// doing db.QueryRow(...).Scan(&dst) then hit a scan error (assigning nil to a
// non-nullable type) rather than sql.ErrNoRows, turning a logical 404 into a
// 500.
//
// Fix point: rows.go Next() must skip a row whose typed reference column
// (schema type NODE, EDGE, or PATH) carries a null value and return io.EOF,
// which database/sql translates to sql.ErrNoRows.
//
// Guardrail: rows whose null column is NOT a NODE/EDGE/PATH reference (scalar
// null, RETURN null, missing property, UNKNOWN type) must still be returned
// as a genuine null row — those are legitimate results, not no-match
// sentinels.

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"geodedb.com/geode/proto"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// mockSchemaMsgWithType builds a schema with a single column carrying both a
// name and a GQL type string (e.g. "NODE", "EDGE", "PATH", "STRING").
func mockSchemaMsgWithType(name, typeName string) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Schema{
					Schema: &proto.SchemaDefinition{
						Columns: []*proto.ColumnDefinition{
							{Name: name, Type: typeName},
						},
					},
				},
			},
		},
	}
}

// mockNullRowPageMsg builds a final DataPage with exactly one null-valued row —
// the no-match sentinel the server emits for a single-column schema.
func mockNullRowPageMsg() *proto.QuicServerMessage {
	values := []*proto.Value{
		{Kind: &proto.Value_NullVal{NullVal: &proto.NullValue{}}},
	}
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{
			Execute: &proto.ExecutionResponse{
				Payload: &proto.ExecutionResponse_Page{
					Page: &proto.DataPage{
						Rows:  []*proto.Row{{Values: values}},
						Final: true,
					},
				},
			},
		},
	}
}

// queryFirstNext runs a query and returns the result of the first Next() call
// along with the destination slice it populated.
func queryFirstNext(t *testing.T, schema, page *proto.QuicServerMessage, query string) ([]driver.Value, error) {
	t.Helper()
	fastDrainTimeout(t)

	transport := newMockTransport()
	conn := newTestConn(transport)
	ctx := context.Background()

	transport.push(schema)
	transport.push(page)

	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	t.Cleanup(func() { _ = rows.Close() })

	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) == 0 {
		dest = make([]driver.Value, 1)
	}
	return dest, rows.Next(dest)
}

// ─────────────────────────────────────────────────────────────────────────────
// PRIMARY reproduction tests — these MUST fail before the fix.
// ─────────────────────────────────────────────────────────────────────────────

// TestRows_NoMatch_NodeColumn_NullValue is the primary reproduction: a NODE
// column carrying a null value is a no-match sentinel and must surface as EOF
// (→ sql.ErrNoRows), not as a real null row.
func TestRows_NoMatch_NodeColumn_NullValue(t *testing.T) {
	dest, err := queryFirstNext(t,
		mockSchemaMsgWithType("n", "NODE"),
		mockNullRowPageMsg(),
		"MATCH (n {id: 'missing'}) RETURN n",
	)
	if err != io.EOF {
		t.Errorf("no-match row returned instead of io.EOF: got err=%v dest=%v", err, dest[0])
	}
}

// TestRows_NoMatch_EdgeColumn_NullValue mirrors the NODE case for EDGE columns.
func TestRows_NoMatch_EdgeColumn_NullValue(t *testing.T) {
	dest, err := queryFirstNext(t,
		mockSchemaMsgWithType("e", "EDGE"),
		mockNullRowPageMsg(),
		"MATCH ()-[e {id: 'missing'}]->() RETURN e",
	)
	if err != io.EOF {
		t.Errorf("no-match row returned instead of io.EOF: got err=%v dest=%v", err, dest[0])
	}
}

// TestRows_NoMatch_PathColumn_NullValue mirrors the NODE case for PATH columns.
func TestRows_NoMatch_PathColumn_NullValue(t *testing.T) {
	dest, err := queryFirstNext(t,
		mockSchemaMsgWithType("p", "PATH"),
		mockNullRowPageMsg(),
		"MATCH p = (a {id: 'missing'})-[*]->(b) RETURN p",
	)
	if err != io.EOF {
		t.Errorf("no-match row returned instead of io.EOF: got err=%v dest=%v", err, dest[0])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NEGATIVE tests — these MUST pass both before AND after the fix, proving the
// filter does not over-reach into legitimate null results.
// ─────────────────────────────────────────────────────────────────────────────

// TestRows_NoMatch_NonRefColumnNotFiltered_String verifies that a null value in
// a scalar (STRING) column is a legitimate result and is still returned as a
// real row — e.g. RETURN n.nonexistent. Must NOT be filtered.
func TestRows_NoMatch_NonRefColumnNotFiltered_String(t *testing.T) {
	_, err := queryFirstNext(t,
		mockSchemaMsgWithType("x", "STRING"),
		mockNullRowPageMsg(),
		"MATCH (n:PropTest) RETURN n.nonexistent",
	)
	if err != nil {
		t.Errorf("string null column incorrectly filtered: got err=%v", err)
	}
}

// TestRows_NoMatch_EmptyTypeNotFiltered verifies that a null value in a column
// with no schema type (UNKNOWN) — e.g. RETURN null — is still returned as a
// real row. Must NOT be filtered.
func TestRows_NoMatch_EmptyTypeNotFiltered(t *testing.T) {
	_, err := queryFirstNext(t,
		mockSchemaMsg([]string{"x"}),
		mockNullRowPageMsg(),
		"RETURN null AS x",
	)
	if err != nil {
		t.Errorf("untyped (UNKNOWN) null column incorrectly filtered: got err=%v", err)
	}
}

// TestRows_NoMatch_NodeColumnNonNullNotFiltered verifies that a NODE column with
// a genuine (non-null) value — a node that actually exists — is returned as a
// real row. Only NODE/EDGE/PATH columns carrying NULL are no-match sentinels;
// non-null reference columns must pass through.
func TestRows_NoMatch_NodeColumnNonNullNotFiltered(t *testing.T) {
	_, err := queryFirstNext(t,
		mockSchemaMsgWithType("n", "NODE"),
		mockPageMsg([][]string{{"exists"}}), // non-null value stands in for a real node
		"MATCH (n {id: 'present'}) RETURN n",
	)
	if err != nil {
		t.Errorf("non-null NODE row incorrectly filtered: got err=%v", err)
	}
}

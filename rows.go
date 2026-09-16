package geode

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"sync"

	"geodedb.com/geode/proto"
)

// ColumnInfo holds metadata about a column.
type ColumnInfo struct {
	Name string
	Type GQLType
}

// Rows represents a result set from a query.
type Rows struct {
	conn *Conn
	// ctx is the context of the query that produced these rows. Paging
	// (fetchNextPage) reads more frames from the wire, so cancellation must
	// be observed between pages and reported as ctx.Err() rather than as a
	// transport error. See abortOnCancel.
	ctx       context.Context
	columns   []ColumnInfo
	pageSize  int
	requestID uint64

	protoBuffer []*proto.Row // Protobuf rows buffer
	bufferIdx   int
	final       bool
	ordered     bool
	orderKeys   []string

	mu     sync.Mutex
	closed bool
}

// Compile-time interface checks.
var (
	_ driver.Rows                           = (*Rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*Rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*Rows)(nil)
)

// Columns returns the column names.
func (r *Rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// abortOnCancel reports the query context's error — wrapped, so callers still
// match it with errors.Is(err, context.Canceled) — once that context is done,
// and retires the connection on the way out. It returns nil while the context
// is live.
//
// Retiring matters because a cancelled stream is left desynchronised: a PULL
// may be in flight and the server still owes pages nobody will read. Marking
// the connection failed makes Conn.IsValid() false, so database/sql discards
// the connection instead of handing the half-read stream to the next borrower.
//
// There is no client-side cancel message to send instead. The 0.8.15 wire
// protocol's QuicClientMessage oneof carries only hello, execute, pull, ping,
// cdc_diag, cdc_ctrl, begin, commit, rollback, savepoint, rollback_to, backup,
// restore and upload_backup (geode src/proto/geode.proto:36-53) — no DISCARD,
// no reset. The protocol is not pull-driven at 0.8.15: after EXECUTE the
// server pushes every remaining page inline as further EXECUTE responses,
// and still answers any PULL the driver sends in addition to those pushed
// pages (geode src/server/quic_connection_handler.zig:7726-7743, node-scan
// path :7565-7599) — so no longer pulling does not by itself stop the
// server. What bounds the abandoned work instead: closing the stream trips
// the per-stream cancel source the handler binds to running scans
// (geode src/server/quic_connection_handler.zig:5729-5742), and the
// wall-clock deadline (deadline_mono_ns) is the backstop for a per-stream
// reset that does not close the whole connection (:5743-5744).
//
// Callers must hold the Conn mutex, which markConnFailed requires: QueryContext
// transfers that lock to the Rows and releaseRows returns it.
func (r *Rows) abortOnCancel() error {
	if r.ctx == nil {
		return nil
	}
	err := r.ctx.Err()
	if err == nil {
		return nil
	}
	r.conn.markConnFailed()
	return fmt.Errorf("geode: rows aborted: %w", err)
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	// final is read inside the same critical section that sets closed:
	// fetchNextPage writes r.final without holding r.mu, so reading it
	// after unlocking would race with an in-flight page fetch.
	final := r.final
	r.mu.Unlock()

	// A Rows abandoned before its final page under a cancelled context leaves
	// the stream mid-result; retire the connection rather than return a
	// desynchronised stream to the pool. Rows that reached their final page
	// are clean and stay poolable even if the context has since been
	// cancelled.
	if !final && r.ctx != nil && r.ctx.Err() != nil {
		r.conn.markConnFailed()
	}

	// A Close on a pushed-page transport (gRPC) cancels the per-Execute
	// context — for EVERY Close, not only an early one. Unlike the cancelled
	// case above, the context may well be perfectly live — `rows.Close()`
	// after one page of a large result is ordinary caller behaviour — so the
	// connection itself is fine and must stay poolable. What is NOT fine is
	// leaving the RPC's context alive: geode's push loop holds the session
	// mutex across the send (src/transport/grpc_server.zig:1065-1090) and
	// handleExecute (:1935) takes the same mutex, so an early Close that left
	// the RPC running would wedge the NEXT statement on this session behind
	// the engine's push loop.
	//
	// A FULLY-READ Rows (final == true) needs the same call for a different
	// reason: correctness, not the wedge. Before this fix, the only things
	// that ever cancelled a finished RPC's context were the next Execute on
	// the SAME connection (SendProto's ordering fire, which runs while this
	// connection is exclusively held again) and the transport's own Close.
	// Between this Close returning the connection to the pool and either of
	// those firing, the context sits alive with nothing watching it — a
	// child of the caller's ctx, whose own cancellation (deadline, an
	// unrelated cancel further up the call tree) fires whenever THAT context
	// decides to, unordered against this connection's next borrower. Racing
	// an unrelated cancellation against a different session's Execute on the
	// same connection is exactly the shape that produced driver.ErrBadConn
	// mid-batch (GAP-SONNY806-07, SONNY-1560). Cancelling here, while the
	// conn is still exclusively held and before it goes back to the pool,
	// retires the context deterministically instead of leaving its fate to
	// whichever caller happens to hold the parent.
	//
	// Cancelling the RPC — not markConnFailed — is therefore the fix: it
	// releases the engine's mutex (or, for the final case, simply retires an
	// already-finished RPC's context) and keeps a healthy connection in the
	// pool. markConnFailed would trade a wedge for a needless eviction and
	// re-dial on every Close.
	//
	// WHICH of the two teardowns is used matters, and that is what `final`
	// decides. A result the engine may STILL be pushing has to be CANCELLED:
	// draining it would mean reading a result nobody wants, page after page,
	// for as long as the engine cares to send it. A result that reached its
	// final page has nothing left on it but the server's end-of-stream, and
	// cancelling THAT emits an RST_STREAM the engine's grpc-zig escalates
	// into a whole-connection teardown — the next RPC on that connection dies
	// with `Unavailable: error reading from server: EOF` (SONNY-806 Task 16e,
	// F-16d-02). So a finished result is FINISHED, the same way ExecContext
	// finishes a completed statement, and only an abandoned one is cancelled.
	if r.conn.transport.PagesArePushed() {
		switch {
		case final:
			finishExecStream(r.conn.transport)
		default:
			if sc, ok := r.conn.transport.(streamCanceller); ok {
				sc.CancelActiveStream()
			}
		}
	}

	// Release the connection lock
	r.conn.releaseRows(r)
	return nil
}

// Next fetches the next row.
func (r *Rows) Next(dest []driver.Value) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRowsClosed
	}
	r.mu.Unlock()

	// Checked before serving a buffered row, so a cancelled query stops
	// immediately instead of draining the page it already holds, and before
	// every PULL fetchNextPage would otherwise issue.
	if err := r.abortOnCancel(); err != nil {
		return err
	}

	for {
		// Check if we have buffered protobuf rows
		if r.bufferIdx < len(r.protoBuffer) {
			row := r.protoBuffer[r.bufferIdx]
			r.bufferIdx++

			if isNoMatchRow(row, r.columns) {
				continue
			}

			// Copy values to dest
			for i := range r.columns {
				if i < len(row.GetValues()) {
					dest[i] = convertProtoValue(row.GetValues()[i])
				} else {
					dest[i] = nil
				}
			}
			return nil
		}

		// No more buffered rows
		if r.final {
			return io.EOF
		}

		// Fetch next page
		if err := r.fetchNextPage(); err != nil {
			return err
		}
	}
}

// convertProtoProperties converts a slice of MapEntry to a map.
func convertProtoProperties(entries []*proto.MapEntry) map[string]any {
	props := make(map[string]any, len(entries))
	for _, entry := range entries {
		props[entry.GetKey()] = convertProtoValue(entry.GetValue())
	}
	return props
}

// convertProtoNode converts a NodeValue to a structured map.
func convertProtoNode(node *proto.NodeValue) map[string]any {
	return map[string]any{
		"id":         int64(node.GetId()), //nolint:gosec // G115: proto IDs fit int64
		"labels":     node.GetLabels(),
		"properties": convertProtoProperties(node.GetProperties()),
	}
}

// convertProtoEdge converts an EdgeValue to a structured map.
func convertProtoEdge(edge *proto.EdgeValue) map[string]any {
	return map[string]any{
		"id":         int64(edge.GetId()),     //nolint:gosec // G115: proto IDs fit int64
		"start_node": int64(edge.GetFromId()), //nolint:gosec // G115: proto IDs fit int64
		"end_node":   int64(edge.GetToId()),   //nolint:gosec // G115: proto IDs fit int64
		"type":       edge.GetLabel(),
		"properties": convertProtoProperties(edge.GetProperties()),
	}
}

// convertProtoPath converts a PathValue to a structured map.
func convertProtoPath(path *proto.PathValue) map[string]any {
	nodes := make([]any, 0, len(path.GetNodes()))
	for _, n := range path.GetNodes() {
		nodes = append(nodes, convertProtoNode(n))
	}
	edges := make([]any, 0, len(path.GetEdges()))
	for _, e := range path.GetEdges() {
		edges = append(edges, convertProtoEdge(e))
	}
	return map[string]any{
		"nodes": nodes,
		"edges": edges,
	}
}

// convertProtoList converts a ListValue to a slice.
func convertProtoList(list *proto.ListValue) []any {
	result := make([]any, 0, len(list.GetValues()))
	for _, item := range list.GetValues() {
		result = append(result, convertProtoValue(item))
	}
	return result
}

// convertProtoExt converts an ExtValue to a driver.Value.
func convertProtoExt(ext *proto.ExtendedValue) driver.Value {
	if ext.GetText() != "" {
		return ext.GetText()
	}
	if len(ext.GetBytes()) > 0 {
		return ext.GetBytes()
	}
	return fmt.Sprintf("%v", ext)
}

// convertProtoValue converts a protobuf Value to a driver.Value.
func convertProtoValue(v *proto.Value) driver.Value {
	if v == nil {
		return nil
	}
	switch k := v.GetKind().(type) {
	case *proto.Value_NullVal:
		_ = k
		return nil
	case *proto.Value_StringVal:
		return k.StringVal.GetValue()
	case *proto.Value_IntVal:
		return k.IntVal.GetValue()
	case *proto.Value_DoubleVal:
		return k.DoubleVal.GetValue()
	case *proto.Value_BoolVal:
		return k.BoolVal
	case *proto.Value_DecimalVal:
		return k.DecimalVal.GetOrigRepr()
	case *proto.Value_BytesVal:
		return k.BytesVal.GetValue()
	case *proto.Value_ListVal:
		if k.ListVal != nil {
			return convertProtoList(k.ListVal)
		}
		return []any(nil)
	case *proto.Value_MapVal:
		if k.MapVal != nil {
			return convertProtoProperties(k.MapVal.GetEntries())
		}
		return map[string]any(nil)
	case *proto.Value_NodeVal:
		if k.NodeVal != nil {
			return convertProtoNode(k.NodeVal)
		}
		return map[string]any(nil)
	case *proto.Value_EdgeVal:
		if k.EdgeVal != nil {
			return convertProtoEdge(k.EdgeVal)
		}
		return map[string]any(nil)
	case *proto.Value_PathVal:
		if k.PathVal != nil {
			return convertProtoPath(k.PathVal)
		}
		return map[string]any(nil)
	case *proto.Value_ExtVal:
		if k.ExtVal != nil {
			return convertProtoExt(k.ExtVal)
		}
		return nil
	default:
		return nil
	}
}

// fetchNextPage fetches the next page of results from the server.
//
// Who moves the page depends on the transport (Transport.PagesArePushed):
//
//   - PULL-driven (QUIC): the driver asks with a PullRequest and reads the
//     answer.
//   - server-pushed (gRPC): the page is already in flight — geode 0.8.15
//     serves Execute as a server-streaming RPC and pushes every page until one
//     is final (src/transport/grpc_server.zig:1009-1101 handleExecuteStreaming)
//     — so the driver only reads. Sending a PULL here is what used to cap gRPC
//     results at one page: GeodeService has no Pull method, so the write itself
//     failed (GAP-SONNY806-04, fixed in Task 14b).
//
// Both arms share the same reader below, so the cancellation contract (Task 9)
// and the final/error/empty handling are identical on either wire.
func (r *Rows) fetchNextPage() error {
	// Never put another PULL on the wire — nor block on another page — for a
	// query the caller abandoned.
	if err := r.abortOnCancel(); err != nil {
		return err
	}

	if !r.conn.transport.PagesArePushed() {
		// Send a PullRequest to get the next page
		r.requestID++
		pullMsg := proto.NewPullMessage(&proto.PullRequest{
			RequestId: r.requestID,
			PageSize:  uint32(r.pageSize), //nolint:gosec // G115: page size fits uint32
		})
		if err := r.conn.transport.SendProto(r.ctx, pullMsg); err != nil {
			// A cancellation racing the write surfaces here as whatever the
			// transport noticed first; report it as the context error.
			if cancelled := r.abortOnCancel(); cancelled != nil {
				return cancelled
			}
			return err
		}
	}

	// This read blocks until the page arrives. Both transports honour r.ctx
	// while parked here — QUIC forces a read deadline the instant it is done,
	// gRPC rides the Execute RPC's own cancellation — so a cancellation
	// unblocks promptly and is reported as the context error, not as the
	// transport read_length/read/recv error it manifests as.
	resp, err := r.conn.transport.ReceiveProto(r.ctx)
	if err != nil {
		if cancelled := r.abortOnCancel(); cancelled != nil {
			return cancelled
		}
		return err
	}

	// Server sends pull data in pull.response, not execute
	var execResp *proto.ExecutionResponse
	if resp.GetPull() != nil {
		execResp = resp.GetPull().GetResponse()
	}
	if execResp == nil {
		execResp = resp.GetExecute()
	}
	if execResp == nil {
		r.final = true
		return io.EOF
	}

	if execResp.GetError() != nil {
		return &DriverError{
			Code:    execResp.GetError().GetCode(),
			Message: execResp.GetError().GetMessage(),
		}
	}

	if execResp.GetPage() != nil {
		r.protoBuffer = execResp.GetPage().GetRows()
		r.bufferIdx = 0
		r.final = execResp.GetPage().GetFinal()
		return nil
	}

	// No more data
	r.final = true
	return fmt.Errorf("unexpected response type")
}

// ColumnTypeDatabaseTypeName returns the database type name for a column.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return string(r.columns[index].Type)
}

// ColumnTypeNullable reports whether a column may be null.
func (r *Rows) ColumnTypeNullable(_ int) (nullable, ok bool) {
	// All columns in GQL can be null
	return true, true
}

// ColumnTypeScanType returns the Go type suitable for scanning.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeOf((*any)(nil)).Elem()
	}
	return r.columns[index].Type.GoType()
}

// IsOrdered returns true if the result set is ordered.
func (r *Rows) IsOrdered() bool {
	return r.ordered
}

// OrderKeys returns the order keys for the result set.
func (r *Rows) OrderKeys() []string {
	return r.orderKeys
}

// isNoMatchRow reports whether row is a server-emitted no-match sentinel.
// The Geode server emits a single null-valued row for NODE/EDGE/PATH columns
// when MATCH finds no results, instead of an empty DataPage. Skip these rows
// and return io.EOF so database/sql surfaces sql.ErrNoRows to the caller.
func isNoMatchRow(row *proto.Row, columns []ColumnInfo) bool {
	if len(columns) == 0 {
		return false
	}
	vals := row.GetValues()
	hasRefCol := false
	for i, col := range columns {
		if col.Type != GQLTypeNode && col.Type != GQLTypeEdge && col.Type != GQLTypePath {
			continue
		}
		hasRefCol = true
		var v *proto.Value
		if i < len(vals) {
			v = vals[i]
		}
		if v != nil {
			if _, isNull := v.GetKind().(*proto.Value_NullVal); !isNull {
				return false // reference column has a real value
			}
		}
	}
	return hasRefCol
}

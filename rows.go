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
// no reset. Two engine-side mechanisms bound the abandoned work instead: the
// protocol is pull-driven (the server produces a page only in answer to a
// PULL, so no longer pulling already stops it), and closing the stream/
// connection trips the per-stream cancel source the handler binds to running
// scans (geode src/server/quic_connection_handler.zig:5730-5742), with the
// wall-clock deadline as the backstop for a per-stream reset.
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
	r.mu.Unlock()

	// A Rows abandoned before its final page under a cancelled context leaves
	// the stream mid-result; retire the connection rather than return a
	// desynchronised stream to the pool. Rows that reached their final page
	// are clean and stay poolable even if the context has since been
	// cancelled.
	if !r.final && r.ctx != nil && r.ctx.Err() != nil {
		r.conn.markConnFailed()
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

// fetchNextPage fetches the next page of results from the server via PULL.
func (r *Rows) fetchNextPage() error {
	// Never put another PULL on the wire for a query the caller abandoned.
	if err := r.abortOnCancel(); err != nil {
		return err
	}

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

	// This read blocks until the page arrives. ReceiveProto watches r.ctx and
	// forces a read deadline the instant it is done, so a cancellation while
	// parked here unblocks promptly — and is reported as the context error,
	// not as the transport read_length/read error it manifests as.
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

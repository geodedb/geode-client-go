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
	conn     *Conn
	ctx      context.Context
	columns  []ColumnInfo
	pageSize int

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

// Close closes the rows iterator.
func (r *Rows) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

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

	for {
		// Check if we have buffered protobuf rows
		if r.bufferIdx < len(r.protoBuffer) {
			row := r.protoBuffer[r.bufferIdx]
			r.bufferIdx++

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
func (r *Rows) fetchNextPage() error {
	resp, err := r.conn.transport.ReceiveProto(r.ctx)
	if err != nil {
		return err
	}

	execResp := resp.GetExecute()
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

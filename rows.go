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

// convertProtoValue converts a protobuf Value to a driver.Value.
func convertProtoValue(v *proto.Value) driver.Value {
	if v == nil {
		return nil
	}
	switch k := v.GetKind().(type) {
	case *proto.Value_NullVal:
		_ = k // null
		return nil
	case *proto.Value_StringVal:
		if k.StringVal != nil {
			return k.StringVal.GetValue()
		}
		return ""
	case *proto.Value_IntVal:
		if k.IntVal != nil {
			return k.IntVal.GetValue()
		}
		return int64(0)
	case *proto.Value_DoubleVal:
		if k.DoubleVal != nil {
			return k.DoubleVal.GetValue()
		}
		return float64(0)
	case *proto.Value_BoolVal:
		return k.BoolVal
	case *proto.Value_DecimalVal:
		if k.DecimalVal != nil {
			return k.DecimalVal.GetOrigRepr()
		}
		return ""
	case *proto.Value_BytesVal:
		if k.BytesVal != nil {
			return k.BytesVal.GetValue()
		}
		return []byte(nil)
	case *proto.Value_ListVal:
		if k.ListVal != nil {
			result := make([]any, 0, len(k.ListVal.GetValues()))
			for _, item := range k.ListVal.GetValues() {
				result = append(result, convertProtoValue(item))
			}
			return result
		}
		return []any(nil)
	case *proto.Value_MapVal:
		if k.MapVal != nil {
			result := make(map[string]any)
			for _, entry := range k.MapVal.GetEntries() {
				result[entry.GetKey()] = convertProtoValue(entry.GetValue())
			}
			return result
		}
		return map[string]any(nil)
	case *proto.Value_NodeVal:
		if k.NodeVal != nil {
			node := k.NodeVal
			props := make(map[string]any)
			for _, entry := range node.GetProperties() {
				props[entry.GetKey()] = convertProtoValue(entry.GetValue())
			}
			return map[string]any{
				"id":         int64(node.GetId()), //nolint:gosec // G115: proto IDs fit int64
				"labels":     node.GetLabels(),
				"properties": props,
			}
		}
		return map[string]any(nil)
	case *proto.Value_EdgeVal:
		if k.EdgeVal != nil {
			edge := k.EdgeVal
			props := make(map[string]any)
			for _, entry := range edge.GetProperties() {
				props[entry.GetKey()] = convertProtoValue(entry.GetValue())
			}
			return map[string]any{
				"id":         int64(edge.GetId()),     //nolint:gosec // G115: proto IDs fit int64
				"start_node": int64(edge.GetFromId()), //nolint:gosec // G115: proto IDs fit int64
				"end_node":   int64(edge.GetToId()),   //nolint:gosec // G115: proto IDs fit int64
				"type":       edge.GetLabel(),
				"properties": props,
			}
		}
		return map[string]any(nil)
	case *proto.Value_PathVal:
		if k.PathVal != nil {
			path := k.PathVal
			nodes := make([]any, 0, len(path.GetNodes()))
			for _, n := range path.GetNodes() {
				nProps := make(map[string]any)
				for _, entry := range n.GetProperties() {
					nProps[entry.GetKey()] = convertProtoValue(entry.GetValue())
				}
				nodes = append(nodes, map[string]any{
					"id":         int64(n.GetId()), //nolint:gosec // G115: proto IDs fit int64
					"labels":     n.GetLabels(),
					"properties": nProps,
				})
			}
			edges := make([]any, 0, len(path.GetEdges()))
			for _, e := range path.GetEdges() {
				eProps := make(map[string]any)
				for _, entry := range e.GetProperties() {
					eProps[entry.GetKey()] = convertProtoValue(entry.GetValue())
				}
				edges = append(edges, map[string]any{
					"id":         int64(e.GetId()),     //nolint:gosec // G115: proto IDs fit int64
					"start_node": int64(e.GetFromId()), //nolint:gosec // G115: proto IDs fit int64
					"end_node":   int64(e.GetToId()),   //nolint:gosec // G115: proto IDs fit int64
					"type":       e.GetLabel(),
					"properties": eProps,
				})
			}
			return map[string]any{
				"nodes": nodes,
				"edges": edges,
			}
		}
		return map[string]any(nil)
	case *proto.Value_ExtVal:
		if k.ExtVal != nil {
			if k.ExtVal.GetText() != "" {
				return k.ExtVal.GetText()
			}
			if len(k.ExtVal.GetBytes()) > 0 {
				return k.ExtVal.GetBytes()
			}
			return fmt.Sprintf("%v", k.ExtVal)
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

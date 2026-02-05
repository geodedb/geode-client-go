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

	protoBuffer []proto.Row // Protobuf rows buffer
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
				if i < len(row.Values) {
					dest[i] = convertProtoValue(&row.Values[i])
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
	if v.NullVal {
		return nil
	}
	if v.StringVal != nil {
		return *v.StringVal
	}
	if v.IntVal != nil {
		return *v.IntVal
	}
	if v.DoubleVal != nil {
		return *v.DoubleVal
	}
	if v.BoolVal != nil {
		return *v.BoolVal
	}
	return nil
}

// fetchNextPage fetches the next page of results from the server.
func (r *Rows) fetchNextPage() error {
	resp, err := r.conn.transport.ReceiveProto(r.ctx)
	if err != nil {
		return err
	}

	if resp.Execute == nil {
		r.final = true
		return io.EOF
	}

	if resp.Execute.Error != nil {
		return &DriverError{
			Code:    resp.Execute.Error.Code,
			Message: resp.Execute.Error.Message,
		}
	}

	if resp.Execute.Page != nil {
		r.protoBuffer = resp.Execute.Page.Rows
		r.bufferIdx = 0
		r.final = resp.Execute.Page.LastPage
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

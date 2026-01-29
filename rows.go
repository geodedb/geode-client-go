package geode

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
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

	buffer    []map[string]any
	bufferIdx int
	final     bool
	ordered   bool
	orderKeys []string

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
		// Check if we have buffered rows
		if r.bufferIdx < len(r.buffer) {
			row := r.buffer[r.bufferIdx]
			r.bufferIdx++

			// Copy values to dest
			for i, col := range r.columns {
				if v, ok := row[col.Name]; ok {
					dest[i] = ConvertValue(v)
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

// fetchNextPage fetches the next page of results from the server.
func (r *Rows) fetchNextPage() error {
	// Send PULL request
	if err := r.conn.transport.Send(r.ctx, BuildPullMessage(1, r.pageSize)); err != nil {
		return err
	}

	// Read response
	data, err := r.conn.transport.Receive(r.ctx)
	if err != nil {
		return err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return err
	}

	// Check for errors
	if frame.IsError() {
		return frame.ToError()
	}

	if frame.Result.Type != RespBindings {
		// Treat other types as end of results
		r.final = true
		return io.EOF
	}

	// Update buffer
	r.buffer = frame.Result.Rows
	r.bufferIdx = 0
	r.final = frame.Result.Final

	return nil
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

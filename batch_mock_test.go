// batch_mock_test.go
package geode

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

// mockDriverSeq generates unique driver names to avoid sql.Register panics.
var mockDriverSeq atomic.Int64

// mockDriver is a minimal database/sql/driver.Driver for unit testing.
// Configure onBegin and onExec to inject errors.
type mockDriver struct {
	onBegin func() error
	onExec  func(query string) error
	commits atomic.Int64
}

func (d *mockDriver) Open(_ string) (driver.Conn, error) {
	return &mockConn{driver: d}, nil
}

type mockConn struct{ driver *mockDriver }

func (c *mockConn) Prepare(q string) (driver.Stmt, error) {
	return &mockStmt{conn: c, query: q}, nil
}
func (c *mockConn) Close() error { return nil }
func (c *mockConn) Begin() (driver.Tx, error) {
	if c.driver.onBegin != nil {
		if err := c.driver.onBegin(); err != nil {
			return nil, err
		}
	}
	return &mockTx{driver: c.driver}, nil
}

type mockTx struct{ driver *mockDriver }

func (t *mockTx) Commit() error   { t.driver.commits.Add(1); return nil }
func (t *mockTx) Rollback() error { return nil }

type mockStmt struct {
	conn  *mockConn
	query string
}

func (s *mockStmt) Close() error  { return nil }
func (s *mockStmt) NumInput() int { return -1 }
func (s *mockStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &mockRows{}, nil
}
func (s *mockStmt) Exec(_ []driver.Value) (driver.Result, error) {
	if s.conn.driver.onExec != nil {
		if err := s.conn.driver.onExec(s.query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

type mockRows struct{}

func (*mockRows) Columns() []string           { return nil }
func (*mockRows) Close() error                { return nil }
func (*mockRows) Next(_ []driver.Value) error { return io.EOF }

// newMockPool registers a fresh mock driver (unique name per call) and returns
// a Pool wired to it. t.Cleanup closes the underlying *sql.DB.
func newMockPool(t testing.TB, md *mockDriver) *Pool {
	t.Helper()
	name := fmt.Sprintf("geode-mock-%d", mockDriverSeq.Add(1))
	sql.Register(name, md)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Pool{db: db, cfg: &Config{}}
}

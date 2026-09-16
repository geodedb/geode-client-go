// driver_connector_close_test.go — SONNY-806, Lane D Task 16d.
//
// The pool a Connector owns is reachable from nothing else. If sql.DB.Close
// does not close it, nothing ever does, and the leak is not merely local: each
// pooled QUIC connection keeps its anchor stream open and the 1Hz probe loop
// keeps a probe stream on it, and each of those streams pins one worker of the
// ENGINE's shared stream pool for its whole life. These tests pin the wiring
// that makes sql.DB.Close release it; the end-to-end proof that a long-lived
// engine survives the cycle is
// TestIntegration_RepeatedOpenCloseDoesNotExhaustOneEngine.

package geode

import (
	"database/sql"
	"io"
	"testing"
	"time"
)

// quicTestConnector builds a QUIC connector without dialing anything.
// NewConnector creates the pool but never contacts the server, so these tests
// need no engine.
func quicTestConnector(t *testing.T) *Connector {
	t.Helper()
	c, err := NewConnector(&Config{
		Host:                "127.0.0.1",
		Port:                "3141",
		Transport:           TransportQUIC,
		InsecureSkipVerify:  true,
		HealthProbeInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	if c.quicPool == nil {
		t.Fatal("a QUIC connector must own a pool")
	}
	return c
}

// TestConnector_IsAnIOCloser pins the interface database/sql actually looks
// for. sql.DB.Close closes the connector only when it implements io.Closer
// (database/sql/sql.go: `if c, ok := db.connector.(io.Closer); ok`), so losing
// this method is silent: everything still compiles, every test still passes,
// and the pool leaks again.
func TestConnector_IsAnIOCloser(t *testing.T) {
	var c any = quicTestConnector(t)
	if _, ok := c.(io.Closer); !ok {
		t.Fatal("*Connector must implement io.Closer or sql.DB.Close will never release its QUIC pool")
	}
}

// TestConnector_CloseClosesTheQUICPool is the unit-level statement of the
// defect: before the fix Close did not exist, so the pool outlived the
// connector.
func TestConnector_CloseClosesTheQUICPool(t *testing.T) {
	c := quicTestConnector(t)
	if c.quicPool.isClosed() {
		t.Fatal("a fresh pool must not be closed")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.quicPool.isClosed() {
		t.Fatal("Connector.Close left the QUIC pool open")
	}
}

// TestConnector_CloseIsIdempotent — database/sql may close a DB more than once
// and callers hold connectors directly, so a second Close must be a no-op
// rather than a panic or an error.
func TestConnector_CloseIsIdempotent(t *testing.T) {
	c := quicTestConnector(t)
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestConnector_CloseWithoutAPoolIsANoOp — a gRPC connector owns no QUIC pool.
func TestConnector_CloseWithoutAPoolIsANoOp(t *testing.T) {
	c, err := NewConnector(&Config{
		Host:      "127.0.0.1",
		Port:      "50051",
		Transport: TransportGRPC,
	})
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	if c.quicPool != nil {
		t.Fatal("a gRPC connector must not own a QUIC pool")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestDBClose_ClosesTheConnectorsPool runs the real database/sql path: no
// connection is ever opened (nothing is dialed), so this asserts exactly one
// thing — that sql.DB.Close reaches Connector.Close.
func TestDBClose_ClosesTheConnectorsPool(t *testing.T) {
	c := quicTestConnector(t)
	db := sql.OpenDB(c)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if !c.quicPool.isClosed() {
		t.Fatal("sql.DB.Close did not close the connector's QUIC pool")
	}
	if pooled, inFlight := c.quicPool.counts(); pooled != 0 || inFlight != 0 {
		t.Fatalf("closed pool still holds conns=%d streams=%d", pooled, inFlight)
	}
}

// TestDriverOpen_ConnCloseClosesItsPrivatePool covers the deprecated one-shot
// Driver.Open path, which builds a Connector per call that nothing else can
// ever close. The connection it returns owns that pool. Dialing is not
// attempted: the pool is attached to a Conn assembled here, exactly as
// Driver.Open attaches it.
func TestDriverOpen_ConnCloseClosesItsPrivatePool(t *testing.T) {
	c := quicTestConnector(t)
	conn := newConn(c.cfg, newMockTransport())
	conn.ownedPool = c.quicPool

	if err := conn.Close(); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
	if !c.quicPool.isClosed() {
		t.Fatal("closing a Driver.Open connection left its private QUIC pool open")
	}
}

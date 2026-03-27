package geode

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewQUICPool_Defaults(t *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 0)
	if pool.maxSize != DefaultQUICPoolSize {
		t.Errorf("expected maxSize %d, got %d", DefaultQUICPoolSize, pool.maxSize)
	}
	if pool.closed {
		t.Error("new pool should not be closed")
	}
	if len(pool.conns) != 0 {
		t.Errorf("expected 0 initial connections, got %d", len(pool.conns))
	}
}

func TestNewQUICPool_CustomSize(t *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 8)
	if pool.maxSize != 8 {
		t.Errorf("expected maxSize 8, got %d", pool.maxSize)
	}
}

func TestQUICPool_CloseIdempotent(t *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 4)

	// First close should succeed
	if err := pool.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Second close should be a no-op
	if err := pool.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}

func TestQUICPool_AcquireAfterClose(t *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 4)
	_ = pool.Close()

	_, err := pool.acquire(t.Context())
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestPooledConn_AuthState(t *testing.T) {
	pc := &pooledConn{}

	if pc.isAuthed() {
		t.Error("new pooled connection should not be authed")
	}

	pc.markAuthed()

	if !pc.isAuthed() {
		t.Error("pooled connection should be authed after markAuthed")
	}
}

func TestPooledConn_ActiveCounter(t *testing.T) {
	pc := &pooledConn{}

	// Simulate concurrent stream acquisition
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt32(&pc.active, 1)
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&pc.active) != 10 {
		t.Errorf("expected 10 active streams, got %d", atomic.LoadInt32(&pc.active))
	}

	// Simulate releasing
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt32(&pc.active, -1)
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&pc.active) != 0 {
		t.Errorf("expected 0 active streams, got %d", atomic.LoadInt32(&pc.active))
	}
}

func TestQUICPool_ReleaseNil(_ *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 4)
	defer pool.Close()

	// Should not panic
	pool.release(nil)
}

func TestQUICPool_RemoveNil(_ *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
	}

	pool := NewQUICPool(cfg, 4)
	defer pool.Close()

	// Should not panic
	pool.remove(nil)
}

func TestConnector_HasQUICPool(t *testing.T) {
	d := &Driver{}

	connector, err := d.OpenConnector("quic://localhost:3141")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c, ok := connector.(*Connector)
	if !ok {
		t.Fatalf("expected *Connector, got %T", connector)
	}

	if c.quicPool == nil {
		t.Error("QUIC connector should have a quicPool")
	}
}

func TestConnector_GRPCNoPool(t *testing.T) {
	d := &Driver{}

	connector, err := d.OpenConnector("grpc://localhost:50051?tls=0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c, ok := connector.(*Connector)
	if !ok {
		t.Fatalf("expected *Connector, got %T", connector)
	}

	if c.quicPool != nil {
		t.Error("gRPC connector should not have a quicPool")
	}
}

package geode

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests cover driver-side admission control on the total number of
// in-flight QUIC streams.
//
// Before the fix the QUICPool bounded only the number of underlying QUIC
// connections; stream count per connection (and thus total in-flight streams)
// was unbounded.  An application using sql.Open("geode", dsn) directly
// inherits Go stdlib's MaxOpenConns=0 (unlimited) and can pin arbitrary
// numbers of *sql.Conn handles, each producing a new QUIC stream.  Past
// ~50 concurrent streams the server's single 50µs pollLoop saturates and
// per-stream BufferedProtoReader.nextMessage spins on WouldBlock until its
// 300s timeout, producing a cluster-wide deadlock (see docs/quic-deadlock.md
// in the divelife-argo investigation).
//
// The fix bounds total in-flight streams via a semaphore in QUICPool.
// New stream requests block until a slot is released or ctx is cancelled.

// TestNewQUICPool_DefaultMaxStreams verifies the pool initializes a non-zero
// stream admission limit by default.  Before the fix the pool had no such
// concept, so the test fails to compile / fails at runtime.
func TestNewQUICPool_DefaultMaxStreams(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141"}
	pool := NewQUICPool(cfg, 0)
	defer pool.Close()

	if pool.maxStreams != DefaultMaxStreams {
		t.Errorf("expected maxStreams %d, got %d", DefaultMaxStreams, pool.maxStreams)
	}
	if pool.maxStreams <= 0 {
		t.Fatal("default maxStreams must be > 0 to provide admission control")
	}
}

// TestNewQUICPool_CustomMaxStreams verifies cfg.MaxStreams overrides the
// default.
func TestNewQUICPool_CustomMaxStreams(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 8}
	pool := NewQUICPool(cfg, 0)
	defer pool.Close()

	if pool.maxStreams != 8 {
		t.Errorf("expected maxStreams 8, got %d", pool.maxStreams)
	}
}

// TestQUICPool_AcquireSlotBlocksAtCapacity is the core regression test for the
// QUIC deadlock bug.  After the fix, once maxStreams slots are taken, further
// acquireSlot calls block until a slot is released or ctx is cancelled.
//
// Before the fix this test fails to compile (the API doesn't exist) — which is
// the strongest possible "test fails before fix" signal.
func TestQUICPool_AcquireSlotBlocksAtCapacity(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 2}
	pool := NewQUICPool(cfg, 4)
	defer pool.Close()

	ctx := context.Background()
	if err := pool.acquireSlot(ctx); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}
	if err := pool.acquireSlot(ctx); err != nil {
		t.Fatalf("second acquireSlot failed: %v", err)
	}

	// Pool is at capacity; a third acquire with a short deadline must time out.
	deadlineCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := pool.acquireSlot(deadlineCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected acquireSlot to block and time out, got nil error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("acquireSlot returned too quickly (%v); expected to block ~50ms", elapsed)
	}
}

// TestQUICPool_ReleaseSlotUnblocks verifies releaseSlot wakes a blocked
// acquireSlot.  This proves the admission control isn't just a one-way gate —
// stream slots are returned to circulation when transports close.
func TestQUICPool_ReleaseSlotUnblocks(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 1}
	pool := NewQUICPool(cfg, 2)
	defer pool.Close()

	ctx := context.Background()
	if err := pool.acquireSlot(ctx); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}

	// Second acquireSlot blocks; release after a short delay should unblock it.
	acquired := make(chan error, 1)
	go func() {
		acquired <- pool.acquireSlot(ctx)
	}()

	// Give the goroutine time to enter the blocking select.
	time.Sleep(20 * time.Millisecond)
	pool.releaseSlot()

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("acquireSlot after release failed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("acquireSlot did not unblock after releaseSlot")
	}
}

// TestQUICPool_AcquireSlotCancellation verifies a blocked acquireSlot returns
// ctx.Err() when the caller's context is cancelled.  Important for query
// deadlines: a 30s context.WithTimeout on Exec/Query must release the goroutine
// from a stuck slot-acquire even if the pool stays saturated.
func TestQUICPool_AcquireSlotCancellation(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 1}
	pool := NewQUICPool(cfg, 2)
	defer pool.Close()

	if err := pool.acquireSlot(context.Background()); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pool.acquireSlot(ctx)
	}()

	// Give the goroutine time to enter the blocking select, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("acquireSlot did not return after ctx cancellation")
	}
}

// TestQUICPool_CloseUnblocksAcquire verifies Close releases all blocked
// acquirers with ErrClosed.  Without this, shutting down a saturated pool
// would leak goroutines that are stuck waiting for slots.
func TestQUICPool_CloseUnblocksAcquire(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 1}
	pool := NewQUICPool(cfg, 2)

	if err := pool.acquireSlot(context.Background()); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- pool.acquireSlot(context.Background())
	}()

	time.Sleep(20 * time.Millisecond)
	_ = pool.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("acquireSlot did not return after pool.Close")
	}
}

// TestQUICPool_AcquireSlotAfterClose verifies acquireSlot returns ErrClosed
// when called on an already-closed pool.
func TestQUICPool_AcquireSlotAfterClose(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 4}
	pool := NewQUICPool(cfg, 2)
	_ = pool.Close()

	err := pool.acquireSlot(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

// TestParseDSN_MaxStreams verifies the ?max_streams DSN parameter parses into
// cfg.MaxStreams.  Before the fix this option is silently ignored.
func TestParseDSN_MaxStreams(t *testing.T) {
	cfg, err := ParseDSN("quic://localhost:3141?max_streams=32")
	if err != nil {
		t.Fatalf("ParseDSN failed: %v", err)
	}
	if cfg.MaxStreams != 32 {
		t.Errorf("expected MaxStreams=32, got %d", cfg.MaxStreams)
	}
}

// TestParseDSN_MaxStreamsInvalid verifies invalid values are rejected.
func TestParseDSN_MaxStreamsInvalid(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"non-numeric", "quic://localhost:3141?max_streams=abc"},
		{"negative", "quic://localhost:3141?max_streams=-1"},
		{"zero", "quic://localhost:3141?max_streams=0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseDSN(tt.dsn); err == nil {
				t.Errorf("expected error for %q, got nil", tt.dsn)
			}
		})
	}
}

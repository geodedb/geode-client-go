package geode

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestSelectFreeConnIndex pins the no-multiplex selection policy: acquire reuses
// only a connection with no in-flight stream (active == 0) and never one that is
// busy. Multiplexing concurrent streams over one QUIC connection wedges all but
// one against the server (BUG-0002), so a busy connection must never be chosen.
func TestSelectFreeConnIndex(t *testing.T) {
	t.Parallel()

	mk := func(active ...int32) []*pooledConn {
		conns := make([]*pooledConn, len(active))
		for i, a := range active {
			pc := &pooledConn{}
			atomic.StoreInt32(&pc.active, a)
			conns[i] = pc
		}
		return conns
	}

	tests := []struct {
		name  string
		conns []*pooledConn
		want  int
	}{
		{"empty", nil, -1},
		{"single free", mk(0), 0},
		{"single busy", mk(1), -1},
		{"all busy", mk(1, 2, 1), -1},
		{"first free wins", mk(0, 0), 0},
		{"free after busy", mk(3, 1, 0), 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := selectFreeConnIndex(tt.conns); got != tt.want {
				t.Errorf("selectFreeConnIndex = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestNewQUICPool_AdmissionBoundedByPoolSize is the admission-layer regression
// for the burst deadlock (BUG-0002). Because the driver dedicates one QUIC
// connection per in-flight stream, the real concurrency ceiling is
// min(maxStreams, maxSize). Admission must therefore be capped at maxSize even
// when maxStreams is much larger — otherwise excess callers are admitted and
// then multiplexed onto the bounded set of connections, which wedges.
//
// Before the fix the semaphore held maxStreams (64) slots, so a third acquire
// against a 2-connection pool would succeed and go on to wedge. After the fix
// admission is min(64, 2) = 2 and the third caller blocks (clean backpressure)
// until a slot frees or its context expires.
func TestNewQUICPool_AdmissionBoundedByPoolSize(t *testing.T) {
	t.Parallel()

	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 64}
	pool := NewQUICPool(cfg, 2) // maxSize = 2, far below maxStreams
	defer pool.Close()

	ctx := context.Background()
	if err := pool.acquireSlot(ctx); err != nil {
		t.Fatalf("first acquireSlot failed: %v", err)
	}
	if err := pool.acquireSlot(ctx); err != nil {
		t.Fatalf("second acquireSlot failed: %v", err)
	}

	// Third acquire must block (admission capped at maxSize=2), not be admitted.
	deadlineCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := pool.acquireSlot(deadlineCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("third acquireSlot was admitted; admission not bounded by pool_size " +
			"(burst would multiplex into a wedge)")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("third acquireSlot returned too quickly (%v); expected to block ~50ms", elapsed)
	}
}

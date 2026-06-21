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

// TestSelectConnWithCapacity pins the multiplex selection policy: acquire reuses
// the LEAST-loaded connection whose in-flight stream count is below the per-conn
// cap (active < max), spreading load evenly; it returns -1 when every connection
// is at capacity (so the caller dials a new conn or, at maxSize, backs off). With
// max=1 it must reduce to the legacy no-multiplex behaviour (only active==0).
func TestSelectConnWithCapacity(t *testing.T) {
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
		max   int
		want  int
	}{
		{"empty", nil, 4, -1},
		{"least loaded under cap", mk(4, 2, 0, 4), 4, 2},
		{"all at cap", mk(4, 4, 4), 4, -1},
		{"prefers strictly least loaded", mk(3, 1, 2), 4, 1},
		{"first of equal load wins", mk(2, 2, 2), 4, 0},
		// max=1 ≡ legacy selectFreeConnIndex (only active==0).
		{"max1 single free", mk(0), 1, 0},
		{"max1 single busy", mk(1), 1, -1},
		{"max1 all busy", mk(1, 2, 1), 1, -1},
		{"max1 free after busy", mk(3, 1, 0), 1, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := selectConnWithCapacity(tt.conns, tt.max); got != tt.want {
				t.Errorf("selectConnWithCapacity(max=%d) = %d, want %d", tt.max, got, tt.want)
			}
		})
	}
}

// TestSelectConnWithCapacity_MatchesLegacyAtMax1 cross-checks that, with max=1,
// the capacity selector agrees with the legacy selectFreeConnIndex on every
// input — guaranteeing back-compat for the no-multiplex (max_streams_per_conn=1)
// configuration.
func TestSelectConnWithCapacity_MatchesLegacyAtMax1(t *testing.T) {
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

	cases := [][]int32{
		nil,
		{0},
		{1},
		{1, 2, 1},
		{0, 0},
		{3, 1, 0},
		{2, 0, 0, 5},
	}
	for _, c := range cases {
		conns := mk(c...)
		if got, want := selectConnWithCapacity(conns, 1), selectFreeConnIndex(conns); got != want {
			t.Errorf("selectConnWithCapacity(%v, 1) = %d, selectFreeConnIndex = %d", c, got, want)
		}
	}
}

// TestNewQUICPool_AdmissionMultiplex is the admission-layer regression for the
// multiplex policy. The total in-flight ceiling is min(maxStreams, maxSize *
// maxStreamsPerConn): each of maxSize connections can carry up to
// maxStreamsPerConn concurrent streams, capped by the configured maxStreams.
//
// This replaces the old TestNewQUICPool_AdmissionBoundedByPoolSize, whose
// premise (admission == maxSize because the driver ran one stream per conn) no
// longer holds once concurrent streams multiplex onto each connection.
//
// With maxSize=2, maxStreamsPerConn=8, maxStreams=64 the ceiling is 2*8 = 16
// (not 2, not 64): 16 acquires succeed without blocking, the 17th blocks until a
// slot is released or its context expires.
func TestNewQUICPool_AdmissionMultiplex(t *testing.T) {
	t.Parallel()

	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 64, MaxStreamsPerConn: 8}
	pool := NewQUICPool(cfg, 2) // maxSize = 2 → ceiling = 2*8 = 16
	defer pool.Close()

	ctx := context.Background()
	for i := 0; i < 16; i++ {
		if err := pool.acquireSlot(ctx); err != nil {
			t.Fatalf("acquireSlot %d/16 failed: %v (ceiling should be 16)", i+1, err)
		}
	}

	// The 17th acquire must block: ceiling is maxSize*maxStreamsPerConn = 16.
	deadlineCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := pool.acquireSlot(deadlineCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("17th acquireSlot was admitted; admission ceiling not " +
			"min(maxStreams, maxSize*maxStreamsPerConn)")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("17th acquireSlot returned too quickly (%v); expected to block ~50ms", elapsed)
	}
}

// TestNewQUICPool_AdmissionCappedByMaxStreams verifies maxStreams still caps the
// ceiling when it is the smaller of the two bounds: with maxSize=8,
// maxStreamsPerConn=8 (product 64) but maxStreams=10, the ceiling is 10.
func TestNewQUICPool_AdmissionCappedByMaxStreams(t *testing.T) {
	t.Parallel()

	cfg := &Config{Host: "localhost", Port: "3141", MaxStreams: 10, MaxStreamsPerConn: 8}
	pool := NewQUICPool(cfg, 8) // product 64, but maxStreams=10 dominates
	defer pool.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := pool.acquireSlot(ctx); err != nil {
			t.Fatalf("acquireSlot %d/10 failed: %v (ceiling should be 10)", i+1, err)
		}
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := pool.acquireSlot(deadlineCtx); err == nil {
		t.Fatal("11th acquireSlot admitted; maxStreams should cap the ceiling at 10")
	}
}

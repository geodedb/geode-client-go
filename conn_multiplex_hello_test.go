//go:build integration

// conn_multiplex_hello_test.go — Task 4 of the multiplex-driver plan.
//
// Verifies that K logical connections opening *simultaneously* on one freshly
// dialed QUIC connection all come up and all query successfully.
//
// Since GAP-SONNY806-08 each of the K streams performs its OWN HELLO: the
// handshake carries the session's tenant and graph binding and the engine
// applies both per stream, so a stream that skipped it would run unbound. This
// test is therefore the concurrency evidence for that design — K simultaneous
// HELLOs on one connection must all be accepted and must leave every stream
// usable. Before the fix the assertion here was the opposite (exactly one
// HELLO, K-1 streams adopting it client-side), which is the defect.
package geode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const helloConcurrentStreams = 8

// TestPooledHello_ConcurrentFirstStreams launches K goroutines that all race to
// be the first stream on a single freshly dialed QUIC connection (maxSize=1 so
// they MUST share one connection). It asserts exactly one underlying pooled
// connection exists, it is authed, and every one of the K logical connections
// can run an auth-requiring query without error.
func TestPooledHello_ConcurrentFirstStreams(t *testing.T) {
	tmpDir := t.TempDir()
	port := findFreeUDPPort(t, 3590, 3699)
	metricsPort := findFreeUDPPort(t, port+1, 3799)
	startProbeServer(t, port, metricsPort, tmpDir)

	cfg := &Config{
		Transport:          TransportQUIC,
		Host:               "127.0.0.1",
		Port:               fmt.Sprintf("%d", port),
		Username:           "geode",
		Password:           "seed",
		InsecureSkipVerify: true,
		PageSize:           DefaultPageSize,
		HelloName:          DefaultHelloName,
		HelloVersion:       DefaultHelloVersion,
		Conformance:        DefaultConformance,
	}

	// maxSize=1 forces every concurrent stream onto ONE QUIC connection, so K
	// HELLOs land on one connection at once. maxStreamsPerConn defaults to 8
	// (= K), so all K streams are admitted onto the single connection
	// concurrently.
	pool := NewQUICPool(cfg, 1)
	t.Cleanup(func() { _ = pool.Close() })

	// Barrier so all goroutines fire connectPooled (acquire → openStream →
	// pooledHello) at the same instant, maximising the concurrency of the K
	// handshakes.
	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		errCount atomic.Int64
		okCount  atomic.Int64
		conns    = make([]*Conn, helloConcurrentStreams)
		errs     = make([]error, helloConcurrentStreams)
	)

	for i := 0; i < helloConcurrentStreams; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			c, err := connectPooled(ctx, cfg, pool)
			if err != nil {
				errs[idx] = fmt.Errorf("connectPooled: %w", err)
				errCount.Add(1)
				return
			}
			conns[idx] = c

			// Auth-requiring query. A stream whose handshake the server did
			// not accept would be torn down and this query would fail.
			rows, err := c.QueryContext(ctx, "MATCH (n) RETURN count(n)", nil)
			if err != nil {
				errs[idx] = fmt.Errorf("query: %w", err)
				errCount.Add(1)
				return
			}
			_ = rows.Close()
			okCount.Add(1)
		}(i)
	}

	close(start)
	wg.Wait()

	t.Cleanup(func() {
		for _, c := range conns {
			if c != nil {
				_ = c.Close()
			}
		}
	})

	// ── Assertions ───────────────────────────────────────────────────────────
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d failed: %v", i, err)
		}
	}
	require.Equal(t, int64(0), errCount.Load(), "no concurrent first-stream should error")
	require.Equal(t, int64(helloConcurrentStreams), okCount.Load(),
		"every concurrent first-stream query must succeed")

	// Exactly one underlying QUIC connection (maxSize=1), and it is authed.
	pool.mu.Lock()
	nConns := len(pool.conns)
	var authed bool
	if nConns == 1 {
		authed = pool.conns[0].isAuthed()
	}
	pool.mu.Unlock()

	require.Equal(t, 1, nConns, "all concurrent streams must share ONE QUIC connection")
	require.True(t, authed, "the shared connection must be authed")

	t.Logf("OK: %d concurrent streams shared one connection, each handshook, all queries succeeded",
		helloConcurrentStreams)
}

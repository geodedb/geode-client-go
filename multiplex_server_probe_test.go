//go:build integration

// multiplex_server_probe_test.go — Task 0 de-risk gate.
//
// Proves (or disproves) that the current geode server handles N concurrent
// independent QUIC streams on ONE connection without wedging.
//
// If it PASSES:  SERVER MULTIPLEXES SAFELY — proceed with the plan.
// If it WEDGES:  SERVER WEDGES ON CONCURRENT STREAMS PER CONN — server must
//
//	be fixed before client multiplexing.
package geode

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"geodedb.com/geode/proto"
	"github.com/quic-go/quic-go"
)

const (
	probeGeodeBinary = "/home/benji/src/devnw/codepros/geode/geode/zig-out/bin/geode"
	probeGoroutines  = 8
	probeDuration    = 20 * time.Second
	probeWedge       = 15 * time.Second
)

// findFreeUDPPort finds a free UDP port in [min, max].
// It binds a UDP socket to probe availability, then releases it.
func findFreeUDPPort(t *testing.T, min, max int) int {
	t.Helper()
	for p := min; p <= max; p++ {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		conn, err := net.ListenPacket("udp4", addr)
		if err == nil {
			conn.Close()
			return p
		}
	}
	t.Fatalf("no free UDP port found in [%d, %d]", min, max)
	return 0
}

// startProbeServer launches a fresh geode server on the given port+metricsPort
// into tmpDir, waits until it is ready, and registers a t.Cleanup to shut it down.
func startProbeServer(t *testing.T, port, metricsPort int, tmpDir string) {
	t.Helper()

	binary := probeGeodeBinary
	if b := os.Getenv("GEODE_BINARY"); b != "" {
		binary = b
	}

	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
	cmd := exec.Command(binary,
		"serve",
		"--listen", listenAddr,
		"--data-dir", tmpDir,
	)
	cmd.Env = append(os.Environ(),
		"GEODE_ADMIN_PASSWORD=seed",
		"GEODE_WAL_SYNC=deferred",
		"GEODE_INDEX_MERGE_KEYS_ONLY=1",
		// REQ-QUIC-HC-030 / server_init.zig: stream multiplexing is DISABLED by
		// default (quic_stream_multiplexing_enabled=false). The server only spawns
		// the streamAcceptorLoop — which services additional streams per QUIC
		// connection — when this is set. Without it, secondary streams are never
		// handled and their queries hang. The client multiplex driver therefore
		// REQUIRES the server to run with this flag.
		"GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1",
		fmt.Sprintf("GEODE_METRICS_PORT=%d", metricsPort),
	)

	// Capture stderr for diagnostics.
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf
	cmd.Stdout = &stderrBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("startProbeServer: failed to start geode binary %q: %v", binary, err)
	}
	t.Logf("probe server PID=%d, listen=%s, data=%s", cmd.Process.Pid, listenAddr, tmpDir)

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		t.Logf("probe server logs:\n%s", stderrBuf.String())
	})

	// Wait until the server is ready — poll the HTTP /health endpoint on the
	// metrics port (faster than QUIC probing, no TLS handshake overhead).
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", metricsPort)
	startedAt := time.Now()
	deadline := startedAt.Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(
			context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", metricsPort),
		)
		if err == nil {
			resp.Close()
			t.Logf("probe server health endpoint reachable after %v (%s)", time.Since(startedAt).Round(time.Millisecond), healthURL)
			// Give the QUIC listener one extra second to stabilise after the
			// HTTP port comes up.
			time.Sleep(time.Second)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("probe server did not become ready within 30s")
}

// probeSession wraps a single QUIC stream with a persistent bufio.Reader so
// that reads across multiple proto messages work correctly (no half-read data loss).
// Each probe goroutine holds one probeSession for its exclusive stream.
type probeSession struct {
	stream *quic.Stream
	br     *bufio.Reader
}

func newProbeSession(stream *quic.Stream) *probeSession {
	return &probeSession{stream: stream, br: bufio.NewReaderSize(stream, 64*1024)}
}

// send writes a length-prefixed proto message to the stream.
func (s *probeSession) send(ctx context.Context, msg *proto.QuicClientMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = s.stream.SetWriteDeadline(dl)
		defer func() { _ = s.stream.SetWriteDeadline(time.Time{}) }()
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data))) //nolint:gosec
	if _, err := s.stream.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := s.stream.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// recv reads one length-prefixed proto message from the stream's persistent bufio.Reader.
func (s *probeSession) recv(ctx context.Context) (*proto.QuicServerMessage, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = s.stream.SetReadDeadline(dl)
	}
	defer func() { _ = s.stream.SetReadDeadline(time.Time{}) }()

	var lenBuf [4]byte
	if _, err := io.ReadFull(s.br, lenBuf[:]); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])
	if msgLen > 16*1024*1024 {
		return nil, fmt.Errorf("message too large: %d", msgLen)
	}
	buf := make([]byte, msgLen)
	if _, err := io.ReadFull(s.br, buf); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read body: %w", err)
	}
	return proto.DecodeQuicServerMessage(buf)
}

// hello performs the HELLO handshake on this session's stream.
func (s *probeSession) hello(ctx context.Context, cfg *Config) (string, error) {
	req := &proto.HelloRequest{
		Username:          cfg.Username,
		Password:          cfg.Password,
		ClientName:        cfg.HelloName,
		ClientVersion:     cfg.HelloVersion,
		WantedConformance: cfg.Conformance,
	}
	if err := s.send(ctx, proto.NewHelloMessage(req)); err != nil {
		return "", fmt.Errorf("hello send: %w", err)
	}
	resp, err := s.recv(ctx)
	if err != nil {
		return "", fmt.Errorf("hello recv: %w", err)
	}
	hr := resp.GetHello()
	if hr == nil {
		return "", fmt.Errorf("hello: expected HELLO response, got %T", resp.GetMsg())
	}
	if !hr.GetSuccess() {
		return "", fmt.Errorf("hello: auth failed: %s", hr.GetErrorMessage())
	}
	return hr.GetGraph(), nil
}

// exec runs a GQL EXECUTE and drains the full server response (schema → page).
func (s *probeSession) exec(ctx context.Context, query string) error {
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{Query: query})
	if err := s.send(ctx, msg); err != nil {
		return fmt.Errorf("exec send %q: %w", query, err)
	}
	resp, err := s.recv(ctx)
	if err != nil {
		return fmt.Errorf("exec recv %q: %w", query, err)
	}
	er := resp.GetExecute()
	if er == nil {
		return fmt.Errorf("exec %q: unexpected response type", query)
	}
	if er.GetError() != nil {
		return fmt.Errorf("exec %q server error %s: %s", query, er.GetError().GetCode(), er.GetError().GetMessage())
	}
	// Drain: if we got schema first, read the data page next.
	if er.GetSchema() != nil {
		if _, err2 := s.recv(ctx); err2 != nil {
			return fmt.Errorf("exec drain page %q: %w", query, err2)
		}
	}
	return nil
}

// queryInt runs a RETURN query and returns the first integer value from the first row.
func (s *probeSession) queryInt(ctx context.Context, query string) (int64, error) {
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{Query: query})
	if err := s.send(ctx, msg); err != nil {
		return 0, fmt.Errorf("queryInt send %q: %w", query, err)
	}
	resp, err := s.recv(ctx)
	if err != nil {
		return 0, fmt.Errorf("queryInt recv %q: %w", query, err)
	}
	er := resp.GetExecute()
	if er == nil {
		return 0, fmt.Errorf("queryInt %q: unexpected response type", query)
	}
	if er.GetError() != nil {
		return 0, fmt.Errorf("queryInt %q server error %s: %s", query, er.GetError().GetCode(), er.GetError().GetMessage())
	}
	// Get the data page — may arrive after schema.
	var pageResp *proto.QuicServerMessage
	if er.GetSchema() != nil {
		pageResp, err = s.recv(ctx)
		if err != nil {
			return 0, fmt.Errorf("queryInt read page %q: %w", query, err)
		}
	} else {
		pageResp = resp
	}
	pr := pageResp.GetExecute()
	if pr == nil {
		return 0, fmt.Errorf("queryInt %q: no execute in page response", query)
	}
	page := pr.GetPage()
	if page == nil {
		return 0, fmt.Errorf("queryInt %q: no page in response", query)
	}
	if len(page.GetRows()) == 0 {
		return 0, nil
	}
	row := page.GetRows()[0]
	if len(row.GetValues()) == 0 {
		return 0, fmt.Errorf("queryInt %q: no values in row", query)
	}
	v := row.GetValues()[0]
	switch x := v.GetKind().(type) {
	case *proto.Value_IntVal:
		return x.IntVal.GetValue(), nil
	case *proto.Value_DoubleVal:
		return int64(x.DoubleVal.GetValue()), nil
	default:
		return 0, fmt.Errorf("queryInt %q: unexpected value kind %T", query, v.GetKind())
	}
}

// TestMultiplexServerProbe is the Task-0 de-risk gate.
//
// It spawns a FRESH geode server, dials ONE QUIC connection, performs HELLO on a
// first stream, then launches probeGoroutines (8) goroutines that each
// OpenStreamSync on that SAME connection and run a sustained loop of:
//
//	MERGE (n:Probe {id:'<unique>'})
//	MERGE (p:Probe {id:'<a>'})-[:R]->(c:Probe {id:'<b>'})
//
// A watchdog fails the test (with a goroutine dump) if total ops stop advancing
// for more than probeWedge (15s).
//
// After the run, asserts every goroutine made real progress and a final
// MATCH (n:Probe) RETURN count(n) matches the number of distinct ids merged.
func TestMultiplexServerProbe(t *testing.T) {
	// ── 1. Spin up a fresh server ────────────────────────────────────────────
	tmpDir := t.TempDir()
	port := findFreeUDPPort(t, 3590, 3699)
	metricsPort := findFreeUDPPort(t, port+1, 3799)
	startProbeServer(t, port, metricsPort, tmpDir)

	cfg := &Config{
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

	// ── 2. Dial ONE QUIC connection ──────────────────────────────────────────
	tlsCfg, err := NewTLSConfig(cfg)
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dialCancel()

	qConn, err := quic.DialAddr(dialCtx, cfg.Addr(), tlsCfg, &quic.Config{
		MaxIdleTimeout:                 quicMaxIdleTimeout,
		KeepAlivePeriod:                quicKeepAlivePeriod,
		InitialStreamReceiveWindow:     quicInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicMaxConnectionReceiveWindow,
		DisablePathMTUDiscovery:        false,
	})
	if err != nil {
		t.Fatalf("dial QUIC: %v", err)
	}
	t.Cleanup(func() { _ = qConn.CloseWithError(0, "test done") })
	dialCancel()

	// ── 3. HELLO on stream 0 ─────────────────────────────────────────────────
	// The server authenticates at the QUIC connection level (not per-stream),
	// so HELLO on one stream authenticates all subsequent streams on this conn.
	helloCtx, helloCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer helloCancel()

	helloRawStream, err := qConn.OpenStreamSync(helloCtx)
	if err != nil {
		t.Fatalf("open HELLO stream: %v", err)
	}
	helloSess := newProbeSession(helloRawStream)
	if _, err := helloSess.hello(helloCtx, cfg); err != nil {
		t.Fatalf("HELLO handshake: %v", err)
	}
	helloCancel()
	// Keep the HELLO stream OPEN for the lifetime of the test. This mirrors the
	// real driver: each pooled transport holds one long-lived stream, and the
	// stream that performed HELLO stays open for the connection's lifetime.
	// Closing it sends a FIN that the server treats as session teardown, which
	// tears down the connection-level auth and closes all sibling streams.
	t.Cleanup(func() { _ = helloRawStream.Close() })

	t.Logf("HELLO done; connection authenticated — spawning %d concurrent stream goroutines", probeGoroutines)

	// ── 4. Run probe goroutines ──────────────────────────────────────────────
	var (
		wg       sync.WaitGroup
		counters [probeGoroutines]atomic.Int64
		total    atomic.Int64
		errsCh   = make(chan error, probeGoroutines*4)
	)

	runCtx, runCancel := context.WithTimeout(context.Background(), probeDuration+10*time.Second)
	defer runCancel()

	deadline := time.Now().Add(probeDuration)

	for g := 0; g < probeGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()

			// Each goroutine opens its own stream on the SHARED qConn and runs
			// queries DIRECTLY — no per-stream HELLO. This mirrors the real driver
			// (conn.go connectPooled / pooledHello): authentication is bound to the
			// QUIC *connection*, performed exactly once on the first stream; every
			// subsequent stream on that connection skips HELLO and sends queries.
			// (Sending a second HELLO on an already-authed conn is rejected by the
			// server with a clean stream close, which is correct behavior.)
			rawStream, err := qConn.OpenStreamSync(runCtx)
			if err != nil {
				errsCh <- fmt.Errorf("goroutine %d: OpenStreamSync: %v", gid, err)
				return
			}
			sess := newProbeSession(rawStream)
			defer rawStream.Close() //nolint:errcheck

			opTimeout := 15 * time.Second
			var localOps int64

			for time.Now().Before(deadline) {
				nodeID := fmt.Sprintf("g%d-op%d", gid, localOps)
				// Op A: MERGE node (idempotent upsert).
				opCtx, opCancel := context.WithTimeout(runCtx, opTimeout)
				mergeQ := fmt.Sprintf("MERGE (n:Probe {id:'%s'})", nodeID)
				if err := sess.exec(opCtx, mergeQ); err != nil {
					opCancel()
					t.Logf("goroutine %d op %d MERGE error: %v", gid, localOps, err)
					errsCh <- fmt.Errorf("goroutine %d: MERGE: %v", gid, err)
					return
				}
				opCancel()

				// Op B: MERGE edge between previous node and current node.
				// On the first op, use a self-edge (prevID == nodeID).
				prevID := nodeID
				if localOps > 0 {
					prevID = fmt.Sprintf("g%d-op%d", gid, localOps-1)
				}
				edgeQ := fmt.Sprintf(
					"MATCH (p:Probe {id:'%s'}),(c:Probe {id:'%s'}) MERGE (p)-[:R]->(c)",
					prevID, nodeID,
				)
				opCtx2, opCancel2 := context.WithTimeout(runCtx, opTimeout)
				if err := sess.exec(opCtx2, edgeQ); err != nil {
					opCancel2()
					t.Logf("goroutine %d op %d EDGE error: %v", gid, localOps, err)
					errsCh <- fmt.Errorf("goroutine %d: EDGE: %v", gid, err)
					return
				}
				opCancel2()

				localOps++
				counters[gid].Store(localOps)
				total.Add(1)
			}
			t.Logf("goroutine %d done: %d op pairs", gid, localOps)
		}(g)
	}

	// ── 5. Watchdog ───────────────────────────────────────────────────────────
	watchdogDone := make(chan struct{})
	var wedged atomic.Bool
	go func() {
		defer close(watchdogDone)
		ticker := time.NewTicker(probeWedge)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				cur := total.Load()
				if cur == last && cur >= 0 {
					// Count how many goroutines are making progress.
					advancing := 0
					for g := 0; g < probeGoroutines; g++ {
						if counters[g].Load() > 0 {
							advancing++
						}
					}
					buf := make([]byte, 2<<20)
					n := runtime.Stack(buf, true)
					t.Logf("WEDGE DETECTED: total ops=%d unchanged for %v, %d/%d goroutines had made progress; goroutine dump:\n%s",
						cur, probeWedge, advancing, probeGoroutines, buf[:n])
					wedged.Store(true)
					runCancel() // unblock goroutines
					return
				}
				last = cur
			}
		}
	}()

	// Wait for workers to finish.
	wg.Wait()
	runCancel()
	<-watchdogDone

	close(errsCh)
	var errs []string
	for e := range errsCh {
		errs = append(errs, e.Error())
	}

	// ── 6. Verdict ────────────────────────────────────────────────────────────
	if wedged.Load() {
		buf := make([]byte, 2<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("SERVER WEDGES ON CONCURRENT STREAMS PER CONN — server must be fixed before client multiplexing.\n"+
			"Wedge goroutine dump:\n%s\nWorker errors: %v", buf[:n], errs)
	}

	if len(errs) > 0 {
		buf := make([]byte, 2<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("SERVER WEDGES ON CONCURRENT STREAMS PER CONN — server must be fixed before client multiplexing.\n"+
			"All goroutines failed with stream errors (server rejected concurrent streams).\n"+
			"Errors:\n  %s\nGoroutine dump:\n%s",
			strings.Join(errs, "\n  "), buf[:n])
	}

	// Per-goroutine progress assertion.
	totalOps := int64(0)
	for g := 0; g < probeGoroutines; g++ {
		ops := counters[g].Load()
		t.Logf("goroutine %d: %d op pairs completed", g, ops)
		if ops == 0 {
			t.Errorf("goroutine %d: made ZERO progress — possible wedge", g)
		}
		totalOps += ops
	}

	t.Logf("total op pairs across all goroutines: %d", totalOps)

	// ── 7. Final node count verification ─────────────────────────────────────
	// Each goroutine merged localOps unique nodes (g<gid>-op<i>).
	// Expected node count = sum of all goroutine op counts (each op creates 1 unique node).
	expectedNodes := totalOps

	finalCtx, finalCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer finalCancel()

	finalStream, err := qConn.OpenStreamSync(finalCtx)
	if err != nil {
		t.Fatalf("open final count stream: %v", err)
	}
	finalSess := newProbeSession(finalStream)
	defer finalStream.Close() //nolint:errcheck

	// Each stream needs its own HELLO (server authenticates per stream).
	if _, err := finalSess.hello(finalCtx, cfg); err != nil {
		t.Fatalf("final count stream HELLO: %v", err)
	}

	got, err := finalSess.queryInt(finalCtx, "MATCH (n:Probe) RETURN count(n)")
	if err != nil {
		t.Fatalf("final count query: %v", err)
	}
	t.Logf("final MATCH (n:Probe) RETURN count(n) = %d, expected = %d", got, expectedNodes)

	if got < expectedNodes {
		t.Errorf("node count mismatch: got %d, expected at least %d (some ops may have been lost)", got, expectedNodes)
	}

	t.Logf("SERVER MULTIPLEXES SAFELY — all %d goroutines made progress on ONE QUIC connection (%d total op pairs, %d nodes)",
		probeGoroutines, totalOps, got)
}

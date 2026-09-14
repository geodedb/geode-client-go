//go:build integration

// integration_health_test.go proves the pool health probe and drain contract
// against a live engine (SONNY-806 / GEO-GO-UP-001, Lane D Task 11):
//
//   - The probe performs REAL network I/O on a long-lived probe stream, and
//     doing so neither disturbs the pooled connection it rides nor tears it
//     down (SONNY-1227).
//   - After the engine process RESTARTS, the pool stops handing out the
//     orphaned pre-restart connection: the probe marks the pool unhealthy
//     within about two intervals, acquire evicts the dead connection, and the
//     next statement succeeds promptly instead of blocking for its full
//     deadline. That is the defect mentats/internal/geodegraph carries a
//     100-line watchdog for.
//   - Drain refuses new work first, then waits, then closes — in that order.
package geode

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// healthGraph is this file's graph. The tenant comes from sharedDSN
// (harnessTenant), so every statement below is tenant-scoped under
// GEODE_STRICT_TENANCY=1 exactly as the rest of the suite is.
const healthGraph = "healthtest"

// healthDSN builds a healthtest DSN with extra parameters appended. A repeated
// key resolves to its last occurrence (see applyQueryParams in config.go), so
// appending an override is safe.
func healthDSN(extra string) string {
	return sharedDSN + "&graph=" + healthGraph + extra
}

// TestIntegration_ProbeIsRealIOAndDoesNotDisturbThePooledConnection is the
// SONNY-1227 de-risk, and the reason the probe stream is long-lived.
//
// geode 0.8.15 tears the whole pooled QUIC connection down when a stream that
// carried a handshake goes away, which is why the driver must never open and
// close a stream just to check liveness. The probe instead opens ONE stream per
// pooled connection and reuses it for every probe, closing it only when the
// connection is being discarded. This test drives many probes through that
// stream, interleaved with real queries, and asserts:
//
//   - the probes performed actual I/O (ProbeCount advanced by one per pooled
//     connection per round — it counts round trips, not calls);
//   - the pooled connection is the SAME object throughout, i.e. nothing was
//     torn down and re-dialed underneath;
//   - queries keep working after every probe.
func TestIntegration_ProbeIsRealIOAndDoesNotDisturbThePooledConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// pool_size=1 makes the identity assertion sharp: there is exactly one
	// pooled QUIC connection, so "same pointer throughout" means the probe
	// never cost us the connection.
	pool, err := NewPool(healthDSN("&pool_size=1&health_probe_ms=0"),
		WithMaxOpenConns(1), WithMaxIdleConns(1))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close() //nolint:errcheck // best effort in cleanup

	var one int64
	if err := pool.QueryRowContext(ctx, "RETURN 1").Scan(&one); err != nil {
		t.Fatalf("warm-up query: %v", err)
	}

	qp := pool.quicPool()
	if qp == nil {
		t.Fatal("pool has no QUIC pool to probe")
	}
	first := onlyPooledConn(t, qp)

	const rounds = 10
	// Counted per pool, not with the process-wide ProbeCount: the rest of the
	// suite has pools of its own probing on their own schedules, so a global
	// counter cannot say anything exact about this one.
	before := qp.probeTrips()
	globalBefore := ProbeCount()
	for i := range rounds {
		if err := qp.Probe(ctx); err != nil {
			t.Fatalf("probe %d: %v", i, err)
		}
		if got := onlyPooledConn(t, qp); got != first {
			t.Fatalf("probe %d replaced the pooled connection: the probe tore it down", i)
		}
		if err := pool.QueryRowContext(ctx, "RETURN ?", int64(i)).Scan(&one); err != nil {
			t.Fatalf("query after probe %d: %v", i, err)
		}
		if one != int64(i) {
			t.Fatalf("query after probe %d returned %d", i, one)
		}
	}

	if got := qp.probeTrips() - before; got != rounds {
		t.Fatalf("the pool performed %d probe round trips over %d rounds on 1 connection, want %d",
			got, rounds, rounds)
	}
	if ProbeCount() <= globalBefore {
		t.Fatal("the process-wide ProbeCount did not advance: no probe performed I/O")
	}

	h := pool.Health(ctx)
	if !h.Healthy {
		t.Fatalf("Health after %d probes = %+v, want Healthy", rounds, h)
	}
	if h.PooledConns != 1 || h.LiveConns != 1 {
		t.Errorf("Health = %+v, want 1 pooled and 1 live connection", h)
	}
	t.Logf("survived %d probe rounds on one pooled connection; health=%+v", rounds, h)
}

// onlyPooledConn returns the pool's single pooled connection, failing if there
// is not exactly one.
func onlyPooledConn(t *testing.T, qp *QUICPool) *pooledConn {
	t.Helper()
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if len(qp.conns) != 1 {
		t.Fatalf("pool holds %d connections, want exactly 1", len(qp.conns))
	}
	return qp.conns[0]
}

// TestIntegration_PoolHealthReportsHealthy is the happy path, and the proof
// that Health answers from a probe rather than from local state: the pool is
// warmed, the probe loop runs, and the status names the connections it
// verified.
func TestIntegration_PoolHealthReportsHealthy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := NewPool(healthDSN("&pool_size=2&health_probe_ms=100"),
		WithMaxOpenConns(2), WithMaxIdleConns(2))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close() //nolint:errcheck // best effort in cleanup

	var one int64
	if err := pool.QueryRowContext(ctx, "RETURN 1").Scan(&one); err != nil {
		t.Fatalf("warm-up query: %v", err)
	}

	h := pool.Health(ctx)
	if !h.Healthy {
		t.Fatalf("Health = %+v, want Healthy", h)
	}
	if h.LastProbeErr != "" {
		t.Errorf("LastProbeErr = %q, want empty", h.LastProbeErr)
	}
	if h.PooledConns < 1 || h.LiveConns != h.PooledConns {
		t.Errorf("Health = %+v, want every pooled connection live", h)
	}
	if time.Since(h.LastProbeAt) > 10*time.Second {
		t.Errorf("LastProbeAt = %v, too stale to answer with", h.LastProbeAt)
	}

	// Health on a cold pool still answers honestly rather than claiming health
	// from nothing: it dials and pings.
	cold, err := NewPool(healthDSN("&health_probe_ms=0"))
	if err != nil {
		t.Fatalf("NewPool (cold): %v", err)
	}
	defer cold.Close() //nolint:errcheck // best effort in cleanup
	if ch := cold.Health(ctx); !ch.Healthy {
		t.Errorf("Health on a cold pool = %+v, want Healthy", ch)
	}
	t.Logf("warm health=%+v", h)
}

// TestIntegration_DrainOrdering asserts the three steps of Drain in order:
// new work is refused, in-flight work is waited for, and only then is
// everything closed.
func TestIntegration_DrainOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := NewPool(healthDSN("&pool_size=4&health_probe_ms=100"),
		WithMaxOpenConns(4), WithMaxIdleConns(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	var one int64
	if err := pool.QueryRowContext(ctx, "RETURN 1").Scan(&one); err != nil {
		t.Fatalf("pre-drain query: %v", err)
	}

	// Hold a statement open across the drain: Rows keep their connection
	// checked out, so Stats().InUse stays 1 until they are closed. Drain must
	// WAIT for that rather than yank the connection away.
	rows, err := pool.QueryContext(ctx, "RETURN 1")
	if err != nil {
		t.Fatalf("in-flight query: %v", err)
	}

	drained := make(chan error, 1)
	go func() {
		drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
		defer drainCancel()
		drained <- pool.Drain(drainCtx)
	}()

	// Step 1 happens before the wait, so a new pin is refused while the drain
	// is still blocked on the in-flight rows.
	deadline := time.Now().Add(10 * time.Second)
	var sessionErr error
	for time.Now().Before(deadline) {
		s, err := pool.Session(ctx)
		if err != nil {
			sessionErr = err
			break
		}
		_ = s.Close()
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(sessionErr, ErrPoolDraining) {
		t.Fatalf("Session during drain = %v, want ErrPoolDraining", sessionErr)
	}

	// Step 2: the drain is still running, because the rows are still open.
	select {
	case err := <-drained:
		_ = rows.Close()
		t.Fatalf("Drain returned (%v) while a statement was still in flight", err)
	case <-time.After(250 * time.Millisecond):
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}

	// Step 3: with nothing in flight the drain finishes and closes everything.
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Drain did not finish after the in-flight statement closed")
	}

	if err := pool.QueryRowContext(ctx, "RETURN 1").Scan(&one); err == nil {
		t.Fatal("query after Drain succeeded, want a closed-pool error")
	}
	if h := pool.Health(ctx); h.Healthy {
		t.Errorf("Health reports healthy after Drain: %+v", h)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
}

// TestIntegration_RecoversFromServerRestart is the regression test for the
// defect this task exists to fix, and the one mentats/internal/geodegraph
// carries a watchdog for.
//
// After the Geode process restarts, the server no longer recognises the
// pre-restart QUIC connection and silently drops its packets. Nothing local
// went wrong, so isQUICConnAlive keeps reporting the connection alive, the
// pool keeps handing it out, and every statement on it blocks for its caller's
// full deadline (quicMaxIdleTimeout is 120s, so it is not the transport that
// rescues them).
//
// The test runs both arms against the SAME restart:
//
//   - control (health_probe_ms=0, i.e. the pre-fix driver): the first
//     post-restart statement must consume most of its deadline;
//   - probed (health_probe_ms=200): the probe must mark the pool unhealthy
//     within about two intervals, the orphaned connection must be evicted, and
//     the first post-restart statement must succeed promptly.
//
// Skipped in manual-server mode (GEODE_TEST_DSN set): only the
// container-managed lane can restart the engine under a live pool.
func TestIntegration_RecoversFromServerRestart(t *testing.T) {
	if testContainer == nil {
		t.Skip("restart test requires the testcontainers-managed engine")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	const probeInterval = 200 * time.Millisecond

	newSingleConnPool := func(name, extra string) *Pool {
		t.Helper()
		p, err := NewPool(healthDSN("&pool_size=1"+extra),
			WithMaxOpenConns(1), WithMaxIdleConns(1))
		if err != nil {
			t.Fatalf("NewPool (%s): %v", name, err)
		}
		t.Cleanup(func() { _ = p.Close() })
		var one int64
		if err := p.QueryRowContext(ctx, "RETURN 1").Scan(&one); err != nil {
			t.Fatalf("warm-up query (%s): %v", name, err)
		}
		return p
	}

	control := newSingleConnPool("control", "&health_probe_ms=0")
	probed := newSingleConnPool("probed", "&health_probe_ms=200")

	qp := probed.quicPool()
	orphaned := onlyPooledConn(t, qp)

	// Let the probe loop complete one successful round first. That is the
	// steady state the fix targets — a pool that has been serving for a while
	// when the server dies — and it is the state the detection bound below
	// describes: the probe stream is already open, so a post-kill probe fails
	// on the small steady-state budget rather than the one-off warmup budget
	// that a brand-new connection's first probe carries.
	waitForFirstProbe(t, qp, 30*time.Second)

	// killedAt is when the engine was CONFIRMED gone, not when the signal was
	// sent: ContainerKill returns once Docker has delivered the signal, and
	// the process lives — and keeps answering probes — for a little longer.
	// Timing detection from the signal would charge the driver for Docker's
	// latency.
	killedAt := killEngine(t, ctx)

	// Watch for the detection from here, concurrently: once the orphaned
	// connection is evicted every later probe records a fresh status too, so
	// reading the status after the fact would time "when the test looked"
	// rather than "when the probe noticed".
	detectedCh := watchForUnhealthy(qp, killedAt, 60*time.Second)

	if err := testContainer.Start(ctx); err != nil {
		t.Fatalf("start container: %v", err)
	}

	// The wait strategy matches the pre-restart log line immediately, so prove
	// the engine is back with a FRESH pool of our own before timing anything.
	waitForEngine(t, ctx)
	t.Logf("engine back after %v", time.Since(killedAt))

	// ── The probed pool: detection, eviction, prompt recovery ──────────────
	//
	// Detection is measured from the kill, not from here: the probe loop runs
	// on its own schedule and will normally have noticed while the engine was
	// still down.
	detected, ok := <-detectedCh
	if !ok {
		t.Fatalf("the probe never reported the pool unhealthy after the kill; last status %+v",
			qp.lastHealth())
	}
	t.Logf("probe marked the pool unhealthy %v after the kill (probe interval %v)",
		detected, probeInterval)
	// Worst case, in the order it happens: the round already in flight when
	// the engine died runs out its budget without being attributable to the
	// kill, the loop waits one interval for the next tick, and that round runs
	// out its own budget. Plus scheduling slack.
	//
	// With the default one-second interval that is a few seconds; the point is
	// that it is a small multiple of the interval and bounded, where before
	// the fix it was unbounded (the control arm below waits out whatever
	// deadline its caller set, and quicMaxIdleTimeout is 120s).
	budget := qp.probeTimeout()
	if want := 2*probeInterval + 2*budget + 400*time.Millisecond; detected > want {
		t.Errorf("probe took %v to notice the kill, want within %v "+
			"(2 intervals of %v plus 2 probe budgets of %v plus slack)",
			detected, want, probeInterval, budget)
	}

	qp.mu.Lock()
	for _, pc := range qp.conns {
		if pc == orphaned {
			qp.mu.Unlock()
			t.Fatal("the orphaned pre-restart connection is still in the pool")
		}
	}
	qp.mu.Unlock()

	probedStart := time.Now()
	probedCtx, probedCancel := context.WithTimeout(ctx, statementDeadline)
	var one int64
	probedErr := probed.QueryRowContext(probedCtx, "RETURN 1").Scan(&one)
	probedElapsed := time.Since(probedStart)
	probedCancel()
	if probedErr != nil {
		t.Fatalf("first post-restart statement on the probed pool: %v (after %v)", probedErr, probedElapsed)
	}
	if probedElapsed > statementDeadline/2 {
		t.Errorf("first post-restart statement took %v of a %v deadline: it stalled",
			probedElapsed, statementDeadline)
	}

	// ── The control: the pre-fix behaviour, measured ───────────────────────
	controlStart := time.Now()
	controlCtx, controlCancel := context.WithTimeout(ctx, statementDeadline)
	controlErr := control.QueryRowContext(controlCtx, "RETURN 1").Scan(&one)
	controlElapsed := time.Since(controlStart)
	controlCancel()

	t.Logf("post-restart first statement: probed=%v (err=%v), control=%v (err=%v)",
		probedElapsed, probedErr, controlElapsed, controlErr)

	if controlElapsed < statementDeadline/2 {
		t.Fatalf("the control pool recovered in %v without a probe: "+
			"the test no longer demonstrates the defect it guards", controlElapsed)
	}
	if controlElapsed <= probedElapsed {
		t.Fatalf("control %v <= probed %v: the probe bought nothing", controlElapsed, probedElapsed)
	}

	// The probed pool keeps serving afterwards; recovery is not a one-shot.
	for i := range 5 {
		if err := probed.QueryRowContext(ctx, "RETURN ?", int64(i)).Scan(&one); err != nil {
			t.Fatalf("statement %d after recovery: %v", i, err)
		}
	}
	if h := probed.Health(ctx); !h.Healthy {
		t.Errorf("Health after recovery = %+v, want Healthy", h)
	}
}

// statementDeadline is the per-statement budget the restart test gives both
// arms. It has to be long enough that "consumed its whole deadline" is
// unmistakably a stall rather than a slow round trip, and short enough that
// the test does not sit on it.
const statementDeadline = 6 * time.Second

// waitForEngine blocks until a FRESH pool can query the restarted engine, so
// the restart test measures the old pool's recovery and not the engine's
// startup. It uses its own pool because the point is to reach the engine
// without any pre-restart connection in the way.
func waitForEngine(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("geode", healthDSN("&health_probe_ms=0"))
		if err != nil {
			t.Fatalf("sql.Open: %v", err)
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		_ = db.Close()
		if lastErr == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("engine did not come back within 90s; last error: %v", lastErr)
}

// watchForUnhealthy reports how long after since the probe loop FIRST recorded
// a failure. It polls from the moment it is started, because the recorded
// status is overwritten by every later probe: reading it once after the fact
// would date the most recent probe, not the one that noticed.
//
// It reads the recorded probe status rather than Pool.Health on purpose: once
// the orphaned connection is evicted the pool is empty, and Health answers an
// empty pool with a fresh dial — correct, and it would hide the very detection
// this measures.
//
// The channel is closed without a value if the budget expires.
func watchForUnhealthy(qp *QUICPool, since time.Time, budget time.Duration) <-chan time.Duration {
	ch := make(chan time.Duration, 1)
	go func() {
		defer close(ch)
		deadline := time.Now().Add(budget)
		for time.Now().Before(deadline) {
			h := qp.lastHealth()
			if !h.Healthy && h.LastProbeErr != "" && h.LastProbeAt.After(since) {
				ch <- h.LastProbeAt.Sub(since)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return ch
}

// waitForFirstProbe blocks until the pool's probe loop has recorded a healthy
// round, i.e. the probe stream is open and the server has answered on it.
func waitForFirstProbe(t *testing.T, qp *QUICPool, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if h := qp.lastHealth(); h.Healthy && h.LiveConns > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the probe loop never recorded a healthy round within %v; last status %+v",
		budget, qp.lastHealth())
}

// killEngine SIGKILLs the engine process.
//
// The kill has to be ungraceful, and that is the whole point of the defect. A
// docker stop gives the engine a SIGTERM and it closes its QUIC connections
// cleanly; quic-go cancels the connection context on the CONNECTION_CLOSE and
// even the unprobed pool recovers at once (measured: 37ms). A crash, an OOM
// kill, a rollout that pulls the process out from under its sockets, a network
// partition — none of those send anything, and THAT is the state this task
// exists for: the client sees a connection that is locally perfect and
// remotely forgotten.
// It returns the moment the container was observed to have exited, so a caller
// can time detection from when the engine actually stopped answering.
func killEngine(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatalf("docker client: %v", err)
	}
	defer cli.Close() //nolint:errcheck // best effort in cleanup

	if _, err := cli.ContainerKill(ctx, testContainer.GetContainerID(),
		client.ContainerKillOptions{Signal: "SIGKILL"}); err != nil {
		t.Fatalf("SIGKILL the engine: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		state, err := testContainer.State(ctx)
		if err != nil {
			t.Fatalf("container state: %v", err)
		}
		if !state.Running {
			return time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the engine container was still running 30s after SIGKILL")
	return time.Time{}
}

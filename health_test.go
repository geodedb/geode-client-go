package geode

// health_test.go drives the pool health probe and the drain state machine
// without a server (SONNY-806 / GEO-GO-UP-001, Lane D Task 11).
//
// The defect under test: isQUICConnAlive only reads the local quic.Conn
// context, so after a Geode restart the pool keeps handing out an orphaned
// pre-restart connection and every operation on it blocks for its caller's
// full deadline. The live half of the proof is in integration_health_test.go
// (which restarts the engine container); this file pins the parts that need
// no network — fail-closed semantics, the DSN parameter, the classification of
// probe errors, the drain state machine and the probe goroutine's lifetime.
//
//	                 first acquire                 Drain / Close
//	  (no probe) ───────────────────▶ probing ──────────────────▶ stopped
//	                                    │  ▲
//	                     probe fails    │  │ probe succeeds
//	                                    ▼  │
//	                              conn evicted, never re-handed out

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fail-closed semantics
// ─────────────────────────────────────────────────────────────────────────────

func TestHealthStatus_ZeroValueIsUnhealthy(t *testing.T) {
	var h HealthStatus
	if h.Healthy {
		t.Error("zero HealthStatus reports Healthy — health must fail closed")
	}
}

func TestPool_HealthOnNilPool(t *testing.T) {
	var p *Pool
	h := p.Health(context.Background())
	if h.Healthy {
		t.Error("nil pool reported healthy")
	}
	if h.LastProbeErr == "" {
		t.Error("nil pool reported no probe error")
	}
	if h.LastProbeAt.IsZero() {
		t.Error("nil pool reported no probe time")
	}
}

func TestPool_HealthOnZeroValuePool(t *testing.T) {
	var p Pool
	h := p.Health(context.Background())
	if h.Healthy {
		t.Errorf("zero-value pool reported healthy: %+v", h)
	}
}

func TestPool_DrainOnNilPoolIsNoError(t *testing.T) {
	var p *Pool
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain on nil pool: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DSN parameter
// ─────────────────────────────────────────────────────────────────────────────

func TestParseDSN_HealthProbeInterval(t *testing.T) {
	cfg, err := ParseDSN("quic://h:3141?health_probe_ms=250")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if cfg.HealthProbeInterval != 250*time.Millisecond {
		t.Errorf("HealthProbeInterval = %v, want 250ms", cfg.HealthProbeInterval)
	}

	def, err := ParseDSN("quic://h:3141")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if def.HealthProbeInterval != DefaultHealthProbeInterval {
		t.Errorf("default HealthProbeInterval = %v, want %v", def.HealthProbeInterval, DefaultHealthProbeInterval)
	}

	off, err := ParseDSN("quic://h:3141?health_probe_ms=0")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if off.HealthProbeInterval != 0 {
		t.Errorf("health_probe_ms=0 gave %v, want 0 (disabled)", off.HealthProbeInterval)
	}

	// An empty DSN still carries the default: ParseDSN returns early for it.
	empty, err := ParseDSN("")
	if err != nil {
		t.Fatalf("ParseDSN(\"\"): %v", err)
	}
	if empty.HealthProbeInterval != DefaultHealthProbeInterval {
		t.Errorf("empty DSN HealthProbeInterval = %v, want %v",
			empty.HealthProbeInterval, DefaultHealthProbeInterval)
	}
}

func TestParseDSN_HealthProbeIntervalRejectsGarbage(t *testing.T) {
	for _, val := range []string{"-1", "abc", "1.5", ""} {
		if _, err := ParseDSN("quic://h:3141?health_probe_ms=" + val); err == nil {
			t.Errorf("health_probe_ms=%q was accepted", val)
		}
	}
}

func TestConfig_StringRoundTripsHealthProbeInterval(t *testing.T) {
	for _, want := range []time.Duration{0, 250 * time.Millisecond, DefaultHealthProbeInterval} {
		cfg := &Config{
			Transport:           TransportQUIC,
			Host:                "h",
			Port:                "3141",
			HealthProbeInterval: want,
		}
		back, err := ParseDSN(cfg.FormatDSN())
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", cfg.FormatDSN(), err)
		}
		if back.HealthProbeInterval != want {
			t.Errorf("round trip of %v gave %v (dsn %q)", want, back.HealthProbeInterval, cfg.FormatDSN())
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Probe error classification: a health surface must not leak topology
// ─────────────────────────────────────────────────────────────────────────────

func TestClassifyProbeErr_IsStaticAndCarriesNoTopology(t *testing.T) {
	cases := []error{
		context.DeadlineExceeded,
		context.Canceled,
		io.EOF,
		&TransportError{Op: "read_length", Addr: "geode-prod-7.internal:3141", cause: io.EOF},
		errors.New("dial tcp 10.1.2.3:3141: connection refused"),
		errProbeUnexpectedFrame,
	}
	for _, err := range cases {
		got := classifyProbeErr(err)
		if got == "" {
			t.Errorf("classifyProbeErr(%v) = \"\"", err)
		}
		for _, leak := range []string{"10.1.2.3", "geode-prod-7", "3141", "internal"} {
			if contains(got, leak) {
				t.Errorf("classifyProbeErr(%v) = %q leaks %q", err, got, leak)
			}
		}
	}
	if classifyProbeErr(nil) != "" {
		t.Errorf("classifyProbeErr(nil) = %q, want \"\"", classifyProbeErr(nil))
	}
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) &&
		func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}()
}

// ─────────────────────────────────────────────────────────────────────────────
// Drain: new work is refused, the pool closes, the state machine is idempotent
// ─────────────────────────────────────────────────────────────────────────────

func TestQUICPool_AcquireRefusesWhileDraining(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)
	qp.draining.Store(true)

	if _, err := qp.acquire(context.Background()); !errors.Is(err, ErrPoolDraining) {
		t.Fatalf("acquire while draining = %v, want ErrPoolDraining", err)
	}
	// The admission slot must come back, or a drain that is later abandoned
	// would have silently shrunk the pool's stream budget.
	if got := len(qp.slotsCh); got != cap(qp.slotsCh) {
		t.Fatalf("admission slots after a refused acquire = %d, want %d", got, cap(qp.slotsCh))
	}
}

func TestQUICPool_DrainIsIdempotent(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)
	ctx := context.Background()
	if err := qp.Drain(ctx); err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	if err := qp.Drain(ctx); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if _, err := qp.acquire(ctx); err == nil {
		t.Fatal("acquire succeeded after Drain")
	}
}

func TestQUICPool_DrainReportsCtxErrWhileStreamsAreStillInFlight(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)
	pc := &pooledConn{}
	pc.active = 1
	qp.conns = append(qp.conns, pc)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := qp.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain with a stream in flight = %v, want DeadlineExceeded", err)
	}
	// Closed either way: Drain never leaves a half-drained pool behind.
	if !qp.closed {
		t.Fatal("Drain left the pool open after its deadline expired")
	}
}

func TestPool_DrainRefusesNewSessions(t *testing.T) {
	p, _ := newSessionTestPool(t, 2)
	p.quic = NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)

	s, err := p.Session(context.Background())
	if err != nil {
		t.Fatalf("Session before Drain: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The point of the sentinel: a caller that pins during a drain is told so
	// immediately instead of blocking on a connection that will never come.
	if _, err := p.Session(context.Background()); !errors.Is(err, ErrPoolDraining) {
		t.Fatalf("Session during drain = %v, want ErrPoolDraining", err)
	}
	if h := p.Health(context.Background()); h.Healthy {
		t.Errorf("Health reports healthy after Drain: %+v", h)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Eviction: a probe-failed connection is never handed out again
// ─────────────────────────────────────────────────────────────────────────────

func TestPruneDeadLocked_DropsProbeFailedConns(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 4)
	live, orphaned := &pooledConn{}, &pooledConn{}
	orphaned.dead.Store(true)
	qp.conns = []*pooledConn{live, orphaned}

	qp.mu.Lock()
	qp.pruneDeadLocked()
	qp.mu.Unlock()

	if len(qp.conns) != 1 || qp.conns[0] != live {
		t.Fatalf("pruneDeadLocked kept %d connections, want only the live one", len(qp.conns))
	}
}

func TestQUICPool_AcquireNeverReturnsAProbeFailedConn(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 1)
	orphaned := &pooledConn{}
	orphaned.dead.Store(true)
	qp.conns = []*pooledConn{orphaned}

	// maxSize is 1 and the only pooled connection is dead, so acquire must
	// drop it and try to dial rather than hand it back. The dial fails (no
	// server in the unit lane) — the assertion is that the caller did NOT get
	// the orphaned connection.
	pc, err := qp.acquire(context.Background())
	if pc == orphaned {
		t.Fatal("acquire handed out a connection that failed its liveness probe")
	}
	if err == nil {
		t.Fatalf("acquire unexpectedly succeeded without a server: %v", pc)
	}
}

func TestPooledConn_IsDead(t *testing.T) {
	var pc pooledConn
	if pc.isDead() {
		t.Error("a fresh pooledConn reports dead")
	}
	pc.dead.Store(true)
	if !pc.isDead() {
		t.Error("a probe-failed pooledConn reports live")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The probe goroutine: lazily started, stopped by Drain/Close, never leaked
// ─────────────────────────────────────────────────────────────────────────────

func TestQUICPool_ProbeLoopIsNotStartedWhenDisabled(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141", HealthProbeInterval: 0}, 2)
	qp.startProbeLoop()
	if qp.probeStarted.Load() {
		t.Fatal("health_probe_ms=0 still started a probe goroutine")
	}
	if err := qp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestQUICPool_ProbeLoopStopsAndLeaksNoGoroutine(t *testing.T) {
	baseline := settledGoroutineCount()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		qp := NewQUICPool(&Config{
			Host: constLocalhost, Port: "3141",
			HealthProbeInterval: time.Millisecond,
		}, 2)
		qp.startProbeLoop()
		if !qp.probeStarted.Load() {
			t.Fatal("startProbeLoop did not start the probe goroutine")
		}
		// Started lazily and exactly once, however many callers ask.
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); qp.startProbeLoop() }()
		}
		wg.Wait()

		if err := qp.Drain(context.Background()); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		// Drain waits for the loop, so the goroutine is already gone here.
		if err := qp.Close(); err != nil {
			t.Fatalf("Close after Drain: %v", err)
		}
	}

	after := settledGoroutineCount()
	if after > baseline+2 {
		t.Fatalf("goroutines grew from %d to %d over %d probe-loop lifecycles: leak",
			baseline, after, rounds)
	}
	t.Logf("goroutines: baseline=%d after %d probe-loop lifecycles=%d", baseline, rounds, after)
}

// settledGoroutineCount reads runtime.NumGoroutine once the count has stopped
// moving, so a goroutine still winding down is not counted as a leak.
func settledGoroutineCount() int {
	last := -1
	for i := 0; i < 100; i++ {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
		if n := runtime.NumGoroutine(); n == last {
			return n
		} else { //nolint:revive // the else keeps last assigned on every path
			last = n
		}
	}
	return runtime.NumGoroutine()
}

// TestQUICPool_ProbeNeverRunsConcurrentlyWithItself asserts the serialisation
// that keeps a probe from opening two probe streams on one pooled connection.
func TestQUICPool_ProbeNeverRunsConcurrentlyWithItself(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)

	var mu sync.Mutex
	var inFlight, maxInFlight int
	qp.probeHook = func() {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = qp.Probe(context.Background()) }()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("%d probes ran concurrently, want 1", maxInFlight)
	}
}

// TestQUICPool_ProbeOnAnEmptyPoolIsNotHealthy asserts the fail-closed rule at
// the pool level: with nothing pooled there is nothing a probe verified, so
// the recorded status must not claim health.
func TestQUICPool_ProbeOnAnEmptyPoolIsNotHealthy(t *testing.T) {
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)
	if err := qp.Probe(context.Background()); !errors.Is(err, errNoPooledConns) {
		t.Fatalf("Probe on an empty pool = %v, want errNoPooledConns", err)
	}
	h := qp.lastHealth()
	if h.Healthy {
		t.Errorf("empty pool recorded Healthy: %+v", h)
	}
	if h.LastProbeErr == "" {
		t.Error("empty pool recorded no probe error")
	}
	if h.LastProbeAt.IsZero() {
		t.Error("empty pool recorded no probe time")
	}
	// And it did so without touching the network: there was nothing to touch.
	if n := qp.probeTrips(); n != 0 {
		t.Errorf("an empty pool performed %d probe round trips, want 0", n)
	}
}

// TestPool_CloseStopsTheProbeLoop asserts Close owns the probe goroutine as
// Drain does. Before the probe existed the QUIC pool was simply dropped on
// Close; a goroutine cannot be dropped.
func TestPool_CloseStopsTheProbeLoop(t *testing.T) {
	baseline := settledGoroutineCount()

	const rounds = 10
	for range rounds {
		p, _ := newSessionTestPool(t, 1)
		p.quic = NewQUICPool(&Config{
			Host: constLocalhost, Port: "3141",
			HealthProbeInterval: time.Millisecond,
		}, 1)
		p.quic.startProbeLoop()

		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !p.quic.closed {
			t.Fatal("Pool.Close left the QUIC pool open")
		}
		if err := p.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}

	if after := settledGoroutineCount(); after > baseline+2 {
		t.Fatalf("goroutines grew from %d to %d over %d Close cycles: leak", baseline, after, rounds)
	}
}

// TestPool_HealthAfterCloseIsUnhealthy pins the other fail-closed edge: Close
// (unlike Drain) leaves the last recorded status healthy, and Health must not
// answer with it.
func TestPool_HealthAfterCloseIsUnhealthy(t *testing.T) {
	p, _ := newSessionTestPool(t, 1)
	p.quic = NewQUICPool(&Config{
		Host: constLocalhost, Port: "3141",
		HealthProbeInterval: time.Second,
	}, 1)
	p.quic.storeHealth(HealthStatus{Healthy: true, PooledConns: 1, LiveConns: 1, LastProbeAt: time.Now()})

	if h := p.Health(context.Background()); !h.Healthy {
		t.Fatalf("Health before Close = %+v, want the recorded healthy status", h)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h := p.Health(context.Background())
	if h.Healthy {
		t.Errorf("Health after Close = %+v, want unhealthy", h)
	}
	if h.LastProbeErr == "" {
		t.Error("Health after Close reported no reason")
	}
}

// TestPooledConn_CondemnedOnlyByConsecutiveFailures pins the strike rule. A
// single failed probe must not evict: under load a probe can lose its race
// with a saturated engine, and eviction closes the connection with whatever
// streams are live on it. Measured, evicting on one failure made
// TestLoad_PreparedStatementStorm fail 2 runs in 5.
func TestPooledConn_CondemnedOnlyByConsecutiveFailures(t *testing.T) {
	var pc pooledConn

	for i := 1; i < probeStrikes; i++ {
		if pc.recordProbeFailure() {
			t.Fatalf("condemned after %d failure(s), want %d", i, probeStrikes)
		}
		if pc.isDead() {
			t.Fatalf("marked dead after %d failure(s)", i)
		}
	}
	if !pc.recordProbeFailure() {
		t.Fatalf("not condemned after %d consecutive failures", probeStrikes)
	}
	if !pc.isDead() {
		t.Fatal("condemned but not marked dead")
	}

	// The strikes must be CONSECUTIVE: a success in between clears them.
	var ok pooledConn
	for range probeStrikes * 3 {
		if ok.recordProbeFailure() {
			t.Fatal("condemned despite a success between every failure")
		}
		ok.recordProbeSuccess()
	}
	if ok.isDead() {
		t.Fatal("a connection that answered between failures was marked dead")
	}
}

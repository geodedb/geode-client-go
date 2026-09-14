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

	"geodedb.com/geode/proto"
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

// ─────────────────────────────────────────────────────────────────────────────
// The probe stream is never closed on a connection that stays in service
// (SONNY-1227)
// ─────────────────────────────────────────────────────────────────────────────

// fakeProbeStream stands in for a probe stream, so the probe state machine can
// be driven without a live QUIC connection. It records whether it was CLOSED —
// the assertion the whole SONNY-1227 rule rests on — and the deadline every
// round trip was given, which is how the per-stream warmup budget is checked.
type fakeProbeStream struct {
	mu sync.Mutex

	closed  bool
	sendErr error
	recvErr error
	nonPing bool
	refuse  bool

	// budgets records time.Until(deadline) as each SendProto saw it, i.e. the
	// budget probeConn gave that round trip.
	budgets []time.Duration
}

func (f *fakeProbeStream) SendProto(ctx context.Context, _ *proto.QuicClientMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		f.budgets = append(f.budgets, time.Until(deadline))
	}
	if f.closed {
		return ErrClosed
	}
	return f.sendErr
}

func (f *fakeProbeStream) ReceiveProto(_ context.Context) (*proto.QuicServerMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrClosed
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	if f.nonPing {
		return &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Begin{Begin: &proto.BeginResponse{}},
		}, nil
	}
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Ping{Ping: &proto.PingResponse{Ok: !f.refuse}},
	}, nil
}

func (f *fakeProbeStream) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	return nil, false, nil
}

func (f *fakeProbeStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeProbeStream) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeProbeStream) RemoteClosed() bool { return false }
func (f *fakeProbeStream) Addr() string       { return "fake://probe" }

// fail makes the next round trip on this stream fail the way a probe that lost
// its race with the server does: the send goes nowhere.
func (f *fakeProbeStream) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendErr = err
}

func (f *fakeProbeStream) lastBudget(t *testing.T) time.Duration {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.budgets) == 0 {
		t.Fatal("the probe never gave this stream a deadline")
	}
	return f.budgets[len(f.budgets)-1]
}

// probeStreamFactory hands out fakeProbeStreams and remembers every one it
// opened, so a test can assert which streams were closed and which were not.
type probeStreamFactory struct {
	mu     sync.Mutex
	opened []*fakeProbeStream
}

func (pf *probeStreamFactory) open(context.Context) (Transport, error) {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	f := &fakeProbeStream{}
	pf.opened = append(pf.opened, f)
	return f, nil
}

func (pf *probeStreamFactory) count() int {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return len(pf.opened)
}

func (pf *probeStreamFactory) nth(t *testing.T, i int) *fakeProbeStream {
	t.Helper()
	pf.mu.Lock()
	defer pf.mu.Unlock()
	if i >= len(pf.opened) {
		t.Fatalf("the probe opened %d streams, wanted at least %d", len(pf.opened), i+1)
	}
	return pf.opened[i]
}

func (pf *probeStreamFactory) current(t *testing.T) *fakeProbeStream {
	t.Helper()
	pf.mu.Lock()
	n := len(pf.opened)
	pf.mu.Unlock()
	return pf.nth(t, n-1)
}

// newProbeTestPool builds a pool holding one authenticated connection whose
// probe stream comes from a factory instead of a QUIC connection.
//
// The probe interval is 0 on purpose: it keeps the background probe loop from
// starting (acquire would start it) so nothing races the assertions, and it
// leaves probeTimeout at its minProbeTimeout floor, which is the steady-state
// budget these tests compare against.
func newProbeTestPool(t *testing.T) (*QUICPool, *pooledConn, *probeStreamFactory) {
	t.Helper()
	qp := NewQUICPool(&Config{Host: constLocalhost, Port: "3141"}, 2)
	pf := &probeStreamFactory{}
	pc := &pooledConn{addr: "fake://probe", probeOpener: pf.open}
	pc.markAuthed("")
	qp.mu.Lock()
	qp.conns = append(qp.conns, pc)
	qp.mu.Unlock()
	t.Cleanup(func() { _ = qp.Close() })
	return qp, pc, pf
}

// pooledContains reports whether the pool still holds pc.
func pooledContains(qp *QUICPool, pc *pooledConn) bool {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	for _, c := range qp.conns {
		if c == pc {
			return true
		}
	}
	return false
}

// TestQUICPool_FailedProbeNeverClosesTheStreamOfALiveConn is the SONNY-1227
// regression, and the reason a failed probe abandons its stream.
//
// At strike 1 the connection is NOT evicted: it stays in the pool and keeps
// serving live request streams. Closing the probe stream on it — which the
// first cut of this code did on every send/receive/desync failure — is exactly
// what geode 0.8.15 answers by tearing the whole pooled QUIC connection down,
// so a single slow probe would have caused the outage the probe exists to
// detect. The stream must be dropped instead, and the next probe must open a
// fresh one.
func TestQUICPool_FailedProbeNeverClosesTheStreamOfALiveConn(t *testing.T) {
	ctx := context.Background()
	qp, pc, pf := newProbeTestPool(t)

	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("first probe: %v", err)
	}
	first := pf.current(t)

	first.fail(errors.New("probe lost its race"))
	if err := qp.Probe(ctx); err == nil {
		t.Fatal("probe with a failing stream returned nil")
	}

	if first.IsClosed() {
		t.Fatal("the failed probe CLOSED its stream on a connection that is still pooled: " +
			"geode 0.8.15 answers that by tearing the whole connection down (SONNY-1227)")
	}
	if !pooledContains(qp, pc) {
		t.Fatal("one failed probe evicted the connection; probeStrikes says it takes two")
	}
	if pc.isDead() {
		t.Fatal("one failed probe condemned the connection")
	}
	if got := pc.abandonedProbes(); got != 1 {
		t.Fatalf("abandoned probe streams = %d, want 1", got)
	}

	// The connection is still in service: acquire hands it out.
	got, err := qp.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after a failed probe: %v", err)
	}
	if got != pc {
		t.Fatal("acquire returned a different connection after one failed probe")
	}
	qp.release(got)

	// The next probe opens a FRESH stream and succeeds on it.
	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("probe after the failure: %v", err)
	}
	if pf.count() != 2 {
		t.Fatalf("the probe opened %d streams, want 2 (one fresh one after the failure)", pf.count())
	}
	if pf.current(t) == first {
		t.Fatal("the probe reused the stream it had already failed on")
	}
	if first.IsClosed() {
		t.Fatal("the abandoned stream was closed while its connection kept serving")
	}
	if h := qp.lastHealth(); !h.Healthy || h.LiveConns != 1 {
		t.Fatalf("health after recovery = %+v, want healthy with 1 live connection", h)
	}
}

// TestQUICPool_EvictionClosesEveryProbeStream is the other half of the rule:
// the streams a probe walked away from are not leaked, they are closed when
// the connection they ride is closed — which is the one moment closing them is
// safe.
func TestQUICPool_EvictionClosesEveryProbeStream(t *testing.T) {
	ctx := context.Background()
	qp, pc, pf := newProbeTestPool(t)

	for i := range probeStrikes {
		if err := qp.Probe(ctx); err != nil {
			t.Fatalf("probe %d (opening the stream): %v", i, err)
		}
		pf.current(t).fail(errors.New("peer did not answer"))
		if err := qp.Probe(ctx); err == nil {
			t.Fatalf("probe %d with a failing stream returned nil", i)
		}
	}

	if !pc.isDead() {
		t.Fatalf("%d consecutive failed probes did not condemn the connection", probeStrikes)
	}
	if pooledContains(qp, pc) {
		t.Fatal("a condemned connection is still in the pool")
	}
	if pf.count() != probeStrikes {
		t.Fatalf("the probe opened %d streams, want %d", pf.count(), probeStrikes)
	}
	for i := range pf.count() {
		if !pf.nth(t, i).IsClosed() {
			t.Errorf("probe stream %d was not closed when its connection was evicted", i)
		}
	}
	if got := pc.abandonedProbes(); got != 0 {
		t.Errorf("evicted connection still holds %d abandoned streams", got)
	}
}

// TestQUICPool_AbandonedProbeStreamsAreBounded pins the bound that makes
// "never close a live connection's probe stream" affordable.
//
// A connection that fails and recovers over and over leaves one stream behind
// per failure. Without a bound that is one leaked server-side stream per
// failure, for the life of the connection; with it, the connection is
// condemned once it has abandoned probeStrikes of them.
func TestQUICPool_AbandonedProbeStreamsAreBounded(t *testing.T) {
	ctx := context.Background()
	qp, pc, pf := newProbeTestPool(t)

	for round := range probeStrikes * 4 {
		if pc.isDead() {
			break
		}
		if err := qp.Probe(ctx); err != nil {
			t.Fatalf("round %d: opening probe: %v", round, err)
		}
		pf.current(t).fail(errors.New("slow moment"))
		if err := qp.Probe(ctx); err == nil {
			t.Fatalf("round %d: failing probe returned nil", round)
		}
		// A success between failures clears the strike count, so only the
		// abandoned-stream bound can ever condemn this connection.
		pc.recordProbeSuccess()
		if got := pc.abandonedProbes(); got > probeStrikes {
			t.Fatalf("round %d: %d abandoned probe streams, bound is %d", round, got, probeStrikes)
		}
	}

	if !pc.isDead() {
		t.Fatalf("a connection that abandoned %d streams was never condemned", pc.abandonedProbes())
	}
	if pooledContains(qp, pc) {
		t.Fatal("the connection that hit the abandoned-stream bound is still pooled")
	}
}

// TestQUICPool_AReplacementProbeStreamIsBudgetedForItsOwnSetup pins the second
// half of the eviction bug: stream setup is paid per STREAM, not per
// connection.
//
// The server materialises a probe stream when its first frame arrives (~70ms
// against 0.8.15). A connection whose stream was abandoned by a failure opens
// a replacement, and budgeting that replacement as a bare round trip (100ms at
// the floor) makes a second failure likely — which is strike 2, and a healthy
// connection evicted. The replacement gets probeStreamSetup on top instead.
func TestQUICPool_AReplacementProbeStreamIsBudgetedForItsOwnSetup(t *testing.T) {
	ctx := context.Background()
	qp, pc, pf := newProbeTestPool(t)

	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("first probe: %v", err)
	}
	first := pf.current(t)
	if got := first.lastBudget(t); got <= maxProbeTimeout {
		t.Fatalf("the first probe on a new stream got %v, want the warmup budget (%v)",
			got, probeWarmupTimeout)
	}

	// A second probe on the SAME stream is a bare round trip.
	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("second probe: %v", err)
	}
	if got := first.lastBudget(t); got > qp.probeTimeout() {
		t.Fatalf("a reused probe stream got %v, want the steady-state budget (%v)",
			got, qp.probeTimeout())
	}

	// Fail it, so the next probe has to open a replacement stream.
	first.fail(errors.New("peer did not answer"))
	if err := qp.Probe(ctx); err == nil {
		t.Fatal("failing probe returned nil")
	}
	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("probe on the replacement stream: %v", err)
	}
	replacement := pf.current(t)
	if replacement == first {
		t.Fatal("the probe reused the abandoned stream")
	}
	// The budget is read as the deadline the round trip was given, so it is a
	// hair under the nominal value; what matters is that it carries a whole
	// stream setup on top of the round trip.
	want := qp.probeTimeout() + probeStreamSetup
	if got := replacement.lastBudget(t); got > want || got < want-50*time.Millisecond {
		t.Fatalf("the replacement stream got %v, want ~%v (one round trip plus one stream setup): "+
			"stream setup is paid per stream, and paying it out of a %v budget is what evicted healthy connections",
			got, want, qp.probeTimeout())
	}
	if pc.isDead() {
		t.Fatal("the connection was condemned despite answering on its replacement stream")
	}
}

// TestQUICPool_CallerCancellationNeverCondemnsAConnection pins the rule that
// keeps Pool.Health harmless: a probe that failed because the CALLER's context
// ended is reported, but it is not evidence about the connection.
//
// Without it, a caller passing a cancelled or very short context to Health
// scores a strike against EVERY connection — and takes its probe stream with
// it — so two such calls condemn the whole pool.
func TestQUICPool_CallerCancellationNeverCondemnsAConnection(t *testing.T) {
	qp, pc, pf := newProbeTestPool(t)

	// Warm the stream so there is one to disturb.
	if err := qp.Probe(context.Background()); err != nil {
		t.Fatalf("warm-up probe: %v", err)
	}
	stream := pf.current(t)

	for i := range probeStrikes + 2 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := qp.Probe(ctx)
		if err == nil {
			t.Fatalf("probe %d on a cancelled context returned nil", i)
		}
		if pc.isDead() {
			t.Fatalf("probe %d on a cancelled context condemned the connection", i)
		}
		if got := pc.strikes.Load(); got != 0 {
			t.Fatalf("probe %d on a cancelled context recorded %d strike(s)", i, got)
		}
		if got := pc.abandonedProbes(); got != 0 {
			t.Fatalf("probe %d on a cancelled context abandoned %d stream(s)", i, got)
		}
		if stream.IsClosed() {
			t.Fatalf("probe %d on a cancelled context closed the probe stream", i)
		}
	}

	if !pooledContains(qp, pc) {
		t.Fatal("cancelled Health calls evicted the connection")
	}
	h := qp.lastHealth()
	if h.Healthy {
		t.Errorf("a cancelled probe reported healthy: %+v", h)
	}
	if h.LastProbeErr == "" {
		t.Error("a cancelled probe reported no reason")
	}

	// And the connection is still probe-able afterwards, on the same stream.
	if err := qp.Probe(context.Background()); err != nil {
		t.Fatalf("probe after the cancellations: %v", err)
	}
	if pf.count() != 1 {
		t.Fatalf("the cancellations cost %d extra probe streams", pf.count()-1)
	}
}

// TestQUICPool_UnauthedConnsAreNotCountedLive pins the reporting half: a
// connection the probe SKIPS because no HELLO has run on it yet was not
// verified, so it cannot count as live — and a pool of nothing but skipped
// connections is not healthy.
func TestQUICPool_UnauthedConnsAreNotCountedLive(t *testing.T) {
	ctx := context.Background()
	qp, pc, pf := newProbeTestPool(t)

	// Take the connection back to its pre-HELLO state.
	pc.mu.Lock()
	pc.authed = false
	pc.mu.Unlock()

	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if pf.count() != 0 {
		t.Fatalf("the probe opened %d streams on an unauthenticated connection", pf.count())
	}
	h := qp.lastHealth()
	if h.Healthy {
		t.Errorf("a pool whose only connection was never verified reports healthy: %+v", h)
	}
	if h.PooledConns != 1 || h.LiveConns != 0 {
		t.Errorf("status = %+v, want 1 pooled and 0 live connections", h)
	}
	if h.LastProbeErr != probeErrUnverified {
		t.Errorf("LastProbeErr = %q, want %q", h.LastProbeErr, probeErrUnverified)
	}

	// An authenticated sibling is still counted, and the skipped one still is
	// not: the status describes what the probe verified.
	authed := &pooledConn{addr: "fake://probe-2", probeOpener: pf.open}
	authed.markAuthed("")
	qp.mu.Lock()
	qp.conns = append(qp.conns, authed)
	qp.mu.Unlock()

	if err := qp.Probe(ctx); err != nil {
		t.Fatalf("probe with one authenticated connection: %v", err)
	}
	h = qp.lastHealth()
	if h.PooledConns != 2 || h.LiveConns != 1 {
		t.Errorf("status = %+v, want 2 pooled and 1 live connection", h)
	}
}

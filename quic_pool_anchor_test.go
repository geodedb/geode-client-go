// quic_pool_anchor_test.go — SONNY-1219 / Lane D (SONNY-806).
//
// The engine ties a QUIC connection's lifetime to the FIRST stream opened on
// it: when the HELLO-bearing stream closes, geode destroys the whole
// connection with CONNECTION_CLOSE code 0 and every sibling multiplexed stream
// dies with it (triage 2026-09-15, geode
// src/server/quic_connection_handler.zig:3548-3589). The driver's answer is an
// ANCHOR stream: a dedicated stream opened at dial time, carrying the HELLO,
// held open for the pooled connection's whole life. No request stream is ever
// the connection owner, so request streams may come and go freely.
//
// These tests drive the anchor state machine through pooledConn.anchorOpener,
// so nothing here needs a live QUIC connection or a running engine.

package geode

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fake anchor stream
// ─────────────────────────────────────────────────────────────────────────────

// fakeAnchorStream stands in for the dedicated anchor stream. The FIRST
// ReceiveProto answers the HELLO; every later one is the anchor watcher's
// blocking read and parks until the test decides what the peer did.
type fakeAnchorStream struct {
	mu sync.Mutex

	closed   bool
	sent     []*proto.QuicClientMessage
	recvCtxs []context.Context

	sendErr  error
	helloErr error
	hello    *proto.HelloResponse

	// gate delivers the watcher's read result. A test writes the error the
	// peer produced (a reset, an unexpected frame) onto it.
	gate chan error
	// closedCh unblocks a parked watcher when the stream is closed locally.
	closedCh  chan struct{}
	closeOnce sync.Once
}

func newFakeAnchorStream() *fakeAnchorStream {
	// HelloResponse.Graph is an optional field, so the bound graph is a pointer.
	graph := "g1"
	return &fakeAnchorStream{
		hello: &proto.HelloResponse{
			Success:      true,
			Graph:        &graph,
			SessionId:    "sess-1",
			Capabilities: []string{"tx", "vectors"},
		},
		gate:     make(chan error, 1),
		closedCh: make(chan struct{}),
	}
}

func (f *fakeAnchorStream) SendProto(_ context.Context, msg *proto.QuicClientMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeAnchorStream) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	f.mu.Lock()
	f.recvCtxs = append(f.recvCtxs, ctx)
	n := len(f.recvCtxs)
	closed, helloErr, hello := f.closed, f.helloErr, f.hello
	f.mu.Unlock()

	if closed {
		return nil, ErrClosed
	}
	if n == 1 {
		if helloErr != nil {
			return nil, helloErr
		}
		return &proto.QuicServerMessage{
			Msg: &proto.QuicServerMessage_Hello{Hello: hello},
		}, nil
	}

	// The watcher's read: park until the peer (the test) does something.
	select {
	case err := <-f.gate:
		return nil, err
	case <-f.closedCh:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeAnchorStream) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	return nil, false, nil
}

func (f *fakeAnchorStream) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.closeOnce.Do(func() { close(f.closedCh) })
	return nil
}

func (f *fakeAnchorStream) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeAnchorStream) PagesArePushed() bool { return false }
func (f *fakeAnchorStream) RemoteClosed() bool   { return false }
func (f *fakeAnchorStream) Addr() string         { return "fake://anchor" }

// helloSent reports whether a HELLO was written on this stream.
func (f *fakeAnchorStream) helloSent() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.sent {
		if msg.GetHello() != nil {
			return true
		}
	}
	return false
}

func (f *fakeAnchorStream) receives() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recvCtxs)
}

func (f *fakeAnchorStream) watcherCtx(t *testing.T) context.Context {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.recvCtxs) < 2 {
		t.Fatalf("the anchor watcher never read: %d receives", len(f.recvCtxs))
	}
	return f.recvCtxs[1]
}

// anchorStreamFactory hands out fakeAnchorStreams and remembers them.
type anchorStreamFactory struct {
	mu     sync.Mutex
	opened []*fakeAnchorStream

	openErr  error
	helloErr error
	sendErr  error
}

func (af *anchorStreamFactory) open(context.Context) (Transport, error) {
	af.mu.Lock()
	defer af.mu.Unlock()
	if af.openErr != nil {
		return nil, af.openErr
	}
	f := newFakeAnchorStream()
	f.helloErr = af.helloErr
	f.sendErr = af.sendErr
	af.opened = append(af.opened, f)
	return f, nil
}

func (af *anchorStreamFactory) count() int {
	af.mu.Lock()
	defer af.mu.Unlock()
	return len(af.opened)
}

func (af *anchorStreamFactory) first(t *testing.T) *fakeAnchorStream {
	t.Helper()
	af.mu.Lock()
	defer af.mu.Unlock()
	if len(af.opened) == 0 {
		t.Fatal("no anchor stream was opened")
	}
	return af.opened[0]
}

// anchorTestCfg is the minimum Config the HELLO needs.
func anchorTestCfg() *Config {
	return &Config{
		Transport:    TransportQUIC,
		Host:         "127.0.0.1",
		Port:         "3141",
		Username:     "geode",
		Password:     "seed",
		HelloName:    DefaultHelloName,
		HelloVersion: DefaultHelloVersion,
		Conformance:  DefaultConformance,
	}
}

// newAnchorTestPool builds a pool that dials nothing: its one connection's
// anchor stream comes from the factory. probeInterval is 0 so no background
// probe loop races the assertions.
func newAnchorTestPool(t *testing.T) (*QUICPool, *pooledConn, *anchorStreamFactory) {
	t.Helper()
	af := &anchorStreamFactory{}
	p := NewQUICPool(anchorTestCfg(), 4)
	// Set before any watcher starts: lets a test join watchAnchor's exit
	// instead of sleeping. Buffered and sent to non-blockingly, so tests that
	// never read it are unaffected.
	p.anchorWatchDone = make(chan struct{}, 1)
	pc := &pooledConn{addr: "fake://anchor", anchorOpener: af.open}
	t.Cleanup(func() {
		_ = p.Close()
		// Ends the anchor watcher for a connection the test never put in the
		// pool; discard is idempotent, so a test that already retired it is
		// unaffected.
		pc.discard("test cleanup")
	})
	return p, pc, af
}

// anchorEventually polls cond until it holds or anchorTestBudget runs out.
const anchorTestBudget = 2 * time.Second

func anchorEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(anchorTestBudget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ─────────────────────────────────────────────────────────────────────────────
// The anchor carries the HELLO and stays open
// ─────────────────────────────────────────────────────────────────────────────

func TestAnchor_HelloRunsOnDedicatedStream(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)

	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}

	if af.count() != 1 {
		t.Fatalf("expected exactly one anchor stream, got %d", af.count())
	}
	anchor := af.first(t)
	if !anchor.helloSent() {
		t.Error("the HELLO was not sent on the anchor stream")
	}
	if anchor.IsClosed() {
		t.Error("the anchor stream was closed; it must be held open for the connection's life")
	}
	if !pc.isAuthed() {
		t.Error("the pooled connection is not authed after its anchor handshake")
	}

	graph, sessionID, caps := pc.helloState()
	if graph != "g1" || sessionID != "sess-1" || len(caps) != 2 {
		t.Errorf("HELLO state not recorded on the pooled connection: graph=%q session=%q caps=%v",
			graph, sessionID, caps)
	}
	if !pc.isGraphBound() {
		t.Error("the connection should be graph-bound: the server bound g1 during HELLO")
	}
}

// A request stream must never have to run the handshake once the anchor did:
// that is the whole point — no request stream owns the connection.
func TestAnchor_RequestStreamsSkipHello(t *testing.T) {
	p, pc, _ := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}

	conn := newConn(p.cfg, &fakeAnchorStream{})
	transport := &QUICTransport{pool: p, pc: pc}
	if err := pooledHello(t.Context(), conn, transport, p); err != nil {
		t.Fatalf("pooledHello on an anchored connection: %v", err)
	}
	if conn.boundGraph != "g1" || conn.sessionID != "sess-1" {
		t.Errorf("the request stream did not adopt the anchor's HELLO state: graph=%q session=%q",
			conn.boundGraph, conn.sessionID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A failed anchor handshake never produces a pooled connection
// ─────────────────────────────────────────────────────────────────────────────

func TestAnchor_HelloFailureClosesAnchorAndLeavesConnUnauthed(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	af.helloErr = &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}

	err := p.anchorConn(t.Context(), pc)
	if err == nil {
		t.Fatal("anchorConn succeeded although the HELLO failed")
	}
	var te *TransportError
	if !errors.As(err, &te) {
		t.Errorf("the anchor failure lost its *TransportError: %v", err)
	}
	if pc.isAuthed() {
		t.Error("a connection whose HELLO failed must not be authed")
	}
	if !af.first(t).IsClosed() {
		t.Error("the anchor stream of a failed handshake must be closed")
	}
}

func TestAnchor_OpenFailureIsReported(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	af.openErr = &TransportError{Op: "open_stream", Addr: "127.0.0.1:3141", cause: io.ErrUnexpectedEOF}

	if err := p.anchorConn(t.Context(), pc); err == nil {
		t.Fatal("anchorConn succeeded although the anchor stream could not be opened")
	}
	if pc.isAuthed() {
		t.Error("a connection with no anchor must not be authed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The peer closing the anchor retires the connection (Task 11: health/drain)
// ─────────────────────────────────────────────────────────────────────────────

func TestAnchor_PeerCloseRetiresIdleConnection(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}
	p.mu.Lock()
	p.conns = append(p.conns, pc)
	p.mu.Unlock()

	anchor := af.first(t)
	anchorEventually(t, "the anchor watcher to start reading", func() bool {
		return anchor.receives() >= 2
	})

	// The peer resets the anchor: the connection can no longer be trusted.
	anchor.gate <- &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}

	anchorEventually(t, "the connection to be condemned", pc.isDead)
	anchorEventually(t, "the connection to leave the pool", func() bool {
		pooled, _ := p.counts()
		return pooled == 0
	})
	anchorEventually(t, "the anchor stream to be closed with its connection", anchor.IsClosed)
}

// A retired connection must DRAIN, not be yanked out from under the request
// streams already running on it: it leaves the pool at once, but is closed only
// when its last stream has finished.
func TestAnchor_PeerCloseDrainsInFlightStreams(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}
	p.mu.Lock()
	p.conns = append(p.conns, pc)
	p.mu.Unlock()

	// One request stream in flight on this connection.
	if err := p.acquireSlot(t.Context()); err != nil {
		t.Fatalf("acquireSlot: %v", err)
	}
	atomic.StoreInt32(&pc.active, 1)

	anchor := af.first(t)
	anchorEventually(t, "the anchor watcher to start reading", func() bool {
		return anchor.receives() >= 2
	})
	anchor.gate <- &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}

	anchorEventually(t, "the connection to leave the pool", func() bool {
		pooled, _ := p.counts()
		return pooled == 0
	})
	if anchor.IsClosed() {
		t.Fatal("the connection was closed with a request stream still in flight; it must drain first")
	}

	p.release(pc)
	anchorEventually(t, "the drained connection to be closed", anchor.IsClosed)
}

// Closing the anchor ourselves — pool close, eviction, discard — is not the
// peer closing it, and must not be reported as a connection failure.
func TestAnchor_LocalCloseDoesNotCondemn(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}
	p.mu.Lock()
	p.conns = append(p.conns, pc)
	p.mu.Unlock()

	anchor := af.first(t)
	anchorEventually(t, "the anchor watcher to start reading", func() bool {
		return anchor.receives() >= 2
	})

	pc.discard("test close")

	anchorEventually(t, "the anchor stream to be closed", anchor.IsClosed)

	// Join the watcher rather than sleeping: its verdict is only final once the
	// goroutine has returned.
	select {
	case <-p.anchorWatchDone:
	case <-time.After(anchorTestBudget):
		t.Fatal("the anchor watcher did not exit after the local close")
	}
	if pc.isRetiring() {
		t.Error("a locally closed anchor was reported as a peer failure")
	}
}

// The anchor must not defeat idle detection: its watcher reads with a context
// that carries no deadline and no cancellation, so it never arms a read
// deadline on the stream and never writes a frame. Connection liveness stays
// with the QUIC idle timeout and the health probe.
func TestAnchor_WatcherDoesNotArmDeadlines(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}

	anchor := af.first(t)
	anchorEventually(t, "the anchor watcher to start reading", func() bool {
		return anchor.receives() >= 2
	})

	ctx := anchor.watcherCtx(t)
	if _, ok := ctx.Deadline(); ok {
		t.Error("the anchor watcher read with a deadline; it would arm a read deadline on the stream")
	}
	if ctx.Done() != nil {
		t.Error("the anchor watcher read with a cancellable context; ReceiveProto would arm a read deadline")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A draining connection survives the liveness prune
// ─────────────────────────────────────────────────────────────────────────────

// retire condemns a connection in two steps that are NOT atomic against the
// pool lock: pc.dead is stored first, and only then does p.remove(pc) queue for
// p.mu. Any goroutine already holding p.mu runs acquire → pruneDeadLocked in
// that window, sees isDead() report true for a connection that is merely
// DRAINING, and discards it — closing qConn under the request streams still in
// flight. That is exactly the yank retire exists to avoid, and swapping the two
// statements in retire would not close it: isDead() also turns true on its own
// once the peer cancels the QUIC connection's context.
//
// So the prune itself must know the difference: a retiring connection with
// in-flight streams leaves the pool but is never discarded there. Its last
// release closes it.
func TestPrune_NeverDiscardsADrainingConnection(t *testing.T) {
	p, pc, af := newAnchorTestPool(t)
	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}
	p.mu.Lock()
	p.conns = append(p.conns, pc)
	p.mu.Unlock()

	// One request stream in flight on this connection.
	if err := p.acquireSlot(t.Context()); err != nil {
		t.Fatalf("acquireSlot: %v", err)
	}
	atomic.StoreInt32(&pc.active, 1)

	anchor := af.first(t)

	// Hold the pool lock for the whole window: retire's p.remove cannot get in,
	// which is precisely the state every acquire runs in.
	p.mu.Lock()
	go p.retire(pc, "anchor stream lost: test")

	deadline := time.Now().Add(anchorTestBudget)
	for !pc.dead.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	condemned := pc.dead.Load()
	if condemned {
		// What acquire does next, with the connection condemned but not yet
		// removed.
		p.pruneDeadLocked()
	}
	stillPooled := len(p.conns)
	p.mu.Unlock()

	if !condemned {
		t.Fatal("retire never condemned the connection; the race window was not reached")
	}
	if anchor.IsClosed() {
		t.Fatal("the prune discarded a DRAINING connection: its anchor was closed " +
			"while a request stream was still in flight")
	}
	if stillPooled != 0 {
		t.Errorf("a condemned connection must leave the pool: %d still pooled", stillPooled)
	}

	// The drain still owns the close: the last release ends the connection.
	p.release(pc)
	anchorEventually(t, "the drained connection to be closed by its last release", anchor.IsClosed)
}

// ─────────────────────────────────────────────────────────────────────────────
// The anchor's admission cost, and dialLocked's anchor-failure path
// ─────────────────────────────────────────────────────────────────────────────
//
// These drive anchorConn, which is the whole of dialLocked's anchor half. The
// two statements they cannot reach are dialLocked's own: the quic.DialAddr
// above anchorConn and the qConn.CloseWithError(0, "anchor handshake failed")
// below it. Both need a LIVE QUIC listener — there is no fake one in this
// repo, and no seam to inject a dialer through: dialLocked calls quic.DialAddr
// directly. Every other pool test is in the same position, which is why they
// all assemble pooledConns by hand. The behaviour those two statements produce
// is asserted against a real engine by the integration lane; what is unit-
// testable — that a failed handshake leaves no pooled connection behind, closes
// its anchor, and costs the pool nothing — is asserted here.

// The anchor is opened directly on the connection, never through acquire, so it
// must consume no admission slot: not against max_streams (the semaphore) and
// not against max_streams_per_conn (pc.active, which is what counts() reports).
func TestAnchor_ConsumesNoAdmissionSlot(t *testing.T) {
	p, pc, _ := newAnchorTestPool(t)

	slotsBefore := len(p.slotsCh)
	if slotsBefore == 0 {
		t.Fatal("the test pool has no admission slots to spend")
	}

	if err := p.anchorConn(t.Context(), pc); err != nil {
		t.Fatalf("anchorConn: %v", err)
	}
	p.mu.Lock()
	p.conns = append(p.conns, pc)
	p.mu.Unlock()

	if got := len(p.slotsCh); got != slotsBefore {
		t.Errorf("the anchor spent an admission slot: %d available, want %d", got, slotsBefore)
	}
	pooled, inFlight := p.counts()
	if pooled != 1 {
		t.Errorf("pooled = %d, want 1", pooled)
	}
	if inFlight != 0 {
		t.Errorf("the anchor was counted as an in-flight request stream: inFlight = %d, want 0", inFlight)
	}
}

// A failed anchor handshake is dialLocked's error return: the connection never
// joins p.conns, its anchor is closed, and the pool's admission budget is
// exactly where it was — a caller whose dial failed must not have leaked a slot
// that the next one then waits for.
func TestDialLocked_AnchorFailureLeavesNoPooledConnAndNoAdmissionCost(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(af *anchorStreamFactory)
	}{
		{"the anchor stream cannot be opened", func(af *anchorStreamFactory) {
			af.openErr = &TransportError{Op: "open_stream", Addr: "127.0.0.1:3141", cause: io.ErrUnexpectedEOF}
		}},
		{"the HELLO on the anchor fails", func(af *anchorStreamFactory) {
			af.helloErr = &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, pc, af := newAnchorTestPool(t)
			tc.apply(af)

			slotsBefore := len(p.slotsCh)

			if err := p.anchorConn(t.Context(), pc); err == nil {
				t.Fatal("anchorConn succeeded although the anchor could not be established")
			}

			// dialLocked returns before `p.conns = append(...)`, so the pool
			// never sees this connection.
			pooled, inFlight := p.counts()
			if pooled != 0 {
				t.Errorf("a connection with no anchor joined the pool: pooled = %d, want 0", pooled)
			}
			if inFlight != 0 {
				t.Errorf("inFlight = %d, want 0", inFlight)
			}
			if pc.isAuthed() {
				t.Error("a connection whose anchor failed must not be authed")
			}
			if got := len(p.slotsCh); got != slotsBefore {
				t.Errorf("the failed anchor leaked an admission slot: %d available, want %d",
					got, slotsBefore)
			}
			if af.count() > 0 && !af.first(t).IsClosed() {
				t.Error("the anchor stream of a failed handshake must be closed")
			}
		})
	}
}

package geode

// session_test.go drives the Session state machine against the shared
// mockTransport (see conn_stream_desync_test.go) through a real *sql.DB, so
// the pin, the retire-on-failure transition and the busy guard are exercised
// exactly as database/sql drives them — no fakes standing in for *sql.Conn.
//
// State machine under test (SONNY-806 / GEO-GO-UP-001, Lane D Task 10):
//
//	         Pool.Session()          op fails the IsValid probe
//	  (none) ──────────────▶ open ──────────────────────────▶ retired
//	                          │                                   │
//	                          │ Close()                           │ Close()
//	                          ▼                                   ▼
//	                        closed ◀───────────────────────────────┘
//
// open additionally carries a re-entrancy flag: while a TX callback is
// running, every Session method reports ErrSessionBusy instead of deadlocking
// or desynchronising the pinned QUIC stream.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	"geodedb.com/geode/proto"
)

// ─────────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────────

// sessionTestConnector hands database/sql a fresh mock-backed *Conn per
// Connect. It records every connection it ever produced, which is how the
// "never silently re-pins" assertions are made: a Session that quietly
// re-pinned would show up here as a second connection.
type sessionTestConnector struct {
	mu         sync.Mutex
	conns      []*Conn
	transports map[uint64]*mockTransport
}

func newSessionTestConnector() *sessionTestConnector {
	return &sessionTestConnector{transports: map[uint64]*mockTransport{}}
}

func (tc *sessionTestConnector) Connect(_ context.Context) (driver.Conn, error) {
	mt := newMockTransport()
	c := newTestConn(mt)

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.conns = append(tc.conns, c)
	tc.transports[c.ConnID()] = mt
	return c, nil
}

func (tc *sessionTestConnector) Driver() driver.Driver { return &registeredDriver }

// connCount reports how many driver connections database/sql has opened
// through this connector.
func (tc *sessionTestConnector) connCount() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return len(tc.conns)
}

// transportFor returns the mock transport behind the connection a Session
// pinned, so a test can fail that exact connection.
func (tc *sessionTestConnector) transportFor(t *testing.T, id uint64) *mockTransport {
	t.Helper()
	tc.mu.Lock()
	defer tc.mu.Unlock()
	mt, ok := tc.transports[id]
	if !ok {
		t.Fatalf("no mock transport recorded for conn id %d", id)
	}
	return mt
}

// newSessionTestPool returns a Pool backed by mock connections.
func newSessionTestPool(t *testing.T, maxOpen int) (*Pool, *sessionTestConnector) {
	t.Helper()
	tc := newSessionTestConnector()
	db := sql.OpenDB(tc)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	t.Cleanup(func() { _ = db.Close() })
	return &Pool{db: db, cfg: &Config{}}, tc
}

// failTransport makes the pinned connection's transport report the underlying
// connection as gone, which is what Conn.IsValid observes after a cancellation
// retirement or a transport error.
func failTransport(mt *mockTransport) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.remoteClosed = true
}

func mockBeginMsg() *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Begin{Begin: &proto.BeginResponse{}},
	}
}

func mockCommitMsg() *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Commit{Commit: &proto.CommitResponse{}},
	}
}

func mockRollbackMsg() *proto.QuicServerMessage {
	return &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Rollback{Rollback: &proto.RollbackResponse{}},
	}
}

// newPinnedSession pins a Session and returns it with the mock transport of
// the connection it pinned.
func newPinnedSession(t *testing.T, p *Pool, tc *sessionTestConnector) (*Session, *mockTransport) {
	t.Helper()
	s, err := p.Session(context.Background())
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, tc.transportFor(t, s.ConnID())
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction and lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestPool_SessionRejectsNilPool(t *testing.T) {
	var p *Pool
	if _, err := p.Session(context.Background()); err == nil {
		t.Fatal("Session on a nil pool returned no error")
	}

	empty := &Pool{}
	if _, err := empty.Session(context.Background()); err == nil {
		t.Fatal("Session on a zero-value pool returned no error")
	}
}

func TestSession_CloseIsIdempotent(t *testing.T) {
	s := &Session{}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close on a zero-value Session: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close on a zero-value Session: %v", err)
	}
}

// TestSession_CloseReleasesExactlyOnce asserts the pinned connection goes back
// to the pool on the first Close and that further Closes neither error nor
// release a second time.
func TestSession_CloseReleasesExactlyOnce(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)

	s, err := p.Session(context.Background())
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if got := p.Stats().InUse; got != 1 {
		t.Fatalf("InUse while pinned = %d, want 1", got)
	}
	for i := range 3 {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	if got := p.Stats().InUse; got != 0 {
		t.Fatalf("InUse after Close = %d, want 0", got)
	}
	if got := tc.connCount(); got != 1 {
		t.Fatalf("connector opened %d connections, want 1", got)
	}
}

func TestSession_MethodsFailAfterClose(t *testing.T) {
	s := &Session{}
	_ = s.Close()

	ctx := context.Background()
	if _, err := s.ExecContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("ExecContext after Close = %v, want ErrSessionClosed", err)
	}
	if _, err := s.QueryContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("QueryContext after Close = %v, want ErrSessionClosed", err)
	}
	if err := s.TX(ctx, nil, func(*sql.Tx) error { return nil }); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("TX after Close = %v, want ErrSessionClosed", err)
	}
}

// TestSession_PinnedMethodsFailAfterClose runs the same assertions against a
// Session that really held a connection, so the closed guard is proven on the
// path where s.conn is non-nil.
func TestSession_PinnedMethodsFailAfterClose(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)
	s, _ := newPinnedSession(t, p, tc)

	id := s.ConnID()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()
	if _, err := s.ExecContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("ExecContext after Close = %v, want ErrSessionClosed", err)
	}
	if _, err := s.QueryContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("QueryContext after Close = %v, want ErrSessionClosed", err)
	}
	if err := s.TX(ctx, nil, func(*sql.Tx) error { return nil }); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("TX after Close = %v, want ErrSessionClosed", err)
	}
	if s.ConnID() != id {
		t.Fatalf("ConnID changed across Close: %d -> %d", id, s.ConnID())
	}
	if got := tc.connCount(); got != 1 {
		t.Fatalf("connector opened %d connections, want 1", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Identity
// ─────────────────────────────────────────────────────────────────────────────

// TestSession_ConnIDIdentifiesThePinnedConnection asserts ConnID reports the
// identity of the DRIVER connection (not a per-Session counter): it equals the
// pinned *Conn's own id, is stable, and differs between two live Sessions.
func TestSession_ConnIDIdentifiesThePinnedConnection(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)

	a, err := p.Session(context.Background())
	if err != nil {
		t.Fatalf("Session a: %v", err)
	}
	defer a.Close()
	b, err := p.Session(context.Background())
	if err != nil {
		t.Fatalf("Session b: %v", err)
	}
	defer b.Close()

	if a.ConnID() == b.ConnID() {
		t.Fatalf("two live Sessions share conn id %d", a.ConnID())
	}
	if a.ConnID() != tc.conns[0].ConnID() || b.ConnID() != tc.conns[1].ConnID() {
		t.Fatalf("ConnID does not match the pinned driver connections: "+
			"sessions (%d,%d), conns (%d,%d)",
			a.ConnID(), b.ConnID(), tc.conns[0].ConnID(), tc.conns[1].ConnID())
	}
	first := a.ConnID()
	if again := a.ConnID(); again != first {
		t.Fatalf("ConnID is not stable: %d then %d", first, again)
	}

	var zero *Session
	if zero.ConnID() != 0 {
		t.Fatal("nil Session reported a non-zero ConnID")
	}
	if (&Session{}).ConnID() != 0 {
		t.Fatal("zero-value Session reported a non-zero ConnID")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Transactions
// ─────────────────────────────────────────────────────────────────────────────

func TestSession_TXCommitsOnNil(t *testing.T) {
	p, tc := newSessionTestPool(t, 1)
	s, mt := newPinnedSession(t, p, tc)

	mt.push(mockBeginMsg())
	mt.push(mockCommitMsg())

	called := false
	if err := s.TX(context.Background(), nil, func(tx *sql.Tx) error {
		called = true
		if tx == nil {
			t.Error("TX called fn with a nil *sql.Tx")
		}
		return nil
	}); err != nil {
		t.Fatalf("TX: %v", err)
	}
	if !called {
		t.Fatal("TX did not run fn")
	}
	assertSentOps(t, mt, constOpBegin, constOpCommit)
}

func TestSession_TXRollsBackOnError(t *testing.T) {
	p, tc := newSessionTestPool(t, 1)
	s, mt := newPinnedSession(t, p, tc)

	mt.push(mockBeginMsg())
	mt.push(mockRollbackMsg())

	sentinel := errors.New("callback failed")
	err := s.TX(context.Background(), nil, func(*sql.Tx) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("TX = %v, want the callback error", err)
	}
	assertSentOps(t, mt, constOpBegin, constOpRollback)
}

// TestSession_TXStaysPinnedAfterAFailedTX asserts a rolled-back transaction
// leaves the Session usable on the SAME connection — a failed generation must
// not cost the caller its pin.
func TestSession_TXStaysPinnedAfterAFailedTX(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)
	s, mt := newPinnedSession(t, p, tc)

	id := s.ConnID()
	mt.push(mockBeginMsg())
	mt.push(mockRollbackMsg())
	if err := s.TX(context.Background(), nil, func(*sql.Tx) error {
		return errors.New("callback failed")
	}); err == nil {
		t.Fatal("TX returned no error")
	}
	if s.ConnID() != id {
		t.Fatalf("ConnID changed after a failed TX: %d -> %d", id, s.ConnID())
	}

	mt.push(mockBeginMsg())
	mt.push(mockCommitMsg())
	if err := s.TX(context.Background(), nil, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("TX after a failed TX: %v", err)
	}
	if s.ConnID() != id {
		t.Fatalf("ConnID changed across the second TX: %d -> %d", id, s.ConnID())
	}
	if got := tc.connCount(); got != 1 {
		t.Fatalf("connector opened %d connections, want 1", got)
	}
}

func TestSession_TXRollsBackAndRepanics(t *testing.T) {
	p, tc := newSessionTestPool(t, 1)
	s, mt := newPinnedSession(t, p, tc)

	mt.push(mockBeginMsg())
	mt.push(mockRollbackMsg())

	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("TX swallowed the panic")
		}
		if got, ok := rec.(string); !ok || got != "boom" {
			t.Fatalf("re-panicked with %v, want \"boom\"", rec)
		}
		assertSentOps(t, mt, constOpBegin, constOpRollback)
		if s.inTX.Load() {
			t.Fatal("TX left the session marked busy after a panic")
		}
	}()

	_ = s.TX(context.Background(), nil, func(*sql.Tx) error { panic("boom") })
}

// TestSession_TXRejectsNesting asserts a re-entrant TX (and any statement
// issued on the Session rather than on the transaction) reports ErrSessionBusy
// instead of deadlocking on the pinned connection's mutex.
func TestSession_TXRejectsNesting(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)
	s, mt := newPinnedSession(t, p, tc)

	mt.push(mockBeginMsg())
	mt.push(mockCommitMsg())

	ctx := context.Background()
	if err := s.TX(ctx, nil, func(*sql.Tx) error {
		if err := s.TX(ctx, nil, func(*sql.Tx) error { return nil }); !errors.Is(err, ErrSessionBusy) {
			t.Errorf("nested TX = %v, want ErrSessionBusy", err)
		}
		if _, err := s.ExecContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionBusy) {
			t.Errorf("ExecContext during TX = %v, want ErrSessionBusy", err)
		}
		if _, err := s.QueryContext(ctx, "RETURN 1"); !errors.Is(err, ErrSessionBusy) {
			t.Errorf("QueryContext during TX = %v, want ErrSessionBusy", err)
		}
		if err := s.Close(); !errors.Is(err, ErrSessionBusy) {
			t.Errorf("Close during TX = %v, want ErrSessionBusy", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("TX: %v", err)
	}

	// The busy flag is cleared, so the session is reusable.
	mt.push(mockBeginMsg())
	mt.push(mockCommitMsg())
	if err := s.TX(ctx, nil, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("TX after a nested-TX rejection: %v", err)
	}
	if got := tc.connCount(); got != 1 {
		t.Fatalf("connector opened %d connections, want 1", got)
	}
}

func TestSession_TXRejectsNilCallback(t *testing.T) {
	p, tc := newSessionTestPool(t, 1)
	s, _ := newPinnedSession(t, p, tc)

	if err := s.TX(context.Background(), nil, nil); err == nil {
		t.Fatal("TX with a nil callback returned no error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Retirement
// ─────────────────────────────────────────────────────────────────────────────

// TestSession_RetiresWhenPinnedConnFails is the unit-level proof of the
// contract the Task 9 cancel path exercises for real: once the pinned
// connection is no longer valid, every Session method fails closed and the
// Session never acquires a different connection.
func TestSession_RetiresWhenPinnedConnFails(t *testing.T) {
	p, tc := newSessionTestPool(t, 4)
	s, mt := newPinnedSession(t, p, tc)
	id := s.ConnID()

	failTransport(mt)

	ctx := context.Background()
	_, err := s.ExecContext(ctx, "RETURN 1")
	assertRetired(t, "ExecContext", err)

	_, err = s.QueryContext(ctx, "RETURN 1")
	assertRetired(t, "QueryContext", err)

	assertRetired(t, "TX", s.TX(ctx, nil, func(*sql.Tx) error {
		t.Error("TX ran fn on a retired session")
		return nil
	}))

	if s.ConnID() != id {
		t.Fatalf("retired Session re-pinned: conn id %d -> %d", id, s.ConnID())
	}
	if got := tc.connCount(); got != 1 {
		t.Fatalf("retired Session caused %d connections to be opened, want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after retirement: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close after retirement: %v", err)
	}
}

// TestSession_RetirementIsReportedAsBothClosedAndBadConn pins the error
// contract: callers matching ErrSessionClosed see an unusable session, and
// callers matching driver.ErrBadConn see why.
func TestSession_RetirementIsReportedAsBothClosedAndBadConn(t *testing.T) {
	if !errors.Is(ErrSessionRetired, ErrSessionClosed) {
		t.Error("ErrSessionRetired does not match ErrSessionClosed")
	}
	if !errors.Is(ErrSessionRetired, driver.ErrBadConn) {
		t.Error("ErrSessionRetired does not match driver.ErrBadConn")
	}
	if errors.Is(ErrSessionClosed, driver.ErrBadConn) {
		t.Error("a plain ErrSessionClosed must not match driver.ErrBadConn")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertions
// ─────────────────────────────────────────────────────────────────────────────

func assertRetired(t *testing.T, op string, err error) {
	t.Helper()
	if !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("%s on a retired session = %v, want ErrSessionRetired", op, err)
	}
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("%s on a retired session = %v, want it to match ErrSessionClosed", op, err)
	}
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("%s on a retired session = %v, want it to match driver.ErrBadConn", op, err)
	}
}

// assertSentOps asserts the exact sequence of transaction control messages the
// session put on the wire.
func assertSentOps(t *testing.T, mt *mockTransport, want ...string) {
	t.Helper()
	mt.mu.Lock()
	defer mt.mu.Unlock()

	got := make([]string, 0, len(mt.sent))
	for _, msg := range mt.sent {
		switch {
		case msg.GetBegin() != nil:
			got = append(got, constOpBegin)
		case msg.GetCommit() != nil:
			got = append(got, constOpCommit)
		case msg.GetRollback() != nil:
			got = append(got, constOpRollback)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("transaction messages = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transaction messages = %v, want %v", got, want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Agreement between the pin-time and per-operation health checks
// ─────────────────────────────────────────────────────────────────────────────

// foreignConn is a driver connection that is NOT a *geode.Conn. It exists to
// pin the one behaviour adoptPinned and checkPinned used to disagree about:
// adoptPinned refused such a connection while checkPinned waved it through as
// healthy.
type foreignConn struct{}

func (foreignConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (foreignConn) Close() error                        { return nil }
func (foreignConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

// foreignConnector hands database/sql connections this driver does not own.
type foreignConnector struct{}

func (foreignConnector) Connect(context.Context) (driver.Conn, error) { return foreignConn{}, nil }
func (foreignConnector) Driver() driver.Driver                        { return &registeredDriver }

func newForeignPool(t *testing.T) *Pool {
	t.Helper()
	db := sql.OpenDB(foreignConnector{})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &Pool{db: db, cfg: &Config{}}
}

// TestPool_SessionRejectsForeignDriverConn is the pin-time half: adoptPinned
// refuses a driver connection that is not a *geode.Conn.
func TestPool_SessionRejectsForeignDriverConn(t *testing.T) {
	p := newForeignPool(t)

	s, err := p.Session(context.Background())
	if err == nil {
		_ = s.Close()
		t.Fatal("Session pinned a connection this driver does not own")
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Session = %v, want an error matching ErrInvalidState", err)
	}
}

// TestSession_CheckPinnedRejectsForeignDriverConn is the per-operation half.
// A Session is built directly (bypassing adoptPinned, which would have refused
// the connection) so checkPinned is the code under test: it must reach the
// same verdict as adoptPinned rather than reporting the connection healthy.
func TestSession_CheckPinnedRejectsForeignDriverConn(t *testing.T) {
	p := newForeignPool(t)

	conn, err := p.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	s := &Session{conn: conn}
	t.Cleanup(func() { _ = s.Close() })

	_, err = s.ExecContext(context.Background(), "RETURN 1")
	if err == nil {
		t.Fatal("ExecContext succeeded on a connection this driver does not own")
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ExecContext = %v, want an error matching ErrInvalidState", err)
	}

	// The disagreement mattered because checkPinned is also the retirement
	// authority: having refused the connection it must retire the Session, not
	// leave it open to try again on the same unreadable connection.
	if _, err := s.QueryContext(context.Background(), "RETURN 1"); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("QueryContext after the refusal = %v, want ErrSessionRetired", err)
	}
}

// TestSession_TXRetiresWhenTheCallbackReportsABadConn asserts TX observes the
// callback's error the way ExecContext and QueryContext observe theirs: a
// callback error that reports the connection is gone retires the Session
// immediately, not on the next operation's probe.
func TestSession_TXRetiresWhenTheCallbackReportsABadConn(t *testing.T) {
	p, tc := newSessionTestPool(t, 2)
	s, mt := newPinnedSession(t, p, tc)

	mt.push(mockBeginMsg())
	mt.push(mockRollbackMsg())

	fnErr := fmt.Errorf("write failed: %w", driver.ErrBadConn)
	err := s.TX(context.Background(), nil, func(*sql.Tx) error { return fnErr })
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("TX = %v, want the callback's bad-connection error", err)
	}

	// Retired by the observation itself: the pinned connection's own health is
	// untouched here (the mock transport still reports it fine), so a Session
	// that waited for checkPinned would still be open.
	if state := sessionState(s.state.Load()); state != sessionRetired {
		t.Fatalf("session state = %d, want retired (%d)", state, sessionRetired)
	}
	if _, err := s.ExecContext(context.Background(), "RETURN 1"); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("ExecContext after the retirement = %v, want ErrSessionRetired", err)
	}
	if s.ConnID() == 0 {
		t.Fatal("a retired Session forgot which connection it pinned")
	}
}

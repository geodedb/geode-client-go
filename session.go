package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Session lifecycle errors.
var (
	// ErrSessionClosed reports use of a Session after Close, or use of a
	// zero-value Session. It also matches ErrSessionRetired, so a caller that
	// only needs to know "this session can no longer be used" can test for
	// this one error.
	ErrSessionClosed = errors.New("geode: session closed")

	// ErrSessionBusy reports an operation issued on a Session while its own
	// TX callback is still running: a nested TX, a statement sent to the
	// Session instead of to the *sql.Tx, or a Close from inside the callback.
	// All three would otherwise deadlock on, or desynchronise, the pinned
	// QUIC stream.
	ErrSessionBusy = errors.New("geode: session busy: a transaction is already running on this session")

	// ErrSessionRetired reports use of a Session whose pinned connection has
	// failed — a cancelled query retired it (see Rows.abortOnCancel), a
	// transport error marked it, or database/sql discarded it. The Session is
	// dead: it deliberately does NOT acquire a replacement connection, because
	// silently re-pinning would let a caller's next statement land on a
	// different connection than the one it asked to ride.
	//
	// It matches both ErrSessionClosed ("unusable") and driver.ErrBadConn
	// ("because the connection failed") under errors.Is.
	ErrSessionRetired = fmt.Errorf(
		"geode: session retired after its pinned connection failed: %w: %w",
		ErrSessionClosed, driver.ErrBadConn)

	// ErrPoolNotInitialized reports Session called on a nil or zero-value Pool.
	ErrPoolNotInitialized = errors.New("geode: pool is not initialised")

	// ErrNilTxFunc reports TX called without a callback.
	ErrNilTxFunc = errors.New("geode: TX requires a non-nil callback")
)

// sessionState is the Session's lifecycle state. Transitions are one-way:
// open → retired → closed, or open → closed.
type sessionState int32

const (
	sessionOpen sessionState = iota
	sessionRetired
	sessionClosed
)

// Session is a connection pinned out of the pool for the caller's exclusive
// use. Every statement issued through it rides the same driver connection —
// and therefore the same QUIC stream — so a multi-statement ingest cannot
// straddle connections and a sequence of transactions provably shares one
// session on the server.
//
// ConnID reports which connection is pinned, and it does not change: once the
// pinned connection fails, the Session is retired rather than re-pinned, and
// every method returns an error matching both ErrSessionClosed and
// driver.ErrBadConn.
//
// Callers MUST Close a Session; leaking one starves the pool. Close is
// idempotent and releases the connection exactly once.
//
// A Session is not safe for concurrent use by multiple goroutines: it owns one
// connection, and that connection serialises everything anyway. Rows returned
// by QueryContext must be closed before the next statement on the same
// Session, exactly as for a *sql.Conn.
//
//	s, err := pool.Session(ctx)
//	if err != nil {
//	    return err
//	}
//	defer s.Close()
//
//	for _, chunk := range chunks {
//	    if err := s.TX(ctx, nil, func(tx *sql.Tx) error {
//	        _, err := tx.ExecContext(ctx, "MERGE (n:Chunk {id: ?})", chunk.ID)
//	        return err
//	    }); err != nil {
//	        return err
//	    }
//	}
//
// Session is implemented on top of database/sql's *sql.Conn: that is where the
// pin already lives, and building a second pinning mechanism at the driver
// level would duplicate — and could contradict — database/sql's own accounting
// of which connection is checked out. What *sql.Conn does not provide is an
// identity for the pinned connection and a guarantee that a failed connection
// is never reused; Session adds both, reading the driver's own Conn through
// (*sql.Conn).Raw.
type Session struct {
	conn *sql.Conn

	// connID and graph are read from the pinned driver connection once, at
	// pin time, and are immutable thereafter.
	connID uint64
	graph  string

	state atomic.Int32 // sessionState
	inTX  atomic.Bool

	closeOnce sync.Once
	closeErr  error
}

// Session pins one connection from the pool for the caller's exclusive use.
// The caller must Close the returned Session to release it.
//
// The context governs only the wait for a free connection; it does not bound
// the Session's lifetime.
//
// # Pinning immediately after a retirement (SONNY-1227)
//
// Tearing down a retired connection can leave one stale connection behind on
// the shared QUIC pool: geode 0.8.15 closes the whole pooled QUIC connection
// when the stream that carried its handshake goes away, and the driver only
// discovers that on the next write. database/sql absorbs this for an UNPINNED
// statement by retrying on another connection; a pinned one cannot, by design,
// because retrying would hand the caller a different connection than the one it
// asked to ride.
//
// So a Session pinned immediately after a retirement may fail on its first
// statement with an error matching driver.ErrBadConn. The contract is: close
// that Session and pin again.
//
//	s, err := pool.Session(ctx)
//	if err != nil {
//	    return err
//	}
//	if _, err := s.ExecContext(ctx, stmt); errors.Is(err, driver.ErrBadConn) {
//	    _ = s.Close()
//	    if s, err = pool.Session(ctx); err != nil { // pin again, once
//	        return err
//	    }
//	}
//
// Session deliberately does not hide this behind a validating round trip at pin
// time: that would cost every pin a PING, and the whole point of the type is
// that nothing re-pins silently. Pool.Health drives the same liveness check on
// the pool's own schedule instead.
func (p *Pool) Session(ctx context.Context) (*Session, error) {
	if p == nil || p.db == nil {
		return nil, fmt.Errorf("geode: Pool.Session: %w", ErrPoolNotInitialized)
	}
	// A pin during a drain is refused rather than served: database/sql may
	// still have an idle connection to hand over, but that connection is about
	// to be closed under the caller, and a Session that discovered its pin had
	// evaporated is exactly the surprise Drain exists to avoid.
	if p.draining.Load() {
		return nil, fmt.Errorf("geode: Pool.Session: %w", ErrPoolDraining)
	}

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("geode: Pool.Session: %w", err)
	}

	s := &Session{conn: conn}
	if err := s.adoptPinned(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("geode: Pool.Session: %w", err)
	}
	return s, nil
}

// adoptPinned records the identity of the connection database/sql just pinned.
// Raw is the only way to reach the driver connection behind a *sql.Conn;
// returning driver.ErrBadConn from inside it makes database/sql discard the
// connection instead of handing a broken one back to the pool.
func (s *Session) adoptPinned() error {
	return s.conn.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			return fmt.Errorf("%w: pinned connection is %T, not *geode.Conn",
				ErrInvalidState, dc)
		}
		if !c.IsValid() {
			return driver.ErrBadConn
		}
		s.connID = c.ConnID()
		s.graph = c.BoundGraph()
		return nil
	})
}

// ConnID returns the process-unique identity of the pinned driver connection.
// Every statement and transaction issued through this Session runs on that
// connection, so two calls that report the same ConnID provably shared one
// connection and one QUIC stream. Two live Sessions never share an id, and an
// id is never reused for a DIFFERENT connection.
//
// An id does not identify a Session, though: database/sql recycles idle driver
// connections, so a Session pinned after an earlier one closed may be handed
// the same pooled connection and will then report the same id. "Same ConnID"
// means "same connection", not "same Session" — which is exactly what the
// pinning assertions need.
//
// A zero-value, nil or closed Session that never pinned anything reports 0.
func (s *Session) ConnID() uint64 {
	if s == nil {
		return 0
	}
	return s.connID
}

// BoundGraph returns the graph the server bound to the pinned connection at
// HELLO time, or "" when the session is unbound. It is the pinned
// connection's own binding (see (*Conn).BoundGraph), so it also confirms that
// pinning did not move the Session off the graph the DSN selected.
func (s *Session) BoundGraph() string {
	if s == nil {
		return ""
	}
	return s.graph
}

// acquire gates every Session operation. It rejects a closed, retired or busy
// Session, and otherwise verifies that the pinned connection is still healthy
// — retiring the Session when it is not.
//
// The health check is the authority for retirement, not the error of the
// previous call: a cancelled query retires the connection through the Rows it
// returned (Task 9), an error the Session never sees.
func (s *Session) acquire(op string) error {
	if s == nil || s.conn == nil {
		return fmt.Errorf("geode: Session.%s: %w", op, ErrSessionClosed)
	}
	if state := sessionState(s.state.Load()); state == sessionClosed {
		return fmt.Errorf("geode: Session.%s: %w", op, ErrSessionClosed)
	} else if state == sessionRetired {
		return fmt.Errorf("geode: Session.%s: %w", op, ErrSessionRetired)
	}
	if s.inTX.Load() {
		return fmt.Errorf("geode: Session.%s: %w", op, ErrSessionBusy)
	}
	if err := s.checkPinned(); err != nil {
		return fmt.Errorf("geode: Session.%s: %w", op, err)
	}
	return nil
}

// checkPinned asks the pinned driver connection whether it is still usable.
// Returning driver.ErrBadConn from inside Raw also tells database/sql to
// discard the connection, so a retired Session cannot leave a poisoned
// connection in the pool.
//
// A driver connection that is not a *geode.Conn is rejected here exactly as
// adoptPinned rejects it at pin time: this Session cannot vouch for a
// connection whose health it cannot read, so treating it as healthy would be
// the one answer that is certainly wrong. The Session is retired and the
// connection discarded, and the caller is told which invariant broke
// (ErrInvalidState) rather than the generic retirement reason.
func (s *Session) checkPinned() error {
	var foreign error
	err := s.conn.Raw(func(dc any) error {
		c, ok := dc.(*Conn)
		if !ok {
			foreign = fmt.Errorf("%w: pinned connection is %T, not *geode.Conn",
				ErrInvalidState, dc)
			return driver.ErrBadConn
		}
		if c.IsValid() {
			return nil
		}
		return driver.ErrBadConn
	})
	if err == nil {
		return nil
	}
	s.retire()
	if foreign != nil {
		return foreign
	}
	return ErrSessionRetired
}

// observe retires the Session when an operation reported that the connection
// is gone. It is the fast path; checkPinned is the authoritative one.
func (s *Session) observe(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		s.retire()
	}
}

// retire moves an open Session to retired. A closed Session stays closed.
func (s *Session) retire() {
	s.state.CompareAndSwap(int32(sessionOpen), int32(sessionRetired))
}

// ExecContext runs a statement on the pinned connection.
func (s *Session) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := s.acquire("ExecContext"); err != nil {
		return nil, err
	}
	res, err := s.conn.ExecContext(ctx, query, args...)
	s.observe(err)
	return res, err
}

// QueryContext runs a query on the pinned connection.
//
// The returned Rows hold the pinned connection until they are closed; no other
// statement may be issued on the Session until then.
func (s *Session) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := s.acquire("QueryContext"); err != nil {
		return nil, err
	}
	rows, err := s.conn.QueryContext(ctx, query, args...)
	s.observe(err)
	return rows, err
}

// TX runs fn inside a transaction on the pinned connection: it commits when fn
// returns nil, rolls back when fn returns an error, and rolls back and
// re-panics when fn panics.
//
// The whole callback runs on the pinned connection, so every statement fn
// issues through tx rides the same QUIC stream as the BEGIN and the COMMIT.
//
// TX is not re-entrant. A nested TX — or any statement issued on the Session
// rather than on tx — returns ErrSessionBusy rather than deadlocking on the
// pinned connection.
//
// A rolled-back transaction does not cost the caller its pin: unless the
// connection itself failed, the Session stays pinned to the same connection
// and ConnID is unchanged.
func (s *Session) TX(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	if fn == nil {
		return fmt.Errorf("geode: Session.TX: %w", ErrNilTxFunc)
	}
	if err := s.acquire("TX"); err != nil {
		return err
	}
	// acquire already rejected a busy Session; this closes the window between
	// that check and the flag being set.
	if !s.inTX.CompareAndSwap(false, true) {
		return fmt.Errorf("geode: Session.TX: %w", ErrSessionBusy)
	}
	defer s.inTX.Store(false)

	tx, err := s.conn.BeginTx(ctx, opts)
	if err != nil {
		s.observe(err)
		return fmt.Errorf("geode: Session.TX: begin: %w", err)
	}

	// A panic must not leave the transaction open on the pinned connection:
	// roll back, then re-panic so the caller's own recovery still sees it.
	// The deferred inTX reset above still runs while panicking.
	defer func() {
		if rec := recover(); rec != nil {
			_ = tx.Rollback()
			panic(rec)
		}
	}()

	if fnErr := fn(tx); fnErr != nil {
		// Symmetry with ExecContext/QueryContext: a callback error that reports
		// the connection is gone retires the Session now, rather than leaving
		// it "open" until the next acquire's probe notices. The rollback below
		// is still attempted — on a dead connection it fails harmlessly and is
		// reported, and the probe remains the authority either way.
		s.observe(fnErr)
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			s.observe(rbErr)
			return errors.Join(fnErr, fmt.Errorf("geode: Session.TX: rollback: %w", rbErr))
		}
		return fnErr
	}

	if err := tx.Commit(); err != nil {
		s.observe(err)
		return fmt.Errorf("geode: Session.TX: commit: %w", err)
	}
	return nil
}

// Close releases the pinned connection back to the pool. It is idempotent,
// safe on a zero-value or nil Session, and releases the connection exactly
// once; later calls report the first Close's error, if any.
//
// Close from inside a TX callback returns ErrSessionBusy without closing
// anything: database/sql would block until the transaction finished, which it
// cannot while the callback is waiting on Close.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	if s.inTX.Load() {
		return fmt.Errorf("geode: Session.Close: %w", ErrSessionBusy)
	}
	s.closeOnce.Do(func() {
		s.state.Store(int32(sessionClosed))
		if s.conn == nil {
			return
		}
		// A retired Session's connection was already handed back to
		// database/sql by the bad-connection probe, so ErrConnDone here means
		// "already released", not a failure to release.
		if err := s.conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			s.closeErr = err
		}
	})
	return s.closeErr
}

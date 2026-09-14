package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

// defaultDeprecatedMethodTimeout is the default timeout for deprecated methods
// (Prepare, Begin, Exec, Query) that use context.Background(). This prevents
// operations from hanging indefinitely. Use the Context variants for custom timeouts.
const defaultDeprecatedMethodTimeout = 30 * time.Second

// Operation name constants used in SecurityError.Type fields and the
// describeServerMessage helper.
const (
	constOpInput    = "input"
	constOpHello    = "hello"
	constOpExecute  = "execute"
	constOpPing     = "ping"
	constOpBegin    = "begin"
	constOpCommit   = "commit"
	constOpRollback = "rollback"
)

// transportErrBadConn converts transport-layer errors to driver.ErrBadConn so
// that database/sql automatically discards the stale connection and retries the
// operation on a fresh one. QUIC streams are closed by the server after idle
// periods; the pool may hold a connection whose stream is already closed but
// whose state still appears valid until the first write fails.
//
// Both *TransportError (which wraps io.EOF and other stream errors) and raw
// io.EOF (which may surface from drainExecStream) are treated as bad-connection
// indicators.
func transportErrBadConn(err error) error {
	var te *TransportError
	if errors.As(err, &te) {
		return driver.ErrBadConn
	}
	if errors.Is(err, io.EOF) {
		return driver.ErrBadConn
	}
	return err
}

// markConnFailed transitions the connection into StateInError and
// conservatively clears every piece of client-side state that could
// otherwise leak stale server-side reality into the next pool user.
//
// Called when an operation (Tx.Commit, Tx.Rollback, ExecContext,
// QueryContext) has committed bytes to the wire but the transport
// then fails — the server may or may not have completed the request,
// so *inTx in particular is unknowable client-side*. The previous
// behavior (leaving inTx=true) caused the next BeginTx on the same
// pooled conn to fail with ErrTxInProgress even though the client
// never actually observed a BEGIN response (R4 regression: Go driver
// leaks inTx=true on transport failure).
//
// IsValid() already rejects StateInError, and ResetSession also
// returns driver.ErrBadConn for it, so the conn is guaranteed to be
// evicted instead of recycled.
//
// Callers must hold c.mu.
func (c *Conn) markConnFailed() {
	c.state = StateInError
	c.inTx = false
	c.activeRows = nil
}

// connSeq numbers driver connections so a caller can assert that two
// operations ran on the same one. It is a client-side identity only: the
// server's own session identifier is shared by every stream multiplexed onto
// one pooled QUIC connection (see adoptHelloState) and therefore cannot
// distinguish two Conns.
//
//nolint:gochecknoglobals // monotonic counter for connection identity
var connSeq atomic.Uint64

// Conn represents a connection to a Geode database.
type Conn struct {
	cfg       *Config
	transport Transport

	// id is this connection's process-unique identity, assigned at
	// construction and immutable thereafter — hence no mutex.
	id uint64

	mu         sync.Mutex
	state      ConnState
	inTx       bool
	activeRows *Rows

	// helloCaps, boundGraph and sessionID are what the server said in its
	// HELLO response: the advertised capability strings, the graph it bound
	// (empty when unbound) and the session identifier.
	//
	// All three are written once during connection setup — either by hello()
	// on the connection that performed the handshake, or by adoptHelloState()
	// on a later stream that inherited an already-authenticated pooled QUIC
	// connection — before the Conn is published to database/sql, and are
	// read-only thereafter. That is why they need no mutex.
	//
	// helloCaps is NOT the SHOW CAPABILITIES feature matrix. The two are
	// different vocabularies and neither is derived from the other; see
	// ServerInfo.HelloCapabilities.
	helloCaps  []string
	boundGraph string
	sessionID  string
}

// adoptHelloState copies the HELLO response metadata recorded on an
// already-authenticated pooled QUIC connection onto this Conn.
//
// Without it, every stream after the first on a pooled connection would report
// no capabilities and no session: pooledHello short-circuits when the
// underlying connection is already authenticated, so only the very first Conn
// ever sees a HelloResponse. Callers must not have published the Conn yet.
func (c *Conn) adoptHelloState(pc *pooledConn) {
	c.boundGraph, c.sessionID, c.helloCaps = pc.helloState()
}

// HelloCapabilities returns the capability strings the server advertised in
// its HELLO response, in order. The slice is a copy; callers may retain it.
//
// These are session/protocol capabilities negotiated at handshake time. They
// are a DIFFERENT vocabulary from `SHOW CAPABILITIES` (see
// Connector.ServerInfo) and neither list is derived from the other.
func (c *Conn) HelloCapabilities() []string {
	return append([]string(nil), c.helloCaps...)
}

// BoundGraph returns the graph the server bound during HELLO, or "" when the
// session is unbound. When non-empty, `USE GRAPH` is rejected for the life of
// the session.
func (c *Conn) BoundGraph() string { return c.boundGraph }

// SessionID returns the session identifier the server issued in its HELLO
// response, or "" when the server issued none.
func (c *Conn) SessionID() string { return c.sessionID }

// ConnID returns a process-unique identifier for this driver connection.
// It never changes for the life of the Conn and is never reused.
//
// Use it to assert that several operations rode the same connection — and
// therefore the same QUIC stream. SessionID cannot answer that question: every
// stream multiplexed onto one pooled QUIC connection inherits the same
// server-issued session id. Session.ConnID surfaces this value for a pinned
// connection.
func (c *Conn) ConnID() uint64 { return c.id }

// drainExecStream reads all remaining ExecutionResponse frames from the stream
// until the execution is complete (final data page + optional metrics/heartbeat).
// This MUST be called when ExecContext gets a schema or non-final page to keep
// the single bidirectional QUIC stream in sync.
func (c *Conn) drainExecStream(ctx context.Context, first *proto.ExecutionResponse) error {
	gotFinal := first.GetPage() != nil && first.GetPage().GetFinal()

	// Check if the first response already has a final page

	for !gotFinal {
		// Blocking read for data page (server always sends one after schema)
		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			return err
		}
		execResp := resp.GetExecute()
		if execResp == nil {
			break // Non-execute response, stop
		}
		if execResp.GetError() != nil {
			return &DriverError{
				Code:    execResp.GetError().GetCode(),
				Message: execResp.GetError().GetMessage(),
			}
		}
		if execResp.GetPage() != nil && execResp.GetPage().GetFinal() {
			gotFinal = true
		}
		// If it's metrics or heartbeat, loop to read next
		if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
			break
		}
	}

	// After final data page, drain any additional frames (metrics, heartbeat)
	// that may already be buffered.
	for {
		msg, ok, err := c.transport.TryReceiveProtoBuffered()
		if err != nil {
			return err
		}
		if !ok {
			break // No more buffered data
		}
		execResp := msg.GetExecute()
		if execResp == nil {
			break // Non-execute message, stop
		}
		// Continue draining metrics/heartbeat frames
		if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
			continue
		}
		break
	}

	return nil
}

// Compile-time interface checks.
var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.Validator          = (*Conn)(nil)
	_ driver.NamedValueChecker  = (*Conn)(nil)
)

// newConn creates a new connection with the given transport.
func newConn(cfg *Config, transport Transport) *Conn {
	return &Conn{
		cfg:       cfg,
		transport: transport,
		state:     StateIdle,
		id:        connSeq.Add(1),
	}
}

// connectPooled creates a new logical connection using a QUIC stream from the
// shared pool. The HELLO handshake is only performed on the first stream of
// each underlying QUIC connection; subsequent streams skip it.
func connectPooled(ctx context.Context, cfg *Config, pool *QUICPool) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.ConnectTimeout)*time.Second)
		defer cancel()
	}

	transport, err := NewPooledQUICTransport(ctx, pool)
	if err != nil {
		return nil, err
	}

	conn := newConn(cfg, transport)

	if err := pooledHello(ctx, conn, transport, pool); err != nil {
		_ = transport.Close()
		return nil, err
	}

	if err := pooledGraphInit(ctx, conn, cfg, transport); err != nil {
		_ = transport.Close()
		return nil, err
	}

	return conn, nil
}

// pooledHello performs the HELLO handshake if this is the first stream on the
// QUIC connection. Uses double-checked locking to ensure exactly one HELLO.
func pooledHello(ctx context.Context, conn *Conn, transport *QUICTransport, pool *QUICPool) error {
	if transport.pc.isAuthed() {
		conn.adoptHelloState(transport.pc)
		return nil
	}

	// Double-check under lock to avoid racing with a concurrent first stream.
	transport.pc.mu.Lock()
	alreadyAuthed := transport.pc.authed
	transport.pc.mu.Unlock()

	if alreadyAuthed {
		conn.adoptHelloState(transport.pc)
		return nil
	}

	resp, err := conn.hello(ctx)
	if err != nil {
		pool.remove(transport.pc)
		return err
	}
	// Record the HELLO metadata on the pooled connection so every later
	// stream that skips the handshake above can still report it.
	transport.pc.markAuthedHello(resp.GetGraph(), resp.GetSessionId(), resp.GetCapabilities())
	return nil
}

// pooledGraphInit handles graph creation and selection for pooled connections.
// When the server already bound the graph via HELLO (DSN path), USE GRAPH
// must be skipped — the server rejects it for DSN-bound sessions.
func pooledGraphInit(ctx context.Context, conn *Conn, cfg *Config, transport *QUICTransport) error {
	if cfg.Graph == "" {
		return nil
	}

	// If the server bound the graph during HELLO (DSN path), skip
	// CREATE/USE GRAPH — the session is already targeting the correct graph.
	if transport.pc.isGraphBound() {
		return nil
	}

	// A read-only connection must not create the graph on the way in, exactly
	// as on the standalone path in connect() below: "cannot even emit a write"
	// includes connection setup, and since execInit carries
	// ExecuteRequest.read_only the engine would refuse CREATE GRAPH with
	// GQLSTATE 42501 (.CreateGraph => true, geode 0.8.15
	// src/query/query_traits.zig:317) and connection setup would fail outright.
	// USE GRAPH alone is not a graph-store mutation server-side, so it still
	// succeeds under the flag — and fails honestly when the graph is absent,
	// which is the correct answer for a reader.
	//
	// This is the DEFAULT path: Connector.Connect → connectPooled reaches it
	// for every sql.Open("geode", …) whose HELLO did not bind the graph.
	if !cfg.ReadOnly {
		// CREATE GRAPH is idempotent — safe even if server already created it.
		if err := conn.execInit(ctx, "CREATE GRAPH IF NOT EXISTS "+cfg.Graph); err != nil {
			return fmt.Errorf("create graph %s: %w", cfg.Graph, err)
		}
	}
	if err := conn.execInit(ctx, "USE GRAPH "+cfg.Graph); err != nil {
		return fmt.Errorf("use graph %s: %w", cfg.Graph, err)
	}
	return nil
}

// connect establishes a connection to the server and performs the HELLO handshake.
func connect(ctx context.Context, cfg *Config) (*Conn, error) {
	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Apply connect timeout if configured
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.ConnectTimeout)*time.Second)
		defer cancel()
	}

	// Create transport based on config
	var transport Transport
	var err error

	switch cfg.Transport {
	case TransportQUIC:
		transport, err = NewQUICTransport(ctx, cfg)
	case TransportGRPC:
		transport, err = NewGRPCTransport(ctx, cfg)
	default:
		return nil, &ConfigError{
			Field:   "transport",
			Message: "unsupported transport type",
		}
	}
	if err != nil {
		return nil, err
	}

	conn := newConn(cfg, transport)

	// Perform HELLO handshake. If the server binds the graph during HELLO
	// (server ≥ v0.2.x), serverGraph will be non-empty and we skip the
	// explicit CREATE/USE GRAPH statements below.
	resp, err := conn.hello(ctx)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	serverGraph := resp.GetGraph()

	// Auto-select graph if configured in DSN and not already bound by server.
	if cfg.Graph != "" && serverGraph == "" {
		// A read-only connection must not create the graph on the way in:
		// "cannot even emit a write" includes connection setup. USE GRAPH on
		// its own is not a graph-store mutation (the engine classifies it
		// read-only, geode 0.8.15 src/query/query_traits.zig:311-360), so it
		// still succeeds under ExecuteRequest.read_only — and fails honestly
		// when the graph does not exist, which is the correct answer for a
		// reader.
		if !cfg.ReadOnly {
			if err := conn.execInit(ctx, "CREATE GRAPH IF NOT EXISTS "+cfg.Graph); err != nil {
				_ = transport.Close()
				return nil, fmt.Errorf("create graph %s: %w", cfg.Graph, err)
			}
		}
		if err := conn.execInit(ctx, "USE GRAPH "+cfg.Graph); err != nil {
			_ = transport.Close()
			return nil, fmt.Errorf("use graph %s: %w", cfg.Graph, err)
		}
	}

	return conn, nil
}

// execInit executes a GQL statement during connection initialization (before
// the connection is handed to the pool). It does not acquire the mutex because
// the connection is not yet shared. This is used for graph selection and other
// post-HELLO setup queries.
//
// Setup statements are deliberately NOT routed through guardReadOnly. The only
// statement a read-only connection sends here is USE GRAPH, and the client
// classifier calls USE mutating by construction: USE is not on the closed
// read-only leading allow-list and is in mutatingKeywords, because a driver
// that let an unlisted keyword through would be failing open (readonly.go).
// The engine disagrees in the safe direction — USE GRAPH selects a session
// target and is not a graph-store mutation (`else => false`, geode 0.8.15
// src/query/query_traits.zig:357-359) — so it is accepted under
// ExecuteRequest.read_only. Guarding here would therefore break every
// read-only DSN that names a graph, refusing connection setup over a statement
// the server itself considers a read. The wire flag below still applies, so
// the engine remains the authority: anything genuinely mutating sent from here
// is refused with GQLSTATE 42501.
func (c *Conn) execInit(ctx context.Context, query string) error {
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:    query,
		ReadOnly: c.cfg.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return err
	}

	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return err
	}

	execResp := resp.GetExecute()
	if execResp == nil {
		return fmt.Errorf("expected Execute response for init query")
	}

	if execResp.GetError() != nil {
		return &DriverError{
			Code:    execResp.GetError().GetCode(),
			Message: execResp.GetError().GetMessage(),
		}
	}

	// Drain the full execution stream.
	if execResp.GetSchema() != nil || execResp.GetPage() != nil {
		if err := c.drainExecStream(ctx, execResp); err != nil {
			return err
		}
	}

	return nil
}

// hello performs the HELLO handshake with the server and returns the full
// HelloResponse.
//
// The whole response is returned, not just the bound graph, because the
// server also advertises its session capabilities and a session id there and
// both were previously decoded and discarded. On success the response fields
// are recorded on the Conn (helloCaps, boundGraph, sessionID) before it
// returns, so a Conn always carries the handshake it performed.
func (c *Conn) hello(ctx context.Context) (*proto.HelloResponse, error) {
	req := &proto.HelloRequest{
		Username:          c.cfg.Username,
		Password:          c.cfg.Password,
		ClientName:        c.cfg.HelloName,
		ClientVersion:     c.cfg.HelloVersion,
		WantedConformance: c.cfg.Conformance,
	}
	if c.cfg.Graph != "" {
		graph := c.cfg.Graph
		req.Graph = &graph
	}
	if c.cfg.Tenant != "" {
		tenant := c.cfg.Tenant
		req.TenantId = &tenant
	}
	if c.cfg.Role != "" {
		role := c.cfg.Role
		req.Role = &role
	}
	msg := proto.NewHelloMessage(req)
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return nil, fmt.Errorf("failed to send HELLO: %w", err)
	}

	// Read and validate HELLO response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to receive HELLO response: %w", err)
	}

	helloResp := resp.GetHello()
	if helloResp == nil {
		return nil, fmt.Errorf("expected HELLO response")
	}

	if !helloResp.GetSuccess() {
		return nil, &DriverError{
			Code:    "AUTH",
			Message: helloResp.GetErrorMessage(),
		}
	}

	// Copy the capability slice: the response message is owned by the
	// transport decode path and must not be aliased past this call.
	c.helloCaps = append([]string(nil), helloResp.GetCapabilities()...)
	c.boundGraph = helloResp.GetGraph()
	c.sessionID = helloResp.GetSessionId()

	return helloResp, nil
}

// Prepare returns a prepared statement.
// Note: Geode doesn't have server-side prepared statements,
// so this is a client-side implementation.
//
// Deprecated: Use PrepareContext instead for explicit timeout control.
// This method uses a default 30-second timeout.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeprecatedMethodTimeout)
	defer cancel()
	return c.PrepareContext(ctx, query)
}

// PrepareContext returns a prepared statement with context.
func (c *Conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return nil, driver.ErrBadConn
	}

	// Validate query
	if err := validate.Query(query); err != nil {
		return nil, &SecurityError{
			Type:    constOpInput,
			Message: err.Error(),
		}
	}

	return &Stmt{
		conn:     c,
		query:    query,
		numInput: countPlaceholders(query),
	}, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return nil
	}

	c.state = StateClosed

	// Close any active rows
	if c.activeRows != nil {
		c.activeRows.closed = true
		c.activeRows = nil
	}

	return c.transport.Close()
}

// Begin starts a transaction.
//
// Deprecated: Use BeginTx instead for explicit timeout control.
// This method uses a default 30-second timeout.
func (c *Conn) Begin() (driver.Tx, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeprecatedMethodTimeout)
	defer cancel()
	return c.BeginTx(ctx, driver.TxOptions{})
}

// BeginTx starts a transaction with context and options.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return nil, driver.ErrBadConn
	}

	if c.inTx {
		return nil, ErrTxInProgress
	}

	if c.activeRows != nil {
		return nil, ErrQueryInProgress
	}

	// 2026-04-28: stream-state desync guard.  If the previous request on
	// this stream left buffered bytes (e.g. an undrained metrics or
	// heartbeat frame), a fresh BeginTx would read those stale bytes as
	// if they were the BEGIN response and fail with a confusing
	// "expected BEGIN response" — and worse, leave the stream in a
	// permanently-desynced state.  Detect that here and fail fast with
	// ErrBadConn so database/sql evicts the conn instead of letting it
	// poison subsequent BeginTx calls.
	if t, ok := c.transport.(*QUICTransport); ok && t.HasBufferedData() {
		c.markConnFailed()
		return nil, transportErrBadConn(fmt.Errorf("stream desync: %d unconsumed bytes from prior request", t.BufferedLen()))
	}

	// Send BEGIN.
	//
	// A read-only connection pins BeginRequest.read_only on regardless of
	// what the caller asked for: opts.ReadOnly may only ever tighten the
	// transaction, never loosen the connection. Read-only transactions are
	// allowed on a read-only connection — only the mutating statements
	// inside them are refused (guardReadOnly), and that refusal happens
	// before any bytes reach the wire, so the transaction stays open and
	// usable for reads and the caller still owns the Commit/Rollback
	// decision.
	msg := proto.NewBeginMessage(&proto.BeginRequest{
		ReadOnly: opts.ReadOnly || c.cfg.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		c.markConnFailed()
		return nil, err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		c.markConnFailed()
		return nil, err
	}

	if resp.GetBegin() == nil {
		// Stream returned an unexpected variant.  Identify which one so
		// future incident analysis can see it without re-instrumenting.
		// Then mark the conn bad so database/sql evicts it — leaving
		// the conn in the pool with a desynced stream is worse than
		// the eviction churn, because every subsequent BeginTx on it
		// will fail the same way.
		c.markConnFailed()
		return nil, fmt.Errorf("expected BEGIN response, got %s", describeServerMessage(resp))
	}

	c.inTx = true
	c.state = StateInTransaction

	return &Tx{conn: c, ctx: ctx}, nil
}

// describeServerMessage returns a short tag for the variant inside the
// QuicServerMessage oneof so error messages can identify what the server
// actually sent when a different variant was expected.  Used by BeginTx /
// Commit / Rollback / Exec error paths.
func describeServerMessage(resp *proto.QuicServerMessage) string {
	if resp == nil {
		return "nil"
	}
	switch {
	case resp.GetHello() != nil:
		return constOpHello
	case resp.GetExecute() != nil:
		ex := resp.GetExecute()
		if ex.GetError() != nil {
			return fmt.Sprintf("execute(error=%s)", ex.GetError().GetMessage())
		}
		return constOpExecute
	case resp.GetPull() != nil:
		return "pull"
	case resp.GetPing() != nil:
		return constOpPing
	case resp.GetBegin() != nil:
		return constOpBegin
	case resp.GetCommit() != nil:
		return constOpCommit
	case resp.GetRollback() != nil:
		return constOpRollback
	case resp.GetSavepoint() != nil:
		return "savepoint"
	case resp.GetRollbackTo() != nil:
		return "rollback_to"
	case resp.GetCdcDiag() != nil:
		return "cdc_diag"
	case resp.GetCdcCtrl() != nil:
		return "cdc_ctrl"
	case resp.GetBackup() != nil:
		return "backup"
	case resp.GetRestore() != nil:
		return "restore"
	case resp.GetUploadBackup() != nil:
		return "upload_backup"
	default:
		return constUnknown
	}
}

// ExecContext executes a query that doesn't return rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return nil, driver.ErrBadConn
	}

	if c.activeRows != nil {
		return nil, ErrQueryInProgress
	}

	// Validate query
	if err := validate.Query(query); err != nil {
		return nil, &SecurityError{
			Type:    constOpInput,
			Message: err.Error(),
		}
	}

	// Bind `?` placeholders to typed wire parameters. No argument value is
	// ever written into the query text (SONNY-806).
	newQuery, protoParams, err := bindParameters(query, args)
	if err != nil {
		return nil, err
	}

	if err := c.guardReadOnly(query); err != nil {
		return nil, err
	}

	// Send Execute
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:    newQuery,
		Params:   protoParams,
		ReadOnly: c.cfg.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		// Bytes went onto the wire but we couldn't confirm what (if
		// anything) the server observed. Mark the conn failed so the
		// pool evicts it — critical inside an active tx where leaving
		// inTx=true would leak into the next BeginTx (R4).
		c.markConnFailed()
		return nil, transportErrBadConn(err)
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		c.markConnFailed()
		return nil, transportErrBadConn(err)
	}

	execResp := resp.GetExecute()
	if execResp == nil {
		// Unexpected payload shape signals stream desync; the conn
		// is no longer trustworthy for tx-state reconciliation.
		c.markConnFailed()
		return nil, fmt.Errorf("expected Execute response")
	}

	if execResp.GetError() != nil {
		return nil, &DriverError{
			Code:    execResp.GetError().GetCode(),
			Message: execResp.GetError().GetMessage(),
		}
	}

	// Drain the full execution response stream (schema → data page → metrics).
	// The server always sends at least a data page after schema.
	// After the final data page, the server may send a metrics frame.
	if execResp.GetSchema() != nil || execResp.GetPage() != nil {
		if err := c.drainExecStream(ctx, execResp); err != nil {
			c.markConnFailed()
			return nil, transportErrBadConn(err)
		}
	}

	return &Result{}, nil
}

// QueryContext executes a query that returns rows.
//
//nolint:funlen // Complex query lifecycle requires handling many protocol states
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	// Note: We don't defer unlock here because we transfer lock ownership to Rows

	if c.state == StateClosed {
		c.mu.Unlock()
		return nil, driver.ErrBadConn
	}

	if c.activeRows != nil {
		c.mu.Unlock()
		return nil, ErrQueryInProgress
	}

	// Validate query
	if err := validate.Query(query); err != nil {
		c.mu.Unlock()
		return nil, &SecurityError{
			Type:    constOpInput,
			Message: err.Error(),
		}
	}

	// Bind `?` placeholders to typed wire parameters. No argument value is
	// ever written into the query text (SONNY-806).
	newQuery, protoParams, err := bindParameters(query, args)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	if err := c.guardReadOnly(query); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// Send Execute
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:    newQuery,
		Params:   protoParams,
		ReadOnly: c.cfg.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		// See ExecContext: transport failure inside an active tx
		// leaked inTx=true before this fix (R4). markConnFailed
		// clears the inTx bit and flags the conn for eviction.
		c.markConnFailed()
		c.mu.Unlock()
		return nil, transportErrBadConn(err)
	}

	// Read response (could be schema, page, error, etc.)
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		c.markConnFailed()
		c.mu.Unlock()
		return nil, transportErrBadConn(err)
	}

	execResp := resp.GetExecute()
	if execResp == nil {
		c.markConnFailed()
		c.mu.Unlock()
		// Name the frame that arrived instead. A stream that answers EXECUTE
		// with something else is desynchronised, and which frame it was is the
		// whole diagnosis (a stale pull response means a previous result set
		// was not drained; a hello means the session was re-handshaked).
		return nil, fmt.Errorf("expected Execute response, got %s", describeServerMessage(resp))
	}

	if execResp.GetError() != nil {
		c.mu.Unlock()
		return nil, &DriverError{
			Code:    execResp.GetError().GetCode(),
			Message: execResp.GetError().GetMessage(),
		}
	}

	// Build column info from schema
	var columns []ColumnInfo
	if execResp.GetSchema() != nil {
		columns = make([]ColumnInfo, len(execResp.GetSchema().GetColumns()))
		for i, col := range execResp.GetSchema().GetColumns() {
			columns[i] = ColumnInfo{
				Name: col.GetName(),
				Type: ParseGQLType(col.GetType()),
			}
		}
	}

	// If the first response was a schema, we need to read the data page next.
	// Use a blocking read since the server always sends a data page after schema.
	if execResp.GetSchema() != nil {
		pageResp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			c.markConnFailed()
			c.mu.Unlock()
			return nil, transportErrBadConn(err)
		}
		pageExecResp := pageResp.GetExecute()
		if pageExecResp != nil && pageExecResp.GetError() != nil {
			c.mu.Unlock()
			return nil, &DriverError{
				Code:    pageExecResp.GetError().GetCode(),
				Message: pageExecResp.GetError().GetMessage(),
			}
		}

		rows := &Rows{
			conn:     c,
			ctx:      ctx,
			columns:  columns,
			pageSize: c.cfg.PageSize,
		}

		if pageExecResp != nil && pageExecResp.GetPage() != nil {
			rows.protoBuffer = pageExecResp.GetPage().GetRows()
			rows.final = pageExecResp.GetPage().GetFinal()
		} else {
			rows.final = true
		}

		c.activeRows = rows
		c.state = StateFetching
		return rows, nil
	}

	// If we got a page directly in the first response
	if execResp.GetPage() != nil {
		rows := &Rows{
			conn:        c,
			ctx:         ctx,
			columns:     columns,
			pageSize:    c.cfg.PageSize,
			protoBuffer: execResp.GetPage().GetRows(),
			final:       execResp.GetPage().GetFinal(),
		}
		c.activeRows = rows
		c.state = StateFetching
		return rows, nil
	}

	// Empty result set
	rows := &Rows{
		conn:     c,
		ctx:      ctx,
		columns:  columns,
		pageSize: c.cfg.PageSize,
		final:    true,
	}

	c.activeRows = rows
	c.state = StateFetching

	// Lock is transferred to rows - it will be unlocked when rows.Close() is called
	return rows, nil
}

// Ping verifies the connection is alive.
func (c *Conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return driver.ErrBadConn
	}

	// Send PING
	msg := proto.NewPingMessage(&proto.PingRequest{})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return err
	}

	pingResp := resp.GetPing()
	if pingResp == nil {
		return fmt.Errorf("expected PING response")
	}

	if !pingResp.GetOk() {
		return fmt.Errorf("ping failed")
	}

	return nil
}

// ResetSession resets the session state.
func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateClosed {
		return driver.ErrBadConn
	}

	// Underlying connection was closed server-side (or lost) while this conn sat
	// idle in the pool. A rollback or any subsequent query would fail or hang on
	// the dead connection, so mark it failed and evict it now instead of reusing
	// it. markConnFailed also clears any lingering inTx bit so the conn cannot
	// come back with a phantom transaction.
	if c.transport.RemoteClosed() {
		c.markConnFailed()
		return driver.ErrBadConn
	}

	// Prior operation left the conn in an irrecoverable state
	// (transport failure, stream desync, aborted tx). Don't try to
	// reconcile; return ErrBadConn so database/sql discards.
	if c.state == StateInError {
		return driver.ErrBadConn
	}

	// Poisoned connection: still mid-fetch or has un-closed rows.
	// Return ErrBadConn so database/sql discards rather than reuses.
	if c.state == StateFetching || c.activeRows != nil {
		return driver.ErrBadConn
	}

	// If in transaction, rollback. On any rollback-path failure the
	// conn's tx state is unknowable (server may or may not have
	// completed the rollback), so we flip to StateInError and return
	// ErrBadConn to force eviction. Leaving inTx=true here was the
	// original R4 leak: a subsequent BeginTx on the same pooled conn
	// saw inTx=true and returned ErrTxInProgress.
	if c.inTx {
		msg := proto.NewRollbackMessage(&proto.RollbackRequest{})
		if err := c.transport.SendProto(ctx, msg); err != nil {
			c.markConnFailed()
			return driver.ErrBadConn
		}

		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			c.markConnFailed()
			return driver.ErrBadConn
		}

		if resp.GetRollback() == nil {
			c.markConnFailed()
			return driver.ErrBadConn
		}

		c.inTx = false
	}

	c.state = StateIdle
	return nil
}

// IsValid returns true if the connection is still valid.
func (c *Conn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state != StateClosed && c.state != StateInError && c.state != StateFetching &&
		c.activeRows == nil && !c.transport.IsClosed() && !c.transport.RemoteClosed()
}

// CheckNamedValue validates a named value before use.
//
// Values of the driver.Value scalar set (plus []byte and time.Time) are
// passed through untouched so bindParameters can encode them as typed wire
// parameters. time.Time is a valid driver.Value on its own — database/sql's
// default converter passes it through unmodified — so it must be listed here
// too, or it would fall to driver.ErrSkip and be rejected by that same
// default converter. Anything else returns driver.ErrSkip, which hands the
// value to database/sql's default converter: it either narrows the value
// into that set (int -> int64, float32 -> float64, a driver.Valuer's
// Value()) or rejects it loudly. Nothing is silently stringified.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		if err := validate.ParamName(nv.Name); err != nil {
			return err
		}
		// Driver-generated placeholders own the "_gq" namespace; a caller
		// name in that namespace could shadow a positional argument.
		if strings.HasPrefix(nv.Name, generatedParamPrefix) {
			return fmt.Errorf("%w: %q", ErrReservedParamName, nv.Name)
		}
	}
	switch nv.Value.(type) {
	case nil, string, []byte, bool, int64, float64, time.Time:
		return nil
	case Vector:
		// Listed ahead of the container check so the intent is explicit: a
		// Vector must reach bindParameters as a Vector. Skipping it would
		// hand it to the default converter, which calls its driver.Valuer
		// Value() and then re-checks the result — losing the concrete type
		// that selects the validated encodeVector path.
		return nil
	}
	// Slices, arrays and maps keep their structural list_val/map_val encoding
	// (ENG-3910); the default converter would reject them outright.
	if isContainerParamValue(nv.Value) {
		return nil
	}
	return driver.ErrSkip
}

// trailingFrameDrainTimeout is the maximum time spent in the blocking drain
// inside releaseRows. The drain catches trailing server frames (metrics,
// heartbeat) that were still in the QUIC socket when the non-blocking
// TryReceiveProtoBuffered drain ran. In practice these frames arrive within
// microseconds; the timeout only fires when the server sends none.
//
// Reduced from 50ms to 1ms (#540): the Geode server does not send trailing
// metrics/heartbeat frames after the final data page, so the blocking drain
// was always hitting the full timeout. 1ms is sufficient to catch any
// in-flight frames on localhost while reducing per-query overhead from 50ms
// to 1ms (19 QPS → ~500 QPS improvement at c=1).
//
// Override in tests to keep the test suite fast.
var trailingFrameDrainTimeout = 1 * time.Millisecond //nolint:gochecknoglobals // test override point

// releaseRows is called by Rows.Close() to release the lock and update state.
func (c *Conn) releaseRows(r *Rows) {
	if c.activeRows == r {
		// Phase 1: drain frames already in the 64 KB bufio.Reader buffer
		// (non-blocking — only checks bytes already read from the socket).
		c.drainBufferedFrames()

		// Phase 2: short blocking drain to catch trailing frames that were
		// still in the QUIC socket (not yet in the bufio buffer) when Phase 1
		// ran. Without this, the stale frame is read by the next query's
		// ReceiveProto call as if it were the response to the new EXECUTE,
		// causing stream desynchronisation (BUG-0001 / GAP-0046).
		c.drainTrailingFrames()

		c.activeRows = nil
		if c.inTx {
			c.state = StateInTransaction
		} else {
			c.state = StateIdle
		}
	}
	c.mu.Unlock()
}

// drainBufferedFrames consumes execute response frames already present in the
// bufio.Reader buffer without blocking on the network.
func (c *Conn) drainBufferedFrames() {
	for {
		msg, ok, err := c.transport.TryReceiveProtoBuffered()
		if err != nil || !ok {
			return
		}
		execResp := msg.GetExecute()
		if execResp == nil {
			return
		}
		if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
			continue
		}
		if execResp.GetPage() != nil {
			continue
		}
		return
	}
}

// drainTrailingFrames performs a short blocking drain (1ms) to catch trailing
// frames still in the QUIC socket. The timeout was reduced from 50ms to 1ms
// since Geode's server does not send trailing metrics/heartbeat frames after
// the final data page in normal operation.
func (c *Conn) drainTrailingFrames() {
	drainCtx, drainCancel := context.WithTimeout(context.Background(), trailingFrameDrainTimeout)
	defer drainCancel()
	for {
		resp, err := c.transport.ReceiveProto(drainCtx)
		if err != nil {
			return // deadline elapsed or transport closed — stream is clean
		}
		execResp := resp.GetExecute()
		if execResp == nil {
			return
		}
		if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
			continue
		}
		if execResp.GetPage() != nil {
			continue
		}
		return
	}
}

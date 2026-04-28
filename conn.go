package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

// defaultDeprecatedMethodTimeout is the default timeout for deprecated methods
// (Prepare, Begin, Exec, Query) that use context.Background(). This prevents
// operations from hanging indefinitely. Use the Context variants for custom timeouts.
const defaultDeprecatedMethodTimeout = 30 * time.Second

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

// Conn represents a connection to a Geode database.
type Conn struct {
	cfg       *Config
	transport Transport

	mu         sync.Mutex
	state      ConnState
	inTx       bool
	activeRows *Rows
}

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
		return nil
	}

	// Double-check under lock to avoid racing with a concurrent first stream.
	transport.pc.mu.Lock()
	alreadyAuthed := transport.pc.authed
	transport.pc.mu.Unlock()

	if alreadyAuthed {
		return nil
	}

	serverGraph, err := conn.hello(ctx)
	if err != nil {
		pool.remove(transport.pc)
		return err
	}
	transport.pc.markAuthed(serverGraph)
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

	// CREATE GRAPH is idempotent — safe even if server already created it.
	if err := conn.execInit(ctx, "CREATE GRAPH IF NOT EXISTS "+cfg.Graph); err != nil {
		return fmt.Errorf("create graph %s: %w", cfg.Graph, err)
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
	serverGraph, err := conn.hello(ctx)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}

	// Auto-select graph if configured in DSN and not already bound by server.
	if cfg.Graph != "" && serverGraph == "" {
		if err := conn.execInit(ctx, "CREATE GRAPH IF NOT EXISTS "+cfg.Graph); err != nil {
			_ = transport.Close()
			return nil, fmt.Errorf("create graph %s: %w", cfg.Graph, err)
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
func (c *Conn) execInit(ctx context.Context, query string) error {
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query: query,
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

// hello performs the HELLO handshake with the server.
// It returns the graph name bound by the server (from HelloResponse.Graph),
// or "" if the server did not bind a graph during the handshake.
func (c *Conn) hello(ctx context.Context) (string, error) {
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
	msg := proto.NewHelloMessage(req)
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return "", fmt.Errorf("failed to send HELLO: %w", err)
	}

	// Read and validate HELLO response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to receive HELLO response: %w", err)
	}

	helloResp := resp.GetHello()
	if helloResp == nil {
		return "", fmt.Errorf("expected HELLO response")
	}

	if !helloResp.GetSuccess() {
		return "", &DriverError{
			Code:    "AUTH",
			Message: helloResp.GetErrorMessage(),
		}
	}

	return helloResp.GetGraph(), nil
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
			Type:    "input",
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

	// Send BEGIN
	msg := proto.NewBeginMessage(&proto.BeginRequest{
		ReadOnly: opts.ReadOnly,
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
		return "hello"
	case resp.GetExecute() != nil:
		ex := resp.GetExecute()
		if ex.GetError() != nil {
			return fmt.Sprintf("execute(error=%s)", ex.GetError().GetMessage())
		}
		return "execute"
	case resp.GetPull() != nil:
		return "pull"
	case resp.GetPing() != nil:
		return "ping"
	case resp.GetBegin() != nil:
		return "begin"
	case resp.GetCommit() != nil:
		return "commit"
	case resp.GetRollback() != nil:
		return "rollback"
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
		return "unknown"
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
			Type:    "input",
			Message: err.Error(),
		}
	}

	// Rewrite placeholders and build params
	newQuery, params := rewritePlaceholders(query, args)

	// Convert params to Param slice for protobuf
	var protoParams []*proto.Param
	for k, v := range params {
		protoParams = append(protoParams, proto.MakeParam(k, v))
	}

	// Send Execute
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:  newQuery,
		Params: protoParams,
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
			Type:    "input",
			Message: err.Error(),
		}
	}

	// Rewrite placeholders and build params
	newQuery, params := rewritePlaceholders(query, args)

	// Convert params to Param slice for protobuf
	var protoParams []*proto.Param
	for k, v := range params {
		protoParams = append(protoParams, proto.MakeParam(k, v))
	}

	// Send Execute
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		Query:  newQuery,
		Params: protoParams,
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
		return nil, fmt.Errorf("expected Execute response")
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
		c.activeRows == nil && !c.transport.IsClosed()
}

// CheckNamedValue validates a named value before use.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		if err := validate.ParamName(nv.Name); err != nil {
			return err
		}
	}
	return nil
}

// rewritePlaceholders replaces ? placeholders by inlining argument values
// directly into the query string. Named $param references are also collected
// into the params map for server-side resolution.
//
// Inlining is necessary because the Geode server does not resolve $param
// references inside property maps (e.g. CREATE/MERGE {key: $p1}) — only in
// expression contexts (RETURN, WHERE). By inlining, all positional args work
// regardless of GQL clause.
func rewritePlaceholders(query string, args []driver.NamedValue) (string, map[string]any) {
	params := make(map[string]any)

	var b strings.Builder
	b.Grow(len(query) + 32*len(args))

	argi := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '?' {
			argi++

			// Find matching arg by ordinal and inline its value.
			var val any
			for _, nv := range args {
				if nv.Ordinal == argi {
					val = nv.Value
					break
				}
			}
			b.WriteString(inlineValue(val))
		} else {
			b.WriteByte(ch)
		}
	}

	// Include explicitly named args for $name references in the query.
	for _, nv := range args {
		if nv.Name != "" {
			params[nv.Name] = nv.Value
		}
	}

	return b.String(), params
}

// gqlStringEscaper escapes a Go string for safe embedding in a GQL
// single-quoted string literal. Backslashes must be escaped first to
// prevent sequences like \' from being misinterpreted as an escaped
// quote by the GQL parser (which supports both ” and \' escaping).
//
//nolint:gochecknoglobals // package-level replacer avoids allocation per call
var gqlStringEscaper = strings.NewReplacer(
	`\`, `\\`,
	"'", "''",
	"\n", `\n`,
	"\r", `\r`,
	"\t", `\t`,
)

// inlineValue converts a Go value to a GQL literal string suitable for
// embedding directly in a query. Strings are single-quoted with backslashes,
// quotes, and control characters properly escaped.
func inlineValue(val any) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case string:
		return "'" + gqlStringEscaper.Replace(v) + "'"
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return "'" + gqlStringEscaper.Replace(fmt.Sprintf("%v", v)) + "'"
	}
}

// countPlaceholders counts the number of ? placeholders in a query.
func countPlaceholders(query string) int {
	count := 0
	for _, ch := range query {
		if ch == '?' {
			count++
		}
	}
	return count
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

package geode

import (
	"context"
	"database/sql/driver"
	"fmt"
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

	// Perform HELLO handshake
	if err := conn.hello(ctx); err != nil {
		_ = transport.Close()
		return nil, err
	}

	return conn, nil
}

// hello performs the HELLO handshake with the server.
func (c *Conn) hello(ctx context.Context) error {
	msg := proto.NewHelloMessage(&proto.HelloRequest{
		Username: c.cfg.Username,
		Password: c.cfg.Password,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return fmt.Errorf("failed to send HELLO: %w", err)
	}

	// Read and validate HELLO response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive HELLO response: %w", err)
	}

	helloResp := resp.GetHello()
	if helloResp == nil {
		return fmt.Errorf("expected HELLO response")
	}

	if !helloResp.GetSuccess() {
		return &DriverError{
			Code:    "AUTH",
			Message: helloResp.GetErrorMessage(),
		}
	}

	return nil
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

	// Send BEGIN
	msg := proto.NewBeginMessage(&proto.BeginRequest{
		ReadOnly: opts.ReadOnly,
	})
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return nil, err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return nil, err
	}

	if resp.GetBegin() == nil {
		return nil, fmt.Errorf("expected BEGIN response")
	}

	c.inTx = true
	c.state = StateInTransaction

	return &Tx{conn: c, ctx: ctx}, nil
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
		return nil, err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return nil, err
	}

	execResp := resp.GetExecute()
	if execResp == nil {
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
			return nil, err
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
		c.mu.Unlock()
		return nil, err
	}

	// Read response (could be schema, page, error, etc.)
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	execResp := resp.GetExecute()
	if execResp == nil {
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
			c.mu.Unlock()
			return nil, err
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

	// If in transaction, rollback
	if c.inTx {
		msg := proto.NewRollbackMessage(&proto.RollbackRequest{})
		if err := c.transport.SendProto(ctx, msg); err != nil {
			return err
		}

		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			return err
		}

		if resp.GetRollback() == nil {
			return fmt.Errorf("expected ROLLBACK response")
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

	return c.state != StateClosed && c.state != StateInError && !c.transport.IsClosed()
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

// rewritePlaceholders replaces ? placeholders with $pN parameters.
func rewritePlaceholders(query string, args []driver.NamedValue) (string, map[string]any) {
	params := make(map[string]any)

	// Replace ? with $pN
	var b strings.Builder
	b.Grow(len(query) + 8*len(args))

	argi := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '?' {
			argi++
			name := fmt.Sprintf("p%d", argi)
			b.WriteByte('$')
			b.WriteString(name)

			// Find matching arg by ordinal
			for _, nv := range args {
				if nv.Ordinal == argi {
					params[name] = nv.Value
					break
				}
			}
		} else {
			b.WriteByte(ch)
		}
	}

	// Include explicitly named args
	for _, nv := range args {
		if nv.Name != "" {
			params[nv.Name] = nv.Value
		}
	}

	return b.String(), params
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

// releaseRows is called by Rows.Close() to release the lock and update state.
func (c *Conn) releaseRows(r *Rows) {
	if c.activeRows == r {
		// Drain any remaining frames (metrics, heartbeat) left in the stream
		// after the final data page was read, to keep the stream in sync.
		for {
			msg, ok, err := c.transport.TryReceiveProtoBuffered()
			if err != nil || !ok {
				break
			}
			execResp := msg.GetExecute()
			if execResp == nil {
				break
			}
			if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
				continue
			}
			break
		}

		c.activeRows = nil
		if c.inTx {
			c.state = StateInTransaction
		} else {
			c.state = StateIdle
		}
	}
	c.mu.Unlock()
}

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

// tryReceiveInlineFrame attempts to read an extra frame already buffered (e.g., inline RUN+PULL).
// Returns nil, nil if no buffered frame is available (this is not an error condition).
func (c *Conn) tryReceiveInlineFrame(_ context.Context) (*proto.ExecutionResponse, error) {
	// Prefer a non-blocking buffered read to avoid partial line timeouts.
	msg, ok, err := c.transport.TryReceiveProtoBuffered()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil //nolint:nilnil // nil, nil indicates no frame available - this is expected
	}
	if msg.Execute != nil {
		return msg.Execute, nil
	}
	return nil, nil //nolint:nilnil // message has no execute field
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
	msg := &proto.QuicClientMessage{
		Hello: &proto.HelloRequest{
			Username: c.cfg.Username,
			Password: c.cfg.Password,
		},
	}
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return fmt.Errorf("failed to send HELLO: %w", err)
	}

	// Read and validate HELLO response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive HELLO response: %w", err)
	}

	if resp.Hello == nil {
		return fmt.Errorf("expected HELLO response")
	}

	if !resp.Hello.Success {
		return &DriverError{
			Code:    "AUTH",
			Message: resp.Hello.ErrorMessage,
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
	msg := &proto.QuicClientMessage{
		Begin: &proto.BeginRequest{
			ReadOnly: opts.ReadOnly,
		},
	}
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return nil, err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return nil, err
	}

	if resp.Begin == nil {
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

	// Convert params to string map for protobuf
	paramsProto := make(map[string]string)
	for k, v := range params {
		paramsProto[k] = fmt.Sprintf("%v", v)
	}

	// Send Execute
	msg := &proto.QuicClientMessage{
		Execute: &proto.ExecuteRequest{
			Query:      newQuery,
			Parameters: paramsProto,
		},
	}
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return nil, err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return nil, err
	}

	if resp.Execute == nil {
		return nil, fmt.Errorf("expected Execute response")
	}

	if resp.Execute.Error != nil {
		return nil, &DriverError{
			Code:    resp.Execute.Error.Code,
			Message: resp.Execute.Error.Message,
		}
	}

	// Drain any inline response to keep the stream in sync.
	if resp.Execute.Schema != nil {
		inlineResp, err := c.tryReceiveInlineFrame(ctx)
		if err != nil {
			return nil, err
		}
		if inlineResp != nil && inlineResp.Error != nil {
			return nil, &DriverError{
				Code:    inlineResp.Error.Code,
				Message: inlineResp.Error.Message,
			}
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

	// Convert params to string map for protobuf
	paramsProto := make(map[string]string)
	for k, v := range params {
		paramsProto[k] = fmt.Sprintf("%v", v)
	}

	// Send Execute
	msg := &proto.QuicClientMessage{
		Execute: &proto.ExecuteRequest{
			Query:      newQuery,
			Parameters: paramsProto,
		},
	}
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

	if resp.Execute == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("expected Execute response")
	}

	execResp := resp.Execute

	if execResp.Error != nil {
		c.mu.Unlock()
		return nil, &DriverError{
			Code:    execResp.Error.Code,
			Message: execResp.Error.Message,
		}
	}

	// Build column info from schema
	var columns []ColumnInfo
	if execResp.Schema != nil {
		columns = make([]ColumnInfo, len(execResp.Schema.Columns))
		for i, col := range execResp.Schema.Columns {
			columns[i] = ColumnInfo{
				Name: col.Name,
				Type: ParseGQLType(col.Type),
			}
		}
	}

	// Check for inline data page
	if inlineResp, err := c.tryReceiveInlineFrame(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	} else if inlineResp != nil {
		if inlineResp.Error != nil {
			c.mu.Unlock()
			return nil, &DriverError{
				Code:    inlineResp.Error.Code,
				Message: inlineResp.Error.Message,
			}
		}

		rows := &Rows{
			conn:     c,
			ctx:      ctx,
			columns:  columns,
			pageSize: c.cfg.PageSize,
		}

		if inlineResp.Page != nil {
			rows.protoBuffer = inlineResp.Page.Rows
			rows.final = inlineResp.Page.LastPage
		} else {
			rows.final = true
		}

		c.activeRows = rows
		c.state = StateFetching
		return rows, nil
	}

	// If we got a page directly in the first response
	if execResp.Page != nil {
		rows := &Rows{
			conn:        c,
			ctx:         ctx,
			columns:     columns,
			pageSize:    c.cfg.PageSize,
			protoBuffer: execResp.Page.Rows,
			final:       execResp.Page.LastPage,
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
	msg := &proto.QuicClientMessage{
		Ping: &proto.PingRequest{},
	}
	if err := c.transport.SendProto(ctx, msg); err != nil {
		return err
	}

	// Read response
	resp, err := c.transport.ReceiveProto(ctx)
	if err != nil {
		return err
	}

	if resp.Ping == nil {
		return fmt.Errorf("expected PING response")
	}

	if !resp.Ping.Ok {
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
		msg := &proto.QuicClientMessage{
			Rollback: &proto.RollbackRequest{},
		}
		if err := c.transport.SendProto(ctx, msg); err != nil {
			return err
		}

		resp, err := c.transport.ReceiveProto(ctx)
		if err != nil {
			return err
		}

		if resp.Rollback == nil {
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
		c.activeRows = nil
		if c.inTx {
			c.state = StateInTransaction
		} else {
			c.state = StateIdle
		}
	}
	c.mu.Unlock()
}

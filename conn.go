package geode

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	"go.codepros.org/geode/internal/validate"
)

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
// Returns nil if no buffered frame is available within a short timeout.
func (c *Conn) tryReceiveInlineFrame(ctx context.Context) (*Frame, error) {
	// Prefer a non-blocking buffered read to avoid partial line timeouts.
	if data, ok, err := c.transport.TryReceiveBuffered(); err != nil {
		return nil, err
	} else if ok {
		frame, err := ParseFrame(data)
		if err != nil {
			return nil, err
		}
		return frame, nil
	}

	return nil, nil
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

	// Create transport
	transport, err := NewQUICTransport(ctx, cfg)
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
	msg := BuildHelloMessage(c.cfg)
	if err := c.transport.Send(ctx, msg); err != nil {
		return fmt.Errorf("failed to send HELLO: %w", err)
	}

	// Read and validate HELLO response
	data, err := c.transport.Receive(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive HELLO response: %w", err)
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return fmt.Errorf("failed to parse HELLO response: %w", err)
	}

	if frame.IsError() {
		return frame.ToError()
	}

	return nil
}

// Prepare returns a prepared statement.
// Note: Geode doesn't have server-side prepared statements,
// so this is a client-side implementation.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement with context.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
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
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
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
	if err := c.transport.Send(ctx, BuildBeginMessage()); err != nil {
		return nil, err
	}

	// Read response
	data, err := c.transport.Receive(ctx)
	if err != nil {
		return nil, err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return nil, err
	}

	if frame.IsError() {
		return nil, frame.ToError()
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

	// Send RUN_GQL
	if err := c.transport.Send(ctx, BuildRunGQLMessage(newQuery, params)); err != nil {
		return nil, err
	}

	// Read response
	data, err := c.transport.Receive(ctx)
	if err != nil {
		return nil, err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return nil, err
	}

	if frame.IsError() {
		return nil, frame.ToError()
	}

	// Drain any inline BINDINGS frame to keep the stream in sync.
	if frame.IsSchema() {
		inlineFrame, err := c.tryReceiveInlineFrame(ctx)
		if err != nil {
			return nil, err
		}
		if inlineFrame != nil && inlineFrame.IsError() {
			return nil, inlineFrame.ToError()
		}
	}

	return &Result{}, nil
}

// QueryContext executes a query that returns rows.
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

	// Send RUN_GQL
	if err := c.transport.Send(ctx, BuildRunGQLMessage(newQuery, params)); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// Read SCHEMA response
	data, err := c.transport.Receive(ctx)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	if frame.IsError() {
		c.mu.Unlock()
		return nil, frame.ToError()
	}

	if !frame.IsSchema() {
		c.mu.Unlock()
		return nil, fmt.Errorf("geode: expected SCHEMA frame, got %s", frame.Result.Type)
	}

	// Build column info
	columns := make([]ColumnInfo, len(frame.Result.Columns))
	for i, col := range frame.Result.Columns {
		columns[i] = ColumnInfo{
			Name: col.Name,
			Type: ParseGQLType(col.Type),
		}
	}

	// Check for inline BINDINGS frame sent with RUN_GQL.
	if inlineFrame, err := c.tryReceiveInlineFrame(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	} else if inlineFrame != nil {
		if inlineFrame.IsError() {
			c.mu.Unlock()
			return nil, inlineFrame.ToError()
		}

		rows := &Rows{
			conn:      c,
			ctx:       ctx,
			columns:   columns,
			pageSize:  c.cfg.PageSize,
			ordered:   inlineFrame.Result.Ordered,
			orderKeys: inlineFrame.Result.OrderKeys,
		}

		switch inlineFrame.Result.Type {
		case RespBindings:
			rows.buffer = inlineFrame.Result.Rows
			// Inline frames represent full results; treat as final by default.
			rows.final = true
		case RespProfile, RespResult, RespStatus:
			rows.final = true
		default:
			c.mu.Unlock()
			return nil, fmt.Errorf("geode: unexpected inline frame type: %s", inlineFrame.Result.Type)
		}

		c.activeRows = rows
		c.state = StateFetching
		return rows, nil
	}

	// Send first PULL
	if err := c.transport.Send(ctx, BuildPullMessage(1, c.cfg.PageSize)); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	// Read BINDINGS response
	data, err = c.transport.Receive(ctx)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	frame, err = ParseFrame(data)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	if frame.IsError() {
		c.mu.Unlock()
		return nil, frame.ToError()
	}

	rows := &Rows{
		conn:      c,
		ctx:       ctx,
		columns:   columns,
		pageSize:  c.cfg.PageSize,
		ordered:   frame.Result.Ordered,
		orderKeys: frame.Result.OrderKeys,
	}

	switch frame.Result.Type {
	case RespBindings:
		rows.buffer = frame.Result.Rows
		rows.final = frame.Result.Final
	case RespProfile, RespResult, RespStatus:
		// No rows to return
		rows.final = true
	default:
		c.mu.Unlock()
		return nil, fmt.Errorf("geode: unexpected frame type after SCHEMA: %s", frame.Result.Type)
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
	if err := c.transport.Send(ctx, BuildPingMessage()); err != nil {
		return err
	}

	// Read response
	data, err := c.transport.Receive(ctx)
	if err != nil {
		return err
	}

	frame, err := ParseFrame(data)
	if err != nil {
		return err
	}

	if frame.IsError() {
		return frame.ToError()
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
		if err := c.transport.Send(ctx, BuildRollbackMessage()); err != nil {
			return err
		}

		data, err := c.transport.Receive(ctx)
		if err != nil {
			return err
		}

		frame, err := ParseFrame(data)
		if err != nil {
			return err
		}

		if frame.IsError() {
			return frame.ToError()
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

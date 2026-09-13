package graph

import (
	"context"
	"fmt"
	"os"
	"strings"

	geode "geodedb.com/geode"
)

// batchConcurrentEnabled reports whether GAP-0900 concurrent batch submission
// is opted in via GEODE_BATCH_CONCURRENT (1/true/yes/on). When enabled, a
// BatchExec whose statements are all simple node MERGEs is sent to the server
// as ONE `/*+BATCH*/`-prefixed multi-statement request, so the server submits
// every op to the group-commit coordinator concurrently (coalescing fsyncs)
// instead of one round-trip per statement. Default off → unchanged behaviour.
func batchConcurrentEnabled() bool {
	switch strings.ToLower(os.Getenv("GEODE_BATCH_CONCURRENT")) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// nodeMergeBatchable reports whether every statement is a node-only MERGE (no
// relationship arrow), the shape the server's `/*+BATCH*/` fast path accepts.
// A mixed/edge batch would make the server fall back to the serial general
// executor (slower than the normal path), so we only take the concurrent path
// when the whole chunk qualifies.
func nodeMergeBatchable(stmts []Statement) bool {
	for i := range stmts {
		q := stmts[i].Query
		if strings.Contains(q, "->") || strings.Contains(q, "<-") {
			return false
		}
		if !strings.Contains(strings.ToUpper(q), "MERGE") {
			return false
		}
	}
	return true
}

// avgStmtBytes is the average byte size estimate per MERGE statement used to
// pre-size the string builder in execConcurrentBatch.
const avgStmtBytes = 96

// avgArgsPerStmt is the average number of bound arguments per MERGE statement
// used to pre-size the args slice in execConcurrentBatch.
const avgArgsPerStmt = 8

// execConcurrentBatch sends all statements as a single `/*+BATCH*/` request.
// Each statement's `?` args are bound (not inlined): db.ExecContext rewrites
// them to `$_gqN` references and carries the values as typed proto.Param
// entries (bindParameters, params.go), so concatenating queries (joined by
// `;`) and args yields one multi-statement program where every value crosses
// the wire as a typed param rather than query text. The engine's batch
// executor admits `literal_or_param` in this position, so the `$_gqN`
// references resolve the same way a literal would.
func (c *Conn) execConcurrentBatch(ctx context.Context, stmts []Statement) error {
	var b strings.Builder
	b.Grow(len(stmts) * avgStmtBytes)
	b.WriteString("/*+BATCH*/ ")
	args := make([]any, 0, len(stmts)*avgArgsPerStmt)
	for i := range stmts {
		q := strings.TrimRight(strings.TrimSpace(stmts[i].Query), ";")
		b.WriteString(q)
		b.WriteString(";\n")
		args = append(args, stmts[i].Args...)
	}
	_, err := c.db.ExecContext(ctx, b.String(), args...)
	return err
}

// retryMultiplier is the exponential backoff multiplier used in the retry
// policy built from Conn options.
const retryMultiplier = 2.0

// retryMaxBackoffFactor scales RetryBackoff up to derive MaxBackoff.
const retryMaxBackoffFactor = 30

// Statement is a GQL query with its bound arguments.
type Statement struct {
	Query string
	Args  []any
}

// BatchResult summarises the outcome of a BatchExec call.
type BatchResult struct {
	Total     int   // number of statements in the batch
	Succeeded int   // number that executed successfully
	Failed    int   // number that failed
	Err       error // error that stopped execution, if any
}

// BatchExec executes stmts sequentially. Each statement is retried according
// to the retry policy derived from the Conn options. Execution stops at the
// first unrecoverable error. An empty slice returns a zero BatchResult with a
// nil error.
func (c *Conn) BatchExec(ctx context.Context, stmts []Statement) (BatchResult, error) {
	if len(stmts) == 0 {
		return BatchResult{}, nil
	}

	policy := geode.RetryPolicy{
		MaxAttempts:    c.opts.MaxRetries,
		InitialBackoff: c.opts.RetryBackoff,
		MaxBackoff:     c.opts.RetryBackoff * retryMaxBackoffFactor,
		Multiplier:     retryMultiplier,
	}

	result := BatchResult{Total: len(stmts)}

	for i, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			result.Failed = result.Total - result.Succeeded
			result.Err = err
			return result, err
		}

		execErr := geode.Retry(ctx, policy, func(ctx context.Context) error {
			_, err := c.db.ExecContext(ctx, stmt.Query, stmt.Args...)
			return err
		})

		if execErr != nil {
			result.Failed = result.Total - result.Succeeded
			result.Err = fmt.Errorf("graph: batch statement %d: %w", i, execErr)
			return result, result.Err
		}

		result.Succeeded++
	}

	return result, nil
}

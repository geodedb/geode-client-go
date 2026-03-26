package graph

import (
	"context"
	"fmt"

	geode "geodedb.com/geode"
)

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

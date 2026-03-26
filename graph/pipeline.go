package graph

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	geode "geodedb.com/geode"
)

// defaultPipelineWorkers is the fallback worker count when Conn.opts.Workers
// is not positive.
const defaultPipelineWorkers = 8

// PipelineResult summarises the outcome of a PipelineExec call.
type PipelineResult struct {
	Total     int   // number of statements submitted
	Succeeded int   // number that executed successfully
	Failed    int   // number that failed
	Err       error // first error encountered, if any
}

// PipelineOption is a functional option for configuring a PipelineExec call.
type PipelineOption func(*pipelineConfig)

type pipelineConfig struct {
	progressFn func(completed, total int)
}

// WithProgress registers a callback that is invoked after each successful
// statement execution. The callback receives the number of completed
// statements and the total number of statements in the pipeline.
func WithProgress(fn func(completed, total int)) PipelineOption {
	return func(cfg *pipelineConfig) {
		cfg.progressFn = fn
	}
}

// PipelineExec executes stmts in parallel using a bounded worker pool. Each
// statement is retried according to the retry policy derived from the Conn
// options. The first unrecoverable error cancels all pending work
// (first-error-stops semantics). An empty slice returns a zero PipelineResult
// with a nil error.
func (c *Conn) PipelineExec(ctx context.Context, stmts []Statement, opts ...PipelineOption) (PipelineResult, error) {
	if len(stmts) == 0 {
		return PipelineResult{}, nil
	}

	cfg := &pipelineConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	workers := c.opts.Workers
	if workers <= 0 {
		workers = defaultPipelineWorkers
	}

	policy := geode.RetryPolicy{
		MaxAttempts:    c.opts.MaxRetries,
		InitialBackoff: c.opts.RetryBackoff,
		MaxBackoff:     c.opts.RetryBackoff * retryMaxBackoffFactor,
		Multiplier:     retryMultiplier,
	}

	total := len(stmts)

	// childCtx is cancelled on the first error to stop remaining goroutines.
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg        sync.WaitGroup
		once      sync.Once
		firstErr  error
		succeeded atomic.Int64
	)

	// semaphore limits concurrency to `workers` goroutines.
	sem := make(chan struct{}, workers)

	for i, stmt := range stmts {
		wg.Add(1)
		go func(idx int, s Statement) {
			defer wg.Done()

			// Acquire semaphore slot.
			select {
			case sem <- struct{}{}:
			case <-childCtx.Done():
				once.Do(func() {
					firstErr = fmt.Errorf("graph: pipeline statement %d: %w", idx, childCtx.Err())
				})
				return
			}
			defer func() { <-sem }()

			execErr := geode.Retry(childCtx, policy, func(ctx context.Context) error {
				_, err := c.db.ExecContext(ctx, s.Query, s.Args...)
				return err
			})

			if execErr != nil {
				once.Do(func() {
					firstErr = fmt.Errorf("graph: pipeline statement %d: %w", idx, execErr)
				})
				cancel()
				return
			}

			n := int(succeeded.Add(1))
			if cfg.progressFn != nil {
				cfg.progressFn(n, total)
			}
		}(i, stmt)
	}

	wg.Wait()

	result := PipelineResult{
		Total:     total,
		Succeeded: int(succeeded.Load()),
		Err:       firstErr,
	}
	result.Failed = result.Total - result.Succeeded

	return result, firstErr
}

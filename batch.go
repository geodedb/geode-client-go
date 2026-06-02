// batch.go
package geode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// Statement is a GQL query paired with its bound arguments.
type Statement struct {
	Query string
	Args  []any
}

const (
	// DefaultChunkSize is the default number of statements per chunk in PipelineExec.
	DefaultChunkSize = 100

	// DefaultMaxWorkers is the default maximum number of parallel workers in PipelineExec.
	DefaultMaxWorkers = 8
)

// BatchExec executes stmts inside a single transaction with automatic retry on
// retryable errors (transport failures, serialization conflicts). A transaction
// pins the underlying QUIC connection for the entire batch, eliminating
// per-statement pool churn that causes EOF errors under concurrent load.
//
// Retry behaviour is governed by [DefaultRetryPolicy] (3 attempts, exponential
// backoff 1 s–30 s, multiplier 2×). Non-retryable errors are returned
// immediately without further attempts.
//
// An empty stmts slice is a no-op.
func (p *Pool) BatchExec(ctx context.Context, stmts []Statement) error {
	if len(stmts) == 0 {
		return nil
	}
	return Retry(ctx, DefaultRetryPolicy(), func(ctx context.Context) error {
		return batchExecTx(ctx, p.db, stmts)
	})
}

// batchExecTx executes all statements within a single transaction.
func batchExecTx(ctx context.Context, db *sql.DB, stmts []Statement) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("geode: batch begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			_ = rbErr
		}
	}()
	for i, stmt := range stmts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := tx.ExecContext(ctx, stmt.Query, stmt.Args...); err != nil {
			return fmt.Errorf("geode: statement %d: %w", i+1, err)
		}
	}
	return tx.Commit()
}

// PipelineExec executes statements in parallel chunks using a worker pool.
// It splits stmts into chunks of chunkSize and runs up to maxWorkers chunks
// concurrently; each chunk is executed by [Pool.BatchExec].
//
// Use 0 for chunkSize or maxWorkers to apply the defaults (100 and 8).
//
// PipelineExec returns the first error encountered. In-flight chunks are
// allowed to complete; cancel ctx to abort them early.
func (p *Pool) PipelineExec(ctx context.Context, stmts []Statement, chunkSize, maxWorkers int) error {
	if len(stmts) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if maxWorkers <= 0 {
		maxWorkers = DefaultMaxWorkers
	}

	total := len(stmts)
	chunks := make([][]Statement, 0, (total/chunkSize)+1)
	for i := 0; i < total; i += chunkSize {
		end := min(i+chunkSize, total)
		chunks = append(chunks, stmts[i:end])
	}

	sem := make(chan struct{}, maxWorkers)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	for _, chunk := range chunks {
		if ctx.Err() != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
			break
		}
		mu.Lock()
		hasErr := firstErr != nil
		mu.Unlock()
		if hasErr {
			break
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(ch []Statement) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := p.BatchExec(ctx, ch); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(chunk)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

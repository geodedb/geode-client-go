// Package graph provides a high-level API for the Geode graph database,
// built on top of the database/sql-compatible driver. It adds batch execution,
// pipeline processing, schema management, and transaction ergonomics that
// go beyond what database/sql offers.
package graph

import (
	"log/slog"
	"time"
)

// Default option values.
const (
	defaultMaxRetries   = 3
	defaultRetryBackoff = time.Second
	defaultBatchSize    = 100
	defaultWorkers      = 8
)

// Options holds configuration for a Conn.
type Options struct {
	MaxRetries   int
	RetryBackoff time.Duration
	Logger       *slog.Logger
	BatchSize    int
	Workers      int
}

// Option is a functional option for configuring a Conn.
type Option func(*Options)

// WithRetry configures the maximum number of retries and the backoff duration
// between attempts.
func WithRetry(maxRetries int, backoff time.Duration) Option {
	return func(o *Options) {
		o.MaxRetries = maxRetries
		o.RetryBackoff = backoff
	}
}

// WithLogger sets a structured logger for the Conn.
func WithLogger(logger *slog.Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// WithBatchSize sets the number of items processed per batch operation.
func WithBatchSize(size int) Option {
	return func(o *Options) {
		o.BatchSize = size
	}
}

// WithWorkers sets the number of concurrent workers used in pipeline and batch
// operations.
func WithWorkers(n int) Option {
	return func(o *Options) {
		o.Workers = n
	}
}

// ApplyOptions returns an Options populated with defaults, then applies each
// provided option in order.
//
// Defaults:
//   - MaxRetries:   3
//   - RetryBackoff: 1s
//   - BatchSize:    100
//   - Workers:      8
func ApplyOptions(opts ...Option) Options {
	o := Options{
		MaxRetries:   defaultMaxRetries,
		RetryBackoff: defaultRetryBackoff,
		BatchSize:    defaultBatchSize,
		Workers:      defaultWorkers,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

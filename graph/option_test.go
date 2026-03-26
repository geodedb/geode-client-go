package graph_test

import (
	"log/slog"
	"testing"
	"time"

	"geodedb.com/geode/graph"
)

func TestWithRetry(t *testing.T) {
	t.Parallel()

	opts := graph.ApplyOptions(graph.WithRetry(5, 2*time.Second))

	if opts.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", opts.MaxRetries)
	}
	if opts.RetryBackoff != 2*time.Second {
		t.Errorf("RetryBackoff = %v, want 2s", opts.RetryBackoff)
	}
}

func TestWithLogger(t *testing.T) {
	t.Parallel()

	logger := slog.Default()
	opts := graph.ApplyOptions(graph.WithLogger(logger))

	if opts.Logger != logger {
		t.Errorf("Logger = %v, want %v", opts.Logger, logger)
	}
}

func TestWithBatchSize(t *testing.T) {
	t.Parallel()

	opts := graph.ApplyOptions(graph.WithBatchSize(250))

	if opts.BatchSize != 250 {
		t.Errorf("BatchSize = %d, want 250", opts.BatchSize)
	}
}

func TestWithWorkers(t *testing.T) {
	t.Parallel()

	opts := graph.ApplyOptions(graph.WithWorkers(16))

	if opts.Workers != 16 {
		t.Errorf("Workers = %d, want 16", opts.Workers)
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()

	opts := graph.ApplyOptions()

	if opts.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", opts.MaxRetries)
	}
	if opts.RetryBackoff != time.Second {
		t.Errorf("RetryBackoff = %v, want 1s", opts.RetryBackoff)
	}
	if opts.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", opts.BatchSize)
	}
	if opts.Workers != 8 {
		t.Errorf("Workers = %d, want 8", opts.Workers)
	}
	if opts.Logger != nil {
		t.Errorf("Logger = %v, want nil", opts.Logger)
	}
}

package geode

import (
	"context"
	"math"
	"time"
)

// Default retry policy values.
const (
	defaultMaxAttempts    = 3
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = 30 * time.Second
	defaultMultiplier     = 2.0
)

// RetryPolicy configures retry behavior for transient errors.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of attempts including the initial call.
	// Values less than 1 are treated as 1.
	MaxAttempts int

	// InitialBackoff is the delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration. The exponential backoff is
	// clamped to this value.
	MaxBackoff time.Duration

	// Multiplier is the exponential backoff multiplier applied on each retry.
	Multiplier float64
}

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults:
// 3 max attempts, 1s initial backoff, 30s max backoff, 2.0 multiplier.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    defaultMaxAttempts,
		InitialBackoff: defaultInitialBackoff,
		MaxBackoff:     defaultMaxBackoff,
		Multiplier:     defaultMultiplier,
	}
}

// backoff calculates the backoff duration for the given attempt (1-indexed).
// The result is clamped to MaxBackoff.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// InitialBackoff * Multiplier^(attempt-1)
	d := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt-1))
	if d > float64(p.MaxBackoff) {
		return p.MaxBackoff
	}
	return time.Duration(d)
}

// Retry executes fn up to policy.MaxAttempts times, retrying only when
// IsRetryableError returns true. Non-retryable errors and non-Geode errors
// are returned immediately without further attempts. Context cancellation is
// checked before each retry sleep.
func Retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		// Only retry Geode errors that are explicitly retryable.
		if !IsRetryableError(lastErr) {
			return lastErr
		}

		// No more attempts remaining.
		if attempt == maxAttempts {
			break
		}

		// Check context before sleeping.
		if err := ctx.Err(); err != nil {
			return err
		}

		delay := policy.backoff(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return lastErr
}

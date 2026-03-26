package geode

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryPolicy_Defaults(t *testing.T) {
	t.Parallel()

	p := DefaultRetryPolicy()

	if p.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", p.MaxAttempts)
	}
	if p.InitialBackoff != time.Second {
		t.Errorf("InitialBackoff = %v, want 1s", p.InitialBackoff)
	}
	if p.MaxBackoff != 30*time.Second {
		t.Errorf("MaxBackoff = %v, want 30s", p.MaxBackoff)
	}
	if p.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", p.Multiplier)
	}
}

func TestRetry_SucceedsFirstAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		return nil
	})

	if err != nil {
		t.Errorf("Retry() = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

func TestRetry_RetriesOnTransportError(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}
	transportErr := &TransportError{Op: "read", cause: errors.New("connection reset")}

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		if calls < 3 {
			return transportErr
		}
		return nil
	})

	if err != nil {
		t.Errorf("Retry() = %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("fn called %d times, want 3", calls)
	}
}

func TestRetry_RetriesOnSerializationError(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}
	serializationErr := &DriverError{Class: StatusSerialization, Code: "TX001", Message: "serialization failure"}

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		if calls < 2 {
			return serializationErr
		}
		return nil
	})

	if err != nil {
		t.Errorf("Retry() = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("fn called %d times, want 2", calls)
	}
}

func TestRetry_StopsOnNonRetryableError(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}
	syntaxErr := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		return syntaxErr
	})

	if !errors.Is(err, syntaxErr) {
		t.Errorf("Retry() = %v, want syntaxErr", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}
	transportErr := &TransportError{Op: "dial", cause: errors.New("refused")}

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		return transportErr
	})

	if !errors.Is(err, transportErr) {
		t.Errorf("Retry() = %v, want transportErr", err)
	}
	if calls != 3 {
		t.Errorf("fn called %d times, want 3", calls)
	}
}

func TestRetry_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2.0,
	}
	transportErr := &TransportError{Op: "read", cause: errors.New("reset")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := Retry(ctx, policy, func(_ context.Context) error {
		return transportErr
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Retry() = %v, want context.Canceled", err)
	}
}

func TestRetry_NonGeodeErrorNotRetried(t *testing.T) {
	t.Parallel()

	calls := 0
	policy := RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}
	plainErr := errors.New("some plain error")

	err := Retry(context.Background(), policy, func(_ context.Context) error {
		calls++
		return plainErr
	})

	if !errors.Is(err, plainErr) {
		t.Errorf("Retry() = %v, want plainErr", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1", calls)
	}
}

func TestRetry_BackoffClampsToMax(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxAttempts:    10,
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     10.0,
	}

	// attempt 3: 1s * 10^2 = 100s, clamped to 5s
	got := policy.backoff(3)
	if got != 5*time.Second {
		t.Errorf("backoff(3) = %v, want 5s", got)
	}
}

package geode

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Default retry policy values.
const (
	defaultMaxAttempts    = 3
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = 30 * time.Second
	defaultMultiplier     = 2.0
)

// backoffJitterFloor is the fraction of the computed delay that a jittered
// wait always pays. The wait is drawn uniformly from [floor*d, d], which
// spreads a herd of callers that were all refused by the same shared resource
// at the same instant — pool exhaustion and engine system errors arrive in
// exactly that shape — while still guaranteeing most of the delay actually
// elapses. Full jitter (drawing from [0, d]) would let an unlucky caller retry
// almost immediately, which is the behaviour the backoff exists to prevent.
const backoffJitterFloor = 0.5

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

	// Classes restricts which retry classes are retried. A nil map means every
	// class except RetryNever, which is the historical behaviour widened from
	// the two retryable ISO status classes.
	//
	// A caller narrows it when a class is one IT must handle rather than one
	// Retry can: an ingest worker holding no idempotency key sets
	// {RetrySameConn: true, RetryBackoff: true} so a connection-level failure
	// is handed back instead of replayed. RetryNever is never retried however
	// the map is set.
	Classes map[RetryClass]bool
}

// DefaultRetryPolicy returns a RetryPolicy with sensible defaults:
// 3 max attempts, 1s initial backoff, 30s max backoff, 2.0 multiplier, and
// every class except RetryNever retried.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    defaultMaxAttempts,
		InitialBackoff: defaultInitialBackoff,
		MaxBackoff:     defaultMaxBackoff,
		Multiplier:     defaultMultiplier,
	}
}

// allows reports whether the policy retries the given class.
func (p RetryPolicy) allows(c RetryClass) bool {
	if c == RetryNever {
		return false
	}
	if p.Classes == nil {
		return true
	}
	return p.Classes[c]
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

// classBackoff is the wait before retrying a failure of class c on attempt
// (1-indexed). It is always bounded by the policy: no draw exceeds
// backoff(attempt), and therefore none exceeds MaxBackoff.
//
// Only RetryBackoff is jittered. It is the class raised by contention for
// something SHARED — the connection pool, the engine itself — so every caller
// refused by it was refused at the same moment and would otherwise retry at
// the same moment. RetrySameConn and RetryNewConn are per-connection failures
// with no such correlation, and giving them a deterministic delay keeps
// existing timing behaviour exactly as it was.
func (p RetryPolicy) classBackoff(c RetryClass, attempt int) time.Duration {
	d := p.backoff(attempt)
	if c != RetryBackoff || d <= 0 {
		return d
	}
	// math/rand is the right tool here: jitter spreads a retry herd, it is not
	// a security decision, and a CSPRNG draw per retry would cost more than it
	// bought.
	spread := backoffJitterFloor + (1-backoffJitterFloor)*rand.Float64() //nolint:gosec // see above
	return time.Duration(float64(d) * spread)
}

// Retry executes fn up to policy.MaxAttempts times, retrying only failures
// whose RetryClass the policy allows.
//
// What each class means here:
//
//   - RetryNever returns immediately, wrapping the failure so the caller can
//     still match it with errors.Is and errors.As and can see which class
//     stopped the retry. Unclassifiable errors are RetryNever, so Retry never
//     loops on a failure it does not understand.
//   - RetrySameConn retries after the policy's delay. Nothing is wrong with
//     the connection.
//   - RetryNewConn retries after the policy's delay, but the CONNECTION must
//     be discarded first, and a bare fn gives Retry no handle on one. Retry
//     relies on database/sql to do it: an unpinned statement that fails with a
//     driver.ErrBadConn-matching error is not handed back to the pool, so the
//     next attempt lands on a different connection. A caller holding a pinned
//     connection must use Pool.RetrySession, which discards and re-pins
//     explicitly — Retry cannot do it for them, because a *sql.Conn the caller
//     captured in fn's closure is invisible from here.
//   - RetryBackoff retries after a JITTERED delay drawn from the policy's
//     bound (see classBackoff).
//
// Context cancellation is checked before each retry sleep.
func Retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	var lastClass RetryClass
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		// Only retry classes this policy allows. Everything else — including
		// everything the driver could not classify — stops here.
		lastClass = Classify(lastErr)
		if !policy.allows(lastClass) {
			return notRetried(lastClass, lastErr)
		}

		// No more attempts remaining.
		if attempt == maxAttempts {
			break
		}

		// Check context before sleeping.
		if err := ctx.Err(); err != nil {
			return err
		}

		delay := policy.classBackoff(lastClass, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return lastErr
}

// notRetried wraps an error Retry declined to retry, naming the class that
// decided it. The cause is wrapped with %w, so errors.Is, errors.As,
// IsDriverError and Classify all keep working on the returned error.
func notRetried(c RetryClass, err error) error {
	return fmt.Errorf("geode: not retried (class %s): %w", c, err)
}

// RetrySession runs fn against a Session pinned out of this pool, retrying per
// the policy, and is the class-aware form of Retry for a caller that needs the
// SAME connection across the statements in one attempt.
//
// It is what gives RetryNewConn its meaning. Retry cannot discard a connection
// it has no handle on; RetrySession owns the Session, so on a RetryNewConn
// failure it closes the pinned one — which returns a connection database/sql
// has already marked bad, so the pool does not hand it out again — and pins a
// fresh one for the next attempt. A RetrySameConn or RetryBackoff failure
// keeps the Session, because the connection is not what failed.
//
// fn receives a live Session on every attempt and must not close it; it must
// also not retain it past its own return, since a later attempt may hold a
// different connection. A Session that failed a SESSION invariant rather than
// a connection one (ErrSessionBusy from a nested TX, say) is RetryNever and is
// returned as-is.
//
// A pin that fails is itself classified: pinning is refused with
// ErrPoolDraining while the pool is shutting down (RetryNever, returned at
// once) and can report a pool-exhaustion failure (RetryBackoff, waited out and
// retried).
//
// fn is NOT partially applied: on a RetryNewConn outcome the whole fn runs
// again from its first line, on a fresh pinned connection, not from wherever
// it failed. Any statement fn already issued against the discarded connection
// happened for real as far as the engine is concerned — RetrySession has no
// way to know which of fn's side effects landed before the failure — so fn
// must itself be idempotent, or must detect and skip its own prior partial
// effects before repeating them, whenever it does more than one statement per
// attempt. This is precisely why 08006 (a generic engine timeout reachable
// from any request path, whose outcome the caller who issued it cannot
// confirm) classifies RetryNever rather than RetryNewConn: the driver cannot
// tell whether fn's operation already applied, so it cannot decide
// idempotency for the caller, and refuses to guess.
func (p *Pool) RetrySession(
	ctx context.Context,
	policy RetryPolicy,
	fn func(ctx context.Context, s *Session) error,
) error {
	if p == nil || p.db == nil {
		return fmt.Errorf("geode: Pool.RetrySession: %w", ErrPoolNotInitialized)
	}
	if fn == nil {
		return fmt.Errorf("geode: Pool.RetrySession: %w", ErrNilTxFunc)
	}

	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var s *Session
	defer func() {
		if s != nil {
			_ = s.Close()
		}
	}()

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		class, err := p.sessionAttempt(ctx, &s, fn)
		if err == nil {
			return nil
		}
		lastErr = err

		if !policy.allows(class) {
			return notRetried(class, err)
		}

		// The connection is what failed: discard it so the next attempt pins a
		// different one. Closing a Session whose connection database/sql has
		// already marked bad releases it without returning it to the pool.
		if class == RetryNewConn && s != nil {
			_ = s.Close()
			s = nil
		}

		if attempt == maxAttempts {
			break
		}
		if werr := sleepBackoff(ctx, policy, class, attempt); werr != nil {
			return werr
		}
	}

	return lastErr
}

// sessionAttempt pins a Session if *s does not already hold one, then runs fn
// on it, reporting the failure and its class. A pin that fails is classified
// like any other failure, so a drain stops the loop at once (RetryNever) while
// pool exhaustion waits and tries again (RetryBackoff).
func (p *Pool) sessionAttempt(
	ctx context.Context,
	s **Session,
	fn func(ctx context.Context, s *Session) error,
) (RetryClass, error) {
	if *s == nil {
		pinned, err := p.Session(ctx)
		if err != nil {
			return Classify(err), err
		}
		*s = pinned
	}
	if err := fn(ctx, *s); err != nil {
		return Classify(err), err
	}
	return RetryNever, nil
}

// sleepBackoff waits out the delay for one retry, or reports why it will not:
// the context ended. Checked before the sleep as well as during it, so an
// already-cancelled caller is told at once rather than after one more delay.
func sleepBackoff(ctx context.Context, policy RetryPolicy, c RetryClass, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := time.NewTimer(policy.classBackoff(c, attempt))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

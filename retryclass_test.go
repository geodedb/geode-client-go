package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryClass
	}{
		{"nil", nil, RetryNever},
		{"context canceled", context.Canceled, RetryNever},
		{"context deadline", context.DeadlineExceeded, RetryNever},
		{"read-only refusal", ErrReadOnly, RetryNever},
		{"tenant required", ErrTenantRequired, RetryNever},
		{"syntax error", &DriverError{Class: StatusSyntax, Code: "S001"}, RetryNever},
		{"auth error", &DriverError{Class: StatusAuth, Code: "A001"}, RetryNever},
		{"constraint", &DriverError{Class: StatusConstraint, Code: "C001"}, RetryNever},
		{"invalid param", &DriverError{Class: StatusInvalidParam, Code: "P001"}, RetryNever},
		{"serialization", &DriverError{Class: StatusSerialization, Code: "40001"}, RetrySameConn},
		{"deadlock", &DriverError{Class: StatusDeadlock, Code: "40502"}, RetrySameConn},
		{"system error", &DriverError{Class: StatusSystem, Code: "58000"}, RetryBackoff},
		{"bad conn", ErrBadConn, RetryNewConn},
		{"closed", ErrClosed, RetryNewConn},
		{"transport", &TransportError{Op: "read_length"}, RetryNewConn},
		{"pool exhausted", ErrPoolExhausted, RetryBackoff},
		{"wrapped serialization", fmt.Errorf("batch: %w",
			&DriverError{Class: StatusSerialization}), RetrySameConn},
		{"unknown", errors.New("boom"), RetryNever},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestRetryClass_String(t *testing.T) {
	want := map[RetryClass]string{
		RetryNever:    "never",
		RetrySameConn: "same_conn",
		RetryNewConn:  "new_conn",
		RetryBackoff:  "backoff",
	}
	for c, s := range want {
		if c.String() != s {
			t.Errorf("RetryClass(%d).String() = %q, want %q", c, c.String(), s)
		}
	}
}

func TestRetry_HonoursClassFilter(t *testing.T) {
	policy := DefaultRetryPolicy()
	policy.MaxAttempts = 3
	policy.InitialBackoff = 0
	// A partial map is the whole point: the classes left out are the ones the
	// caller wants handed back instead of replayed.
	policy.Classes = map[RetryClass]bool{RetryNewConn: true} //nolint:exhaustive // serialization NOT retried

	attempts := 0
	err := Retry(context.Background(), policy, func(context.Context) error {
		attempts++
		return &DriverError{Class: StatusSerialization}
	})
	if err == nil {
		t.Fatal("err = nil, want the serialization error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (class not in the filter)", attempts)
	}
}

func TestRetry_RetriesAllowedClass(t *testing.T) {
	policy := DefaultRetryPolicy()
	policy.MaxAttempts = 3
	policy.InitialBackoff = 0

	attempts := 0
	err := Retry(context.Background(), policy, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &DriverError{Class: StatusSerialization}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Exhaustive classification: every sentinel the package exports
// ─────────────────────────────────────────────────────────────────────────────

// TestClassify_EverySentinel pins a class on EVERY sentinel error this package
// exports. The table is exhaustive by construction: a new sentinel that is not
// added here is a sentinel nobody decided a retry answer for, and the count
// assertion below is what makes that omission fail rather than pass quietly.
//
// The default for anything unlisted is RetryNever (fail closed), so an omission
// is never DANGEROUS — it is merely undecided, and undecided is what this test
// refuses to let ship.
func TestClassify_EverySentinel(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryClass
	}{
		// Deterministic refusals: the driver or the engine said no, and will
		// say no identically to a second attempt.
		{"ErrReadOnly", ErrReadOnly, RetryNever},
		{"ErrTenantRequired", ErrTenantRequired, RetryNever},
		{"ErrTenantScopeRequired", ErrTenantScopeRequired, RetryNever},
		{"ErrParamCount", ErrParamCount, RetryNever},
		{"ErrReservedParamName", ErrReservedParamName, RetryNever},
		{"ErrVectorTooLarge", ErrVectorTooLarge, RetryNever},
		{"ErrVectorEmpty", ErrVectorEmpty, RetryNever},
		{"ErrVectorNotFinite", ErrVectorNotFinite, RetryNever},
		{"ErrInvalidIdent", ErrInvalidIdent, RetryNever},
		{"ErrInvalidString", ErrInvalidString, RetryNever},
		{"ErrMalformedCapabilityRows", ErrMalformedCapabilityRows, RetryNever},
		{"ErrSessionClosed", ErrSessionClosed, RetryNever},
		{"ErrSessionBusy", ErrSessionBusy, RetryNever},
		{"ErrNilTxFunc", ErrNilTxFunc, RetryNever},
		{"ErrPoolNotInitialized", ErrPoolNotInitialized, RetryNever},
		{"ErrQueryInProgress", ErrQueryInProgress, RetryNever},
		{"ErrTxInProgress", ErrTxInProgress, RetryNever},
		{"ErrNoTx", ErrNoTx, RetryNever},
		{"ErrStmtClosed", ErrStmtClosed, RetryNever},
		{"ErrRowsClosed", ErrRowsClosed, RetryNever},
		{"ErrCapabilityUnsupported", ErrCapabilityUnsupported, RetryNever},
		{"ErrPlanKindMismatch", ErrPlanKindMismatch, RetryNever},

		// The pool is going away; no connection it could hand back outlives it.
		{"ErrPoolDraining", ErrPoolDraining, RetryNever},

		// Resource pressure: the same request may well succeed later.
		{"ErrPoolExhausted", ErrPoolExhausted, RetryBackoff},

		// Connection-level: the retry has to land somewhere else.
		{"ErrBadConn", ErrBadConn, RetryNewConn},
		{"driver.ErrBadConn", driver.ErrBadConn, RetryNewConn},
		{"ErrClosed", ErrClosed, RetryNewConn},
		{"ErrInvalidState", ErrInvalidState, RetryNewConn},
		{"ErrTxDone", ErrTxDone, RetryNewConn},
		{"ErrSessionRetired", ErrSessionRetired, RetryNewConn},
	}

	// 30 cases: the 29 sentinels this package declares, plus driver.ErrBadConn,
	// which the Session layer wraps into ErrSessionRetired and which
	// database/sql raises directly. Bump it deliberately when a sentinel is
	// added, having decided its class.
	if len(cases) != 30 {
		t.Fatalf("the sentinel table holds %d cases, want 30 — a sentinel was "+
			"added or removed without deciding its retry class", len(cases))
	}

	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %v, want %v", c.name, got, c.want)
		}
		// Every sentinel must survive wrapping: callers see them wrapped far
		// more often than bare.
		wrapped := fmt.Errorf("ingest batch 7: %w", c.err)
		if got := Classify(wrapped); got != c.want {
			t.Errorf("Classify(wrapped %s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestClassify_SessionRetiredOutranksSessionClosed pins the one ordering in
// Classify that a reader would otherwise get wrong.
//
// ErrSessionRetired wraps BOTH ErrSessionClosed ("unusable") and
// driver.ErrBadConn ("because the connection failed"), so errors.Is matches it
// against a RetryNever sentinel and a RetryNewConn sentinel at the same time.
// The cause is what decides: a retired Session is retryable on a different
// connection, while a Session the caller closed is not retryable at all.
func TestClassify_SessionRetiredOutranksSessionClosed(t *testing.T) {
	if !errors.Is(ErrSessionRetired, ErrSessionClosed) {
		t.Fatal("ErrSessionRetired no longer matches ErrSessionClosed; this test's premise is gone")
	}
	if got := Classify(ErrSessionRetired); got != RetryNewConn {
		t.Errorf("Classify(ErrSessionRetired) = %v, want %v", got, RetryNewConn)
	}
	if got := Classify(ErrSessionClosed); got != RetryNever {
		t.Errorf("Classify(ErrSessionClosed) = %v, want %v", got, RetryNever)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Exhaustive classification: every GQLSTATE class the driver maps
// ─────────────────────────────────────────────────────────────────────────────

// TestClassify_EveryGQLStateClass pins a class on every status class this
// driver names (errors.go) and on every GQLSTATE the pinned engine (0.8.15) is
// known to emit, with the engine source that emits it cited alongside.
func TestClassify_EveryGQLStateClass(t *testing.T) {
	cases := []struct {
		name  string
		state string
		want  RetryClass
	}{
		// Success and warning states are not failures at all; a caller that
		// somehow routes one here must not loop on it.
		{"00000 success", StatusSuccess, RetryNever},
		{"01000 warning", StatusWarning, RetryNever},
		{"02000 no data", StatusNoData, RetryNever},

		// 0A / 22 / 23 / 28 / 42: deterministic refusals.
		{"0A000 feature not supported", "0A000", RetryNever},
		{"22023 invalid parameter", StatusInvalidParam, RetryNever},
		{"22000 vector dimension mismatch", "22000", RetryNever},
		{"23000 constraint violation", StatusConstraint, RetryNever},
		{"28000 authorization", StatusAuth, RetryNever},
		{"42000 syntax", StatusSyntax, RetryNever},
		{"42501 permission denied", StatusPermissionDenied, RetryNever},
		{"42704 undefined object", "42704", RetryNever},
		{"42P07 duplicate object", StatusDuplicate, RetryNever},

		// 25: the engine and this connection disagree about transaction state,
		// and that disagreement is per connection.
		{"25000 invalid transaction state", StatusTransactionState, RetryNewConn},

		// 40: transaction rollback. Nothing applied, so it is safe to re-run,
		// and the connection is fine.
		{"40001 serialization failure", StatusSerialization, RetrySameConn},
		{"40502 deadlock", StatusDeadlock, RetrySameConn},

		// 08: connection exception.
		{"08000 connection exception", "08000", RetryNewConn},

		// 53: insufficient resources. The working set does not shrink on a
		// second attempt (geode proto_util.zig, WriteQueryBudgetExceeded /
		// ReadQueryBudgetExceeded => "53200").
		{"53200 out of memory", "53200", RetryNever},

		// 57: operator intervention (geode proto_util.zig, ReadQueryCancelled
		// => "57014"). Retrying re-runs the statement that stalled.
		{"57014 query canceled", "57014", RetryNever},

		// 58: system error, and the engine's catch-all for an unmapped error
		// (proto_util.zig `else => .{ .code = "58000" }`). Unknown but
		// plausibly transient.
		{"58000 system error", StatusSystem, RetryBackoff},

		// Not a GQLSTATE: engine error codes travel in the same field.
		{"non-state code", "IllegalOrderKey", RetryNever},
		{"empty", "", RetryNever},
	}

	for _, c := range cases {
		// The GQLSTATE arrives in Class on driver-raised errors and in Code on
		// the Execute path (conn.go). Both spellings must classify alike.
		if got := Classify(&DriverError{Class: c.state}); got != c.want {
			t.Errorf("Classify(DriverError{Class:%q}) = %v, want %v (%s)",
				c.state, got, c.want, c.name)
		}
		if got := Classify(&DriverError{Code: c.state}); got != c.want {
			t.Errorf("Classify(DriverError{Code:%q}) = %v, want %v (%s)",
				c.state, got, c.want, c.name)
		}
	}
}

// TestClassify_ClusterWriteOutcomes pins the three clustered-write outcomes the
// engine deliberately gave DISTINCT codes so a client would stop conflating
// them (geode src/server/proto_util.zig, the RC-2 note: "committed write ->
// client Timeout -> retry double-insert").
//
//   - Timeout -> 08006. The write MIGHT have committed and this node could not
//     confirm it. Retrying without an idempotency key is what minted the
//     duplicate row, and only the caller knows whether it holds one — so the
//     driver refuses to decide: RetryNever.
//   - ClusterNotLeader / ClusterWriteNotReplicable -> 40001. The write
//     definitively did NOT apply, so it is safe to retry — but both messages
//     say "retry against the current leader", and the connection that answered
//     is on a node that is not it. RetrySameConn would ask the same follower
//     forever, so these are RetryNewConn even though their class is 40.
func TestClassify_ClusterWriteOutcomes(t *testing.T) {
	cases := []struct {
		name string
		err  *DriverError
		want RetryClass
	}{
		{
			"timeout, outcome unknown",
			&DriverError{Code: "08006", Message: "Timeout: cluster write outcome unknown — the entry may have committed but local apply could not be confirmed in time; retry ONLY with an idempotency key"},
			RetryNever,
		},
		{
			"not leader",
			&DriverError{Code: "40001", Message: "ClusterNotLeader: cluster write not applied here — this node is not the leader; retry against the current leader"},
			RetryNewConn,
		},
		{
			"write not replicable",
			&DriverError{Code: "40001", Message: "ClusterWriteNotReplicable: cluster write not applied here — the leader could not replicate this write shape; retry against the current leader"},
			RetryNewConn,
		},
		{
			// A plain 40001 with no leadership token is an ordinary
			// serialization failure and stays on its connection.
			"plain serialization failure",
			&DriverError{Code: "40001", Message: "serialization failure"},
			RetrySameConn,
		},
		{
			// 08P01 is an authentication refusal wearing a connection-exception
			// code (see IsAuthError); no new connection answers it.
			"password reset required",
			&DriverError{Code: "08P01", Message: "password reset required"},
			RetryNever,
		},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestClassify_TenantRefusalBeatsItsStatusClass pins that the strict-tenancy
// refusal is classified as the configuration error it is, not as the
// permission-denied class it shares with ordinary authorization denials. Both
// are RetryNever, so this guards the REASON rather than the answer — but the
// sentinel sweep running before the status-class switch is what keeps it that
// way if 42501 is ever reclassified.
func TestClassify_TenantRefusalBeatsItsStatusClass(t *testing.T) {
	refusal := &DriverError{
		Code:    StatusPermissionDenied,
		Message: "Tenant scope required: strict tenancy is enabled and this session has no concrete tenant scope",
	}
	if !errors.Is(refusal, ErrTenantScopeRequired) {
		t.Fatal("the engine's tenancy refusal no longer matches ErrTenantScopeRequired")
	}
	if got := Classify(refusal); got != RetryNever {
		t.Errorf("Classify(tenant scope refusal) = %v, want %v", got, RetryNever)
	}
}

// TestClassify_TypedDriverErrors covers the error TYPES that are not sentinels
// and not DriverErrors, each of which must classify without falling through to
// the unknown-error default by accident.
func TestClassify_TypedDriverErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryClass
	}{
		{"transport", &TransportError{Op: "read_length"}, RetryNewConn},
		{"transport with cause", &TransportError{Op: "dial", cause: io.EOF}, RetryNewConn},
		{"config", &ConfigError{Field: "pool_size", Value: "x", Message: "not a number"}, RetryNever},
		{"security", &SecurityError{Type: "tls", Message: "bad cert"}, RetryNever},
		{"state", &StateError{Current: StateInError, Op: "Exec"}, RetryNever},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("Classify(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Property: Classify is total
// ─────────────────────────────────────────────────────────────────────────────

// TestClassify_NeverPanics is the property that makes Classify safe to call on
// the error path, which is where a panic is least affordable: whatever shape an
// error arrives in — nil, bare, wrapped any number of times, joined with
// others, a typed nil pointer, or a type whose own Is/As methods misbehave —
// Classify returns one of the four classes and does not panic.
//
// The generator composes every shape against every seed, so it covers wrapping
// and joining in combination rather than one at a time.
func TestClassify_NeverPanics(t *testing.T) {
	seeds := []error{
		nil,
		errors.New("boom"),
		context.Canceled,
		ErrBadConn,
		ErrSessionRetired,
		ErrPoolExhausted,
		&DriverError{Class: StatusSerialization},
		&DriverError{Code: "40001", Message: "ClusterNotLeader: ..."},
		&DriverError{Code: "0"},   // a state too short to slice
		&DriverError{Code: "4"},   // ditto, one byte
		&TransportError{Op: "rx"}, // nil cause: Error() formats it as <nil>
		(*DriverError)(nil),       // typed nil: Error()/StatusClass()/IsRetryable() are nil-safe
		(*TransportError)(nil),    // typed nil: Error() is nil-safe
		&hostileError{},
		fmt.Errorf("empty wrap: %w", errors.New("")),
	}

	shapes := []struct {
		name string
		make func(error) error
	}{
		{"bare", func(e error) error { return e }},
		{"wrapped", func(e error) error { return fmt.Errorf("ctx: %w", e) }},
		{"double wrapped", func(e error) error {
			return fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", e))
		}},
		{"joined with nil", func(e error) error { return errors.Join(e, nil) }},
		{"joined with a stranger", func(e error) error {
			return errors.Join(e, errors.New("unrelated"))
		}},
		{"joined with a driver error", func(e error) error {
			return errors.Join(e, &DriverError{Class: StatusSystem})
		}},
		{"joined with itself", func(e error) error { return errors.Join(e, e) }},
		{"wrapped join", func(e error) error {
			return fmt.Errorf("batch: %w", errors.Join(e, ErrPoolExhausted))
		}},
	}

	valid := map[RetryClass]bool{
		RetryNever: true, RetrySameConn: true, RetryNewConn: true, RetryBackoff: true,
	}

	for si, seed := range seeds {
		for _, shape := range shapes {
			err := shape.make(seed)
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("Classify panicked on seed %d as %s: %v", si, shape.name, r)
					}
				}()
				got := Classify(err)
				if !valid[got] {
					t.Errorf("Classify(seed %d as %s) = RetryClass(%d), not one of the four classes",
						si, shape.name, int(got))
				}
				// String is part of the same contract: it is a metric label,
				// and a label that panics takes the metric pipeline with it.
				if got.String() == "" {
					t.Errorf("RetryClass(%d).String() is empty", int(got))
				}
			}()
		}
	}

	// Classes outside the defined set still answer, because a value read back
	// from a persisted metric or a cast integer is not under our control.
	for _, c := range []RetryClass{-1, 99, RetryClass(1 << 30)} {
		if c.String() != "never" {
			t.Errorf("RetryClass(%d).String() = %q, want the fail-closed %q",
				int(c), c.String(), "never")
		}
	}
}

// hostileError is an error whose Is and As report a match for everything. It
// exists to prove Classify cannot be driven into a bad state by an error type
// it did not write: the answer must still be one of the four classes.
type hostileError struct{}

func (*hostileError) Error() string           { return "hostile" }
func (*hostileError) Is(error) bool           { return true }
func (*hostileError) As(target any) bool      { _ = target; return false }
func (*hostileError) Unwrap() []error         { return nil }
func (*hostileError) StatusClass() string     { return StatusSystem }
func (*hostileError) IsRetryable() bool       { return true }
func (*hostileError) Temporary() bool         { return true }
func (*hostileError) Timeout() bool           { return true }
func (*hostileError) Format(any) (int, error) { return 0, nil }

// ─────────────────────────────────────────────────────────────────────────────
// IsRetryableError compatibility
// ─────────────────────────────────────────────────────────────────────────────

// TestIsRetryableError_AgreesWithClassify pins the compatibility contract: the
// bool is exactly "the class is not RetryNever", so the two can never disagree
// about WHETHER to retry.
func TestIsRetryableError_AgreesWithClassify(t *testing.T) {
	errs := []error{
		nil,
		errors.New("boom"),
		context.Canceled,
		ErrReadOnly,
		ErrBadConn,
		ErrPoolExhausted,
		ErrPoolDraining,
		ErrSessionRetired,
		&DriverError{Class: StatusSerialization},
		&DriverError{Class: StatusSyntax},
		&DriverError{Class: StatusSystem},
		&TransportError{Op: "read"},
	}
	for _, err := range errs {
		want := Classify(err) != RetryNever
		if got := IsRetryableError(err); got != want {
			t.Errorf("IsRetryableError(%v) = %v, but Classify = %v", err, got, Classify(err))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Retry semantics per class
// ─────────────────────────────────────────────────────────────────────────────

func TestRetryPolicy_Allows(t *testing.T) {
	all := []RetryClass{RetryNever, RetrySameConn, RetryNewConn, RetryBackoff}

	// A nil map means every class except RetryNever.
	var open RetryPolicy
	for _, c := range all {
		want := c != RetryNever
		if got := open.allows(c); got != want {
			t.Errorf("nil Classes: allows(%v) = %v, want %v", c, got, want)
		}
	}

	// An explicit map narrows it — and can never widen it to RetryNever.
	narrow := RetryPolicy{Classes: map[RetryClass]bool{ //nolint:exhaustive // a partial filter is what is under test
		RetryBackoff: true,
		RetryNever:   true, // ignored: RetryNever is never retried
	}}
	for _, c := range all {
		want := c == RetryBackoff
		if got := narrow.allows(c); got != want {
			t.Errorf("narrow Classes: allows(%v) = %v, want %v", c, got, want)
		}
	}

	// An empty (non-nil) map retries nothing, which is a legitimate "classify
	// but never retry" configuration.
	none := RetryPolicy{Classes: map[RetryClass]bool{}}
	for _, c := range all {
		if none.allows(c) {
			t.Errorf("empty Classes: allows(%v) = true, want false", c)
		}
	}
}

// TestRetry_NotRetriedErrorStaysMatchable pins that wrapping the returned error
// with its class costs the caller nothing: errors.Is, errors.As, IsDriverError
// and Classify all still work through it.
func TestRetry_NotRetriedErrorStaysMatchable(t *testing.T) {
	cause := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}
	policy := DefaultRetryPolicy()
	policy.InitialBackoff = 0

	err := Retry(context.Background(), policy, func(context.Context) error { return cause })
	if err == nil {
		t.Fatal("Retry returned nil for a syntax error")
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is lost the cause through the class wrapper: %v", err)
	}
	if de, ok := IsDriverError(err); !ok || de.Code != "SY001" {
		t.Errorf("IsDriverError lost the cause through the class wrapper: %v", err)
	}
	if got := Classify(err); got != RetryNever {
		t.Errorf("Classify of a wrapped non-retryable = %v, want %v", got, RetryNever)
	}
	if !strings.Contains(err.Error(), RetryNever.String()) {
		t.Errorf("the returned error does not name the class that stopped the retry: %q", err)
	}
}

// TestRetry_PerClassAttemptCounts walks each class through Retry with a policy
// that allows everything, so the class decides only how long the wait is — and
// every retryable class must actually exhaust its attempts.
func TestRetry_PerClassAttemptCounts(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantClass   RetryClass
		wantAttempt int
	}{
		{"same conn", &DriverError{Class: StatusSerialization}, RetrySameConn, 3},
		{"new conn", &TransportError{Op: "read"}, RetryNewConn, 3},
		{"backoff", ErrPoolExhausted, RetryBackoff, 3},
		{"never", &DriverError{Class: StatusSyntax}, RetryNever, 1},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.wantClass {
			t.Fatalf("%s: premise broken, Classify = %v want %v", c.name, got, c.wantClass)
		}
		policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond,
			MaxBackoff: 2 * time.Millisecond, Multiplier: 2.0}
		attempts := 0
		err := Retry(context.Background(), policy, func(context.Context) error {
			attempts++
			return c.err
		})
		if attempts != c.wantAttempt {
			t.Errorf("%s: %d attempts, want %d", c.name, attempts, c.wantAttempt)
		}
		if !errors.Is(err, c.err) {
			t.Errorf("%s: Retry lost the error: %v", c.name, err)
		}
	}
}

// TestRetryPolicy_ClassBackoffIsJitteredAndBounded pins the two halves of the
// backoff contract: RetryBackoff is spread so a herd refused by one shared
// resource does not return in lockstep, and no class ever waits longer than the
// policy's own bound.
func TestRetryPolicy_ClassBackoffIsJitteredAndBounded(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 10, InitialBackoff: time.Second,
		MaxBackoff: 5 * time.Second, Multiplier: 2.0}

	// Deterministic classes keep the exact policy delay.
	for _, c := range []RetryClass{RetrySameConn, RetryNewConn} {
		for attempt := 1; attempt <= 5; attempt++ {
			if got, want := policy.classBackoff(c, attempt), policy.backoff(attempt); got != want {
				t.Errorf("classBackoff(%v, %d) = %v, want the undelayed %v", c, attempt, got, want)
			}
		}
	}

	// RetryBackoff is drawn from [floor*d, d] — bounded above by the policy and
	// below by the floor, so the delay is spread but never skipped.
	seen := map[time.Duration]bool{}
	for range 200 {
		d := policy.classBackoff(RetryBackoff, 2)
		bound := policy.backoff(2)
		if d > bound {
			t.Fatalf("jittered backoff %v exceeds the policy bound %v", d, bound)
		}
		if floor := time.Duration(float64(bound) * backoffJitterFloor); d < floor {
			t.Fatalf("jittered backoff %v is below the floor %v", d, floor)
		}
		if d > policy.MaxBackoff {
			t.Fatalf("jittered backoff %v exceeds MaxBackoff %v", d, policy.MaxBackoff)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Errorf("200 draws produced %d distinct delays; the backoff is not jittered", len(seen))
	}

	// A zero delay stays zero however it is classified, so a test or a caller
	// that disables backoff is not given one by the jitter.
	zero := RetryPolicy{MaxAttempts: 3, Multiplier: 2.0}
	for _, c := range []RetryClass{RetryNever, RetrySameConn, RetryNewConn, RetryBackoff} {
		if got := zero.classBackoff(c, 1); got != 0 {
			t.Errorf("zero-backoff policy gave %v for %v", got, c)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool.RetrySession guards
// ─────────────────────────────────────────────────────────────────────────────

// TestPool_RetrySessionGuards covers the paths that need no server. The
// class-driven re-pin itself is proved against a live engine in
// integration_retryclass_test.go, because "the connection was actually
// discarded" is not observable without one.
func TestPool_RetrySessionGuards(t *testing.T) {
	ctx := context.Background()
	policy := DefaultRetryPolicy()

	var nilPool *Pool
	if err := nilPool.RetrySession(ctx, policy, func(context.Context, *Session) error {
		t.Fatal("fn ran on a nil pool")
		return nil
	}); !errors.Is(err, ErrPoolNotInitialized) {
		t.Errorf("nil pool: %v, want ErrPoolNotInitialized", err)
	}

	var zero Pool
	if err := zero.RetrySession(ctx, policy, func(context.Context, *Session) error {
		t.Fatal("fn ran on a zero-value pool")
		return nil
	}); !errors.Is(err, ErrPoolNotInitialized) {
		t.Errorf("zero-value pool: %v, want ErrPoolNotInitialized", err)
	}
}

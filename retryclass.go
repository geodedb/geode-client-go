package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
)

// RetryClass describes how, if at all, a failed operation should be retried.
//
// Classification is conservative: an error the driver does not recognise is
// RetryNever, so a caller never loops on an unknown permanent failure. That
// choice is deliberate and load-bearing — a bulk ingest worker that retries
// what it cannot classify turns one permanent failure into a hot loop against
// an engine that is already refusing it.
type RetryClass int

const (
	// RetryNever marks a deterministic failure: syntax, authorization,
	// constraint violation, invalid parameter, read-only refusal, a cancelled
	// context, a draining pool — or anything this driver cannot classify.
	// Retrying produces the same result, or a result that is not safe to ask
	// for twice.
	RetryNever RetryClass = iota

	// RetrySameConn marks a concurrency conflict (serialization failure,
	// deadlock). Nothing is wrong with the connection and nothing was
	// applied; the same connection can retry.
	RetrySameConn

	// RetryNewConn marks a connection-level failure. The operation must be
	// retried on a DIFFERENT connection: the current one is untrustworthy or
	// gone. database/sql does this for driver.ErrBadConn on unpinned
	// statements; a pinned caller must discard its Session and pin again (see
	// Pool.RetrySession).
	RetryNewConn

	// RetryBackoff marks a resource or system failure (pool exhausted, engine
	// system error). The same request may well succeed later, but only after a
	// delay — retrying immediately is what turns a busy engine into an
	// overloaded one.
	RetryBackoff
)

// String returns a stable lowercase name suitable for a metric attribute.
// The value set is small and bounded, so it is safe as a telemetry label.
func (c RetryClass) String() string {
	switch c {
	case RetrySameConn:
		return "same_conn"
	case RetryNewConn:
		return "new_conn"
	case RetryBackoff:
		return "backoff"
	case RetryNever:
		return "never"
	}
	return "never"
}

// GQLSTATE classes and codes this driver classifies by hand, beyond the
// StatusXxx constants in errors.go. Each is pinned to the engine source that
// emits it at the pinned engine version (0.8.15).
const (
	// stateClusterWriteUnknown is a TIMEOUT: the operation MAY have committed
	// but this node could not confirm its local apply within budget (geode
	// src/server/proto_util.zig, a generic `error.Timeout => .{ .code =
	// "08006", ... }` arm that any request path can reach, not something
	// specific to clustered writes). The outcome is UNKNOWN, which is why it
	// is deliberately not in the 08 connection-exception family's
	// RetryNewConn answer below: retrying an operation whose outcome is
	// unknown is what minted a duplicate row (the engine's own RC-2 note). It
	// is retryable ONLY with an idempotency key, which is the caller's
	// decision, not the driver's.
	stateClusterWriteUnknown = "08006"

	// gqlStateClassLen is the length of a GQLSTATE's leading CLASS, the first
	// two characters that carry the ISO/IEC 39075 category. Anything shorter is
	// not a GQLSTATE at all — engine error codes such as "IllegalOrderKey"
	// travel in the same field.
	gqlStateClassLen = 2

	// statePasswordResetRequired is the Geode-specific authentication
	// extension in the 08 class (see IsAuthError). It is an authentication
	// refusal wearing a connection-exception code, and no new connection
	// answers it.
	statePasswordResetRequired = "08P01"
)

// Engine message prefixes that qualify a GQLSTATE the driver would otherwise
// classify only by its class. The engine keeps the error NAME at the head of
// the message as "a stable, wire-discoverable token" precisely so clients can
// key on it (geode src/server/proto_util.zig).
const (
	// clusterNotLeaderPrefix and clusterNotReplicablePrefix are the two
	// clustered-write refusals the engine maps onto 40001. Both definitively
	// did NOT apply — nothing mutated, safe to retry — but both say "retry
	// against the current leader", and this node is not it. Retrying on the
	// SAME connection asks the same follower again, forever; the retry has to
	// land on a different connection.
	clusterNotLeaderPrefix     = "ClusterNotLeader"
	clusterNotReplicablePrefix = "ClusterWriteNotReplicable"
)

// Classify maps an error to its retry class, unwrapping and un-joining as
// needed. It never panics: a nil error, a wrapped error, a joined error and an
// error whose Is/As methods misbehave all produce a class.
//
// The order of the checks is part of the contract, because several of these
// errors match more than one predicate:
//
//   - Cancellation is tested first: a cancelled context is the caller's
//     decision and outranks whatever the connection reported alongside it.
//   - ErrSessionRetired is tested before ErrSessionClosed, which it wraps.
//     "Unusable because the connection failed" is RetryNewConn; "unusable
//     because you closed it" is RetryNever.
//   - ErrPoolDraining is tested before the connection-level sentinels: a
//     draining pool has no new connection to give, so RetryNewConn would spin
//     until the attempts ran out.
//   - Server status classes are tested last, so a typed sentinel the driver
//     raised always beats a status class inferred from the wire.
func Classify(err error) RetryClass {
	if err == nil {
		return RetryNever
	}

	// Cancellation and deadlines are the caller's decision, never ours.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return RetryNever
	}

	// The pool is on its way out; no connection it could hand back will
	// outlive the drain.
	if errors.Is(err, ErrPoolDraining) {
		return RetryNever
	}

	// A retired Session names its own cause: the pinned connection failed.
	// It also matches ErrSessionClosed, so it must be tested first.
	if errors.Is(err, ErrSessionRetired) {
		return RetryNewConn
	}

	// Deterministic client-side refusals: the driver rejected this before or
	// instead of asking the engine, and it will reject it identically again.
	if isDeterministicRefusal(err) {
		return RetryNever
	}

	// Resource pressure: back off rather than spin.
	if errors.Is(err, ErrPoolExhausted) {
		return RetryBackoff
	}

	// Connection-level failures need a different connection.
	if isConnectionFailure(err) {
		return RetryNewConn
	}
	var te *TransportError
	if errors.As(err, &te) && te != nil {
		return RetryNewConn
	}

	// Client-side configuration and security failures need a human, not a
	// retry. Checked before DriverError because both report a status class
	// that would otherwise be read as the engine's.
	var ce *ConfigError
	if errors.As(err, &ce) && ce != nil {
		return RetryNever
	}
	var se *SecurityError
	if errors.As(err, &se) && se != nil {
		return RetryNever
	}

	// Server-reported status classes.
	var de *DriverError
	if errors.As(err, &de) && de != nil {
		return classifyGQLState(de)
	}

	// Unrecognised. Fail closed.
	return RetryNever
}

// isDeterministicRefusal reports whether err is one of the driver's own
// refusals: every one of them describes a caller mistake or a configuration
// fact, not a transient condition, so a second attempt produces the identical
// error.
//
// ErrTenantScopeRequired is in this set and is ALSO matched by
// (*DriverError).Is (the message-qualified 42501 refusal), which is why this
// sweep runs before the status-class switch: a strict-tenancy refusal is a
// configuration error whatever class the wire gave it.
func isDeterministicRefusal(err error) bool {
	switch {
	case errors.Is(err, ErrReadOnly),
		errors.Is(err, ErrTenantRequired),
		errors.Is(err, ErrTenantScopeRequired),
		errors.Is(err, ErrParamCount),
		errors.Is(err, ErrReservedParamName),
		errors.Is(err, ErrVectorTooLarge),
		errors.Is(err, ErrVectorEmpty),
		errors.Is(err, ErrVectorNotFinite),
		errors.Is(err, ErrInvalidIdent),
		errors.Is(err, ErrInvalidString),
		errors.Is(err, ErrSessionClosed),
		errors.Is(err, ErrSessionBusy),
		errors.Is(err, ErrNilTxFunc),
		errors.Is(err, ErrPoolNotInitialized),
		errors.Is(err, ErrQueryInProgress),
		errors.Is(err, ErrTxInProgress),
		errors.Is(err, ErrNoTx),
		errors.Is(err, ErrStmtClosed),
		errors.Is(err, ErrRowsClosed),
		errors.Is(err, ErrMalformedCapabilityRows),
		errors.Is(err, ErrCapabilityUnsupported),
		errors.Is(err, ErrPlanKindMismatch):
		return true
	}
	return false
}

// isConnectionFailure reports whether err says the connection this operation
// rode is untrustworthy or gone, so the retry must land on a different one.
//
// ErrTxDone and ErrInvalidState are here rather than under "caller mistake"
// because both also arrive from a connection whose transaction state
// desynchronised under it — the state the engine and the driver disagree about
// is per-connection, and a fresh connection is the only thing that resolves it.
func isConnectionFailure(err error) bool {
	switch {
	case errors.Is(err, ErrBadConn),
		errors.Is(err, driver.ErrBadConn),
		errors.Is(err, ErrClosed),
		errors.Is(err, ErrInvalidState),
		errors.Is(err, ErrTxDone):
		return true
	}
	return false
}

// classifyGQLState maps a server-reported error to its retry class by GQLSTATE.
//
// The state is read from Class when set and from Code otherwise: the GQLSTATE
// arrives in Code on the Execute error path (conn.go) while hand-built and
// driver-raised errors carry it in Class. Both spellings are live at engine
// 0.8.15 — the vector refusals, for instance, arrive as Code="22000" /
// Code="42704" with Class empty.
func classifyGQLState(de *DriverError) RetryClass {
	if de == nil {
		return RetryNever
	}
	state := de.Class
	if state == "" {
		state = de.Code
	}

	// Whole-code special cases, where the class alone gives the wrong answer.
	switch state {
	case stateClusterWriteUnknown:
		// The write may have committed. Fail closed: only a caller holding an
		// idempotency key may retry, and only it knows that.
		return RetryNever
	case statePasswordResetRequired:
		return RetryNever
	}

	if len(state) < gqlStateClassLen {
		// Not a GQLSTATE at all — an engine error code like "IllegalOrderKey",
		// or nothing. Nothing to classify.
		return RetryNever
	}

	switch state[:gqlStateClassLen] {
	case "40":
		// ISO transaction-rollback class: the statement definitively did not
		// apply, so it is safe to run again. 40001 serialization failure and
		// 40502 deadlock resolve on the same connection.
		//
		// The engine also maps its two clustered-write refusals onto 40001
		// (geode src/server/proto_util.zig, error.ClusterNotLeader and
		// error.ClusterWriteNotReplicable). Those are equally safe to retry,
		// but not HERE: both say "retry against the current leader", and the
		// connection that answered is by definition not on it.
		if isClusterLeadershipRefusal(de.Message) {
			return RetryNewConn
		}
		return RetrySameConn

	case "08":
		// Connection exception. The connection is gone or unusable; another
		// one may work. 08006 and 08P01 were handled above.
		return RetryNewConn

	case "25":
		// Invalid transaction state: the engine and this connection disagree
		// about whether a transaction is open. That disagreement is per
		// connection and survives a retry on it.
		return RetryNewConn

	case "58":
		// System error — also the engine's catch-all for an unmapped error
		// (proto_util.zig `else => .{ .code = "58000" }`). Unknown but
		// plausibly transient, and the one class where backing off is the
		// honest answer.
		return RetryBackoff

	case "53":
		// Insufficient resources: the read/write executor budget was exceeded
		// (53200). Deterministic for the same statement — the working set does
		// not shrink on a second attempt — so the fix is a narrower query or a
		// larger budget, not a retry.
		return RetryNever

	case "57":
		// Operator intervention (57014 query canceled: the client
		// disconnected, or the stall watchdog aborted a wedged handler).
		// Retrying re-runs the statement that stalled.
		return RetryNever
	}

	// Every remaining class the driver maps is deterministic: 00/01/02
	// success-and-warning states, 0A feature not supported, 22 invalid data,
	// 23 constraint violation, 28 authorization, 2F routine exception, 42
	// syntax and access-rule violation (including 42501 permission denied and
	// 42704 undefined object). Anything else is unrecognised, and unrecognised
	// fails closed.
	return RetryNever
}

// isClusterLeadershipRefusal reports whether a 40-class message is one of the
// engine's clustered-write leadership refusals. The engine retains the error
// name at the head of the message as a stable, wire-discoverable token for
// exactly this purpose (geode src/server/proto_util.zig).
func isClusterLeadershipRefusal(msg string) bool {
	return strings.HasPrefix(msg, clusterNotLeaderPrefix) ||
		strings.HasPrefix(msg, clusterNotReplicablePrefix)
}

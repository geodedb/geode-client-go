package geode

import (
	"errors"
	"fmt"
	"strings"
)

// ISO/IEC 39075 status class constants.
const (
	StatusSuccess          = "00000" // Successful completion
	StatusNoData           = "02000" // No data
	StatusWarning          = "01000" // Warning
	StatusTransactionState = "25000" // Invalid transaction state
	StatusAuth             = "28000" // Authorization error
	StatusSerialization    = "40001" // Serialization failure (retryable)
	StatusDeadlock         = "40502" // Transaction deadlock (retryable)
	StatusSyntax           = "42000" // Syntax error
	StatusConstraint       = "23000" // Constraint violation
	StatusDuplicate        = "42P07" // Duplicate object
	StatusInvalidParam     = "22023" // Invalid parameter value
	StatusSystem           = "58000" // System error
	// StatusPermissionDenied is the engine's permission-denied class. The
	// engine maps error.Unauthorized, error.TenantScopeRequired and
	// error.SearchNotPermitted onto it (geode src/server/proto_util.zig).
	StatusPermissionDenied = "42501"
)

// Sentinel errors for common conditions.
var (
	ErrClosed          = errors.New("geode: connection closed")
	ErrQueryInProgress = errors.New("geode: query already in progress")
	ErrTxInProgress    = errors.New("geode: transaction already in progress")
	ErrNoTx            = errors.New("geode: no transaction in progress")
	ErrTxDone          = errors.New("geode: transaction already committed or rolled back")
	ErrStmtClosed      = errors.New("geode: statement closed")
	ErrRowsClosed      = errors.New("geode: rows closed")
	ErrBadConn         = errors.New("geode: bad connection")
	ErrInvalidState    = errors.New("geode: invalid connection state")
	// ErrPoolExhausted is returned when every QUIC connection in the pool is
	// busy and the pool is already at its maximum size. The driver never
	// multiplexes concurrent streams over one connection (that wedges against
	// the server — BUG-0002), so it fails fast with this clear error instead of
	// deadlocking. Raise the pool size via DSN ?pool_size=N to allow more
	// concurrent connections.
	ErrPoolExhausted = errors.New("geode: connection pool exhausted (all connections busy at pool_size)")

	// ErrTenantRequired reports a configuration that enables strict tenancy
	// without naming a concrete tenant. The engine rejects tenant-scoped
	// operations from a session whose tenant is unset, empty, or "default"
	// (geode/docs/security/MULTI_TENANT_HARDENING.md); failing at config
	// time turns that into a startup error instead of a runtime surprise.
	ErrTenantRequired = errors.New("geode: tenant is required when strict tenancy is enabled")

	// ErrTenantScopeRequired is the typed form of the engine's strict-tenancy
	// refusal. A strict-tenancy engine answers a tenant-scoped operation from
	// a session with no concrete tenant scope with GQLSTATE 42501 and the
	// message "Tenant scope required: ..." (geode
	// src/server/proto_util.zig, error.TenantScopeRequired). Callers gate
	// startup on errors.Is(err, ErrTenantScopeRequired) rather than matching
	// the engine's prose.
	ErrTenantScopeRequired = errors.New("geode: strict tenancy requires a concrete tenant scope")
)

// Error is the interface for all Geode errors.
type Error interface {
	error

	// StatusClass returns ISO/IEC 39075 status class.
	StatusClass() string

	// IsRetryable indicates if the operation can be retried.
	IsRetryable() bool
}

// DriverError represents errors returned from the Geode server.
type DriverError struct {
	Class      string   // ISO/IEC 39075 status class (00000, 25000, etc.)
	Subclass   string   // Status subclass
	Code       string   // Error code (US001, IllegalOrderKey, etc.)
	Message    string   // Human-readable message
	Anchor     string   // Source location in query
	Additional []string // Additional diagnostic info
	Findings   []string // Flagger findings
	cause      error    // Underlying cause
}

// Error implements the error interface.
func (e *DriverError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("geode: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("geode: %s", e.Code)
}

// Unwrap returns the underlying cause.
func (e *DriverError) Unwrap() error {
	return e.cause
}

// StatusClass returns the ISO/IEC 39075 status class.
func (e *DriverError) StatusClass() string {
	return e.Class
}

// IsRetryable indicates if the operation can be retried.
func (e *DriverError) IsRetryable() bool {
	switch e.Class {
	case StatusSerialization, StatusDeadlock:
		return true
	default:
		return false
	}
}

// tenantScopeRequiredPrefix is the stable leading text of the engine's
// strict-tenancy refusal message. Pinned to
// geode src/server/proto_util.zig:
//
//	error.TenantScopeRequired => .{ .code = "42501", .message =
//	    "Tenant scope required: strict tenancy is enabled and this session
//	     has no concrete tenant scope" },
//
// Only the prefix is matched: 42501 alone is the whole permission-denied
// class (plain authorization denials and the FLE READ_PLAINTEXT refusal share
// it), so the code by itself cannot distinguish a tenancy refusal.
const tenantScopeRequiredPrefix = "Tenant scope required"

// isTenantScopeRefusal reports whether this server error is the engine's
// strict-tenancy refusal. Both Code and Class are consulted because the
// GQLSTATE arrives in Code on the Execute error path (conn.go) while
// hand-built errors may carry it in Class.
func (e *DriverError) isTenantScopeRefusal() bool {
	if e.Code != StatusPermissionDenied && e.Class != StatusPermissionDenied {
		return false
	}
	return strings.HasPrefix(e.Message, tenantScopeRequiredPrefix)
}

// Is provides errors.Is support.
func (e *DriverError) Is(target error) bool {
	if target == ErrTenantScopeRequired {
		return e.isTenantScopeRefusal()
	}
	if t, ok := target.(*DriverError); ok {
		if t.Code != "" && e.Code == t.Code {
			return true
		}
		if t.Class != "" && e.Class == t.Class {
			return true
		}
	}
	return false
}

// TransportError represents transport-layer failures.
//
// Security Note: The Addr field contains the remote server address for
// diagnostic purposes. When logging errors to public locations (e.g., client
// telemetry, user-facing UI), callers should sanitize or omit this field
// to prevent internal network topology disclosure. For internal logs and
// debugging, the full address is valuable for troubleshooting.
type TransportError struct {
	Op    string // Operation that failed
	Addr  string // Remote address (if applicable) - see Security Note above
	cause error  // Underlying cause
}

// Error implements the error interface.
func (e *TransportError) Error() string {
	if e.Addr != "" {
		return fmt.Sprintf("geode: transport %s to %s: %v", e.Op, e.Addr, e.cause)
	}
	return fmt.Sprintf("geode: transport %s: %v", e.Op, e.cause)
}

// Unwrap returns the underlying cause.
func (e *TransportError) Unwrap() error {
	return e.cause
}

// StatusClass returns the status class (always system error for transport).
func (e *TransportError) StatusClass() string {
	return StatusSystem
}

// IsRetryable indicates if the operation can be retried.
func (e *TransportError) IsRetryable() bool {
	return true // Transport errors are generally retryable
}

// Temporary indicates if this is a temporary error.
func (e *TransportError) Temporary() bool {
	return true
}

// ConfigError represents DSN/configuration errors.
type ConfigError struct {
	Field   string // Configuration field name
	Value   string // Invalid value
	Message string // Error message
}

// Error implements the error interface.
func (e *ConfigError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("geode: config %s=%q: %s", e.Field, e.Value, e.Message)
	}
	return fmt.Sprintf("geode: config %s: %s", e.Field, e.Message)
}

// StatusClass returns the status class.
func (e *ConfigError) StatusClass() string {
	return StatusInvalidParam
}

// IsRetryable indicates if the operation can be retried.
func (e *ConfigError) IsRetryable() bool {
	return false // Config errors require user intervention
}

// SecurityError represents security validation failures.
type SecurityError struct {
	Type    string // Error type: "tls", "input", "validation"
	Message string // Error message
	cause   error  // Underlying cause
}

// Error implements the error interface.
func (e *SecurityError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("geode: security %s: %s: %v", e.Type, e.Message, e.cause)
	}
	return fmt.Sprintf("geode: security %s: %s", e.Type, e.Message)
}

// Unwrap returns the underlying cause.
func (e *SecurityError) Unwrap() error {
	return e.cause
}

// StatusClass returns the status class.
func (e *SecurityError) StatusClass() string {
	return StatusAuth
}

// IsRetryable indicates if the operation can be retried.
func (e *SecurityError) IsRetryable() bool {
	return false // Security errors require user intervention
}

// StateError represents invalid connection state transitions.
type StateError struct {
	Current  ConnState // Current state
	Expected ConnState // Expected state (if applicable)
	Op       string    // Operation attempted
	Message  string    // Additional message
}

// Error implements the error interface.
func (e *StateError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("geode: state error: %s (state=%s): %s", e.Op, e.Current, e.Message)
	}
	return fmt.Sprintf("geode: state error: %s (state=%s)", e.Op, e.Current)
}

// StatusClass returns the status class.
func (e *StateError) StatusClass() string {
	return StatusTransactionState
}

// IsRetryable indicates if the operation can be retried.
func (e *StateError) IsRetryable() bool {
	return false
}

// ConnState represents the connection state.
type ConnState int

const (
	StateIdle ConnState = iota
	StateExecuting
	StateInTransaction
	StateFetching
	StateClosed
	StateInError
)

// String returns a string representation of the connection state.
func (s ConnState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateExecuting:
		return "executing"
	case StateInTransaction:
		return "in_transaction"
	case StateFetching:
		return "fetching"
	case StateClosed:
		return "closed"
	case StateInError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// IsDriverError checks if err is a DriverError and returns it.
func IsDriverError(err error) (*DriverError, bool) {
	var de *DriverError
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}

// IsRetryableError checks if an error is retryable.
func IsRetryableError(err error) bool {
	var e Error
	if errors.As(err, &e) {
		return e.IsRetryable()
	}
	return false
}

// IsAuthError reports whether err is a Geode authentication or
// authorization failure (GQL status class 28000 or the Geode-specific
// 08P01 password-reset-required extension). Returns false for nil and
// for errors that do not carry a driver status class.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	var de *DriverError
	if !errors.As(err, &de) {
		return false
	}
	switch de.StatusClass() {
	case StatusAuth, // "28000"
		"08P01": // password reset required (Geode-specific)
		return true
	}
	return false
}

// IsSyntaxError reports whether err is a GQL parse or syntax error
// (status class 42000). The Geode engine returns 42000 for unrecognised
// statements, including admin DDL verbs not yet implemented in the
// connected server build. Handlers can use this to degrade gracefully.
func IsSyntaxError(err error) bool {
	if err == nil {
		return false
	}
	var de *DriverError
	if !errors.As(err, &de) {
		return false
	}
	return de.StatusClass() == StatusSyntax // "42000"
}

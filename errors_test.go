package geode

import (
	"errors"
	"fmt"
	"testing"
)

func TestDriverError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *DriverError
		expected string
	}{
		{
			"with message",
			&DriverError{Code: "US001", Message: "unsupported feature"},
			"geode: US001: unsupported feature",
		},
		{
			"without message",
			&DriverError{Code: "US001"},
			"geode: US001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestDriverError_StatusClass(t *testing.T) {
	err := &DriverError{Class: StatusSerialization}
	if err.StatusClass() != StatusSerialization {
		t.Errorf("StatusClass() = %q, want %q", err.StatusClass(), StatusSerialization)
	}
}

func TestDriverError_IsRetryable(t *testing.T) {
	tests := []struct {
		class    string
		expected bool
	}{
		{StatusSerialization, true},
		{StatusDeadlock, true},
		{StatusSyntax, false},
		{StatusAuth, false},
		{StatusConstraint, false},
	}

	for _, tt := range tests {
		t.Run(tt.class, func(t *testing.T) {
			err := &DriverError{Class: tt.class}
			if err.IsRetryable() != tt.expected {
				t.Errorf("IsRetryable() = %v, want %v", err.IsRetryable(), tt.expected)
			}
		})
	}
}

func TestDriverError_Is(t *testing.T) {
	err1 := &DriverError{Code: "US001", Class: StatusSyntax}

	// Match by code
	if !errors.Is(err1, &DriverError{Code: "US001"}) {
		t.Error("Is() should match by code")
	}

	// Match by class
	if !errors.Is(err1, &DriverError{Class: StatusSyntax}) {
		t.Error("Is() should match by class")
	}

	// No match
	if errors.Is(err1, &DriverError{Code: "OTHER"}) {
		t.Error("Is() should not match different code")
	}
}

func TestDriverError_Unwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &DriverError{Code: "US001", cause: cause}

	if err.Unwrap() != cause {
		t.Error("Unwrap() should return cause")
	}
}

func TestTransportError_Error(t *testing.T) {
	cause := errors.New("connection refused")

	tests := []struct {
		name     string
		err      *TransportError
		expected string
	}{
		{
			"with address",
			&TransportError{Op: "dial", Addr: "localhost:3141", cause: cause},
			"geode: transport dial to localhost:3141: connection refused",
		},
		{
			"without address",
			&TransportError{Op: "read", cause: cause},
			"geode: transport read: connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTransportError_IsRetryable(t *testing.T) {
	err := &TransportError{Op: "dial"}
	if !err.IsRetryable() {
		t.Error("TransportError should be retryable")
	}
}

func TestConfigError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *ConfigError
		expected string
	}{
		{
			"with value",
			&ConfigError{Field: "page_size", Value: "-1", Message: "must be positive"},
			`geode: config page_size="-1": must be positive`,
		},
		{
			"without value",
			&ConfigError{Field: "host", Message: "is required"},
			"geode: config host: is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestSecurityError_Error(t *testing.T) {
	cause := errors.New("certificate expired")

	tests := []struct {
		name     string
		err      *SecurityError
		expected string
	}{
		{
			"with cause",
			&SecurityError{Type: "tls", Message: "invalid certificate", cause: cause},
			"geode: security tls: invalid certificate: certificate expired",
		},
		{
			"without cause",
			&SecurityError{Type: "input", Message: "null bytes not allowed"},
			"geode: security input: null bytes not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestStateError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *StateError
		expected string
	}{
		{
			"with message",
			&StateError{Op: "Query", Current: StateClosed, Message: "connection closed"},
			"geode: state error: Query (state=closed): connection closed",
		},
		{
			"without message",
			&StateError{Op: "Begin", Current: StateInTransaction},
			"geode: state error: Begin (state=in_transaction)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestConnState_String(t *testing.T) {
	tests := []struct {
		state    ConnState
		expected string
	}{
		{StateIdle, "idle"},
		{StateExecuting, "executing"},
		{StateInTransaction, "in_transaction"},
		{StateFetching, "fetching"},
		{StateClosed, "closed"},
		{StateInError, "error"},
		{ConnState(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestIsDriverError(t *testing.T) {
	err := &DriverError{Code: "US001"}

	de, ok := IsDriverError(err)
	if !ok {
		t.Error("IsDriverError should return true for DriverError")
	}
	if de.Code != "US001" {
		t.Errorf("IsDriverError should return the error")
	}

	_, ok = IsDriverError(errors.New("other error"))
	if ok {
		t.Error("IsDriverError should return false for non-DriverError")
	}
}

func TestIsRetryableError(t *testing.T) {
	retryable := &DriverError{Class: StatusSerialization}
	if !IsRetryableError(retryable) {
		t.Error("IsRetryableError should return true for retryable error")
	}

	notRetryable := &DriverError{Class: StatusSyntax}
	if IsRetryableError(notRetryable) {
		t.Error("IsRetryableError should return false for non-retryable error")
	}

	if IsRetryableError(errors.New("random error")) {
		t.Error("IsRetryableError should return false for non-Error interface")
	}
}

func TestIsAuthError(t *testing.T) {
	// StatusClass() returns e.Class, so construct with Class set.
	authErr := &DriverError{Class: StatusAuth, Message: "test error"}

	// Test direct error
	if !IsAuthError(authErr) {
		t.Error("IsAuthError(28000 DriverError) = false, want true")
	}
	// Test wrapped error (errors.As must unwrap)
	if !IsAuthError(fmt.Errorf("wrap: %w", authErr)) {
		t.Error("IsAuthError(wrapped 28000) = false, want true")
	}
	// Test Geode-specific 08P01 password-reset-required extension
	if !IsAuthError(&DriverError{Class: "08P01", Message: "password reset required"}) {
		t.Error("IsAuthError(08P01) = false, want true")
	}
	// Test wrong class
	syntaxErr := &DriverError{Class: StatusSyntax, Message: "test error"}
	if IsAuthError(syntaxErr) {
		t.Error("IsAuthError(42000) = true, want false")
	}
	// Test nil
	if IsAuthError(nil) {
		t.Error("IsAuthError(nil) = true, want false")
	}
	// Test non-driver error
	if IsAuthError(errors.New("plain error")) {
		t.Error("IsAuthError(non-driver) = true, want false")
	}
}

func TestIsSyntaxError(t *testing.T) {
	syntaxErr := &DriverError{Class: StatusSyntax, Message: "test error"}
	if !IsSyntaxError(syntaxErr) {
		t.Error("IsSyntaxError(42000) = false, want true")
	}
	if !IsSyntaxError(fmt.Errorf("wrap: %w", syntaxErr)) {
		t.Error("IsSyntaxError(wrapped 42000) = false, want true")
	}
	authErr := &DriverError{Class: StatusAuth, Message: "test error"}
	if IsSyntaxError(authErr) {
		t.Error("IsSyntaxError(28000) = true, want false")
	}
	if IsSyntaxError(nil) {
		t.Error("IsSyntaxError(nil) = true, want false")
	}
	// Test non-driver error
	if IsSyntaxError(errors.New("plain error")) {
		t.Error("IsSyntaxError(non-driver) = true, want false")
	}
}

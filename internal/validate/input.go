// Package validate provides input validation utilities.
package validate

import (
	"fmt"
	"strings"
	"unicode"
)

// MaxQueryLength is the maximum allowed query length (1MB).
const MaxQueryLength = 1 << 20

// Query validates and sanitizes a GQL query string.
// It returns an error if the query contains invalid characters or exceeds size limits.
func Query(query string) error {
	// Check for null bytes
	if strings.ContainsRune(query, 0) {
		return &ValidationError{
			Field:   "query",
			Message: "null bytes not allowed",
		}
	}

	// Check for control characters (except newline, tab, carriage return)
	for i, r := range query {
		if r < 32 && r != '\n' && r != '\t' && r != '\r' {
			return &ValidationError{
				Field:   "query",
				Message: fmt.Sprintf("control character at position %d", i),
			}
		}
	}

	// Enforce maximum query length
	if len(query) > MaxQueryLength {
		return &ValidationError{
			Field:   "query",
			Message: fmt.Sprintf("query exceeds maximum length (%d bytes)", MaxQueryLength),
		}
	}

	return nil
}

// ParamName validates a query parameter name.
// Parameter names must start with a letter or underscore and contain only
// letters, digits, and underscores.
func ParamName(name string) error {
	if name == "" {
		return nil // Empty names are allowed (positional parameters)
	}

	if len(name) > 128 {
		return &ValidationError{
			Field:   "param_name",
			Value:   name,
			Message: "parameter name too long (max 128 characters)",
		}
	}

	for i, r := range name {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return &ValidationError{
					Field:   "param_name",
					Value:   name,
					Message: "parameter name must start with a letter or underscore",
				}
			}
		} else {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				return &ValidationError{
					Field:   "param_name",
					Value:   name,
					Message: "parameter name contains invalid character",
				}
			}
		}
	}

	return nil
}

// Hostname validates a hostname or IP address.
func Hostname(host string) error {
	if host == "" {
		return &ValidationError{
			Field:   "host",
			Message: "hostname is required",
		}
	}

	// Check for null bytes
	if strings.ContainsRune(host, 0) {
		return &ValidationError{
			Field:   "host",
			Value:   host,
			Message: "null bytes not allowed in hostname",
		}
	}

	// Basic validation - more specific validation happens at connection time
	if len(host) > 253 {
		return &ValidationError{
			Field:   "host",
			Value:   host,
			Message: "hostname too long",
		}
	}

	return nil
}

// ValidationError represents a validation failure.
type ValidationError struct {
	Field   string
	Value   string
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("validation error: %s=%q: %s", e.Field, e.Value, e.Message)
	}
	return fmt.Sprintf("validation error: %s: %s", e.Field, e.Message)
}

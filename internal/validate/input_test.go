package validate

import (
	"strings"
	"testing"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"valid simple", "RETURN 1", false},
		{"valid with newline", "MATCH (n)\nRETURN n", false},
		{"valid with tab", "RETURN\t1", false},
		{"valid with carriage return", "RETURN 1\r\n", false},
		{"null byte", "SELECT * FROM \x00 users", true},
		{"control character", "SELECT \x01 FROM users", true},
		{"empty", "", false},
		{"max length", strings.Repeat("a", MaxQueryLength), false},
		{"exceeds max length", strings.Repeat("a", MaxQueryLength+1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParamName(t *testing.T) {
	tests := []struct {
		name    string
		param   string
		wantErr bool
	}{
		{"valid letter start", "name", false},
		{"valid underscore start", "_name", false},
		{"valid with digits", "name123", false},
		{"empty", "", false},
		{"starts with digit", "1name", true},
		{"contains space", "na me", true},
		{"contains dash", "my-param", true},
		{"max length", strings.Repeat("a", 128), false},
		{"exceeds max length", strings.Repeat("a", 129), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParamName(tt.param)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParamName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHostname(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"valid hostname", "localhost", false},
		{"valid IP", "192.168.1.1", false},
		{"valid IPv6", "::1", false},
		{"valid fqdn", "example.com", false},
		{"empty", "", true},
		{"null byte", "host\x00name", true},
		{"max length", strings.Repeat("a", 253), false},
		{"exceeds max length", strings.Repeat("a", 254), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Hostname(tt.host)
			if (err != nil) != tt.wantErr {
				t.Errorf("Hostname() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	// With value
	e1 := &ValidationError{Field: "host", Value: "bad\x00value", Message: "invalid"}
	if !strings.Contains(e1.Error(), "host") || !strings.Contains(e1.Error(), "invalid") {
		t.Errorf("unexpected error string: %s", e1.Error())
	}

	// Without value
	e2 := &ValidationError{Field: "query", Message: "too long"}
	if !strings.Contains(e2.Error(), "query") || !strings.Contains(e2.Error(), "too long") {
		t.Errorf("unexpected error string: %s", e2.Error())
	}
}

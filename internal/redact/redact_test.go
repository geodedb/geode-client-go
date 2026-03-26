package redact_test

import (
	"testing"

	"geodedb.com/geode/internal/redact"
)

func TestDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "empty string unchanged",
			dsn:  "",
			want: "",
		},
		{
			name: "no credentials unchanged",
			dsn:  "quic://localhost:3141",
			want: "quic://localhost:3141",
		},
		{
			name: "no credentials with path unchanged",
			dsn:  "quic://localhost:3141/mygraph",
			want: "quic://localhost:3141/mygraph",
		},
		{ //nolint:gosec // G101: intentional fake credential in test data for redaction verification
			name: "URI with user and password",
			dsn:  "quic://admin:secret@localhost:3141",
			want: "quic://***:***@localhost:3141",
		},
		{
			name: "URI with user only",
			dsn:  "quic://admin@localhost:3141",
			want: "quic://***@localhost:3141",
		},
		{ //nolint:gosec // G101: intentional fake credential in test data for redaction verification
			name: "URI with user and password and path",
			dsn:  "quic://admin:secret@localhost:3141/mygraph",
			want: "quic://***:***@localhost:3141/mygraph",
		},
		{ //nolint:gosec // G101: intentional fake credential in test data for redaction verification
			name: "grpc URI with user and password",
			dsn:  "grpc://admin:secret@localhost:50051",
			want: "grpc://***:***@localhost:50051",
		},
		{
			name: "query param user and pass",
			dsn:  "quic://localhost:3141?user=admin&pass=secret",
			want: "quic://localhost:3141?user=***&pass=***",
		},
		{
			name: "query param username and password",
			dsn:  "quic://localhost:3141?username=admin&password=secret",
			want: "quic://localhost:3141?username=***&password=***",
		},
		{
			name: "query param user only",
			dsn:  "quic://localhost:3141?user=admin",
			want: "quic://localhost:3141?user=***",
		},
		{
			name: "query param pass only",
			dsn:  "quic://localhost:3141?pass=secret",
			want: "quic://localhost:3141?pass=***",
		},
		{
			name: "query param with non-credential params preserved",
			dsn:  "quic://localhost:3141?user=admin&pass=secret&page_size=500",
			want: "quic://localhost:3141?user=***&pass=***&page_size=500",
		},
		{
			name: "simple DSN with password query param",
			dsn:  "localhost:3141?password=secret",
			want: "localhost:3141?password=***",
		},
		{
			name: "simple DSN with user and pass query params",
			dsn:  "localhost:3141?user=admin&pass=secret",
			want: "localhost:3141?user=***&pass=***",
		},
		{
			name: "URI credentials and query param credentials combined",
			dsn:  "quic://admin:secret@localhost:3141?username=other&password=other2",
			want: "quic://***:***@localhost:3141?username=***&password=***",
		},
		{
			name: "non-credential query params only unchanged",
			dsn:  "quic://localhost:3141?page_size=1000&hello_name=myapp",
			want: "quic://localhost:3141?page_size=1000&hello_name=myapp",
		},
		{
			name: "simple host no credentials unchanged",
			dsn:  "localhost:3141",
			want: "localhost:3141",
		},
		{
			name: "bare user:pass@host without scheme",
			dsn:  "admin:secret@localhost:3141",
			want: "***:***@localhost:3141",
		},
		{
			name: "bare user@host without scheme",
			dsn:  "admin@localhost:3141",
			want: "***@localhost:3141",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := redact.DSN(tt.dsn)
			if got != tt.want {
				t.Errorf("DSN(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}

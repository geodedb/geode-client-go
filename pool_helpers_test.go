package geode_test

import (
	"strings"
	"testing"

	geode "geodedb.com/geode"
)

func TestNewPoolFromConfig_InvalidConfig(t *testing.T) {
	cfg := &geode.Config{
		Transport: geode.TransportQUIC,
		Port:      geode.DefaultPort,
		// Host intentionally empty — must fail validation
	}
	if _, err := geode.NewPoolFromConfig(cfg); err == nil {
		t.Fatal("NewPoolFromConfig with empty host: expected error, got nil")
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		// credentials that must NOT appear in output
		forbidden []string
	}{
		{ //nolint:gosec // test DSN uses intentionally fake credentials for redaction testing
			name:      "userinfo",
			in:        "quic://alice:s3cr3t@localhost:3141",
			forbidden: []string{"alice", "s3cr3t"},
		},
		{
			name:      "query params",
			in:        "quic://localhost:3141?user=alice&pass=s3cr3t&graph=mydb",
			forbidden: []string{"alice", "s3cr3t"},
		},
		{
			name:      "no creds",
			in:        "quic://localhost:3141",
			forbidden: nil,
		},
		{
			name:      "empty",
			in:        "",
			forbidden: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := geode.RedactDSN(tc.in)
			for _, forbidden := range tc.forbidden {
				if strings.Contains(got, forbidden) {
					t.Errorf("RedactDSN(%q) contains %q: got %q", tc.in, forbidden, got)
				}
			}
			if tc.in != "" && tc.in == got && tc.forbidden != nil {
				t.Errorf("RedactDSN(%q) returned unchanged string containing credentials", tc.in)
			}
		})
	}
}

func TestPool_TX_Commit(t *testing.T) {
	// TX with a function that returns nil must commit.
	// We can't test against a real DB here, but we CAN test
	// that a nil fn error leads to Commit() being called.
	// Test by verifying Pool.TX plumbing compiles and the panic-recovery path works.
	// (Full integration test requires a live DB — skip here)
	t.Skip("requires live Geode server; tested in integration_test.go")
}

func TestPool_TX_Rollback(t *testing.T) {
	t.Skip("requires live Geode server")
}

func TestRedactDSN_NilSafe(t *testing.T) {
	// RedactDSN must not panic on empty input.
	got := geode.RedactDSN("")
	if got != "" {
		t.Errorf("RedactDSN('') = %q, want ''", got)
	}
}

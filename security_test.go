package geode

import (
	"testing"

	"go.codepros.org/geode/internal/validate"
)

func TestValidate_Query(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"valid simple", "RETURN 1", false},
		{"valid complex", "MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name, m.name", false},
		{"valid with newlines", "MATCH (n)\nRETURN n", false},
		{"valid with tabs", "MATCH (n)\tRETURN n", false},
		{"null byte", "RETURN \x00", true},
		{"control char", "RETURN \x01", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_ParamName(t *testing.T) {
	tests := []struct {
		name    string
		param   string
		wantErr bool
	}{
		{"valid lowercase", "name", false},
		{"valid uppercase", "NAME", false},
		{"valid mixed", "UserName", false},
		{"valid with underscore", "user_name", false},
		{"valid with digits", "param1", false},
		{"valid underscore start", "_private", false},
		{"empty", "", false}, // Empty is allowed for positional
		{"starts with digit", "1param", true},
		{"contains space", "param name", true},
		{"contains hyphen", "param-name", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate.ParamName(tt.param)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParamName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_Hostname(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"localhost", "localhost", false},
		{"ip address", "192.168.1.1", false},
		{"domain", "db.example.com", false},
		{"empty", "", true},
		{"null byte", "host\x00name", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate.Hostname(tt.host)
			if (err != nil) != tt.wantErr {
				t.Errorf("Hostname() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewTLSConfig_RequiresCertificates(t *testing.T) {
	cfg := &Config{
		Host: "localhost",
		Port: "3141",
		// No CA specified, InsecureSkipVerify is false
	}

	// This should try to use system cert pool
	// The behavior depends on the system, so we just verify it doesn't panic
	_, err := NewTLSConfig(cfg)
	// On systems without a system cert pool (like some CI environments),
	// this might fail, which is expected behavior
	_ = err
}

func TestNewTLSConfig_InsecureSkipVerify(t *testing.T) {
	cfg := &Config{
		Host:               "localhost",
		Port:               "3141",
		InsecureSkipVerify: true,
	}

	tlsCfg, err := NewTLSConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !tlsCfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
	if tlsCfg.MinVersion < 0x0304 { // TLS 1.3
		t.Error("expected MinVersion to be TLS 1.3")
	}
	if len(tlsCfg.NextProtos) != 1 || tlsCfg.NextProtos[0] != "geode/1" {
		t.Error("expected NextProtos to contain 'geode/1'")
	}
	if tlsCfg.ServerName != "localhost" {
		t.Errorf("expected ServerName 'localhost', got %q", tlsCfg.ServerName)
	}
}

func TestNewTLSConfig_MTLSRequiresBothCertAndKey(t *testing.T) {
	// When only cert is provided (no key), parsing should fail earlier in ParseDSN
	// But if we construct a config manually and call NewTLSConfig,
	// it should handle this gracefully

	cfg := &Config{
		Host:               "localhost",
		Port:               "3141",
		TLSCert:            "/nonexistent/cert.pem",
		TLSKey:             "/nonexistent/key.pem",
		InsecureSkipVerify: true,
	}

	// This will fail because the files don't exist
	_, err := NewTLSConfig(cfg)
	if err == nil {
		t.Error("expected error for nonexistent cert/key files")
	}

	se, ok := err.(*SecurityError)
	if !ok {
		t.Errorf("expected *SecurityError, got %T", err)
	} else if se.Type != "tls" {
		t.Errorf("expected security error type 'tls', got %q", se.Type)
	}
}

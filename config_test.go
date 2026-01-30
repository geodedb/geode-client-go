package geode

import (
	"os"
	"testing"
)

func TestParseDSN_Empty(t *testing.T) {
	cfg, err := ParseDSN("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", cfg.Host)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("expected port %q, got %q", DefaultPort, cfg.Port)
	}
	if cfg.PageSize != DefaultPageSize {
		t.Errorf("expected page_size %d, got %d", DefaultPageSize, cfg.PageSize)
	}
}

func TestParseDSN_Simple(t *testing.T) {
	tests := []struct {
		dsn      string
		host     string
		port     string
		pageSize int
	}{
		{"localhost:3141", "localhost", "3141", DefaultPageSize},
		{"192.168.1.1:8080", "192.168.1.1", "8080", DefaultPageSize},
		{"myhost", "myhost", DefaultPort, DefaultPageSize},
		{"localhost:3141?page_size=500", "localhost", "3141", 500},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Host != tt.host {
				t.Errorf("expected host %q, got %q", tt.host, cfg.Host)
			}
			if cfg.Port != tt.port {
				t.Errorf("expected port %q, got %q", tt.port, cfg.Port)
			}
			if cfg.PageSize != tt.pageSize {
				t.Errorf("expected page_size %d, got %d", tt.pageSize, cfg.PageSize)
			}
		})
	}
}

func TestParseDSN_URL(t *testing.T) {
	tests := []struct {
		dsn      string
		host     string
		port     string
		pageSize int
	}{
		{"geode://localhost:3141", "localhost", "3141", DefaultPageSize},
		{"geode://192.168.1.1:8080", "192.168.1.1", "8080", DefaultPageSize},
		{"geode://myhost:3141?page_size=100", "myhost", "3141", 100},
		{"geode://localhost", "localhost", DefaultPort, DefaultPageSize},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Host != tt.host {
				t.Errorf("expected host %q, got %q", tt.host, cfg.Host)
			}
			if cfg.Port != tt.port {
				t.Errorf("expected port %q, got %q", tt.port, cfg.Port)
			}
			if cfg.PageSize != tt.pageSize {
				t.Errorf("expected page_size %d, got %d", tt.pageSize, cfg.PageSize)
			}
		})
	}
}

func TestParseDSN_Options(t *testing.T) {
	dsn := "localhost:3141?page_size=200&hello_name=myapp&hello_ver=1.0&conformance=full&ca=/path/to/ca.crt&insecure=true"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PageSize != 200 {
		t.Errorf("expected page_size 200, got %d", cfg.PageSize)
	}
	if cfg.HelloName != "myapp" {
		t.Errorf("expected hello_name 'myapp', got %q", cfg.HelloName)
	}
	if cfg.HelloVersion != "1.0" {
		t.Errorf("expected hello_ver '1.0', got %q", cfg.HelloVersion)
	}
	if cfg.Conformance != "full" {
		t.Errorf("expected conformance 'full', got %q", cfg.Conformance)
	}
	if cfg.TLSCA != "/path/to/ca.crt" {
		t.Errorf("expected ca '/path/to/ca.crt', got %q", cfg.TLSCA)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected insecure=true")
	}
}

func TestParseDSN_InvalidPageSize(t *testing.T) {
	tests := []string{
		"localhost:3141?page_size=0",
		"localhost:3141?page_size=-1",
		"localhost:3141?page_size=abc",
	}

	for _, dsn := range tests {
		t.Run(dsn, func(t *testing.T) {
			_, err := ParseDSN(dsn)
			if err == nil {
				t.Error("expected error for invalid page_size")
			}
			var cfgErr *ConfigError
			if !isConfigError(err, &cfgErr) {
				t.Errorf("expected ConfigError, got %T", err)
			}
		})
	}
}

func TestParseDSN_MTLSValidation(t *testing.T) {
	// cert without key
	_, err := ParseDSN("localhost:3141?cert=/path/to/cert.pem")
	if err == nil {
		t.Error("expected error when cert is provided without key")
	}

	// key without cert
	_, err = ParseDSN("localhost:3141?key=/path/to/key.pem")
	if err == nil {
		t.Error("expected error when key is provided without cert")
	}

	// both provided - should succeed
	cfg, err := ParseDSN("localhost:3141?cert=/path/to/cert.pem&key=/path/to/key.pem")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.TLSCert != "/path/to/cert.pem" {
		t.Errorf("expected cert path, got %q", cfg.TLSCert)
	}
	if cfg.TLSKey != "/path/to/key.pem" {
		t.Errorf("expected key path, got %q", cfg.TLSKey)
	}
}

func TestParseDSN_EnvVars(t *testing.T) {
	// Set environment variables
	os.Setenv("GEODE_HOST", "envhost")
	os.Setenv("GEODE_PORT", "9999")
	os.Setenv("GEODE_TLS_CA", "/env/ca.crt")
	defer func() {
		os.Unsetenv("GEODE_HOST")
		os.Unsetenv("GEODE_PORT")
		os.Unsetenv("GEODE_TLS_CA")
	}()

	cfg, err := ParseDSN("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Host != "envhost" {
		t.Errorf("expected host from env 'envhost', got %q", cfg.Host)
	}
	if cfg.Port != "9999" {
		t.Errorf("expected port from env '9999', got %q", cfg.Port)
	}
	if cfg.TLSCA != "/env/ca.crt" {
		t.Errorf("expected ca from env '/env/ca.crt', got %q", cfg.TLSCA)
	}
}

func TestConfig_Addr(t *testing.T) {
	cfg := &Config{Host: "localhost", Port: "3141"}
	if cfg.Addr() != "localhost:3141" {
		t.Errorf("expected 'localhost:3141', got %q", cfg.Addr())
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"valid", &Config{Host: "localhost", Port: "3141", PageSize: 100}, false},
		{"missing host", &Config{Host: "", Port: "3141", PageSize: 100}, true},
		{"missing port", &Config{Host: "localhost", Port: "", PageSize: 100}, true},
		{"invalid port", &Config{Host: "localhost", Port: "abc", PageSize: 100}, true},
		{"zero page_size", &Config{Host: "localhost", Port: "3141", PageSize: 0}, true},
		{"negative page_size", &Config{Host: "localhost", Port: "3141", PageSize: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Clone(t *testing.T) {
	cfg := &Config{
		Host:         "localhost",
		Port:         "3141",
		PageSize:     500,
		HelloName:    "test",
		HelloVersion: "1.0",
	}

	clone := cfg.Clone()
	if clone == cfg {
		t.Error("Clone() should return a different pointer")
	}
	if clone.Host != cfg.Host || clone.Port != cfg.Port || clone.PageSize != cfg.PageSize {
		t.Error("Clone() should copy all fields")
	}

	// Verify nil handling
	var nilCfg *Config
	if nilCfg.Clone() != nil {
		t.Error("Clone() of nil should return nil")
	}
}

func TestParseDSN_HostnameValidation(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"valid hostname", "localhost:3141", false},
		{"valid IP", "192.168.1.1:3141", false},
		{"valid URL hostname", "geode://myhost:3141", false},
		{"null byte in simple DSN", "host\x00name:3141", true},
		{"null byte in URL DSN", "geode://host\x00name:3141", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseDSN_ConnectTimeout(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		wantTimeout int
		wantErr     bool
	}{
		{"default no timeout", "localhost:3141", 0, false},
		{"explicit timeout", "localhost:3141?connect_timeout=30", 30, false},
		{"zero timeout", "localhost:3141?connect_timeout=0", 0, false},
		{"invalid timeout", "localhost:3141?connect_timeout=abc", 0, true},
		{"negative timeout", "localhost:3141?connect_timeout=-1", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && cfg.ConnectTimeout != tt.wantTimeout {
				t.Errorf("ConnectTimeout = %d, want %d", cfg.ConnectTimeout, tt.wantTimeout)
			}
		})
	}
}

func TestConfig_Validate_HostnameValidation(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"valid", "localhost", false},
		{"valid IP", "192.168.1.1", false},
		{"null byte", "host\x00name", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Host: tt.host, Port: "3141", PageSize: 100}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func isConfigError(err error, target **ConfigError) bool {
	if ce, ok := err.(*ConfigError); ok {
		*target = ce
		return true
	}
	return false
}

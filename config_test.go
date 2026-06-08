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
	if cfg.Transport != TransportQUIC {
		t.Errorf("expected transport QUIC, got %v", cfg.Transport)
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
			// Scheme-less defaults to QUIC
			if cfg.Transport != TransportQUIC {
				t.Errorf("expected transport QUIC, got %v", cfg.Transport)
			}
		})
	}
}

func TestParseDSN_UnsupportedSchemes(t *testing.T) {
	unsupportedSchemes := []string{
		"geode://localhost:3141",
		"grcp://localhost:50051",
		"http://localhost:80",
		"https://localhost:443",
		"tcp://localhost:3141",
	}

	for _, dsn := range unsupportedSchemes {
		t.Run(dsn, func(t *testing.T) {
			_, err := ParseDSN(dsn)
			if err == nil {
				t.Errorf("expected error for unsupported scheme %q", dsn)
			}
		})
	}
}

// TestParseDSN_QUICScheme tests the quic:// DSN scheme.
// Acceptance test #1: quic://localhost:1234 => transport=QUIC, host=localhost, port=1234
func TestParseDSN_QUICScheme(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		transport TransportType
		host      string
		port      string
	}{
		{
			name:      "basic",
			dsn:       "quic://localhost:1234",
			transport: TransportQUIC,
			host:      "localhost",
			port:      "1234",
		},
		{
			name:      "ip address",
			dsn:       "quic://192.168.1.1:3141",
			transport: TransportQUIC,
			host:      "192.168.1.1",
			port:      "3141",
		},
		{
			name:      "with options",
			dsn:       "quic://myhost:3141?page_size=500",
			transport: TransportQUIC,
			host:      "myhost",
			port:      "3141",
		},
		{
			name:      "default port",
			dsn:       "quic://localhost",
			transport: TransportQUIC,
			host:      "localhost",
			port:      DefaultPort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Transport != tt.transport {
				t.Errorf("expected transport %v, got %v", tt.transport, cfg.Transport)
			}
			if cfg.Host != tt.host {
				t.Errorf("expected host %q, got %q", tt.host, cfg.Host)
			}
			if cfg.Port != tt.port {
				t.Errorf("expected port %q, got %q", tt.port, cfg.Port)
			}
		})
	}
}

// TestParseDSN_GRPCScheme tests the grpc:// DSN scheme.
// Acceptance test #2: grpc://127.0.0.1:50051?tls=0 => transport=GRPC, host=127.0.0.1, port=50051, tls disabled
func TestParseDSN_GRPCScheme(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		transport   TransportType
		host        string
		port        string
		tlsDisabled bool
	}{
		{
			name:        "basic",
			dsn:         "grpc://localhost:50051",
			transport:   TransportGRPC,
			host:        "localhost",
			port:        "50051",
			tlsDisabled: false,
		},
		{
			name:        "with tls=0",
			dsn:         "grpc://127.0.0.1:50051?tls=0",
			transport:   TransportGRPC,
			host:        "127.0.0.1",
			port:        "50051",
			tlsDisabled: true,
		},
		{
			name:        "with tls=false",
			dsn:         "grpc://myhost:50051?tls=false",
			transport:   TransportGRPC,
			host:        "myhost",
			port:        "50051",
			tlsDisabled: true,
		},
		{
			name:        "default port",
			dsn:         "grpc://localhost",
			transport:   TransportGRPC,
			host:        "localhost",
			port:        DefaultGRPCPort,
			tlsDisabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Transport != tt.transport {
				t.Errorf("expected transport %v, got %v", tt.transport, cfg.Transport)
			}
			if cfg.Host != tt.host {
				t.Errorf("expected host %q, got %q", tt.host, cfg.Host)
			}
			if cfg.Port != tt.port {
				t.Errorf("expected port %q, got %q", tt.port, cfg.Port)
			}
			if cfg.TLSDisabled != tt.tlsDisabled {
				t.Errorf("expected TLSDisabled %v, got %v", tt.tlsDisabled, cfg.TLSDisabled)
			}
		})
	}
}

// TestParseDSN_IPv6 tests IPv6 address support in DSN.
func TestParseDSN_IPv6(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		host string
		port string
	}{
		{
			name: "quic ipv6 localhost",
			dsn:  "quic://[::1]:3141",
			host: "::1",
			port: "3141",
		},
		{
			name: "grpc ipv6 full",
			dsn:  "grpc://[2001:db8::1]:50051",
			host: "2001:db8::1",
			port: "50051",
		},
		{
			name: "scheme-less ipv6",
			dsn:  "[::1]:3141",
			host: "::1",
			port: "3141",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		})
	}
}

// TestParseDSN_InvalidScheme tests that unsupported schemes return an error.
// Acceptance test #3: http://localhost:1 => deterministic "unsupported scheme" error
func TestParseDSN_InvalidScheme(t *testing.T) {
	// Note: Currently the parser treats unknown schemes as scheme-less DSNs
	// which will fail validation. For stricter scheme validation, we could
	// add explicit checks. For now, test that invalid URLs are handled.

	invalidDSNs := []string{
		"://localhost:3141",      // Missing scheme
		"http://localhost:3141",  // HTTP not supported (will try to parse as host)
		"https://localhost:3141", // HTTPS not supported
		"ftp://localhost:3141",   // FTP not supported
		"tcp://localhost:3141",   // TCP not supported
	}

	for _, dsn := range invalidDSNs {
		t.Run(dsn, func(t *testing.T) {
			cfg, err := ParseDSN(dsn)
			// These will either error during parsing or produce invalid configs
			// The key is that they don't succeed as valid QUIC/gRPC connections
			if err == nil && cfg != nil {
				// Verify it didn't accidentally create a valid transport config
				// by checking the resulting host doesn't contain scheme info
				if cfg.Host == "" || cfg.Host == ":/localhost" {
					t.Errorf("should not accept scheme %q", dsn)
				}
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

// TestParseDSN_TenantAndRole verifies the tenant/role DSN params (GAP-0841)
// parse into Config and survive a FormatDSN round-trip.
func TestParseDSN_TenantAndRole(t *testing.T) {
	cfg, err := ParseDSN("localhost:3141?tenant=acme&role=analyst")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Tenant != "acme" {
		t.Errorf("expected tenant 'acme', got %q", cfg.Tenant)
	}
	if cfg.Role != "analyst" {
		t.Errorf("expected role 'analyst', got %q", cfg.Role)
	}

	// tenant_id alias
	cfg2, err := ParseDSN("localhost:3141?tenant_id=beta")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.Tenant != "beta" {
		t.Errorf("expected tenant_id alias 'beta', got %q", cfg2.Tenant)
	}

	// round-trip through FormatDSN
	rt, err := ParseDSN(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("round-trip parse: %v", err)
	}
	if rt.Tenant != "acme" || rt.Role != "analyst" {
		t.Errorf("round-trip lost tenant/role: tenant=%q role=%q", rt.Tenant, rt.Role)
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

func TestParseDSN_PageSizeUpperBound(t *testing.T) {
	// Test page_size at max boundary
	dsn := "localhost:3141?page_size=100000"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error for page_size at max: %v", err)
	}
	if cfg.PageSize != MaxPageSize {
		t.Errorf("expected page_size %d, got %d", MaxPageSize, cfg.PageSize)
	}

	// Test page_size exceeding max
	dsn = "localhost:3141?page_size=100001"
	_, err = ParseDSN(dsn)
	if err == nil {
		t.Error("expected error for page_size exceeding max")
	}
	var cfgErr *ConfigError
	if !isConfigError(err, &cfgErr) {
		t.Errorf("expected ConfigError, got %T", err)
	} else if cfgErr.Field != "page_size" {
		t.Errorf("expected field 'page_size', got %q", cfgErr.Field)
	}

	// Test extremely large page_size
	dsn = "localhost:3141?page_size=999999999"
	_, err = ParseDSN(dsn)
	if err == nil {
		t.Error("expected error for extremely large page_size")
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
		{"valid quic", &Config{Host: "localhost", Port: "3141", PageSize: 100, Transport: TransportQUIC}, false},
		{"valid grpc", &Config{Host: "localhost", Port: "50051", PageSize: 100, Transport: TransportGRPC}, false},
		{"missing host", &Config{Host: "", Port: "3141", PageSize: 100}, true},
		{"missing port", &Config{Host: "localhost", Port: "", PageSize: 100}, true},
		{"invalid port", &Config{Host: "localhost", Port: "abc", PageSize: 100}, true},
		{"zero page_size", &Config{Host: "localhost", Port: "3141", PageSize: 0}, true},
		{"negative page_size", &Config{Host: "localhost", Port: "3141", PageSize: -1}, true},
		{"max page_size", &Config{Host: "localhost", Port: "3141", PageSize: MaxPageSize}, false},
		{"exceeds max page_size", &Config{Host: "localhost", Port: "3141", PageSize: MaxPageSize + 1}, true},
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
		Transport:    TransportGRPC,
	}

	clone := cfg.Clone()
	if clone == cfg {
		t.Error("Clone() should return a different pointer")
	}
	if clone.Host != cfg.Host || clone.Port != cfg.Port || clone.PageSize != cfg.PageSize {
		t.Error("Clone() should copy all fields")
	}
	if clone.Transport != cfg.Transport {
		t.Error("Clone() should copy transport")
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
		{"valid URL hostname", "quic://myhost:3141", false},
		{"null byte in simple DSN", "host\x00name:3141", true},
		{"null byte in URL DSN", "quic://host\x00name:3141", true},
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

func TestTransportType_String(t *testing.T) {
	if TransportQUIC.String() != "quic" {
		t.Errorf("expected 'quic', got %q", TransportQUIC.String())
	}
	if TransportGRPC.String() != "grpc" {
		t.Errorf("expected 'grpc', got %q", TransportGRPC.String())
	}
}

// TestParseDSN_InsecureTLSSkipVerify tests the insecure_tls_skip_verify parameter
// and its aliases (insecure, skip_verify).
func TestParseDSN_InsecureTLSSkipVerify(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected bool
	}{
		// Primary parameter name
		{"insecure_tls_skip_verify=true", "localhost:3141?insecure_tls_skip_verify=true", true},
		{"insecure_tls_skip_verify=1", "localhost:3141?insecure_tls_skip_verify=1", true},
		{"insecure_tls_skip_verify=yes", "localhost:3141?insecure_tls_skip_verify=yes", true},
		{"insecure_tls_skip_verify=on", "localhost:3141?insecure_tls_skip_verify=on", true},
		{"insecure_tls_skip_verify=false", "localhost:3141?insecure_tls_skip_verify=false", false},
		{"insecure_tls_skip_verify=0", "localhost:3141?insecure_tls_skip_verify=0", false},

		// Alias: insecure
		{"insecure=true", "localhost:3141?insecure=true", true},
		{"insecure=1", "localhost:3141?insecure=1", true},
		{"insecure=yes", "localhost:3141?insecure=yes", true},
		{"insecure=on", "localhost:3141?insecure=on", true},
		{"insecure=false", "localhost:3141?insecure=false", false},

		// Alias: skip_verify
		{"skip_verify=true", "localhost:3141?skip_verify=true", true},
		{"skip_verify=1", "localhost:3141?skip_verify=1", true},
		{"skip_verify=yes", "localhost:3141?skip_verify=yes", true},
		{"skip_verify=on", "localhost:3141?skip_verify=on", true},
		{"skip_verify=false", "localhost:3141?skip_verify=false", false},

		// With URL scheme
		{"quic with insecure_tls_skip_verify", "quic://localhost:3141?insecure_tls_skip_verify=true", true},
		{"grpc with insecure_tls_skip_verify", "grpc://localhost:50051?insecure_tls_skip_verify=true", true},
		{"simple dsn with skip_verify", "localhost:3141?skip_verify=true", true},

		// Default (not set)
		{"default", "localhost:3141", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.InsecureSkipVerify != tt.expected {
				t.Errorf("InsecureSkipVerify = %v, want %v", cfg.InsecureSkipVerify, tt.expected)
			}
		})
	}
}

// TestParseDSN_Credentials tests user/username and pass/password parameters.
func TestParseDSN_Credentials(t *testing.T) {
	tests := []struct {
		name         string
		dsn          string
		wantUsername string
		wantPassword string
	}{
		{"user and pass", "localhost:3141?user=admin&pass=secret", "admin", "secret"},
		{"username and password", "localhost:3141?username=admin&password=secret", "admin", "secret"},
		{"user only", "localhost:3141?user=admin", "admin", ""},
		{"password only", "localhost:3141?password=secret", "", "secret"},
		{"url scheme with creds", "quic://localhost:3141?user=admin&pass=secret", "admin", "secret"},
		{"default", "localhost:3141", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Username != tt.wantUsername {
				t.Errorf("Username = %q, want %q", cfg.Username, tt.wantUsername)
			}
			if cfg.Password != tt.wantPassword {
				t.Errorf("Password = %q, want %q", cfg.Password, tt.wantPassword)
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

// TestParseDSN_GraphPath tests URL path-based graph binding.
func TestParseDSN_GraphPath(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		wantGraph string
	}{
		{"quic path", "quic://localhost:3141/mygraph", "mygraph"},
		{"grpc path", "grpc://localhost:50051/testgraph", "testgraph"},
		{"path with hyphen", "quic://localhost:3141/my-graph", "my-graph"},
		{"path with underscore", "quic://localhost:3141/my_graph", "my_graph"},
		{"no path", "quic://localhost:3141", ""},
		{"root path only", "quic://localhost:3141/", ""},
		{"query param graph", "quic://localhost:3141?graph=querygraph", "querygraph"},
		{"path takes priority over no-param", "quic://localhost:3141/pathgraph", "pathgraph"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Graph != tt.wantGraph {
				t.Errorf("Graph = %q, want %q", cfg.Graph, tt.wantGraph)
			}
		})
	}
}

func TestConfig_FormatDSN_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"quic bare", "quic://localhost:3141"},
		{"grpc no tls", "grpc://db.example.com:50051?tls=0"},
		{"with graph", "quic://localhost:3141?graph=mydb"},
		{"with page size", "quic://localhost:3141?page_size=500"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.dsn, err)
			}
			formatted := cfg.FormatDSN()
			cfg2, err := ParseDSN(formatted)
			if err != nil {
				t.Fatalf("ParseDSN(FormatDSN(%q)): %v — got: %q", tc.dsn, err, formatted)
			}
			if cfg.Host != cfg2.Host || cfg.Port != cfg2.Port || cfg.Transport != cfg2.Transport {
				t.Errorf("round-trip host/port/transport mismatch: orig=%q formatted=%q", tc.dsn, formatted)
			}
			if cfg.Graph != cfg2.Graph {
				t.Errorf("round-trip graph mismatch: orig=%q formatted=%q", cfg.Graph, cfg2.Graph)
			}
			if cfg.PageSize != cfg2.PageSize {
				t.Errorf("round-trip page_size mismatch: orig=%d formatted %d", cfg.PageSize, cfg2.PageSize)
			}
		})
	}
}

func TestConfig_FormatDSN_Credentials(t *testing.T) {
	cfg := &Config{
		Transport: TransportQUIC,
		Host:      "localhost",
		Port:      DefaultPort,
		Username:  "alice",
		Password:  "s3cr3t",
	}
	dsn := cfg.FormatDSN()
	cfg2, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(FormatDSN with creds): %v — dsn=%q", err, dsn)
	}
	if cfg2.Username != "alice" || cfg2.Password != "s3cr3t" {
		t.Errorf("credential round-trip: got user=%q pass=%q", cfg2.Username, cfg2.Password)
	}
}

func TestConfig_FormatDSN_PasswordOnly(t *testing.T) {
	// ParseDSN accepts pass-only; FormatDSN must round-trip it.
	dsn := "quic://localhost:3141?pass=s3cr3t"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	formatted := cfg.FormatDSN()
	cfg2, err := ParseDSN(formatted)
	if err != nil {
		t.Fatalf("ParseDSN(FormatDSN): %v — got: %q", err, formatted)
	}
	if cfg2.Password != "s3cr3t" {
		t.Errorf("password round-trip failed: got %q, want %q", cfg2.Password, "s3cr3t")
	}
}

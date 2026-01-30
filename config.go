package geode

import (
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"geodedb.com/geode/internal/validate"
)

const (
	// DefaultPort is the default Geode server port.
	DefaultPort = "3141"

	// DefaultPageSize is the default number of rows per page.
	DefaultPageSize = 1000

	// DefaultHelloName is the default client name sent in HELLO.
	DefaultHelloName = "geode-go"

	// DefaultHelloVersion is the default client version sent in HELLO.
	DefaultHelloVersion = "0.1"

	// DefaultConformance is the default GQL conformance level.
	DefaultConformance = "min"

	// MaxQueryLength is the maximum allowed query length.
	MaxQueryLength = 1 << 20 // 1MB
)

// Config holds the configuration for a Geode connection.
type Config struct {
	// Host is the server hostname or IP address.
	Host string

	// Port is the server port.
	Port string

	// PageSize is the number of rows to fetch per page.
	PageSize int

	// HelloName is the client name sent in HELLO.
	HelloName string

	// HelloVersion is the client version sent in HELLO.
	HelloVersion string

	// Conformance is the GQL conformance level.
	Conformance string

	// TLS configuration
	TLSCA   string // Path to CA certificate
	TLSCert string // Path to client certificate (for mTLS)
	TLSKey  string // Path to client private key (for mTLS)

	// InsecureSkipVerify allows skipping TLS verification (for testing only).
	// This should never be used in production.
	InsecureSkipVerify bool

	// ConnectTimeout is the maximum time to wait for a connection.
	// Zero means no timeout. This helps prevent connection storms.
	// For rate limiting, use database/sql's connection pool settings:
	//   db.SetMaxOpenConns(n) - limits concurrent connections
	//   db.SetMaxIdleConns(n) - limits idle connections
	//   db.SetConnMaxLifetime(d) - limits connection age
	//   db.SetConnMaxIdleTime(d) - limits idle time
	ConnectTimeout int // in seconds, 0 = no timeout
}

// Addr returns the server address in host:port format.
func (c *Config) Addr() string {
	return net.JoinHostPort(c.Host, c.Port)
}

// Clone returns a deep copy of the configuration.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	clone := *c
	return &clone
}

// ParseDSN parses a data source name into a Config.
//
// Supported formats:
//   - geode://host:port?options
//   - host:port?options
//   - host:port
//   - host (uses default port)
//
// Supported options:
//   - page_size: Results page size (default: 1000)
//   - hello_name: Client name (default: "geode-go")
//   - hello_ver: Client version (default: "0.1")
//   - conformance: GQL conformance level (default: "min")
//   - ca: Path to CA certificate
//   - cert: Path to client certificate (for mTLS)
//   - key: Path to client key (for mTLS)
//   - insecure: Skip TLS verification (testing only, not recommended)
//
// Environment variables:
//   - GEODE_HOST: Default host if DSN is empty
//   - GEODE_PORT: Default port if not specified
//   - GEODE_TLS_CA: Default CA certificate path
func ParseDSN(dsn string) (*Config, error) {
	cfg := &Config{
		Host:         "localhost",
		Port:         DefaultPort,
		PageSize:     DefaultPageSize,
		HelloName:    DefaultHelloName,
		HelloVersion: DefaultHelloVersion,
		Conformance:  DefaultConformance,
	}

	// Apply environment variable defaults
	if host := os.Getenv("GEODE_HOST"); host != "" {
		cfg.Host = host
	}
	if port := os.Getenv("GEODE_PORT"); port != "" {
		cfg.Port = port
	}
	if ca := os.Getenv("GEODE_TLS_CA"); ca != "" {
		cfg.TLSCA = ca
	}

	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return cfg, nil
	}

	// Check for geode:// URL scheme
	if strings.HasPrefix(dsn, "geode://") {
		return parseURLDSN(dsn, cfg)
	}

	// Parse as host:port?options
	return parseSimpleDSN(dsn, cfg)
}

// parseURLDSN parses a geode:// URL format DSN.
func parseURLDSN(dsn string, cfg *Config) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, &ConfigError{
			Field:   "dsn",
			Value:   dsn,
			Message: "invalid URL format",
		}
	}

	if u.Host == "" {
		return nil, &ConfigError{
			Field:   "host",
			Message: "host is required",
		}
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// No port specified, use the host as-is
		host = u.Host
		port = DefaultPort
	}

	if host == "" {
		return nil, &ConfigError{
			Field:   "host",
			Message: "host is required",
		}
	}

	// Validate hostname for security (null bytes, length limits)
	if err := validate.Hostname(host); err != nil {
		return nil, &ConfigError{
			Field:   "host",
			Value:   host,
			Message: err.Error(),
		}
	}

	cfg.Host = host
	cfg.Port = port

	// Parse query parameters
	if err := applyQueryParams(u.Query(), cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// parseSimpleDSN parses a simple host:port?options format DSN.
func parseSimpleDSN(dsn string, cfg *Config) (*Config, error) {
	// Split off query string if present
	var queryStr string
	if idx := strings.Index(dsn, "?"); idx >= 0 {
		queryStr = dsn[idx+1:]
		dsn = dsn[:idx]
	}

	// Parse host:port
	if dsn != "" {
		host, port, err := net.SplitHostPort(dsn)
		if err != nil {
			// No port specified, treat entire string as host
			host = dsn
			port = DefaultPort
		}

		if host == "" {
			return nil, &ConfigError{
				Field:   "host",
				Message: "host is required",
			}
		}

		// Validate hostname for security (null bytes, length limits)
		if err := validate.Hostname(host); err != nil {
			return nil, &ConfigError{
				Field:   "host",
				Value:   host,
				Message: err.Error(),
			}
		}

		cfg.Host = host
		cfg.Port = port
	}

	// Parse query parameters
	if queryStr != "" {
		params, err := url.ParseQuery(queryStr)
		if err != nil {
			return nil, &ConfigError{
				Field:   "options",
				Value:   queryStr,
				Message: "invalid query parameters",
			}
		}
		if err := applyQueryParams(params, cfg); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// applyQueryParams applies URL query parameters to the config.
func applyQueryParams(params url.Values, cfg *Config) error {
	for key, values := range params {
		if len(values) == 0 {
			continue
		}
		val := values[0]

		switch key {
		case "page_size":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return &ConfigError{
					Field:   "page_size",
					Value:   val,
					Message: "must be a positive integer",
				}
			}
			cfg.PageSize = n

		case "hello_name":
			if val == "" {
				return &ConfigError{
					Field:   "hello_name",
					Message: "cannot be empty",
				}
			}
			cfg.HelloName = val

		case "hello_ver":
			if val == "" {
				return &ConfigError{
					Field:   "hello_ver",
					Message: "cannot be empty",
				}
			}
			cfg.HelloVersion = val

		case "conformance":
			cfg.Conformance = val

		case "ca":
			cfg.TLSCA = val

		case "cert":
			cfg.TLSCert = val

		case "key":
			cfg.TLSKey = val

		case "insecure":
			cfg.InsecureSkipVerify = val == "true" || val == "1"

		case "connect_timeout":
			n, err := strconv.Atoi(val)
			if err != nil || n < 0 {
				return &ConfigError{
					Field:   "connect_timeout",
					Value:   val,
					Message: "must be a non-negative integer (seconds)",
				}
			}
			cfg.ConnectTimeout = n

		default:
			// Ignore unknown parameters for forward compatibility
		}
	}

	// Validate mTLS configuration
	if (cfg.TLSCert != "" && cfg.TLSKey == "") || (cfg.TLSCert == "" && cfg.TLSKey != "") {
		return &ConfigError{
			Field:   "tls",
			Message: "both cert and key must be provided for mTLS",
		}
	}

	return nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Host == "" {
		return &ConfigError{
			Field:   "host",
			Message: "host is required",
		}
	}

	// Validate hostname for security (null bytes, length limits)
	if err := validate.Hostname(c.Host); err != nil {
		return &ConfigError{
			Field:   "host",
			Value:   c.Host,
			Message: err.Error(),
		}
	}

	if c.Port == "" {
		return &ConfigError{
			Field:   "port",
			Message: "port is required",
		}
	}

	// Validate port is numeric
	if _, err := strconv.Atoi(c.Port); err != nil {
		return &ConfigError{
			Field:   "port",
			Value:   c.Port,
			Message: "must be a valid port number",
		}
	}

	if c.PageSize <= 0 {
		return &ConfigError{
			Field:   "page_size",
			Value:   strconv.Itoa(c.PageSize),
			Message: "must be positive",
		}
	}

	return nil
}

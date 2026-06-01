package geode

import (
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"geodedb.com/geode/internal/validate"
)

// TransportType specifies the transport protocol to use.
type TransportType int

const (
	// TransportQUIC uses QUIC transport with TLS 1.3 (default).
	TransportQUIC TransportType = iota

	// TransportGRPC uses gRPC transport.
	TransportGRPC
)

// String returns the string representation of the transport type.
func (t TransportType) String() string {
	switch t {
	case TransportQUIC:
		return "quic"
	case TransportGRPC:
		return "grpc"
	default:
		return "unknown"
	}
}

const (
	// DefaultPort is the default Geode server port for QUIC.
	DefaultPort = "3141"

	// DefaultGRPCPort is the default Geode server port for gRPC.
	DefaultGRPCPort = "50051"

	// DefaultPageSize is the default number of rows per page.
	DefaultPageSize = 1000

	// MaxPageSize is the maximum allowed page size.
	// This prevents potential DoS via memory exhaustion with extremely large page sizes.
	MaxPageSize = 100000

	// DefaultHelloName is the default client name sent in HELLO.
	DefaultHelloName = "geode-go"

	// DefaultHelloVersion is the default client version sent in HELLO.
	DefaultHelloVersion = "0.1"

	// DefaultConformance is the default GQL conformance level.
	DefaultConformance = "min"

	// MaxQueryLength is the maximum allowed query length.
	MaxQueryLength = 1 << 20 // 1MB

	// DSN query parameter keys.
	paramTLS = "tls"
)

// Config holds the configuration for a Geode connection.
type Config struct {
	// Transport specifies the transport protocol (QUIC or gRPC).
	Transport TransportType

	// Host is the server hostname or IP address.
	Host string

	// Port is the server port.
	Port string

	// Username for authentication.
	Username string

	// Password for authentication.
	Password string

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

	// TLSDisabled disables TLS entirely (plaintext gRPC, for testing only).
	TLSDisabled bool

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

	// Graph is the name of the graph to select after connecting.
	// When set, the driver automatically executes
	// CREATE GRAPH IF NOT EXISTS <graph> and USE GRAPH <graph>
	// after the HELLO handshake. This ensures every pooled connection
	// targets the correct graph without manual initialization.
	Graph string

	// PoolSize is the maximum number of underlying QUIC connections in the
	// shared pool that streams multiplex over.  Each `db.Conn(ctx)` returns
	// a stream-level handle on one of these QUIC connections, so the
	// effective parallelism of concurrent transactions is bounded by this
	// pool size when callers pin many sql.Conn handles.  Zero or negative
	// values fall back to DefaultQUICPoolSize.
	//
	// Tune via DSN: `?pool_size=16`.
	PoolSize int

	// MaxStreams bounds the total number of in-flight QUIC streams across
	// all pooled connections.  Stream creation blocks (context-aware) when
	// at this limit.  Zero or negative values fall back to DefaultMaxStreams.
	//
	// This is driver-side backpressure for applications that use
	// sql.Open("geode", dsn) directly without setting MaxOpenConns on the
	// *sql.DB they receive.  Geode 0.5.x's per-stream BufferedProtoReader
	// saturates around ~50 concurrent streams; the default (64) leaves a
	// modest spike margin without risking pollLoop overload.
	//
	// Tune via DSN: `?max_streams=64`.
	MaxStreams int
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

// FormatDSN serialises the Config back to a DSN string suitable for
// ParseDSN. It is the inverse of ParseDSN.
//
// Only non-default values are emitted as query parameters to keep the
// DSN minimal. Credentials are embedded in the userinfo segment
// (scheme://user:pass@host:port) which ParseDSN already handles.
func (c *Config) FormatDSN() string {
	scheme := c.Transport.String()

	host := net.JoinHostPort(c.Host, c.Port)

	q := url.Values{}
	if c.PageSize != 0 && c.PageSize != DefaultPageSize {
		q.Set("page_size", strconv.Itoa(c.PageSize))
	}
	if c.HelloName != "" && c.HelloName != DefaultHelloName {
		q.Set("hello_name", c.HelloName)
	}
	if c.HelloVersion != "" && c.HelloVersion != DefaultHelloVersion {
		q.Set("hello_ver", c.HelloVersion)
	}
	if c.Conformance != "" && c.Conformance != DefaultConformance {
		q.Set("conformance", c.Conformance)
	}
	if c.TLSCA != "" {
		q.Set("ca", c.TLSCA)
	}
	if c.TLSCert != "" {
		q.Set("cert", c.TLSCert)
	}
	if c.TLSKey != "" {
		q.Set("key", c.TLSKey)
	}
	if c.InsecureSkipVerify {
		q.Set("insecure_tls_skip_verify", "true")
	}
	if c.TLSDisabled {
		q.Set("tls", "0")
	}
	if c.Graph != "" {
		q.Set("graph", c.Graph)
	}
	if c.ConnectTimeout != 0 {
		q.Set("connect_timeout", strconv.Itoa(c.ConnectTimeout))
	}
	if c.PoolSize > 0 && c.PoolSize != DefaultQUICPoolSize {
		q.Set("pool_size", strconv.Itoa(c.PoolSize))
	}
	if c.MaxStreams > 0 && c.MaxStreams != DefaultMaxStreams {
		q.Set("max_streams", strconv.Itoa(c.MaxStreams))
	}

	var userinfo string
	switch {
	case c.Username != "" && c.Password != "":
		userinfo = url.UserPassword(c.Username, c.Password).String() + "@"
	case c.Username != "":
		userinfo = url.User(c.Username).String() + "@"
	case c.Password != "":
		// password-only: userinfo requires a username, so use query param
		q.Set("pass", c.Password)
	}

	dsn := scheme + "://" + userinfo + host
	if len(q) > 0 {
		dsn += "?" + q.Encode()
	}
	return dsn
}

// ParseDSN parses a data source name into a Config.
//
// See geode/docs/DSN.md for the complete DSN specification.
//
// Supported formats:
//   - quic://host:port?options  (QUIC transport)
//   - grpc://host:port?options  (gRPC transport)
//   - host:port?options         (QUIC transport, scheme-less)
//   - host:port                 (QUIC transport)
//   - host                      (QUIC transport, uses default port)
//
// IPv6 hosts are supported:
//   - quic://[::1]:3141
//   - grpc://[2001:db8::1]:50051
//
// Supported options:
//   - page_size: Results page size (default: 1000)
//   - hello_name: Client name (default: "geode-go")
//   - hello_ver: Client version (default: "0.1")
//   - conformance: GQL conformance level (default: "min")
//   - ca: Path to CA certificate
//   - cert: Path to client certificate (for mTLS)
//   - key: Path to client key (for mTLS)
//   - insecure_tls_skip_verify: Skip TLS verification (testing only, aliases: insecure, skip_verify)
//   - tls: Enable/disable TLS ("0" or "false" to disable, gRPC only)
//
// Environment variables:
//   - GEODE_HOST: Default host if DSN is empty
//   - GEODE_PORT: Default port if not specified
//   - GEODE_TLS_CA: Default CA certificate path
func ParseDSN(dsn string) (*Config, error) {
	cfg := &Config{
		Transport:    TransportQUIC,
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

	// Detect scheme
	switch {
	case strings.HasPrefix(dsn, "quic://"):
		cfg.Transport = TransportQUIC
		cfg.Port = DefaultPort
		return parseURLDSN(dsn, cfg)
	case strings.HasPrefix(dsn, "grpc://"):
		cfg.Transport = TransportGRPC
		cfg.Port = DefaultGRPCPort
		return parseURLDSN(dsn, cfg)
	default:
		// Check for unsupported schemes
		if idx := strings.Index(dsn, "://"); idx > 0 {
			scheme := dsn[:idx]
			return nil, &ConfigError{
				Field:   "scheme",
				Value:   scheme,
				Message: "unsupported scheme, use quic:// or grpc://",
			}
		}
		// Scheme-less - defaults to QUIC
		cfg.Transport = TransportQUIC
		cfg.Port = DefaultPort
		return parseSimpleDSN(dsn, cfg)
	}
}

// parseURLDSN parses a URL format DSN.
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
		// No port specified, use the host as-is with default port
		host = u.Host
		// port already set to default based on scheme
	} else {
		cfg.Port = port
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

	// Extract graph from URL path (e.g. quic://host:3141/mygraph)
	if u.Path != "" && u.Path != "/" {
		cfg.Graph = strings.TrimPrefix(u.Path, "/")
	}

	// Extract credentials from URL userinfo (user:pass@host)
	if u.User != nil {
		if username := u.User.Username(); username != "" {
			cfg.Username = username
		}
		if password, ok := u.User.Password(); ok {
			cfg.Password = password
		}
	}

	// Parse query parameters (may override userinfo credentials)
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

	// Parse host:port (supports IPv6 in brackets)
	if dsn != "" {
		host, port, err := net.SplitHostPort(dsn)
		if err != nil {
			// No port specified, treat entire string as host
			host = dsn
			// port already set to default
		} else {
			cfg.Port = port
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
//
//nolint:gocognit,gocyclo,cyclop,funlen // This is a straightforward switch statement for parsing DSN parameters.
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
			if n > MaxPageSize {
				return &ConfigError{
					Field:   "page_size",
					Value:   val,
					Message: "exceeds maximum allowed value of " + strconv.Itoa(MaxPageSize),
				}
			}
			cfg.PageSize = n

		case "pool_size":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return &ConfigError{
					Field:   "pool_size",
					Value:   val,
					Message: "must be a positive integer",
				}
			}
			cfg.PoolSize = n

		case "max_streams":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return &ConfigError{
					Field:   "max_streams",
					Value:   val,
					Message: "must be a positive integer",
				}
			}
			cfg.MaxStreams = n

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

		case "insecure_tls_skip_verify", "insecure", "skip_verify":
			cfg.InsecureSkipVerify = val == "true" || val == "1" || val == "yes" || val == "on"

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

		case paramTLS:
			// tls=0 or tls=false disables TLS entirely (plaintext gRPC, for testing)
			if val == "0" || val == "false" {
				cfg.TLSDisabled = true
			}

		case "user", "username":
			cfg.Username = val

		case "pass", "password":
			cfg.Password = val

		case "graph":
			cfg.Graph = val

		default:
			// Ignore unknown parameters for forward compatibility
		}
	}

	// Validate mTLS configuration
	if (cfg.TLSCert != "" && cfg.TLSKey == "") || (cfg.TLSCert == "" && cfg.TLSKey != "") {
		return &ConfigError{
			Field:   paramTLS,
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

	if c.PageSize > MaxPageSize {
		return &ConfigError{
			Field:   "page_size",
			Value:   strconv.Itoa(c.PageSize),
			Message: "exceeds maximum allowed value of " + strconv.Itoa(MaxPageSize),
		}
	}

	// Validate transport type
	if c.Transport != TransportQUIC && c.Transport != TransportGRPC {
		return &ConfigError{
			Field:   "transport",
			Message: "unsupported transport type",
		}
	}

	return nil
}

// UnsupportedSchemeError is returned when an unsupported DSN scheme is used.
type UnsupportedSchemeError struct {
	Scheme string
}

func (e *UnsupportedSchemeError) Error() string {
	return "geode: unsupported scheme: " + e.Scheme
}

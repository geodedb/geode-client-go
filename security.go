package geode

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
)

// maxCertFileSize is the maximum size for certificate files (1MB).
// This prevents DoS through memory exhaustion with large files.
const maxCertFileSize = 1 << 20 // 1MB

// NewTLSConfig creates a TLS configuration for connecting to a Geode server.
//
// The configuration enforces TLS 1.3 minimum and uses the ALPN protocol "geode/1".
// Certificate verification is always enabled unless explicitly disabled via
// Config.InsecureSkipVerify (which should only be used for testing).
//
// CA certificates are loaded from (in order of precedence):
//  1. Config.TLSCA (explicit path)
//  2. GEODE_TLS_CA environment variable
//  3. System certificate pool
//
// If Config.TLSCert and Config.TLSKey are provided, mutual TLS (mTLS) is enabled.
func NewTLSConfig(cfg *Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"geode/1"},
		ServerName: cfg.Host,
	}

	// Load CA certificates
	pool, err := loadCACertificates(cfg)
	if err != nil {
		return nil, err
	}

	if pool != nil {
		tlsCfg.RootCAs = pool
	}

	// Handle InsecureSkipVerify (testing only)
	if cfg.InsecureSkipVerify {
		tlsCfg.InsecureSkipVerify = true
	} else if pool == nil {
		// Try to use system pool
		systemPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, &SecurityError{
				Type:    "tls",
				Message: "no CA certificates available and system pool unavailable",
				cause:   err,
			}
		}
		tlsCfg.RootCAs = systemPool
	}

	// Load client certificate for mTLS
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := loadClientCertificate(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// loadCACertificates loads CA certificates from configured paths.
func loadCACertificates(cfg *Config) (*x509.CertPool, error) {
	// Collect candidate paths
	var paths []string
	explicit := false

	if cfg.TLSCA != "" {
		paths = append(paths, cfg.TLSCA)
		explicit = true
	}

	if env := os.Getenv("GEODE_TLS_CA"); env != "" && cfg.TLSCA == "" {
		paths = append(paths, env)
		explicit = true
	}

	if len(paths) == 0 {
		return nil, nil //nolint:nilnil // nil pool signals to use system pool
	}

	pool := x509.NewCertPool()
	var loaded bool

	for _, path := range paths {
		data, err := readLimitedFile(path, "CA certificate")
		if err != nil {
			if os.IsNotExist(err) && !explicit {
				continue
			}
			return nil, err
		}

		if !pool.AppendCertsFromPEM(data) {
			return nil, &SecurityError{
				Type:    "tls",
				Message: fmt.Sprintf("failed to parse CA certificate: %s", path),
			}
		}
		loaded = true
	}

	if !loaded {
		if explicit {
			return nil, &SecurityError{
				Type:    "tls",
				Message: "no valid CA certificates found in configured path",
			}
		}
		return nil, nil //nolint:nilnil // nil pool signals to use system pool
	}

	return pool, nil
}

// loadClientCertificate loads a client certificate and key for mTLS.
// Both files are checked against maxCertFileSize to prevent memory exhaustion.
func loadClientCertificate(certPath, keyPath string) (tls.Certificate, error) {
	certData, err := readLimitedFile(certPath, "client certificate")
	if err != nil {
		return tls.Certificate{}, err
	}

	keyData, err := readLimitedFile(keyPath, "client key")
	if err != nil {
		return tls.Certificate{}, err
	}

	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return tls.Certificate{}, &SecurityError{
			Type:    "tls",
			Message: "failed to load client certificate",
			cause:   err,
		}
	}
	return cert, nil
}

func readLimitedFile(path, label string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("failed to stat %s: %s", label, path),
			cause:   err,
		}
	}
	if !info.Mode().IsRegular() {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("%s must be a regular file: %s", label, path),
		}
	}
	if info.Size() > maxCertFileSize {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("%s file too large: %s (%d bytes, max %d)", label, path, info.Size(), maxCertFileSize),
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("failed to read %s: %s", label, path),
			cause:   err,
		}
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxCertFileSize+1))
	if err != nil {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("failed to read %s: %s", label, path),
			cause:   err,
		}
	}
	if int64(len(data)) > maxCertFileSize {
		return nil, &SecurityError{
			Type:    "tls",
			Message: fmt.Sprintf("%s file too large: %s (max %d)", label, path, maxCertFileSize),
		}
	}

	return data, nil
}

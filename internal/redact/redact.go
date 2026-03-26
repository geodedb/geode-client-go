// Package redact provides utilities to mask sensitive information before logging.
package redact

import (
	"strings"
)

const (
	masked = "***"

	// schemeSlashSlash is the length of "://" used to locate the authority in a URI.
	schemeSlashSlash = 3
)

// isCredentialParam reports whether the given query parameter key holds a credential.
func isCredentialParam(key string) bool {
	switch strings.ToLower(key) {
	case "user", "username", "pass", "password":
		return true
	default:
		return false
	}
}

// DSN returns the DSN string with any credentials masked, suitable for safe logging.
// It handles:
//   - URI-style credentials: quic://user:pass@host → quic://***:***@host
//   - Query parameter credentials: ?user=x&pass=y → ?user=***&pass=***
func DSN(dsn string) string {
	if dsn == "" {
		return dsn
	}

	// Split into the base part (before '?') and query string.
	base, query, hasQuery := strings.Cut(dsn, "?")

	// Redact URI userinfo (user:pass@ or user@).
	base = redactUserInfo(base)

	// Redact credential query parameters.
	if hasQuery {
		query = redactQueryParams(query)
		return base + "?" + query
	}

	return base
}

// redactUserInfo masks the userinfo portion of a URI (the user:pass@ segment).
func redactUserInfo(uri string) string {
	// Find "://" to locate the authority section.
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd == -1 {
		// No scheme; check if there's a bare user@host pattern.
		// Simple DSNs like "localhost:3141" won't have '@'.
		atIdx := strings.LastIndex(uri, "@")
		if atIdx == -1 {
			return uri
		}
		return redactAtSegment(uri, 0, atIdx)
	}

	authorityStart := schemeEnd + schemeSlashSlash
	authority := uri[authorityStart:]

	atIdx := strings.LastIndex(authority, "@")
	if atIdx == -1 {
		return uri
	}

	// authority[:atIdx] is the userinfo; authority[atIdx+1:] is host[:port][/path].
	userinfo := authority[:atIdx]
	rest := authority[atIdx:]

	// Mask user and password separately.
	colonIdx := strings.Index(userinfo, ":")
	var maskedUserinfo string
	if colonIdx == -1 {
		maskedUserinfo = masked
	} else {
		maskedUserinfo = masked + ":" + masked
	}

	return uri[:authorityStart] + maskedUserinfo + rest
}

// redactAtSegment handles the case where a bare "user@host" or "user:pass@host"
// appears without a scheme prefix.
func redactAtSegment(uri string, start, atIdx int) string {
	userinfo := uri[start:atIdx]
	rest := uri[atIdx:]

	colonIdx := strings.Index(userinfo, ":")
	var maskedUserinfo string
	if colonIdx == -1 {
		maskedUserinfo = masked
	} else {
		maskedUserinfo = masked + ":" + masked
	}

	return uri[:start] + maskedUserinfo + rest
}

// redactQueryParams replaces the values of credential parameters with "***"
// while preserving all other parameters and their order.
func redactQueryParams(query string) string {
	if query == "" {
		return query
	}

	params := strings.Split(query, "&")
	for i, param := range params {
		key, _, hasVal := strings.Cut(param, "=")
		if !hasVal {
			continue
		}
		if isCredentialParam(key) {
			params[i] = key + "=" + masked
		}
	}

	return strings.Join(params, "&")
}

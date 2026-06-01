package geode

import (
	"errors"
	"strings"
)

// ErrInvalidIdent is returned by QuoteIdent when the input contains
// characters that cannot appear in a bare GQL identifier.
var ErrInvalidIdent = errors.New("geode: identifier contains forbidden characters")

// ErrInvalidString is returned by QuoteString when the input contains
// ASCII control characters that cannot safely be escaped inside a GQL
// single-quoted string literal.
var ErrInvalidString = errors.New("geode: string literal contains forbidden characters")

// SanitizeGraphName returns name stripped of any character that is not
// alphanumeric, underscore, or hyphen, so it can be inlined safely into
// a USE GRAPH statement without opening an injection vector.
func SanitizeGraphName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		}
	}
	return b.String()
}

// QuoteIdent validates and returns name in a form safe to inline into a
// GQL admin statement that takes an identifier (CREATE USER, ALTER USER, etc.).
// Valid identifiers match [A-Za-z_][A-Za-z_0-9-]*. Returns ErrInvalidIdent
// for empty strings or inputs containing characters outside this set.
func QuoteIdent(name string) (string, error) {
	if name == "" {
		return "", ErrInvalidIdent
	}
	first := true
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			// Always allowed.
		case r >= '0' && r <= '9':
			// Digits allowed anywhere except as the first character.
			if first {
				return "", ErrInvalidIdent
			}
		case r == '-':
			if first {
				return "", ErrInvalidIdent
			}
		default:
			return "", ErrInvalidIdent
		}
		first = false
	}
	return name, nil
}

// QuoteString returns s wrapped in GQL single quotes, with backslashes and
// single quotes properly escaped. ASCII control characters (< 0x20 or 0x7F)
// are rejected with ErrInvalidString. Escape order: backslash first (\ → \\),
// then single quotes (' → ”), matching the internal gqlStringEscaper in conn.go.
func QuoteString(s string) (string, error) {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return "", ErrInvalidString
		}
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'", nil
}

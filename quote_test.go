package geode_test

import (
	"errors"
	"testing"

	geode "geodedb.com/geode"
)

func TestSanitizeGraphName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"myGraph", "myGraph"},
		{"my-graph_01", "my-graph_01"},
		{"bad name!", "badname"},
		{"", ""},
		{"有害", ""},
		{"   ", ""},
	}
	for _, tc := range tests {
		if got := geode.SanitizeGraphName(tc.in); got != tc.want {
			t.Errorf("SanitizeGraphName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	ok := []string{"abc", "ABC", "_foo", "foo_bar", "foo-bar", "foo9", "9starts", "005_add_index"}
	for _, name := range ok {
		got, err := geode.QuoteIdent(name)
		if err != nil {
			t.Errorf("QuoteIdent(%q) unexpected error: %v", name, err)
		}
		if got != name {
			t.Errorf("QuoteIdent(%q) = %q, want %q", name, got, name)
		}
	}

	bad := []string{"", "-starts", "has space", "has;semi", "has\x00null"}
	for _, name := range bad {
		if _, err := geode.QuoteIdent(name); err == nil {
			t.Errorf("QuoteIdent(%q) expected error, got nil", name)
		}
	}
}

func TestQuoteString(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"hello", `'hello'`},
		{"it's", `'it''s'`},
		{`back\slash`, `'back\\slash'`},
		{`back\'quote`, `'back\\''quote'`},
	}
	for _, tc := range tests {
		got, err := geode.QuoteString(tc.in)
		if err != nil {
			t.Errorf("QuoteString(%q) unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("QuoteString(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	controls := []string{"\n", "\r", "\t", "\x00", "\x1f", "\x7f"}
	for _, s := range controls {
		if _, err := geode.QuoteString(s); !errors.Is(err, geode.ErrInvalidString) {
			t.Errorf("QuoteString(%q) expected ErrInvalidString, got %v", s, err)
		}
	}
}

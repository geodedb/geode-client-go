package geode

import (
	"fmt"
	"strings"
	"time"
)

// timeFormats lists the time layouts tried in order from most to least specific.
//
//nolint:gochecknoglobals // read-only slice of format strings; no mutation risk.
var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// stripMonotonic removes Go's monotonic clock suffix (` m=+N.NNN` or
// ` m=-N.NNN`) from a time string before parsing.
func stripMonotonic(s string) string {
	if idx := strings.Index(s, " m="); idx != -1 {
		return s[:idx]
	}
	return s
}

// ParseTime parses a time string returned by the Geode server.
// It strips the Go monotonic clock suffix if present, then tries each
// supported layout in order from most specific to least specific.
// An error is returned for empty strings or unrecognised input.
func ParseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("geode: ParseTime: empty string")
	}

	s = stripMonotonic(s)

	for _, layout := range timeFormats {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("geode: ParseTime: cannot parse %q as a time value", s)
}

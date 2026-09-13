package geode

import (
	"fmt"
	"strings"
	"time"
)

// goStringerLayout is the layout time.Time.String() uses internally (and
// therefore what fmt.Sprintf("%v", t) produces): a zone offset AND a zone
// abbreviation trail every value, e.g. "2024-03-15 10:30:45.123456789 +0000
// UTC". This is what encodeParamValue (params.go) sends for a time.Time
// argument — it must be tried, or a round-trip through ScanTime fails for
// every non-UTC-abbreviation-free value (none of the other layouts below
// include a zone name, so they never match a string containing one).
const goStringerLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

// timeFormats lists the time layouts tried in order from most to least specific.
//
//nolint:gochecknoglobals // read-only slice of format strings; no mutation risk.
var timeFormats = []string{
	goStringerLayout,
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

// TimeScanner implements [sql.Scanner] for Geode time strings.
// Construct one with [ScanTime] and pass it directly to rows.Scan:
//
//	var t time.Time
//	rows.Scan(geode.ScanTime(&t))
type TimeScanner struct {
	dest *time.Time
}

// ScanTime returns a [sql.Scanner] that parses a Geode time string into dest.
// Geode encodes time.Time values as strings (via fmt.Sprintf("%v", t)), so they
// arrive as strings like "2006-01-02 15:04:05.999999999 -0700 MST". The
// monotonic clock suffix "m=..." is stripped before parsing.
func ScanTime(dest *time.Time) *TimeScanner {
	return &TimeScanner{dest: dest}
}

// Scan implements [sql.Scanner].
func (ts *TimeScanner) Scan(src any) error {
	if src == nil {
		*ts.dest = time.Time{}
		return nil
	}
	switch v := src.(type) {
	case time.Time:
		*ts.dest = v
		return nil
	case string:
		if v == "" {
			*ts.dest = time.Time{}
			return nil
		}
		t, err := ParseTime(v)
		if err != nil {
			return fmt.Errorf("geode: ScanTime: %w", err)
		}
		*ts.dest = t
		return nil
	default:
		return fmt.Errorf("geode: ScanTime: unsupported type %T", src)
	}
}

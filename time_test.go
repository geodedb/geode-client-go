package geode

import (
	"testing"
	"time"
)

func TestStripMonotonic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "with monotonic suffix",
			input: "2024-01-15T10:30:00Z m=+1.234567890",
			want:  "2024-01-15T10:30:00Z",
		},
		{
			name:  "without suffix unchanged",
			input: "2024-01-15T10:30:00Z",
			want:  "2024-01-15T10:30:00Z",
		},
		{
			name:  "datetime space format with suffix",
			input: "2024-01-15 10:30:00 m=+0.000000001",
			want:  "2024-01-15 10:30:00",
		},
		{
			name:  "negative monotonic suffix",
			input: "2024-01-15T10:30:00Z m=-1.234567890",
			want:  "2024-01-15T10:30:00Z",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := stripMonotonic(tc.input)
			if got != tc.want {
				t.Errorf("stripMonotonic(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(t *testing.T, got time.Time)
	}{
		{
			name:  "RFC3339 UTC",
			input: "2024-01-15T10:30:00Z",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "RFC3339 with timezone offset",
			input: "2024-01-15T10:30:00+05:30",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				loc := time.FixedZone("", 5*3600+30*60)
				want := time.Date(2024, 1, 15, 10, 30, 0, 0, loc)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "RFC3339Nano",
			input: "2024-01-15T10:30:00.123456789Z",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "date only",
			input: "2024-01-15",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "datetime without timezone",
			input: "2024-01-15 10:30:00",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "datetime with fractional seconds",
			input: "2024-01-15 10:30:00.123456",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 10, 30, 0, 123456000, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "with monotonic clock suffix",
			input: "2024-01-15T10:30:00Z m=+1.234567890",
			check: func(t *testing.T, got time.Time) {
				t.Helper()
				want := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:    "empty string returns error",
			input:   "",
			wantErr: true,
		},
		{
			name:    "garbage input returns error",
			input:   "not-a-time",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTime(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseTime(%q) expected error, got nil (value: %v)", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTime(%q) unexpected error: %v", tc.input, err)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

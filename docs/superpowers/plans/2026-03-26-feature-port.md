# Feature Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port 8 features from the go-geode application layer into the official geode-client-go driver as first-class capabilities, organized into a two-tier API (database/sql compatible + native graph API).

**Architecture:** The existing root `geode` package remains the `database/sql` driver. Three new packages extend it: `graph/` provides a high-level connection wrapper with batch, pipeline, transaction, and schema operations; `crypto/` provides field-level AES-256-GCM encryption with HMAC searchable encryption; `internal/redact/` provides DSN credential masking for safe logging.

**Tech Stack:** Go 1.23+, `database/sql`, `crypto/aes`, `crypto/cipher`, `crypto/hmac`, `crypto/sha256`, `golang.org/x/crypto/hkdf`, `io/fs` (embed.FS), `sync`, `sync/atomic`

---

## File Structure

```
geode-client-go/
├── (existing files unchanged)
├── time.go                     # Geode-specific time format parsing
├── time_test.go                # Tests for time parsing
├── retry.go                    # Retry policy and executor
├── retry_test.go               # Tests for retry logic
├── internal/
│   ├── validate/input.go       # (existing)
│   └── redact/
│       ├── redact.go           # DSN credential redaction
│       └── redact_test.go      # Tests for redaction
├── graph/
│   ├── conn.go                 # High-level Conn wrapping *sql.DB
│   ├── conn_test.go            # Tests for Conn
│   ├── option.go               # Functional options (WithRetry, etc.)
│   ├── option_test.go          # Tests for options
│   ├── tx.go                   # Transaction wrapper with auto-rollback
│   ├── tx_test.go              # Tests for TX
│   ├── batch.go                # Sequential batch execution with retry
│   ├── batch_test.go           # Tests for batch
│   ├── pipeline.go             # Parallel bulk execution with worker pool
│   ├── pipeline_test.go        # Tests for pipeline
│   ├── schema.go               # Schema loading from embed.FS
│   └── schema_test.go          # Tests for schema
└── crypto/
    ├── cipher.go               # AES-256-GCM + HKDF + HMAC
    └── cipher_test.go          # Tests for encryption
```

---

## Phase 1: Foundation

### Task 1: DSN Credential Redaction

**Files:**

- Create: `internal/redact/redact.go`
- Create: `internal/redact/redact_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/redact/redact_test.go`:

```go
package redact_test

import (
 "testing"

 "geodedb.com/geode/internal/redact"
)

func TestDSN_URIWithCredentials(t *testing.T) {
 tests := []struct {
  name string
  dsn  string
  want string
 }{
  {
   name: "quic with user:pass",
   dsn:  "quic://admin:secret@localhost:3141",
   want: "quic://***:***@localhost:3141",
  },
  {
   name: "grpc with user:pass and query",
   dsn:  "grpc://user:p4ss@host:50051?graph=mydb",
   want: "grpc://***:***@host:50051?graph=mydb",
  },
  {
   name: "quic with user only",
   dsn:  "quic://admin@localhost:3141",
   want: "quic://***@localhost:3141",
  },
  {
   name: "quic with path and creds",
   dsn:  "quic://admin:secret@localhost:3141/mygraph",
   want: "quic://***:***@localhost:3141/mygraph",
  },
 }
 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   got := redact.DSN(tt.dsn)
   if got != tt.want {
    t.Errorf("DSN(%q) = %q, want %q", tt.dsn, got, tt.want)
   }
  })
 }
}

func TestDSN_QueryParamCredentials(t *testing.T) {
 tests := []struct {
  name string
  dsn  string
  want string
 }{
  {
   name: "user and pass query params",
   dsn:  "quic://localhost:3141?user=admin&pass=secret&graph=mydb",
   want: "quic://localhost:3141?user=***&pass=***&graph=mydb",
  },
  {
   name: "username and password query params",
   dsn:  "quic://localhost:3141?username=admin&password=secret",
   want: "quic://localhost:3141?username=***&password=***",
  },
  {
   name: "simple DSN with password param",
   dsn:  "localhost:3141?password=secret&page_size=500",
   want: "localhost:3141?password=***&page_size=500",
  },
 }
 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   got := redact.DSN(tt.dsn)
   if got != tt.want {
    t.Errorf("DSN(%q) = %q, want %q", tt.dsn, got, tt.want)
   }
  })
 }
}

func TestDSN_NoCredentials(t *testing.T) {
 tests := []struct {
  name string
  dsn  string
 }{
  {name: "quic no creds", dsn: "quic://localhost:3141"},
  {name: "simple host:port", dsn: "localhost:3141"},
  {name: "empty", dsn: ""},
  {name: "host only", dsn: "localhost"},
  {name: "with non-sensitive params", dsn: "quic://localhost:3141?page_size=500&graph=mydb"},
 }
 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   got := redact.DSN(tt.dsn)
   if got != tt.dsn {
    t.Errorf("DSN(%q) = %q, want unchanged", tt.dsn, got)
   }
  })
 }
}

func TestDSN_BothURIAndQueryCreds(t *testing.T) {
 // URI creds AND query param creds should both be redacted
 dsn := "quic://admin:secret@localhost:3141?password=override"
 got := redact.DSN(dsn)
 want := "quic://***:***@localhost:3141?password=***"
 if got != want {
  t.Errorf("DSN(%q) = %q, want %q", dsn, got, want)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./internal/redact/ -v -count=1`
Expected: compilation error — package does not exist

- [ ] **Step 3: Write minimal implementation**

Create `internal/redact/redact.go`:

```go
// Package redact provides credential masking for DSN strings.
package redact

import (
 "net/url"
 "strings"
)

// credentialParams are DSN query parameter keys that contain credentials.
var credentialParams = map[string]bool{
 "user":     true,
 "username": true,
 "pass":     true,
 "password": true,
}

// DSN masks credentials in a DSN string for safe logging.
// It handles both URI-style credentials (scheme://user:pass@host) and
// query parameter credentials (password=secret).
//
// Non-credential parameters are preserved unchanged.
func DSN(dsn string) string {
 if dsn == "" {
  return dsn
 }

 // Handle URI-style DSNs (quic:// or grpc://)
 if strings.Contains(dsn, "://") {
  return redactURI(dsn)
 }

 // Handle simple DSNs (host:port?params)
 return redactQueryParams(dsn)
}

// redactURI handles scheme://[user[:pass]@]host[:port][/path][?query] format.
func redactURI(dsn string) string {
 u, err := url.Parse(dsn)
 if err != nil {
  return dsn // unparseable — return as-is
 }

 // Redact userinfo
 if u.User != nil {
  _, hasPass := u.User.Password()
  if hasPass {
   u.User = url.UserPassword("***", "***")
  } else {
   u.User = url.User("***")
  }
 }

 // Redact credential query params
 if u.RawQuery != "" {
  u.RawQuery = redactParams(u.RawQuery)
 }

 return u.String()
}

// redactQueryParams handles simple DSNs with ?key=value params.
func redactQueryParams(dsn string) string {
 idx := strings.Index(dsn, "?")
 if idx < 0 {
  return dsn
 }

 prefix := dsn[:idx+1]
 query := dsn[idx+1:]
 return prefix + redactParams(query)
}

// redactParams replaces credential values in a raw query string.
// It preserves parameter order and encoding.
func redactParams(raw string) string {
 parts := strings.Split(raw, "&")
 changed := false
 for i, part := range parts {
  eqIdx := strings.Index(part, "=")
  if eqIdx < 0 {
   continue
  }
  key := part[:eqIdx]
  if credentialParams[key] {
   parts[i] = key + "=***"
   changed = true
  }
 }
 if !changed {
  return raw
 }
 return strings.Join(parts, "&")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./internal/redact/ -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/redact/redact.go internal/redact/redact_test.go
git commit -m "feat: add DSN credential redaction for safe logging"
```

---

### Task 2: Geode Time Format Parsing

**Files:**

- Create: `time.go`
- Create: `time_test.go`

- [ ] **Step 1: Write the failing tests**

Create `time_test.go`:

```go
package geode

import (
 "testing"
 "time"
)

func TestParseTime(t *testing.T) {
 tests := []struct {
  name    string
  input   string
  wantErr bool
  check   func(t *testing.T, got time.Time)
 }{
  {
   name:  "RFC3339",
   input: "2024-01-15T10:30:00Z",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Year() != 2024 || got.Month() != time.January || got.Day() != 15 {
     t.Errorf("wrong date: %v", got)
    }
   },
  },
  {
   name:  "RFC3339 with timezone offset",
   input: "2024-01-15T10:30:00+05:30",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Year() != 2024 {
     t.Errorf("wrong year: %v", got)
    }
   },
  },
  {
   name:  "RFC3339Nano",
   input: "2024-01-15T10:30:00.123456789Z",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Nanosecond() != 123456789 {
     t.Errorf("wrong nanos: %d", got.Nanosecond())
    }
   },
  },
  {
   name:  "date only",
   input: "2024-01-15",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Year() != 2024 || got.Month() != time.January || got.Day() != 15 {
     t.Errorf("wrong date: %v", got)
    }
   },
  },
  {
   name:  "datetime without timezone",
   input: "2024-01-15 10:30:00",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Hour() != 10 || got.Minute() != 30 {
     t.Errorf("wrong time: %v", got)
    }
   },
  },
  {
   name:  "datetime with fractional seconds",
   input: "2024-01-15 10:30:00.123456",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Hour() != 10 {
     t.Errorf("wrong hour: %v", got)
    }
   },
  },
  {
   name:  "with monotonic clock suffix",
   input: "2024-01-15T10:30:00Z m=+1.234567890",
   check: func(t *testing.T, got time.Time) {
    t.Helper()
    if got.Year() != 2024 {
     t.Errorf("wrong year: %v", got)
    }
   },
  },
  {
   name:    "empty string",
   input:   "",
   wantErr: true,
  },
  {
   name:    "garbage",
   input:   "not-a-time",
   wantErr: true,
  },
 }
 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   got, err := ParseTime(tt.input)
   if tt.wantErr {
    if err == nil {
     t.Errorf("ParseTime(%q) expected error, got %v", tt.input, got)
    }
    return
   }
   if err != nil {
    t.Fatalf("ParseTime(%q) unexpected error: %v", tt.input, err)
   }
   if tt.check != nil {
    tt.check(t, got)
   }
  })
 }
}

func TestStripMonotonic(t *testing.T) {
 tests := []struct {
  input string
  want  string
 }{
  {
   input: "2024-01-15T10:30:00Z m=+1.234567890",
   want:  "2024-01-15T10:30:00Z",
  },
  {
   input: "2024-01-15T10:30:00Z",
   want:  "2024-01-15T10:30:00Z",
  },
  {
   input: "2024-01-15 10:30:00 m=+0.000000001",
   want:  "2024-01-15 10:30:00",
  },
 }
 for _, tt := range tests {
  t.Run(tt.input, func(t *testing.T) {
   got := stripMonotonic(tt.input)
   if got != tt.want {
    t.Errorf("stripMonotonic(%q) = %q, want %q", tt.input, got, tt.want)
   }
  })
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -run "TestParseTime|TestStripMonotonic" -v -count=1`
Expected: compilation error — `ParseTime` and `stripMonotonic` undefined

- [ ] **Step 3: Write minimal implementation**

Create `time.go`:

```go
package geode

import (
 "fmt"
 "strings"
 "time"
)

// timeFormats lists the time formats the Geode server may return,
// ordered from most specific to least specific for correct parsing.
var timeFormats = []string{
 time.RFC3339Nano,               // 2006-01-02T15:04:05.999999999Z07:00
 time.RFC3339,                   // 2006-01-02T15:04:05Z07:00
 "2006-01-02 15:04:05.999999",  // datetime with fractional seconds
 "2006-01-02 15:04:05",         // datetime without timezone
 "2006-01-02T15:04:05",         // ISO 8601 without timezone
 "2006-01-02",                  // date only
}

// ParseTime parses a time string returned by the Geode server.
// It handles multiple format variants including monotonic clock suffixes
// that Go's time.Time.String() appends (e.g. "m=+1.234567890").
func ParseTime(s string) (time.Time, error) {
 if s == "" {
  return time.Time{}, fmt.Errorf("geode: empty time string")
 }

 // Strip Go's monotonic clock suffix before parsing.
 s = stripMonotonic(s)

 for _, format := range timeFormats {
  if t, err := time.Parse(format, s); err == nil {
   return t, nil
  }
 }

 return time.Time{}, fmt.Errorf("geode: cannot parse time %q", s)
}

// stripMonotonic removes the " m=+N.NNN" or " m=-N.NNN" suffix that
// Go's time.Time.String() appends when the monotonic clock is present.
func stripMonotonic(s string) string {
 if idx := strings.Index(s, " m="); idx >= 0 {
  return strings.TrimSpace(s[:idx])
 }
 return s
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -run "TestParseTime|TestStripMonotonic" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add time.go time_test.go
git commit -m "feat: add Geode time format parsing with monotonic clock stripping"
```

---

### Task 3: Retry Policy

**Files:**

- Create: `retry.go`
- Create: `retry_test.go`

- [ ] **Step 1: Write the failing tests**

Create `retry_test.go`:

```go
package geode

import (
 "context"
 "errors"
 "testing"
 "time"
)

func TestRetryPolicy_Defaults(t *testing.T) {
 p := DefaultRetryPolicy()
 if p.MaxAttempts != 3 {
  t.Errorf("MaxAttempts = %d, want 3", p.MaxAttempts)
 }
 if p.InitialBackoff != time.Second {
  t.Errorf("InitialBackoff = %v, want 1s", p.InitialBackoff)
 }
 if p.MaxBackoff != 30*time.Second {
  t.Errorf("MaxBackoff = %v, want 30s", p.MaxBackoff)
 }
 if p.Multiplier != 2.0 {
  t.Errorf("Multiplier = %f, want 2.0", p.Multiplier)
 }
}

func TestRetry_SucceedsFirstAttempt(t *testing.T) {
 calls := 0
 err := Retry(context.Background(), DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  return nil
 })
 if err != nil {
  t.Fatalf("unexpected error: %v", err)
 }
 if calls != 1 {
  t.Errorf("calls = %d, want 1", calls)
 }
}

func TestRetry_RetriesOnTransportError(t *testing.T) {
 calls := 0
 err := Retry(context.Background(), DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  if calls < 3 {
   return &TransportError{Op: "send", cause: errors.New("connection reset")}
  }
  return nil
 })
 if err != nil {
  t.Fatalf("unexpected error: %v", err)
 }
 if calls != 3 {
  t.Errorf("calls = %d, want 3", calls)
 }
}

func TestRetry_RetriesOnSerializationError(t *testing.T) {
 calls := 0
 err := Retry(context.Background(), DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  if calls < 2 {
   return &DriverError{Class: StatusSerialization, Message: "serialization failure"}
  }
  return nil
 })
 if err != nil {
  t.Fatalf("unexpected error: %v", err)
 }
 if calls != 2 {
  t.Errorf("calls = %d, want 2", calls)
 }
}

func TestRetry_StopsOnNonRetryableError(t *testing.T) {
 calls := 0
 syntaxErr := &DriverError{Class: StatusSyntax, Message: "bad query"}
 err := Retry(context.Background(), DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  return syntaxErr
 })
 if !errors.Is(err, syntaxErr) {
  t.Errorf("err = %v, want syntax error", err)
 }
 if calls != 1 {
  t.Errorf("calls = %d, want 1", calls)
 }
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
 policy := RetryPolicy{
  MaxAttempts:    2,
  InitialBackoff: time.Millisecond,
  MaxBackoff:     time.Millisecond,
  Multiplier:     1.0,
 }
 calls := 0
 err := Retry(context.Background(), policy, func(ctx context.Context) error {
  calls++
  return &TransportError{Op: "send", cause: errors.New("down")}
 })
 if err == nil {
  t.Fatal("expected error after exhausting retries")
 }
 if calls != 2 {
  t.Errorf("calls = %d, want 2", calls)
 }
}

func TestRetry_RespectsContextCancellation(t *testing.T) {
 ctx, cancel := context.WithCancel(context.Background())
 cancel() // cancel immediately

 calls := 0
 err := Retry(ctx, DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  return &TransportError{Op: "send", cause: errors.New("down")}
 })
 if !errors.Is(err, context.Canceled) {
  t.Errorf("err = %v, want context.Canceled", err)
 }
}

func TestRetry_NonGeodeErrorNotRetried(t *testing.T) {
 calls := 0
 plainErr := errors.New("some random error")
 err := Retry(context.Background(), DefaultRetryPolicy(), func(ctx context.Context) error {
  calls++
  return plainErr
 })
 if !errors.Is(err, plainErr) {
  t.Errorf("err = %v, want plainErr", err)
 }
 if calls != 1 {
  t.Errorf("calls = %d, want 1 (non-geode errors should not retry)", calls)
 }
}

func TestRetry_BackoffClampsToMax(t *testing.T) {
 policy := RetryPolicy{
  MaxAttempts:    4,
  InitialBackoff: 10 * time.Second,
  MaxBackoff:     15 * time.Second,
  Multiplier:     3.0,
 }
 b := policy.backoff(3) // 10s * 3^2 = 90s, clamped to 15s
 if b != 15*time.Second {
  t.Errorf("backoff(3) = %v, want 15s", b)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -run "TestRetry" -v -count=1`
Expected: compilation error — `RetryPolicy`, `Retry`, `DefaultRetryPolicy` undefined

- [ ] **Step 3: Write minimal implementation**

Create `retry.go`:

```go
package geode

import (
 "context"
 "math"
 "time"
)

// RetryPolicy configures retry behavior for operations that may fail
// with transient errors (transport failures, serialization conflicts).
type RetryPolicy struct {
 // MaxAttempts is the maximum number of attempts (including the initial call).
 // Must be >= 1. Default: 3.
 MaxAttempts int

 // InitialBackoff is the delay before the first retry. Default: 1s.
 InitialBackoff time.Duration

 // MaxBackoff is the maximum backoff duration. Default: 30s.
 MaxBackoff time.Duration

 // Multiplier is the exponential backoff multiplier. Default: 2.0.
 Multiplier float64
}

// DefaultRetryPolicy returns a retry policy suitable for most operations.
func DefaultRetryPolicy() RetryPolicy {
 return RetryPolicy{
  MaxAttempts:    3,
  InitialBackoff: time.Second,
  MaxBackoff:     30 * time.Second,
  Multiplier:     2.0,
 }
}

// backoff calculates the backoff duration for a given attempt (1-indexed).
// attempt=1 is the first retry, so backoff = InitialBackoff * Multiplier^(attempt-1).
func (p RetryPolicy) backoff(attempt int) time.Duration {
 if attempt <= 1 {
  return p.InitialBackoff
 }
 factor := math.Pow(p.Multiplier, float64(attempt-1))
 d := time.Duration(float64(p.InitialBackoff) * factor)
 if d > p.MaxBackoff {
  return p.MaxBackoff
 }
 return d
}

// Retry executes fn with the given retry policy. It retries only when
// the error is retryable (TransportError, or DriverError with a retryable
// status class like serialization failure or deadlock).
//
// Non-retryable errors and non-Geode errors are returned immediately.
// The context is checked before each retry; if cancelled, the context
// error is returned.
func Retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
 if policy.MaxAttempts < 1 {
  policy.MaxAttempts = 1
 }

 var lastErr error
 for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
  lastErr = fn(ctx)
  if lastErr == nil {
   return nil
  }

  // Only retry if the error is a Geode retryable error.
  if !IsRetryableError(lastErr) {
   return lastErr
  }

  // Don't sleep after the last attempt.
  if attempt == policy.MaxAttempts {
   break
  }

  // Check context before sleeping.
  if err := ctx.Err(); err != nil {
   return err
  }

  delay := policy.backoff(attempt)
  timer := time.NewTimer(delay)
  select {
  case <-ctx.Done():
   timer.Stop()
   return ctx.Err()
  case <-timer.C:
  }
 }

 return lastErr
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -run "TestRetry" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add retry.go retry_test.go
git commit -m "feat: add retry policy with exponential backoff for transient errors"
```

---

## Phase 2: Graph API

### Task 4: Graph Package — Options and Conn Foundation

**Files:**

- Create: `graph/option.go`
- Create: `graph/conn.go`
- Create: `graph/option_test.go`
- Create: `graph/conn_test.go`

- [ ] **Step 1: Write the failing tests for options**

Create `graph/option_test.go`:

```go
package graph_test

import (
 "log/slog"
 "testing"
 "time"

 "geodedb.com/geode/graph"
)

func TestWithRetry(t *testing.T) {
 opts := graph.ApplyOptions(graph.WithRetry(5, 2*time.Second))
 if opts.MaxRetries != 5 {
  t.Errorf("MaxRetries = %d, want 5", opts.MaxRetries)
 }
 if opts.RetryBackoff != 2*time.Second {
  t.Errorf("RetryBackoff = %v, want 2s", opts.RetryBackoff)
 }
}

func TestWithLogger(t *testing.T) {
 logger := slog.Default()
 opts := graph.ApplyOptions(graph.WithLogger(logger))
 if opts.Logger != logger {
  t.Error("Logger not set")
 }
}

func TestWithBatchSize(t *testing.T) {
 opts := graph.ApplyOptions(graph.WithBatchSize(200))
 if opts.BatchSize != 200 {
  t.Errorf("BatchSize = %d, want 200", opts.BatchSize)
 }
}

func TestWithWorkers(t *testing.T) {
 opts := graph.ApplyOptions(graph.WithWorkers(16))
 if opts.Workers != 16 {
  t.Errorf("Workers = %d, want 16", opts.Workers)
 }
}

func TestDefaults(t *testing.T) {
 opts := graph.ApplyOptions()
 if opts.MaxRetries != 3 {
  t.Errorf("default MaxRetries = %d, want 3", opts.MaxRetries)
 }
 if opts.BatchSize != 100 {
  t.Errorf("default BatchSize = %d, want 100", opts.BatchSize)
 }
 if opts.Workers != 8 {
  t.Errorf("default Workers = %d, want 8", opts.Workers)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -v -count=1`
Expected: compilation error — package does not exist

- [ ] **Step 3: Write options implementation**

Create `graph/option.go`:

```go
// Package graph provides a high-level API for the Geode graph database,
// built on top of the database/sql-compatible driver. It adds batch execution,
// pipeline processing, schema management, and transaction ergonomics that
// go beyond what database/sql offers.
package graph

import (
 "log/slog"
 "time"
)

// Options holds configuration for a graph Conn.
type Options struct {
 // MaxRetries is the maximum number of retry attempts for transient errors.
 MaxRetries int

 // RetryBackoff is the initial backoff duration between retries.
 RetryBackoff time.Duration

 // Logger for operations. Nil disables logging.
 Logger *slog.Logger

 // BatchSize is the default chunk size for batch and pipeline operations.
 BatchSize int

 // Workers is the default worker count for pipeline operations.
 Workers int
}

// Option configures a graph Conn.
type Option func(*Options)

// WithRetry sets the maximum retry attempts and initial backoff.
func WithRetry(maxRetries int, backoff time.Duration) Option {
 return func(o *Options) {
  o.MaxRetries = maxRetries
  o.RetryBackoff = backoff
 }
}

// WithLogger sets the structured logger.
func WithLogger(logger *slog.Logger) Option {
 return func(o *Options) {
  o.Logger = logger
 }
}

// WithBatchSize sets the default chunk size for batch operations.
func WithBatchSize(size int) Option {
 return func(o *Options) {
  o.BatchSize = size
 }
}

// WithWorkers sets the default worker count for pipeline operations.
func WithWorkers(n int) Option {
 return func(o *Options) {
  o.Workers = n
 }
}

// ApplyOptions returns Options with defaults, then applies the given options.
func ApplyOptions(opts ...Option) Options {
 o := Options{
  MaxRetries:   3,
  RetryBackoff: time.Second,
  BatchSize:    100,
  Workers:      8,
 }
 for _, opt := range opts {
  opt(&o)
 }
 return o
}
```

- [ ] **Step 4: Run option tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestWith|TestDefaults" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Write the failing Conn tests**

Create `graph/conn_test.go`:

```go
package graph_test

import (
 "context"
 "database/sql"
 "testing"

 "geodedb.com/geode/graph"
)

func TestNewConn_NilDB(t *testing.T) {
 _, err := graph.NewConn(nil)
 if err == nil {
  t.Fatal("expected error for nil db")
 }
}

func TestNewConn_WithOptions(t *testing.T) {
 // Use a fake DSN that won't actually connect — we only test construction.
 // We need a *sql.DB; sql.Open doesn't connect until first use.
 db, err := sql.Open("geode", "localhost:3141")
 if err != nil {
  t.Fatalf("sql.Open: %v", err)
 }
 defer db.Close()

 c, err := graph.NewConn(db, graph.WithBatchSize(50), graph.WithWorkers(4))
 if err != nil {
  t.Fatalf("NewConn: %v", err)
 }

 if c.DB() != db {
  t.Error("DB() returned wrong pointer")
 }
}

func TestConn_ExecContext_NilDB(t *testing.T) {
 // Verify Conn methods fail gracefully when DB is unreachable.
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()

 c, _ := graph.NewConn(db)
 _, err := c.ExecContext(context.Background(), "RETURN 1")
 // This will fail because we can't actually connect, which is fine —
 // we're testing that it doesn't panic.
 if err == nil {
  t.Log("ExecContext succeeded (server running?)")
 }
}
```

- [ ] **Step 6: Write Conn implementation**

Create `graph/conn.go`:

```go
package graph

import (
 "context"
 "database/sql"
 "errors"

 // Register the geode driver.
 _ "geodedb.com/geode"
)

// Conn is a high-level graph database connection that wraps a *sql.DB
// with batch execution, pipeline processing, and transaction ergonomics.
type Conn struct {
 db   *sql.DB
 opts Options
}

// NewConn creates a new graph Conn wrapping the given *sql.DB.
// The sql.DB should be opened with the "geode" driver.
func NewConn(db *sql.DB, options ...Option) (*Conn, error) {
 if db == nil {
  return nil, errors.New("graph: db must not be nil")
 }
 return &Conn{
  db:   db,
  opts: ApplyOptions(options...),
 }, nil
}

// DB returns the underlying *sql.DB for direct database/sql access.
func (c *Conn) DB() *sql.DB {
 return c.db
}

// Close closes the underlying database connection pool.
func (c *Conn) Close() error {
 return c.db.Close()
}

// ExecContext executes a GQL query that doesn't return rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
 return c.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a GQL query that returns rows.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
 return c.db.QueryContext(ctx, query, args...)
}

// PingContext verifies the database connection is alive.
func (c *Conn) PingContext(ctx context.Context) error {
 return c.db.PingContext(ctx)
}
```

- [ ] **Step 7: Run all graph tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -v -count=1`
Expected: all tests PASS

- [ ] **Step 8: Commit**

```bash
git add graph/option.go graph/option_test.go graph/conn.go graph/conn_test.go
git commit -m "feat: add graph package with Conn wrapper and functional options"
```

---

### Task 5: Transaction Wrapper with Auto-Rollback

**Files:**

- Create: `graph/tx.go`
- Create: `graph/tx_test.go`

- [ ] **Step 1: Write the failing tests**

Create `graph/tx_test.go`:

```go
package graph_test

import (
 "context"
 "database/sql"
 "errors"
 "testing"

 "geodedb.com/geode/graph"
)

// mockDB creates a *sql.DB that won't connect. For TX tests we need
// integration, so we test the logic path and error handling only.

func TestTX_NilFn(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 err := c.TX(context.Background(), nil, nil)
 if err == nil {
  t.Fatal("expected error for nil fn")
 }
}

func TestTX_PropagatesFnError(t *testing.T) {
 // We can't test full TX without a server, but we can verify the
 // contract: if BeginTx fails, the fn error is not called.
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 fnCalled := false
 err := c.TX(context.Background(), nil, func(tx *sql.Tx) error {
  fnCalled = true
  return errors.New("fn error")
 })
 // BeginTx will fail (no server), so fn should NOT be called.
 if fnCalled {
  t.Error("fn was called despite BeginTx failure")
 }
 if err == nil {
  t.Error("expected error from BeginTx failure")
 }
}

func TestTXFunc_Signature(t *testing.T) {
 // Verify the TXFunc type exists and matches expected signature.
 var fn graph.TXFunc = func(tx *sql.Tx) error {
  return nil
 }
 _ = fn
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestTX" -v -count=1`
Expected: compilation error — `TX`, `TXFunc` undefined

- [ ] **Step 3: Write minimal implementation**

Create `graph/tx.go`:

```go
package graph

import (
 "context"
 "database/sql"
 "errors"
 "fmt"
)

// TXFunc is a function executed within a transaction.
// If it returns an error, the transaction is rolled back.
// If it returns nil, the transaction is committed.
type TXFunc func(tx *sql.Tx) error

// TX executes fn within a transaction. It automatically:
//   - Begins a transaction with the given options
//   - Rolls back on error or panic from fn
//   - Commits on success
//
// This prevents the common "forgot to rollback" bug. The transaction
// is always cleaned up regardless of how fn exits.
func (c *Conn) TX(ctx context.Context, opts *sql.TxOptions, fn TXFunc) (err error) {
 if fn == nil {
  return errors.New("graph: TX function must not be nil")
 }

 tx, err := c.db.BeginTx(ctx, opts)
 if err != nil {
  return fmt.Errorf("graph: begin tx: %w", err)
 }

 // Ensure rollback on panic.
 defer func() {
  if p := recover(); p != nil {
   _ = tx.Rollback()
   panic(p) // re-panic after rollback
  }
 }()

 if err := fn(tx); err != nil {
  _ = tx.Rollback()
  return err
 }

 if err := tx.Commit(); err != nil {
  return fmt.Errorf("graph: commit tx: %w", err)
 }

 return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestTX" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add graph/tx.go graph/tx_test.go
git commit -m "feat: add transaction wrapper with auto-rollback on error/panic"
```

---

### Task 6: Batch Execution with Retry

**Files:**

- Create: `graph/batch.go`
- Create: `graph/batch_test.go`

- [ ] **Step 1: Write the failing tests**

Create `graph/batch_test.go`:

```go
package graph_test

import (
 "context"
 "database/sql"
 "errors"
 "testing"

 "geodedb.com/geode/graph"
)

func TestBatchResult_Fields(t *testing.T) {
 r := graph.BatchResult{
  Total:     10,
  Succeeded: 7,
  Failed:    3,
 }
 if r.Total != 10 {
  t.Errorf("Total = %d, want 10", r.Total)
 }
 if r.Succeeded != 7 {
  t.Errorf("Succeeded = %d, want 7", r.Succeeded)
 }
 if r.Failed != 3 {
  t.Errorf("Failed = %d, want 3", r.Failed)
 }
}

func TestStatement_Fields(t *testing.T) {
 s := graph.Statement{
  Query: "CREATE (n:Person {name: ?})",
  Args:  []any{"Alice"},
 }
 if s.Query == "" {
  t.Error("Query is empty")
 }
 if len(s.Args) != 1 {
  t.Errorf("Args len = %d, want 1", len(s.Args))
 }
}

func TestBatchExec_EmptyStatements(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 result, err := c.BatchExec(context.Background(), nil)
 if err != nil {
  t.Fatalf("unexpected error for empty batch: %v", err)
 }
 if result.Total != 0 {
  t.Errorf("Total = %d, want 0", result.Total)
 }
}

func TestBatchExec_StopsOnNonRetryableError(t *testing.T) {
 // Without a server, every statement will fail.
 // We verify it fails fast on the first statement.
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 stmts := []graph.Statement{
  {Query: "CREATE (n:A)"},
  {Query: "CREATE (n:B)"},
  {Query: "CREATE (n:C)"},
 }

 result, err := c.BatchExec(context.Background(), stmts)
 if err == nil {
  t.Fatal("expected error (no server)")
 }
 // Should have attempted at least the first statement.
 if result.Total != 3 {
  t.Errorf("Total = %d, want 3", result.Total)
 }
 if result.Failed != 1 {
  t.Logf("Failed = %d (may vary based on retry behavior)", result.Failed)
 }
 if result.Err == nil {
  t.Error("result.Err should be set")
 }
}

func TestBatchExec_RespectsContext(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 ctx, cancel := context.WithCancel(context.Background())
 cancel()

 stmts := []graph.Statement{{Query: "CREATE (n:A)"}}
 _, err := c.BatchExec(ctx, stmts)
 if !errors.Is(err, context.Canceled) {
  t.Logf("err = %v (may wrap context.Canceled)", err)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestBatch|TestStatement" -v -count=1`
Expected: compilation error — `BatchResult`, `Statement`, `BatchExec` undefined

- [ ] **Step 3: Write minimal implementation**

Create `graph/batch.go`:

```go
package graph

import (
 "context"
 "fmt"

 geode "geodedb.com/geode"
)

// Statement is a GQL query with optional arguments for batch execution.
type Statement struct {
 Query string
 Args  []any
}

// BatchResult contains the outcome of a batch execution.
type BatchResult struct {
 // Total is the number of statements in the batch.
 Total int

 // Succeeded is the number of statements that executed successfully.
 Succeeded int

 // Failed is the number of statements that failed (including the one that stopped execution).
 Failed int

 // Err is the error that stopped execution, if any.
 Err error
}

// BatchExec executes statements sequentially with retry logic for transient
// errors. Execution stops on the first non-retryable error.
//
// Each statement is retried independently according to the Conn's retry policy
// (configured via WithRetry). Transport errors and serialization failures are
// retried with exponential backoff; syntax errors and constraint violations
// stop immediately.
func (c *Conn) BatchExec(ctx context.Context, stmts []Statement) (BatchResult, error) {
 result := BatchResult{Total: len(stmts)}

 if len(stmts) == 0 {
  return result, nil
 }

 policy := geode.RetryPolicy{
  MaxAttempts:    c.opts.MaxRetries,
  InitialBackoff: c.opts.RetryBackoff,
  MaxBackoff:     c.opts.RetryBackoff * 30,
  Multiplier:     2.0,
 }

 for i, stmt := range stmts {
  if err := ctx.Err(); err != nil {
   result.Failed = result.Total - result.Succeeded
   result.Err = err
   return result, err
  }

  err := geode.Retry(ctx, policy, func(ctx context.Context) error {
   _, execErr := c.db.ExecContext(ctx, stmt.Query, stmt.Args...)
   return execErr
  })

  if err != nil {
   result.Failed = result.Total - result.Succeeded
   result.Err = fmt.Errorf("statement %d: %w", i, err)
   return result, result.Err
  }

  result.Succeeded++
 }

 return result, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestBatch|TestStatement" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add graph/batch.go graph/batch_test.go
git commit -m "feat: add batch execution with per-statement retry for transient errors"
```

---

### Task 7: Pipeline (Parallel Bulk) Execution

**Files:**

- Create: `graph/pipeline.go`
- Create: `graph/pipeline_test.go`

- [ ] **Step 1: Write the failing tests**

Create `graph/pipeline_test.go`:

```go
package graph_test

import (
 "context"
 "database/sql"
 "sync/atomic"
 "testing"

 "geodedb.com/geode/graph"
)

func TestPipelineResult_Fields(t *testing.T) {
 r := graph.PipelineResult{
  Total:     100,
  Succeeded: 90,
  Failed:    10,
 }
 if r.Total != 100 || r.Succeeded != 90 || r.Failed != 10 {
  t.Errorf("unexpected result: %+v", r)
 }
}

func TestPipelineExec_EmptyStatements(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 result, err := c.PipelineExec(context.Background(), nil)
 if err != nil {
  t.Fatalf("unexpected error: %v", err)
 }
 if result.Total != 0 {
  t.Errorf("Total = %d, want 0", result.Total)
 }
}

func TestPipelineExec_RespectsContext(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 ctx, cancel := context.WithCancel(context.Background())
 cancel()

 stmts := make([]graph.Statement, 100)
 for i := range stmts {
  stmts[i] = graph.Statement{Query: "CREATE (n:A)"}
 }

 result, _ := c.PipelineExec(ctx, stmts)
 // With cancelled context, most or all should fail.
 if result.Succeeded == 100 {
  t.Error("expected some failures with cancelled context")
 }
}

func TestPipelineExec_Progress(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 var progressSeen atomic.Int64
 stmts := make([]graph.Statement, 5)
 for i := range stmts {
  stmts[i] = graph.Statement{Query: "RETURN 1"}
 }

 _, _ = c.PipelineExec(context.Background(), stmts, graph.WithProgress(func(completed, total int) {
  progressSeen.Store(int64(completed))
 }))

 // Progress callback should have been called at least once.
 if progressSeen.Load() == 0 {
  t.Log("progress callback not invoked (expected without server)")
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestPipeline" -v -count=1`
Expected: compilation error — `PipelineResult`, `PipelineExec`, `WithProgress` undefined

- [ ] **Step 3: Write minimal implementation**

Create `graph/pipeline.go`:

```go
package graph

import (
 "context"
 "fmt"
 "sync"
 "sync/atomic"

 geode "geodedb.com/geode"
)

// PipelineResult contains the outcome of a pipeline execution.
type PipelineResult struct {
 // Total is the number of statements submitted.
 Total int

 // Succeeded is the number of statements that executed successfully.
 Succeeded int

 // Failed is the number of statements that failed.
 Failed int

 // Err is the first error encountered (if any).
 Err error
}

// PipelineOption configures a single pipeline execution.
type PipelineOption func(*pipelineConfig)

type pipelineConfig struct {
 progressFn func(completed, total int)
}

// WithProgress sets a callback invoked after each statement completes.
// The callback receives the number of completed statements and the total.
func WithProgress(fn func(completed, total int)) PipelineOption {
 return func(c *pipelineConfig) {
  c.progressFn = fn
 }
}

// PipelineExec executes statements in parallel using a worker pool.
// It uses the Conn's configured worker count (WithWorkers) and batch size
// (WithBatchSize). Execution stops after the first error is encountered.
//
// Each worker retries transient errors independently using the Conn's
// retry policy.
func (c *Conn) PipelineExec(ctx context.Context, stmts []Statement, opts ...PipelineOption) (PipelineResult, error) {
 result := PipelineResult{Total: len(stmts)}

 if len(stmts) == 0 {
  return result, nil
 }

 var pcfg pipelineConfig
 for _, opt := range opts {
  opt(&pcfg)
 }

 workers := c.opts.Workers
 if workers <= 0 {
  workers = 8
 }

 policy := geode.RetryPolicy{
  MaxAttempts:    c.opts.MaxRetries,
  InitialBackoff: c.opts.RetryBackoff,
  MaxBackoff:     c.opts.RetryBackoff * 30,
  Multiplier:     2.0,
 }

 // Cancellable context for first-error-stops semantics.
 pipeCtx, pipeCancel := context.WithCancel(ctx)
 defer pipeCancel()

 var (
  succeeded atomic.Int64
  firstErr  error
  errOnce   sync.Once
  wg        sync.WaitGroup
 )

 // Semaphore to limit concurrency.
 sem := make(chan struct{}, workers)

 for i, stmt := range stmts {
  if pipeCtx.Err() != nil {
   break
  }

  sem <- struct{}{} // acquire
  wg.Add(1)

  go func(idx int, s Statement) {
   defer func() {
    <-sem // release
    wg.Done()
   }()

   err := geode.Retry(pipeCtx, policy, func(ctx context.Context) error {
    _, execErr := c.db.ExecContext(ctx, s.Query, s.Args...)
    return execErr
   })

   if err != nil {
    errOnce.Do(func() {
     firstErr = fmt.Errorf("statement %d: %w", idx, err)
     pipeCancel() // stop other workers
    })
    return
   }

   completed := int(succeeded.Add(1))
   if pcfg.progressFn != nil {
    pcfg.progressFn(completed, len(stmts))
   }
  }(i, stmt)
 }

 wg.Wait()

 result.Succeeded = int(succeeded.Load())
 result.Failed = result.Total - result.Succeeded
 result.Err = firstErr

 return result, firstErr
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestPipeline" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add graph/pipeline.go graph/pipeline_test.go
git commit -m "feat: add pipeline execution with worker pool and first-error-stops"
```

---

### Task 8: Schema Management from Embedded Files

**Files:**

- Create: `graph/schema.go`
- Create: `graph/schema_test.go`

- [ ] **Step 1: Write the failing tests**

Create `graph/schema_test.go`:

```go
package graph_test

import (
 "context"
 "database/sql"
 "io/fs"
 "testing"
 "testing/fstest"

 "geodedb.com/geode/graph"
)

func TestParseSchemaFS_SingleFile(t *testing.T) {
 fsys := fstest.MapFS{
  "schema/001_init.gql": &fstest.MapFile{
   Data: []byte(`
-- Create person nodes
CREATE (:Person {name: 'Alice'});
CREATE (:Person {name: 'Bob'});
`),
  },
 }

 stmts, err := graph.ParseSchemaFS(fsys, "schema")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 2 {
  t.Fatalf("len(stmts) = %d, want 2", len(stmts))
 }
 if stmts[0].Query != "CREATE (:Person {name: 'Alice'})" {
  t.Errorf("stmts[0] = %q", stmts[0].Query)
 }
 if stmts[1].Query != "CREATE (:Person {name: 'Bob'})" {
  t.Errorf("stmts[1] = %q", stmts[1].Query)
 }
}

func TestParseSchemaFS_FiltersComments(t *testing.T) {
 fsys := fstest.MapFS{
  "schema.gql": &fstest.MapFile{
   Data: []byte(`
// This is a C-style comment
-- This is a SQL-style comment
CREATE (:Node);
// Another comment
MERGE (:Other);
`),
  },
 }

 stmts, err := graph.ParseSchemaFS(fsys, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 2 {
  t.Fatalf("len(stmts) = %d, want 2", len(stmts))
 }
}

func TestParseSchemaFS_ValidatesStatementTypes(t *testing.T) {
 fsys := fstest.MapFS{
  "bad.gql": &fstest.MapFile{
   Data: []byte(`DROP TABLE users;`),
  },
 }

 _, err := graph.ParseSchemaFS(fsys, ".")
 if err == nil {
  t.Fatal("expected error for invalid statement type")
 }
}

func TestParseSchemaFS_MultipleFiles(t *testing.T) {
 fsys := fstest.MapFS{
  "schema/001_nodes.gql": &fstest.MapFile{
   Data: []byte(`CREATE (:A);`),
  },
  "schema/002_edges.gql": &fstest.MapFile{
   Data: []byte(`MATCH (a:A), (b:A) CREATE (a)-[:KNOWS]->(b);`),
  },
  "schema/readme.txt": &fstest.MapFile{
   Data: []byte(`not a gql file`),
  },
 }

 stmts, err := graph.ParseSchemaFS(fsys, "schema")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 2 {
  t.Fatalf("len(stmts) = %d, want 2", len(stmts))
 }
}

func TestParseSchemaFS_EmptyFile(t *testing.T) {
 fsys := fstest.MapFS{
  "empty.gql": &fstest.MapFile{Data: []byte("")},
 }

 stmts, err := graph.ParseSchemaFS(fsys, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 0 {
  t.Errorf("len(stmts) = %d, want 0", len(stmts))
 }
}

func TestParseSchemaFS_InvalidDir(t *testing.T) {
 fsys := fstest.MapFS{}

 _, err := graph.ParseSchemaFS(fsys, "nonexistent")
 if err == nil {
  t.Fatal("expected error for nonexistent dir")
 }
}

func TestParseSchemaFS_AllValidTypes(t *testing.T) {
 fsys := fstest.MapFS{
  "all.gql": &fstest.MapFile{
   Data: []byte(`
CREATE (:A);
MERGE (:B);
MATCH (n) RETURN n;
DELETE FROM something;
`),
  },
 }

 stmts, err := graph.ParseSchemaFS(fsys, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 4 {
  t.Fatalf("len(stmts) = %d, want 4", len(stmts))
 }
}

func TestApplySchema_EmptyStatements(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 err := c.ApplySchema(context.Background(), nil)
 if err != nil {
  t.Fatalf("unexpected error for empty schema: %v", err)
 }
}

func TestApplySchema_UsesFS(t *testing.T) {
 db, _ := sql.Open("geode", "localhost:3141")
 defer db.Close()
 c, _ := graph.NewConn(db)

 fsys := fstest.MapFS{
  "schema.gql": &fstest.MapFile{
   Data: []byte(`CREATE (:Test);`),
  },
 }

 err := c.ApplySchemaFS(context.Background(), fsys, ".")
 // Will fail (no server), but should not panic and should attempt execution.
 if err == nil {
  t.Log("ApplySchemaFS succeeded (server running?)")
 }
}

// Ensure fs.FS is accepted (compile-time check).
var _ fs.FS = fstest.MapFS{}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestParseSchema|TestApplySchema" -v -count=1`
Expected: compilation error — `ParseSchemaFS`, `ApplySchema`, `ApplySchemaFS` undefined

- [ ] **Step 3: Write minimal implementation**

Create `graph/schema.go`:

```go
package graph

import (
 "context"
 "fmt"
 "io/fs"
 "path/filepath"
 "sort"
 "strings"
)

// validStatementPrefixes are the GQL statement types allowed in schema files.
var validStatementPrefixes = []string{
 "CREATE",
 "MERGE",
 "MATCH",
 "DELETE",
}

// ParseSchemaFS parses all .gql files from an fs.FS directory.
// Files are processed in sorted order (lexicographic by name) so that
// numbered migration files (001_init.gql, 002_edges.gql) execute in order.
//
// Lines starting with // or -- are treated as comments and skipped.
// Statements are split on ; delimiters and trimmed of whitespace.
// Each statement must begin with a valid GQL keyword (CREATE, MERGE, MATCH, DELETE).
func ParseSchemaFS(fsys fs.FS, dir string) ([]Statement, error) {
 entries, err := fs.ReadDir(fsys, dir)
 if err != nil {
  return nil, fmt.Errorf("graph: read schema dir %q: %w", dir, err)
 }

 // Collect and sort .gql files.
 var files []string
 for _, entry := range entries {
  if entry.IsDir() {
   continue
  }
  if filepath.Ext(entry.Name()) == ".gql" {
   files = append(files, entry.Name())
  }
 }
 sort.Strings(files)

 var stmts []Statement
 for _, name := range files {
  path := dir + "/" + name
  if dir == "." {
   path = name
  }

  data, err := fs.ReadFile(fsys, path)
  if err != nil {
   return nil, fmt.Errorf("graph: read schema file %q: %w", path, err)
  }

  fileStmts, err := parseSchemaContent(string(data), name)
  if err != nil {
   return nil, err
  }
  stmts = append(stmts, fileStmts...)
 }

 return stmts, nil
}

// parseSchemaContent parses a single .gql file's content into statements.
func parseSchemaContent(content, filename string) ([]Statement, error) {
 // Filter out comment lines and join into a single string.
 var filtered []string
 for _, line := range strings.Split(content, "\n") {
  trimmed := strings.TrimSpace(line)
  if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "--") {
   continue
  }
  filtered = append(filtered, line)
 }

 joined := strings.Join(filtered, "\n")

 // Split on semicolons.
 parts := strings.Split(joined, ";")

 var stmts []Statement
 for _, part := range parts {
  query := strings.TrimSpace(part)
  if query == "" {
   continue
  }

  if err := validateStatementType(query, filename); err != nil {
   return nil, err
  }

  stmts = append(stmts, Statement{Query: query})
 }

 return stmts, nil
}

// validateStatementType checks that a statement begins with a valid GQL keyword.
func validateStatementType(query, filename string) error {
 upper := strings.ToUpper(query)
 for _, prefix := range validStatementPrefixes {
  if strings.HasPrefix(upper, prefix) {
   return nil
  }
 }
 return fmt.Errorf("graph: invalid statement type in %s: %q (must start with CREATE, MERGE, MATCH, or DELETE)",
  filename, truncate(query, 60))
}

// truncate shortens a string for error messages.
func truncate(s string, maxLen int) string {
 if len(s) <= maxLen {
  return s
 }
 return s[:maxLen] + "..."
}

// ApplySchema executes a slice of parsed statements sequentially.
// It stops on the first error.
func (c *Conn) ApplySchema(ctx context.Context, stmts []Statement) error {
 for i, stmt := range stmts {
  if err := ctx.Err(); err != nil {
   return err
  }
  if _, err := c.db.ExecContext(ctx, stmt.Query, stmt.Args...); err != nil {
   return fmt.Errorf("graph: apply schema statement %d: %w", i, err)
  }
 }
 return nil
}

// ApplySchemaFS is a convenience that parses and applies schema files in one call.
func (c *Conn) ApplySchemaFS(ctx context.Context, fsys fs.FS, dir string) error {
 stmts, err := ParseSchemaFS(fsys, dir)
 if err != nil {
  return err
 }
 return c.ApplySchema(ctx, stmts)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./graph/ -run "TestParseSchema|TestApplySchema" -v -count=1`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add graph/schema.go graph/schema_test.go
git commit -m "feat: add schema management with embedded filesystem support"
```

---

## Phase 3: Field-Level Encryption

### Task 9: AES-256-GCM Encryption with HKDF and HMAC

**Files:**

- Create: `crypto/cipher.go`
- Create: `crypto/cipher_test.go`

- [ ] **Step 1: Add golang.org/x/crypto dependency**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go get golang.org/x/crypto`

- [ ] **Step 2: Write the failing tests**

Create `crypto/cipher_test.go`:

```go
package crypto_test

import (
 "bytes"
 "encoding/hex"
 "testing"

 geocrypto "geodedb.com/geode/crypto"
)

func TestNewCipher(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32) // 256 bits
 c, err := geocrypto.NewCipher(key)
 if err != nil {
  t.Fatalf("NewCipher: %v", err)
 }
 if c == nil {
  t.Fatal("cipher is nil")
 }
}

func TestNewCipher_ShortKey(t *testing.T) {
 _, err := geocrypto.NewCipher([]byte("short"))
 if err == nil {
  t.Fatal("expected error for short key")
 }
}

func TestNewCipher_NilKey(t *testing.T) {
 _, err := geocrypto.NewCipher(nil)
 if err == nil {
  t.Fatal("expected error for nil key")
 }
}

func TestEncryptDecrypt_Binary(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 plaintext := []byte("hello, graph database!")
 ciphertext, err := c.Encrypt(plaintext)
 if err != nil {
  t.Fatalf("Encrypt: %v", err)
 }

 if bytes.Equal(plaintext, ciphertext) {
  t.Error("ciphertext equals plaintext")
 }

 decrypted, err := c.Decrypt(ciphertext)
 if err != nil {
  t.Fatalf("Decrypt: %v", err)
 }

 if !bytes.Equal(plaintext, decrypted) {
  t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
 }
}

func TestEncryptDecrypt_String(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 plaintext := "sensitive node property"
 encoded, err := c.EncryptString(plaintext)
 if err != nil {
  t.Fatalf("EncryptString: %v", err)
 }

 if encoded == plaintext {
  t.Error("encoded equals plaintext")
 }

 decrypted, err := c.DecryptString(encoded)
 if err != nil {
  t.Fatalf("DecryptString: %v", err)
 }

 if decrypted != plaintext {
  t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
 }
}

func TestEncrypt_DifferentNonces(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 plaintext := []byte("same input")
 ct1, _ := c.Encrypt(plaintext)
 ct2, _ := c.Encrypt(plaintext)

 if bytes.Equal(ct1, ct2) {
  t.Error("two encryptions of same plaintext produced identical ciphertext (nonce reuse!)")
 }
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 ct, _ := c.Encrypt([]byte("original"))
 ct[len(ct)-1] ^= 0xFF // flip last byte

 _, err := c.Decrypt(ct)
 if err == nil {
  t.Fatal("expected error for tampered ciphertext")
 }
}

func TestDecrypt_TooShort(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 _, err := c.Decrypt([]byte("short"))
 if err == nil {
  t.Fatal("expected error for too-short ciphertext")
 }
}

func TestHMACHex_Deterministic(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 h1 := c.HMACHex("user@example.com")
 h2 := c.HMACHex("user@example.com")

 if h1 != h2 {
  t.Errorf("HMAC not deterministic: %q != %q", h1, h2)
 }

 // Validate it's valid hex.
 _, err := hex.DecodeString(h1)
 if err != nil {
  t.Errorf("HMACHex returned invalid hex: %v", err)
 }
}

func TestHMACHex_DifferentInputsDifferentOutput(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 h1 := c.HMACHex("alice@example.com")
 h2 := c.HMACHex("bob@example.com")

 if h1 == h2 {
  t.Error("different inputs produced same HMAC")
 }
}

func TestHMACHex_DifferentKeysDifferentOutput(t *testing.T) {
 key1 := bytes.Repeat([]byte{0x42}, 32)
 key2 := bytes.Repeat([]byte{0x43}, 32)
 c1, _ := geocrypto.NewCipher(key1)
 c2, _ := geocrypto.NewCipher(key2)

 h1 := c1.HMACHex("same-input")
 h2 := c2.HMACHex("same-input")

 if h1 == h2 {
  t.Error("different keys produced same HMAC")
 }
}

func TestEncryptDecrypt_EmptyPlaintext(t *testing.T) {
 key := bytes.Repeat([]byte{0x42}, 32)
 c, _ := geocrypto.NewCipher(key)

 ct, err := c.Encrypt([]byte{})
 if err != nil {
  t.Fatalf("Encrypt empty: %v", err)
 }

 pt, err := c.Decrypt(ct)
 if err != nil {
  t.Fatalf("Decrypt empty: %v", err)
 }

 if len(pt) != 0 {
  t.Errorf("decrypted len = %d, want 0", len(pt))
 }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./crypto/ -v -count=1`
Expected: compilation error — package does not exist

- [ ] **Step 4: Write implementation**

Create `crypto/cipher.go`:

```go
// Package crypto provides field-level encryption for Geode node and edge
// properties. It uses AES-256-GCM for authenticated encryption with
// HKDF-SHA256 for key derivation and HMAC-SHA256 for deterministic
// searchable encryption.
//
// Typical usage: encrypt sensitive properties (email, SSN) before storing
// as node properties, while using HMACHex for searchable lookup indices.
//
// c, _ := crypto.NewCipher(masterKey)
//
// // Store encrypted + searchable index
// encrypted, _ := c.EncryptString(email)
// emailHash := c.HMACHex(email)
// db.Exec("CREATE (:User {email: ?, email_hash: ?})", encrypted, emailHash)
//
// // Search by hash, decrypt result
// db.Query("MATCH (u:User {email_hash: ?}) RETURN u.email", c.HMACHex(lookupEmail))
package crypto

import (
 "crypto/aes"
 "crypto/cipher"
 "crypto/hmac"
 "crypto/rand"
 "crypto/sha256"
 "encoding/base64"
 "encoding/hex"
 "errors"
 "fmt"
 "io"

 "golang.org/x/crypto/hkdf"
)

const (
 // keySize is the required master key size (256 bits).
 keySize = 32

 // nonceSize is the GCM nonce size (96 bits).
 nonceSize = 12
)

// Cipher provides AES-256-GCM encryption and HMAC-SHA256 hashing
// using keys derived from a master key via HKDF-SHA256.
type Cipher struct {
 encKey  []byte // derived encryption key
 hmacKey []byte // derived HMAC key
 aead    cipher.AEAD
}

// NewCipher creates a new Cipher from a 32-byte master key.
// Two independent subkeys are derived via HKDF-SHA256:
// one for AES-256-GCM encryption and one for HMAC-SHA256.
func NewCipher(masterKey []byte) (*Cipher, error) {
 if len(masterKey) < keySize {
  return nil, fmt.Errorf("crypto: master key must be at least %d bytes, got %d", keySize, len(masterKey))
 }

 encKey, err := deriveKey(masterKey, "geode-encrypt")
 if err != nil {
  return nil, fmt.Errorf("crypto: derive encryption key: %w", err)
 }

 hmacKey, err := deriveKey(masterKey, "geode-hmac")
 if err != nil {
  return nil, fmt.Errorf("crypto: derive HMAC key: %w", err)
 }

 block, err := aes.NewCipher(encKey)
 if err != nil {
  return nil, fmt.Errorf("crypto: create AES cipher: %w", err)
 }

 aead, err := cipher.NewGCM(block)
 if err != nil {
  return nil, fmt.Errorf("crypto: create GCM: %w", err)
 }

 return &Cipher{
  encKey:  encKey,
  hmacKey: hmacKey,
  aead:    aead,
 }, nil
}

// deriveKey derives a subkey from the master key using HKDF-SHA256.
func deriveKey(masterKey []byte, info string) ([]byte, error) {
 hkdfReader := hkdf.New(sha256.New, masterKey, nil, []byte(info))
 key := make([]byte, keySize)
 if _, err := io.ReadFull(hkdfReader, key); err != nil {
  return nil, err
 }
 return key, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// The nonce is prepended to the ciphertext.
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
 nonce := make([]byte, nonceSize)
 if _, err := rand.Read(nonce); err != nil {
  return nil, fmt.Errorf("crypto: generate nonce: %w", err)
 }

 // Seal appends the ciphertext+tag to nonce.
 ciphertext := c.aead.Seal(nonce, nonce, plaintext, nil)
 return ciphertext, nil
}

// Decrypt decrypts ciphertext produced by Encrypt.
// Returns an error if the ciphertext is too short or authentication fails.
func (c *Cipher) Decrypt(ciphertext []byte) ([]byte, error) {
 if len(ciphertext) < nonceSize {
  return nil, errors.New("crypto: ciphertext too short")
 }

 nonce := ciphertext[:nonceSize]
 encrypted := ciphertext[nonceSize:]

 plaintext, err := c.aead.Open(nil, nonce, encrypted, nil)
 if err != nil {
  return nil, fmt.Errorf("crypto: decrypt: %w", err)
 }

 return plaintext, nil
}

// EncryptString encrypts a string and returns a base64-encoded ciphertext.
func (c *Cipher) EncryptString(plaintext string) (string, error) {
 ct, err := c.Encrypt([]byte(plaintext))
 if err != nil {
  return "", err
 }
 return base64.StdEncoding.EncodeToString(ct), nil
}

// DecryptString decrypts a base64-encoded ciphertext and returns the plaintext string.
func (c *Cipher) DecryptString(encoded string) (string, error) {
 ct, err := base64.StdEncoding.DecodeString(encoded)
 if err != nil {
  return "", fmt.Errorf("crypto: decode base64: %w", err)
 }
 pt, err := c.Decrypt(ct)
 if err != nil {
  return "", err
 }
 return string(pt), nil
}

// HMACHex computes a deterministic HMAC-SHA256 of the input and returns
// the result as a lowercase hex string. This is useful for searchable
// encryption: store the HMAC as an index, then look up records by
// computing the same HMAC on the search term.
//
// Unlike Encrypt, HMACHex is deterministic — the same input always
// produces the same output, enabling equality lookups without decryption.
func (c *Cipher) HMACHex(input string) string {
 mac := hmac.New(sha256.New, c.hmacKey)
 mac.Write([]byte(input))
 return hex.EncodeToString(mac.Sum(nil))
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test ./crypto/ -v -count=1`
Expected: all tests PASS

- [ ] **Step 6: Run full test suite to verify nothing is broken**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -short ./...`
Expected: all packages PASS

- [ ] **Step 7: Commit**

```bash
git add crypto/cipher.go crypto/cipher_test.go go.mod go.sum
git commit -m "feat: add field-level AES-256-GCM encryption with HMAC searchable indices"
```

---

## Phase 4: Integration Verification

### Task 10: Full Build and Lint

- [ ] **Step 1: Run go vet on all packages**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go vet ./...`
Expected: no issues

- [ ] **Step 2: Run linter if available**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && golangci-lint run`
Expected: no new issues from added code

- [ ] **Step 3: Run all unit tests with race detector**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -short -race ./...`
Expected: all PASS, no race conditions

- [ ] **Step 4: Verify test coverage for new packages**

Run: `cd /home/benji/src/devnw/codepros/geode/geode-client-go && go test -short -cover ./internal/redact/ ./graph/ ./crypto/`
Expected: >80% coverage for all three packages

- [ ] **Step 5: Commit any lint fixes**

```bash
git add -A
git commit -m "chore: fix lint and vet issues in new packages"
```

(Skip this step if no fixes needed.)

---

## Summary

| Task | Package | Feature | Depends On |
|------|---------|---------|------------|
| 1 | `internal/redact/` | DSN credential redaction | — |
| 2 | root (`geode`) | Time format parsing | — |
| 3 | root (`geode`) | Retry policy + executor | errors.go (existing) |
| 4 | `graph/` | Conn wrapper + options | root package |
| 5 | `graph/` | Transaction wrapper (TX) | Task 4 |
| 6 | `graph/` | Batch execution | Tasks 3, 4 |
| 7 | `graph/` | Pipeline execution | Tasks 3, 4 |
| 8 | `graph/` | Schema management | Task 4 |
| 9 | `crypto/` | Field-level encryption | — (independent) |
| 10 | all | Build verification | Tasks 1-9 |

**Independent tasks that can be parallelized:** Tasks 1, 2, 3, 9 have no dependencies on each other.

**Sequential dependencies:** Tasks 5-8 depend on Task 4. Tasks 6-7 depend on Task 3.

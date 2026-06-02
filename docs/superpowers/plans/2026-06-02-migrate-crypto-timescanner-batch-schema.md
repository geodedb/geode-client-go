# Migrate crypto, TimeScanner, BatchExec, PipelineExec, ParseSchemaFS from devnw.dev/geode

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port five higher-level features from `devnw.dev/geode` (the old wrapper library) into `geodedb.com/geode` (this driver), with full unit tests, benchmarks, and fuzz coverage.

**Architecture:** Each feature is a clean addition — no existing code changes except `time.go` (add `TimeScanner`/`ScanTime`) and `pool.go` (receives no changes; new `batch.go` adds methods on `Pool`). The `crypto/` sub-package is self-contained under `geodedb.com/geode/crypto`. `schema.go` and `batch.go` are new root-package files. All existing tests continue to pass unchanged.

**Tech Stack:** Go 1.25+, `database/sql`, `database/sql/driver`, `golang.org/x/crypto/hkdf` (already in go.sum), `testing/embed`, standard library only.

---

## Files Overview

| File | Action | Responsibility |
|---|---|---|
| `crypto/cipher.go` | **Create** | AES-256-GCM field-level encryption with HKDF-SHA256 key derivation |
| `crypto/cipher_test.go` | **Create** | Unit tests for Cipher: roundtrip, error cases, HMAC determinism |
| `crypto/cipher_bench_test.go` | **Create** | Benchmarks for Encrypt, Decrypt, EncryptString, HMACHex at multiple payload sizes |
| `crypto/cipher_fuzz_test.go` | **Create** | Fuzz: roundtrip property + arbitrary-bytes Decrypt never panics |
| `time.go` | **Modify** | Add `TimeScanner` struct + `ScanTime()` constructor beside existing `ParseTime` |
| `time_test.go` | **Modify** | Add `TestScanTime_*` table-driven tests |
| `batch_mock_test.go` | **Create** | In-package mock `database/sql/driver` for white-box batch/pipeline unit tests |
| `batch.go` | **Create** | `Statement` type, `DefaultChunkSize`, `DefaultMaxWorkers`, `Pool.BatchExec`, `Pool.PipelineExec` |
| `batch_test.go` | **Create** | Unit tests for BatchExec and PipelineExec using the mock driver |
| `batch_bench_test.go` | **Create** | Benchmarks for BatchExec and PipelineExec (no live server; use mock) |
| `schema.go` | **Create** | `ParseSchemaFS(fs embed.FS, dir string) ([]Statement, error)` |
| `schema_test.go` | **Create** | Table-driven tests for ParseSchemaFS using `testing/fstest.MapFS` |
| `fuzz_test.go` | **Modify** | Add `FuzzParseTime` and `FuzzScanTime` for the new scanner |

---

## Task 1: Create `crypto/cipher.go`

**Files:**

- Create: `crypto/cipher.go`

- [ ] **Step 1.1: Create the directory and write the cipher implementation**

```go
// crypto/cipher.go
package crypto

import (
 "crypto/aes"
 "crypto/cipher"
 "crypto/hmac"
 "crypto/rand"
 "crypto/sha256"
 "encoding/base64"
 "errors"
 "fmt"
 "io"

 "golang.org/x/crypto/hkdf"
)

const (
 nonceSize = 12 // AES-GCM standard nonce size
 keySize   = 32 // AES-256
)

// Cipher provides AES-256-GCM encryption with HKDF-SHA256 key derivation.
// Keys are derived from a master key using separate HKDF contexts so that the
// encryption key and HMAC key are cryptographically independent.
type Cipher struct {
 aead    cipher.AEAD
 hmacKey []byte
}

// NewCipher creates a Cipher from masterKey. masterKey must be at least
// 32 bytes; shorter keys return an error. Two independent AES-256 and HMAC
// keys are derived from masterKey using HKDF-SHA256.
func NewCipher(masterKey []byte) (*Cipher, error) {
 if len(masterKey) < keySize {
  return nil, fmt.Errorf("geode/crypto: master key must be at least %d bytes, got %d", keySize, len(masterKey))
 }

 encReader := hkdf.New(sha256.New, masterKey, nil, []byte("geode-encrypt"))
 encKey := make([]byte, keySize)
 if _, err := io.ReadFull(encReader, encKey); err != nil {
  return nil, fmt.Errorf("geode/crypto: HKDF encrypt key: %w", err)
 }

 hmacReader := hkdf.New(sha256.New, masterKey, nil, []byte("geode-hmac"))
 hmacKey := make([]byte, keySize)
 if _, err := io.ReadFull(hmacReader, hmacKey); err != nil {
  return nil, fmt.Errorf("geode/crypto: HKDF hmac key: %w", err)
 }

 block, err := aes.NewCipher(encKey)
 if err != nil {
  return nil, fmt.Errorf("geode/crypto: AES cipher: %w", err)
 }
 aead, err := cipher.NewGCM(block)
 if err != nil {
  return nil, fmt.Errorf("geode/crypto: GCM: %w", err)
 }

 return &Cipher{aead: aead, hmacKey: hmacKey}, nil
}

// Encrypt encrypts plaintext with AES-256-GCM. The returned ciphertext has a
// 12-byte random nonce prepended; each call produces a different ciphertext.
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
 nonce := make([]byte, nonceSize)
 if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
  return nil, fmt.Errorf("geode/crypto: generate nonce: %w", err)
 }
 return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext produced by Encrypt.
// Returns an error if the ciphertext is too short or authentication fails.
func (c *Cipher) Decrypt(ciphertext []byte) ([]byte, error) {
 if len(ciphertext) < nonceSize {
  return nil, errors.New("geode/crypto: ciphertext too short")
 }
 nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
 plaintext, err := c.aead.Open(nil, nonce, ct, nil)
 if err != nil {
  return nil, fmt.Errorf("geode/crypto: decrypt: %w", err)
 }
 return plaintext, nil
}

// EncryptString encrypts plaintext and returns the result as a standard
// base64-encoded string.
func (c *Cipher) EncryptString(plaintext string) (string, error) {
 ct, err := c.Encrypt([]byte(plaintext))
 if err != nil {
  return "", err
 }
 return base64.StdEncoding.EncodeToString(ct), nil
}

// DecryptString decodes the base64 string and decrypts it.
func (c *Cipher) DecryptString(encoded string) (string, error) {
 ct, err := base64.StdEncoding.DecodeString(encoded)
 if err != nil {
  return "", fmt.Errorf("geode/crypto: base64 decode: %w", err)
 }
 pt, err := c.Decrypt(ct)
 if err != nil {
  return "", err
 }
 return string(pt), nil
}

// HMACHex computes HMAC-SHA256 of data using the derived HMAC key and returns
// a 64-character lowercase hex digest. Use this for deterministic lookups on
// encrypted fields (e.g., storing email_hash alongside an encrypted email
// column so equality queries remain possible without decrypting).
func (c *Cipher) HMACHex(data string) string {
 mac := hmac.New(sha256.New, c.hmacKey)
 mac.Write([]byte(data))
 return fmt.Sprintf("%x", mac.Sum(nil))
}
```

- [ ] **Step 1.2: Verify the package builds**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go
go build ./crypto/...
```

Expected: no output (clean build).

- [ ] **Step 1.3: Commit**

```bash
git add crypto/cipher.go
git commit -m "feat(crypto): add AES-256-GCM Cipher sub-package with HKDF key derivation"
```

---

## Task 2: Add `crypto/cipher_test.go` unit tests

**Files:**

- Create: `crypto/cipher_test.go`

- [ ] **Step 2.1: Write unit tests (they will pass since the implementation is already in)**

```go
// crypto/cipher_test.go
package crypto

import (
 "bytes"
 "strings"
 "testing"
)

func make32ByteKey(b byte) []byte {
 k := make([]byte, 32)
 for i := range k {
  k[i] = b
 }
 return k
}

func TestNewCipher_KeyTooShort(t *testing.T) {
 t.Parallel()
 _, err := NewCipher(make([]byte, 31))
 if err == nil {
  t.Fatal("expected error for 31-byte key, got nil")
 }
 if !strings.Contains(err.Error(), "at least 32 bytes") {
  t.Errorf("error message %q should mention key length requirement", err.Error())
 }
}

func TestNewCipher_ExactMinimumKey(t *testing.T) {
 t.Parallel()
 _, err := NewCipher(make32ByteKey(0x01))
 if err != nil {
  t.Fatalf("unexpected error for 32-byte key: %v", err)
 }
}

func TestNewCipher_LongerKey(t *testing.T) {
 t.Parallel()
 _, err := NewCipher(make([]byte, 64))
 if err != nil {
  t.Fatalf("unexpected error for 64-byte key: %v", err)
 }
}

func TestCipher_EncryptDecryptRoundtrip(t *testing.T) {
 t.Parallel()
 c, err := NewCipher(make32ByteKey(0xAB))
 if err != nil {
  t.Fatalf("NewCipher: %v", err)
 }

 plaintexts := [][]byte{
  []byte("hello, world"),
  []byte(""),
  []byte("a"),
  bytes.Repeat([]byte("x"), 1024),
  {0x00, 0x01, 0xFF, 0xFE}, // binary data
 }

 for _, pt := range plaintexts {
  ct, err := c.Encrypt(pt)
  if err != nil {
   t.Fatalf("Encrypt(%q): %v", pt, err)
  }
  got, err := c.Decrypt(ct)
  if err != nil {
   t.Fatalf("Decrypt: %v", err)
  }
  if !bytes.Equal(got, pt) {
   t.Errorf("roundtrip mismatch: got %q, want %q", got, pt)
  }
 }
}

func TestCipher_EncryptProducesUniqueOutputs(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0x55))
 pt := []byte("same plaintext")

 ct1, _ := c.Encrypt(pt)
 ct2, _ := c.Encrypt(pt)
 if bytes.Equal(ct1, ct2) {
  t.Error("two Encrypt calls with same plaintext produced identical ciphertext (nonce reuse?)")
 }
}

func TestCipher_Decrypt_TooShort(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0x01))

 _, err := c.Decrypt(make([]byte, nonceSize-1))
 if err == nil {
  t.Fatal("expected error for ciphertext shorter than nonce, got nil")
 }
}

func TestCipher_Decrypt_Tampered(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0x01))

 ct, _ := c.Encrypt([]byte("secret"))
 ct[len(ct)-1] ^= 0xFF // flip last byte — authentication must fail
 _, err := c.Decrypt(ct)
 if err == nil {
  t.Fatal("expected authentication error for tampered ciphertext, got nil")
 }
}

func TestCipher_EncryptString_DecryptString_Roundtrip(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0x77))

 inputs := []string{
  "hello",
  "",
  "unicode: 日本語",
  strings.Repeat("long", 256),
 }
 for _, s := range inputs {
  enc, err := c.EncryptString(s)
  if err != nil {
   t.Fatalf("EncryptString(%q): %v", s, err)
  }
  got, err := c.DecryptString(enc)
  if err != nil {
   t.Fatalf("DecryptString: %v", err)
  }
  if got != s {
   t.Errorf("roundtrip: got %q, want %q", got, s)
  }
 }
}

func TestCipher_DecryptString_InvalidBase64(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0x01))

 _, err := c.DecryptString("not-valid-base64!!!")
 if err == nil {
  t.Fatal("expected error for invalid base64, got nil")
 }
}

func TestCipher_HMACHex_Deterministic(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0xCC))

 h1 := c.HMACHex("user@example.com")
 h2 := c.HMACHex("user@example.com")
 if h1 != h2 {
  t.Errorf("HMACHex not deterministic: %q != %q", h1, h2)
 }
 if len(h1) != 64 {
  t.Errorf("HMACHex length = %d, want 64 (SHA-256 hex)", len(h1))
 }
}

func TestCipher_HMACHex_DifferentInputsDifferentOutputs(t *testing.T) {
 t.Parallel()
 c, _ := NewCipher(make32ByteKey(0xCC))

 h1 := c.HMACHex("alice@example.com")
 h2 := c.HMACHex("bob@example.com")
 if h1 == h2 {
  t.Error("different inputs produced identical HMAC digests")
 }
}

func TestCipher_HMACHex_DifferentKeysProduceDifferentDigests(t *testing.T) {
 t.Parallel()
 c1, _ := NewCipher(make32ByteKey(0x11))
 c2, _ := NewCipher(make32ByteKey(0x22))

 data := "same data"
 if c1.HMACHex(data) == c2.HMACHex(data) {
  t.Error("different master keys produced identical HMAC digests for the same input")
 }
}
```

- [ ] **Step 2.2: Run tests**

```bash
go test -v -short ./crypto/...
```

Expected: all tests `PASS`.

- [ ] **Step 2.3: Commit**

```bash
git add crypto/cipher_test.go
git commit -m "test(crypto): add unit tests for Cipher"
```

---

## Task 3: Add `crypto/cipher_bench_test.go` and `crypto/cipher_fuzz_test.go`

**Files:**

- Create: `crypto/cipher_bench_test.go`
- Create: `crypto/cipher_fuzz_test.go`

- [ ] **Step 3.1: Write benchmarks**

```go
// crypto/cipher_bench_test.go
package crypto

import (
 "testing"
)

var benchKey = func() []byte {
 k := make([]byte, 32)
 for i := range k {
  k[i] = byte(i)
 }
 return k
}()

var benchCipher = func() *Cipher {
 c, err := NewCipher(benchKey)
 if err != nil {
  panic(err)
 }
 return c
}()

func BenchmarkEncrypt(b *testing.B) {
 sizes := []int{64, 256, 1024, 4096}
 for _, n := range sizes {
  pt := make([]byte, n)
  b.Run(fmt.Sprintf("%dB", n), func(b *testing.B) {
   b.SetBytes(int64(n))
   b.ReportAllocs()
   for b.Loop() {
    if _, err := benchCipher.Encrypt(pt); err != nil {
     b.Fatal(err)
    }
   }
  })
 }
}

func BenchmarkDecrypt(b *testing.B) {
 sizes := []int{64, 256, 1024, 4096}
 for _, n := range sizes {
  pt := make([]byte, n)
  ct, err := benchCipher.Encrypt(pt)
  if err != nil {
   b.Fatal(err)
  }
  b.Run(fmt.Sprintf("%dB", n), func(b *testing.B) {
   b.SetBytes(int64(n))
   b.ReportAllocs()
   for b.Loop() {
    if _, err := benchCipher.Decrypt(ct); err != nil {
     b.Fatal(err)
    }
   }
  })
 }
}

func BenchmarkEncryptString(b *testing.B) {
 input := "user@example.com"
 b.SetBytes(int64(len(input)))
 b.ReportAllocs()
 for b.Loop() {
  if _, err := benchCipher.EncryptString(input); err != nil {
   b.Fatal(err)
  }
 }
}

func BenchmarkDecryptString(b *testing.B) {
 enc, err := benchCipher.EncryptString("user@example.com")
 if err != nil {
  b.Fatal(err)
 }
 b.ReportAllocs()
 for b.Loop() {
  if _, err := benchCipher.DecryptString(enc); err != nil {
   b.Fatal(err)
  }
 }
}

func BenchmarkHMACHex(b *testing.B) {
 b.ReportAllocs()
 for b.Loop() {
  _ = benchCipher.HMACHex("user@example.com")
 }
}

func BenchmarkNewCipher(b *testing.B) {
 key := make([]byte, 32)
 b.ReportAllocs()
 for b.Loop() {
  if _, err := NewCipher(key); err != nil {
   b.Fatal(err)
  }
 }
}
```

Note: The benchmark file uses `fmt` — add that import at the top:

```go
import (
    "fmt"
    "testing"
)
```

- [ ] **Step 3.2: Write fuzz tests**

```go
// crypto/cipher_fuzz_test.go
package crypto

import (
 "bytes"
 "testing"
)

// FuzzEncryptDecrypt verifies the encrypt→decrypt roundtrip property holds
// for any plaintext input.
func FuzzEncryptDecrypt(f *testing.F) {
 f.Add([]byte("hello"))
 f.Add([]byte(""))
 f.Add([]byte{0x00, 0xFF, 0xAB})
 f.Add(make([]byte, 1024))

 key := make([]byte, 32)
 for i := range key {
  key[i] = byte(i + 1)
 }
 c, err := NewCipher(key)
 if err != nil {
  f.Fatal(err)
 }

 f.Fuzz(func(t *testing.T, plaintext []byte) {
  ct, err := c.Encrypt(plaintext)
  if err != nil {
   t.Skip() // Encrypt should not fail for valid plaintext
  }
  got, err := c.Decrypt(ct)
  if err != nil {
   t.Fatalf("Decrypt failed after Encrypt: %v", err)
  }
  if !bytes.Equal(got, plaintext) {
   t.Fatalf("roundtrip mismatch: got %x, want %x", got, plaintext)
  }
 })
}

// FuzzDecrypt verifies that Decrypt never panics on arbitrary byte input.
func FuzzDecrypt(f *testing.F) {
 f.Add([]byte{})
 f.Add(make([]byte, nonceSize-1))
 f.Add(make([]byte, nonceSize))
 f.Add(make([]byte, nonceSize+16))

 key := make([]byte, 32)
 c, _ := NewCipher(key)

 f.Fuzz(func(t *testing.T, data []byte) {
  // Must not panic; error is expected for arbitrary data
  _, _ = c.Decrypt(data)
 })
}

// FuzzHMACHex verifies HMACHex never panics and always returns a 64-char string.
func FuzzHMACHex(f *testing.F) {
 f.Add("")
 f.Add("user@example.com")
 f.Add(string(make([]byte, 1024)))

 key := make([]byte, 32)
 c, _ := NewCipher(key)

 f.Fuzz(func(t *testing.T, data string) {
  h := c.HMACHex(data)
  if len(h) != 64 {
   t.Fatalf("HMACHex(%q) returned %d chars, want 64", data, len(h))
  }
 })
}
```

- [ ] **Step 3.3: Run benchmarks briefly**

```bash
go test -bench=. -benchmem -benchtime=1s ./crypto/...
```

Expected: output lines starting with `Benchmark` followed by timing/allocation data. No failures.

- [ ] **Step 3.4: Run fuzz briefly (5 seconds each)**

```bash
go test -fuzz=FuzzEncryptDecrypt -fuzztime=5s ./crypto/...
go test -fuzz=FuzzDecrypt -fuzztime=5s ./crypto/...
```

Expected: no failures (`fuzz: elapsed: Xs, ...`). Corpus entries may be added to `testdata/fuzz/`.

- [ ] **Step 3.5: Commit**

```bash
git add crypto/cipher_bench_test.go crypto/cipher_fuzz_test.go
git commit -m "test(crypto): add benchmarks and fuzz tests for Cipher"
```

---

## Task 4: Add `TimeScanner` and `ScanTime` to `time.go`

**Files:**

- Modify: `time.go`
- Modify: `time_test.go`

- [ ] **Step 4.1: Write the failing test first**

Append to `time_test.go`:

```go
func TestScanTime(t *testing.T) {
 t.Parallel()

 tests := []struct {
  name    string
  src     any
  wantErr bool
  want    time.Time
 }{
  {
   name: "nil src sets zero time",
   src:  nil,
   want: time.Time{},
  },
  {
   name: "time.Time src is passed through",
   src:  time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC),
   want: time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC),
  },
  {
   name: "empty string sets zero time",
   src:  "",
   want: time.Time{},
  },
  {
   name: "valid RFC3339 string is parsed",
   src:  "2024-01-15T10:30:00Z",
   want: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
  },
  {
   name: "datetime string with monotonic suffix",
   src:  "2024-06-01T00:00:00Z m=+12.345",
   want: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
  },
  {
   name:    "unparseable string returns error",
   src:     "not-a-time",
   wantErr: true,
  },
  {
   name:    "integer src returns error",
   src:     int64(12345),
   wantErr: true,
  },
 }

 for _, tc := range tests {
  t.Run(tc.name, func(t *testing.T) {
   t.Parallel()
   var dest time.Time
   err := ScanTime(&dest).Scan(tc.src)
   if tc.wantErr {
    if err == nil {
     t.Fatalf("Scan(%T(%v)) expected error, got nil", tc.src, tc.src)
    }
    return
   }
   if err != nil {
    t.Fatalf("Scan(%T(%v)) unexpected error: %v", tc.src, tc.src, err)
   }
   if !dest.Equal(tc.want) {
    t.Errorf("Scan(%v) = %v, want %v", tc.src, dest, tc.want)
   }
  })
 }
}
```

- [ ] **Step 4.2: Run to confirm test fails**

```bash
go test -run TestScanTime -v ./...
```

Expected: `# geodedb.com/geode [...] undefined: ScanTime` (compile error or `FAIL`).

- [ ] **Step 4.3: Add `TimeScanner` and `ScanTime` to `time.go`**

Append to the end of `time.go` (after the existing `ParseTime` function):

```go

// TimeScanner implements [sql.Scanner] for Geode time strings.
// Construct one with [ScanTime] and pass it to rows.Scan:
//
// var t time.Time
// rows.Scan(geode.ScanTime(&t))
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
```

Also add `"fmt"` to the existing `time.go` imports (it currently only has `"fmt"` and `"strings"` and `"time"` — verify).

- [ ] **Step 4.4: Run tests**

```bash
go test -run TestScanTime -v ./...
```

Expected: all `TestScanTime/*` subtests `PASS`.

- [ ] **Step 4.5: Run full test suite to confirm no regressions**

```bash
go test -v -short ./...
```

Expected: all existing tests still pass.

- [ ] **Step 4.6: Add `FuzzScanTime` to `fuzz_test.go`**

Append to `fuzz_test.go`:

```go
func FuzzScanTime(f *testing.F) {
 f.Add("")
 f.Add("2024-01-15T10:30:00Z")
 f.Add("2024-01-15 10:30:00")
 f.Add("not-a-time")
 f.Add("2024-01-15T10:30:00Z m=+1.234")

 f.Fuzz(func(_ *testing.T, s string) {
  var dest time.Time
  // Must not panic; error is expected for arbitrary input
  _ = ScanTime(&dest).Scan(s)
 })
}
```

- [ ] **Step 4.7: Verify fuzz compiles and runs briefly**

```bash
go test -fuzz=FuzzScanTime -fuzztime=5s ./...
```

Expected: no panics, no failures.

- [ ] **Step 4.8: Commit**

```bash
git add time.go time_test.go fuzz_test.go
git commit -m "feat: add TimeScanner/ScanTime sql.Scanner adapter for Geode time strings"
```

---

## Task 5: Create mock driver for batch unit tests

**Files:**

- Create: `batch_mock_test.go`

This is a white-box (package `geode`) mock SQL driver used only in batch unit tests. It records operations and allows callers to inject errors.

- [ ] **Step 5.1: Write the mock driver**

```go
// batch_mock_test.go
package geode

import (
 "database/sql"
 "database/sql/driver"
 "fmt"
 "io"
 "sync/atomic"
 "testing"
)

// mockDriverSeq generates unique driver names to avoid sql.Register panics.
var mockDriverSeq atomic.Int64

// mockDriver is a minimal database/sql/driver.Driver for unit testing.
// Configure onBegin and onExec to inject errors; zero values mean no error.
type mockDriver struct {
 // onBegin is called when a transaction is opened; return non-nil to fail.
 onBegin func() error
 // onExec is called for each statement execution; return non-nil to fail.
 onExec func(query string) error
 // commits counts successful tx.Commit calls.
 commits atomic.Int64
}

func (d *mockDriver) Open(_ string) (driver.Conn, error) {
 return &mockConn{driver: d}, nil
}

type mockConn struct{ driver *mockDriver }

func (c *mockConn) Prepare(q string) (driver.Stmt, error) {
 return &mockStmt{conn: c, query: q}, nil
}
func (c *mockConn) Close() error { return nil }
func (c *mockConn) Begin() (driver.Tx, error) {
 if c.driver.onBegin != nil {
  if err := c.driver.onBegin(); err != nil {
   return nil, err
  }
 }
 return &mockTx{driver: c.driver}, nil
}

type mockTx struct{ driver *mockDriver }

func (t *mockTx) Commit() error   { t.driver.commits.Add(1); return nil }
func (t *mockTx) Rollback() error { return nil }

type mockStmt struct {
 conn  *mockConn
 query string
}

func (s *mockStmt) Close() error   { return nil }
func (s *mockStmt) NumInput() int  { return -1 }
func (s *mockStmt) Query(_ []driver.Value) (driver.Rows, error) {
 return &mockRows{}, nil
}
func (s *mockStmt) Exec(_ []driver.Value) (driver.Result, error) {
 if s.conn.driver.onExec != nil {
  if err := s.conn.driver.onExec(s.query); err != nil {
   return nil, err
  }
 }
 return driver.RowsAffected(1), nil
}

type mockRows struct{}

func (*mockRows) Columns() []string             { return nil }
func (*mockRows) Close() error                  { return nil }
func (*mockRows) Next(_ []driver.Value) error   { return io.EOF }

// newMockPool registers a fresh mock driver (with a unique name) and returns
// a Pool wired to it. t.Cleanup closes the underlying *sql.DB.
func newMockPool(t *testing.T, md *mockDriver) *Pool {
 t.Helper()
 name := fmt.Sprintf("geode-mock-%d", mockDriverSeq.Add(1))
 sql.Register(name, md)
 db, err := sql.Open(name, "")
 if err != nil {
  t.Fatalf("sql.Open(%q): %v", name, err)
 }
 t.Cleanup(func() { _ = db.Close() })
 return &Pool{db: db, cfg: &Config{}}
}
```

- [ ] **Step 5.2: Verify the file compiles**

```bash
go test -run ^$ -v ./...
```

Expected: no compile errors (zero tests run is fine).

---

## Task 6: Create `batch.go` and `batch_test.go` — `Statement`, `BatchExec`

**Files:**

- Create: `batch.go`
- Create: `batch_test.go`

- [ ] **Step 6.1: Write the failing tests for `Statement` and `BatchExec`**

```go
// batch_test.go
package geode

import (
 "context"
 "errors"
 "testing"
)

// ── Statement ────────────────────────────────────────────────────────────────

func TestStatement_ZeroValue(t *testing.T) {
 var s Statement
 if s.Query != "" {
  t.Errorf("zero Statement.Query = %q, want empty", s.Query)
 }
 if s.Args != nil {
  t.Errorf("zero Statement.Args should be nil")
 }
}

func TestStatement_WithArgs(t *testing.T) {
 s := Statement{Query: "MATCH (n) RETURN n.id", Args: []any{"x", 42}}
 if s.Query != "MATCH (n) RETURN n.id" {
  t.Errorf("unexpected Query: %q", s.Query)
 }
 if len(s.Args) != 2 {
  t.Errorf("len(Args) = %d, want 2", len(s.Args))
 }
}

// ── BatchExec ────────────────────────────────────────────────────────────────

func TestBatchExec_EmptySlice(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 err := p.BatchExec(context.Background(), nil)
 if err != nil {
  t.Fatalf("BatchExec(nil): unexpected error: %v", err)
 }
 err = p.BatchExec(context.Background(), []Statement{})
 if err != nil {
  t.Fatalf("BatchExec([]): unexpected error: %v", err)
 }
 if md.commits.Load() != 0 {
  t.Errorf("commits = %d, want 0 for empty input", md.commits.Load())
 }
}

func TestBatchExec_AllSucceed(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 stmts := []Statement{
  {Query: "CREATE (n:A)"},
  {Query: "CREATE (n:B)"},
  {Query: "CREATE (n:C)"},
 }
 err := p.BatchExec(context.Background(), stmts)
 if err != nil {
  t.Fatalf("BatchExec: unexpected error: %v", err)
 }
 if md.commits.Load() != 1 {
  t.Errorf("commits = %d, want 1 (all stmts in single tx)", md.commits.Load())
 }
}

func TestBatchExec_NonRetryableErrorStopsImmediately(t *testing.T) {
 t.Parallel()
 execCount := 0
 syntaxErr := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}
 md := &mockDriver{
  onExec: func(q string) error {
   execCount++
   if execCount == 2 {
    return syntaxErr
   }
   return nil
  },
 }
 p := newMockPool(t, md)

 stmts := []Statement{
  {Query: "CREATE (n:A)"},
  {Query: "INVALID STATEMENT"},
  {Query: "CREATE (n:C)"},
 }
 err := p.BatchExec(context.Background(), stmts)
 if err == nil {
  t.Fatal("expected error from BatchExec, got nil")
 }
 if !errors.Is(err, syntaxErr) {
  t.Errorf("error = %v, want syntaxErr wrapped", err)
 }
}

func TestBatchExec_ContextCancelled(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 ctx, cancel := context.WithCancel(context.Background())
 cancel()

 err := p.BatchExec(ctx, []Statement{{Query: "CREATE (n:A)"}})
 if err == nil {
  t.Fatal("expected error for cancelled context, got nil")
 }
 if !errors.Is(err, context.Canceled) {
  t.Errorf("error = %v, want context.Canceled", err)
 }
}

func TestBatchExec_SingleStatement(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 err := p.BatchExec(context.Background(), []Statement{{Query: "CREATE (n:X)"}})
 if err != nil {
  t.Fatalf("BatchExec: %v", err)
 }
 if md.commits.Load() != 1 {
  t.Errorf("commits = %d, want 1", md.commits.Load())
 }
}
```

- [ ] **Step 6.2: Run tests — confirm compile error**

```bash
go test -run TestBatchExec -v ./...
```

Expected: compile error `undefined: BatchExec` or similar.

- [ ] **Step 6.3: Write `batch.go`**

```go
// batch.go
package geode

import (
 "context"
 "database/sql"
 "errors"
 "fmt"
 "sync"
 "sync/atomic"
)

// Statement is a GQL query paired with its bound arguments.
type Statement struct {
 Query string
 Args  []any
}

const (
 // DefaultChunkSize is the default number of statements per chunk in PipelineExec.
 DefaultChunkSize = 100

 // DefaultMaxWorkers is the default maximum number of parallel workers in PipelineExec.
 DefaultMaxWorkers = 8
)

// BatchExec executes stmts inside a single transaction with automatic retry on
// retryable errors (transport failures, serialization conflicts). A transaction
// pins the underlying QUIC connection for the entire batch, eliminating
// per-statement pool churn that causes EOF errors under concurrent load.
//
// Retry behaviour is governed by [DefaultRetryPolicy] (3 attempts, exponential
// backoff 1 s–30 s, multiplier 2×). Non-retryable errors are returned
// immediately without further attempts.
//
// An empty stmts slice is a no-op.
func (p *Pool) BatchExec(ctx context.Context, stmts []Statement) error {
 if len(stmts) == 0 {
  return nil
 }
 return Retry(ctx, DefaultRetryPolicy(), func(ctx context.Context) error {
  return batchExecTx(ctx, p.db, stmts)
 })
}

// batchExecTx executes all statements within a single transaction.
// An error from any statement rolls back the transaction.
func batchExecTx(ctx context.Context, db *sql.DB, stmts []Statement) error {
 tx, err := db.BeginTx(ctx, nil)
 if err != nil {
  return fmt.Errorf("geode: batch begin tx: %w", err)
 }
 defer func() {
  if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
   _ = rbErr
  }
 }()
 for i, stmt := range stmts {
  if ctx.Err() != nil {
   return ctx.Err()
  }
  if _, err := tx.ExecContext(ctx, stmt.Query, stmt.Args...); err != nil {
   return fmt.Errorf("geode: statement %d: %w", i+1, err)
  }
 }
 return tx.Commit()
}

// PipelineExec executes statements in parallel chunks using a worker pool.
// It splits stmts into chunks of chunkSize and runs up to maxWorkers chunks
// concurrently; each chunk is executed by [Pool.BatchExec].
//
// Use 0 for chunkSize or maxWorkers to apply the defaults (100 and 8).
//
// PipelineExec returns the first error encountered. In-flight chunks are
// allowed to complete; cancel ctx to abort them early.
func (p *Pool) PipelineExec(ctx context.Context, stmts []Statement, chunkSize, maxWorkers int) error {
 if len(stmts) == 0 {
  return nil
 }
 if chunkSize <= 0 {
  chunkSize = DefaultChunkSize
 }
 if maxWorkers <= 0 {
  maxWorkers = DefaultMaxWorkers
 }

 total := len(stmts)
 chunks := make([][]Statement, 0, (total/chunkSize)+1)
 for i := 0; i < total; i += chunkSize {
  end := min(i+chunkSize, total)
  chunks = append(chunks, stmts[i:end])
 }

 sem := make(chan struct{}, maxWorkers)
 var (
  wg       sync.WaitGroup
  mu       sync.Mutex
  firstErr error
  flushed  atomic.Int64
 )

 for _, chunk := range chunks {
  if ctx.Err() != nil {
   break
  }
  mu.Lock()
  hasErr := firstErr != nil
  mu.Unlock()
  if hasErr {
   break
  }

  sem <- struct{}{}
  wg.Add(1)
  go func(ch []Statement) {
   defer wg.Done()
   defer func() { <-sem }()
   if err := p.BatchExec(ctx, ch); err != nil {
    mu.Lock()
    if firstErr == nil {
     firstErr = err
    }
    mu.Unlock()
    return
   }
   flushed.Add(int64(len(ch)))
  }(chunk)
 }
 wg.Wait()

 mu.Lock()
 defer mu.Unlock()
 return firstErr
}
```

- [ ] **Step 6.4: Run tests**

```bash
go test -run "TestStatement|TestBatchExec" -v ./...
```

Expected: all listed tests `PASS`.

- [ ] **Step 6.5: Run full suite**

```bash
go test -v -short ./...
```

Expected: no regressions.

- [ ] **Step 6.6: Commit**

```bash
git add batch.go batch_mock_test.go batch_test.go
git commit -m "feat: add Statement type, Pool.BatchExec with retry, Pool.PipelineExec"
```

---

## Task 7: Add PipelineExec unit tests to `batch_test.go`

**Files:**

- Modify: `batch_test.go`

- [ ] **Step 7.1: Add PipelineExec tests**

Append to `batch_test.go`:

```go
// ── PipelineExec ─────────────────────────────────────────────────────────────

func TestPipelineExec_EmptySlice(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 err := p.PipelineExec(context.Background(), nil, 0, 0)
 if err != nil {
  t.Fatalf("PipelineExec(nil): %v", err)
 }
 err = p.PipelineExec(context.Background(), []Statement{}, 0, 0)
 if err != nil {
  t.Fatalf("PipelineExec([]): %v", err)
 }
 if md.commits.Load() != 0 {
  t.Errorf("commits = %d, want 0 for empty input", md.commits.Load())
 }
}

func TestPipelineExec_AllSucceed(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 // 10 statements, chunk=3 → ceil(10/3)=4 chunks → 4 commits
 stmts := make([]Statement, 10)
 for i := range stmts {
  stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:N%d)", i)}
 }
 err := p.PipelineExec(context.Background(), stmts, 3, 2)
 if err != nil {
  t.Fatalf("PipelineExec: %v", err)
 }
 // 10 stmts / chunk=3 → chunks: [3,3,3,1] → 4 commits
 if md.commits.Load() != 4 {
  t.Errorf("commits = %d, want 4", md.commits.Load())
 }
}

func TestPipelineExec_DefaultChunkAndWorkers(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 // 50 statements, default chunk=100 → 1 chunk → 1 commit
 stmts := make([]Statement, 50)
 for i := range stmts {
  stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:X%d)", i)}
 }
 err := p.PipelineExec(context.Background(), stmts, 0, 0)
 if err != nil {
  t.Fatalf("PipelineExec: %v", err)
 }
 if md.commits.Load() != 1 {
  t.Errorf("commits = %d, want 1 (fits in default chunk)", md.commits.Load())
 }
}

func TestPipelineExec_ErrorPropagates(t *testing.T) {
 t.Parallel()
 syntaxErr := &DriverError{Class: StatusSyntax, Code: "SY001", Message: "syntax error"}
 md := &mockDriver{
  onExec: func(_ string) error {
   return syntaxErr
  },
 }
 p := newMockPool(t, md)

 stmts := []Statement{{Query: "BAD STATEMENT"}}
 err := p.PipelineExec(context.Background(), stmts, 1, 1)
 if err == nil {
  t.Fatal("expected error from PipelineExec, got nil")
 }
}

func TestPipelineExec_ContextCancelled(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 ctx, cancel := context.WithCancel(context.Background())
 cancel()

 stmts := make([]Statement, 200)
 for i := range stmts {
  stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:C%d)", i)}
 }
 err := p.PipelineExec(ctx, stmts, 10, 4)
 if err == nil {
  t.Fatal("expected error for cancelled context, got nil")
 }
}

func TestPipelineExec_ChunkSizeOne(t *testing.T) {
 t.Parallel()
 md := &mockDriver{}
 p := newMockPool(t, md)

 stmts := make([]Statement, 5)
 for i := range stmts {
  stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:Y%d)", i)}
 }
 err := p.PipelineExec(context.Background(), stmts, 1, 5)
 if err != nil {
  t.Fatalf("PipelineExec: %v", err)
 }
 // 5 stmts / chunk=1 → 5 commits
 if md.commits.Load() != 5 {
  t.Errorf("commits = %d, want 5", md.commits.Load())
 }
}

func TestDefaultConstants(t *testing.T) {
 if DefaultChunkSize != 100 {
  t.Errorf("DefaultChunkSize = %d, want 100", DefaultChunkSize)
 }
 if DefaultMaxWorkers != 8 {
  t.Errorf("DefaultMaxWorkers = %d, want 8", DefaultMaxWorkers)
 }
}
```

Note: `batch_test.go` now uses `fmt` — add `"fmt"` to its imports.

- [ ] **Step 7.2: Run tests**

```bash
go test -run "TestPipeline|TestDefault" -v ./...
```

Expected: all `PASS`.

- [ ] **Step 7.3: Run full suite**

```bash
go test -v -short ./...
```

- [ ] **Step 7.4: Commit**

```bash
git add batch_test.go
git commit -m "test: add PipelineExec unit tests"
```

---

## Task 8: Add `batch_bench_test.go` — benchmarks for BatchExec and PipelineExec

**Files:**

- Create: `batch_bench_test.go`

- [ ] **Step 8.1: Write benchmarks**

```go
// batch_bench_test.go
package geode

import (
 "context"
 "fmt"
 "testing"
)

// buildStmts creates n synthetic CREATE statements for use in benchmarks.
func buildStmts(n int) []Statement {
 stmts := make([]Statement, n)
 for i := range stmts {
  stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:Bench%d)", i)}
 }
 return stmts
}

func BenchmarkBatchExec(b *testing.B) {
 sizes := []int{1, 10, 100, 500}
 for _, n := range sizes {
  stmts := buildStmts(n)
  b.Run(fmt.Sprintf("%dstmts", n), func(b *testing.B) {
   md := &mockDriver{}
   p := newMockPool(b, md)
   b.ResetTimer()
   b.ReportAllocs()
   for b.Loop() {
    if err := p.BatchExec(context.Background(), stmts); err != nil {
     b.Fatal(err)
    }
   }
  })
 }
}

func BenchmarkPipelineExec(b *testing.B) {
 type cfg struct {
  stmts   int
  chunk   int
  workers int
 }
 cases := []cfg{
  {100, 10, 4},
  {500, 50, 8},
  {1000, 100, 8},
 }
 for _, tc := range cases {
  stmts := buildStmts(tc.stmts)
  name := fmt.Sprintf("%dstmts_c%d_w%d", tc.stmts, tc.chunk, tc.workers)
  b.Run(name, func(b *testing.B) {
   md := &mockDriver{}
   p := newMockPool(b, md)
   b.ResetTimer()
   b.ReportAllocs()
   for b.Loop() {
    if err := p.PipelineExec(context.Background(), stmts, tc.chunk, tc.workers); err != nil {
     b.Fatal(err)
    }
   }
  })
 }
}
```

Note: uses `b.Loop()` (Go 1.24+ benchmark idiom, consistent with module's Go 1.25 minimum).

- [ ] **Step 8.2: Run benchmarks**

```bash
go test -bench="BenchmarkBatchExec|BenchmarkPipelineExec" -benchmem -benchtime=2s ./...
```

Expected: benchmark output with ns/op and B/op data. No failures.

- [ ] **Step 8.3: Commit**

```bash
git add batch_bench_test.go
git commit -m "bench: add BatchExec and PipelineExec benchmarks"
```

---

## Task 9: Create `schema.go` and `schema_test.go`

**Files:**

- Create: `schema.go`
- Create: `schema_test.go`

- [ ] **Step 9.1: Write the failing tests**

```go
// schema_test.go
package geode

import (
 "testing"
 "testing/fstest"
)

func TestParseSchemaFS_EmptyDir(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{}
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS(empty): %v", err)
 }
 if len(stmts) != 0 {
  t.Errorf("expected 0 statements, got %d", len(stmts))
 }
}

func TestParseSchemaFS_NonExistentDir(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{}
 _, err := ParseSchemaFS(fs, "nosuchdir")
 if err == nil {
  t.Fatal("expected error for missing dir, got nil")
 }
}

func TestParseSchemaFS_IgnoresNonGQL(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "schema.sql": {Data: []byte("CREATE TABLE t (id INT)")},
  "notes.txt":  {Data: []byte("some notes")},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 0 {
  t.Errorf("expected 0 statements (no .gql files), got %d", len(stmts))
 }
}

func TestParseSchemaFS_BasicStatements(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "01_nodes.gql": {Data: []byte(`
CREATE NODE TYPE Person (name STRING, age INT)
CREATE NODE TYPE Company (name STRING)
`)},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 2 {
  t.Fatalf("expected 2 statements, got %d: %v", len(stmts), stmts)
 }
 if stmts[0].Query != "CREATE NODE TYPE Person (name STRING, age INT)" {
  t.Errorf("stmt[0].Query = %q", stmts[0].Query)
 }
 if stmts[1].Query != "CREATE NODE TYPE Company (name STRING)" {
  t.Errorf("stmt[1].Query = %q", stmts[1].Query)
 }
}

func TestParseSchemaFS_SemicolonSplitting(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "schema.gql": {Data: []byte("CREATE (a); CREATE (b); CREATE (c)")},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 3 {
  t.Fatalf("expected 3 statements from semicolon split, got %d", len(stmts))
 }
}

func TestParseSchemaFS_FiltersComments(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "schema.gql": {Data: []byte(`
// This is a comment
-- This is also a comment
CREATE NODE TYPE Widget (id INT)
`)},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 1 {
  t.Fatalf("expected 1 statement (comments skipped), got %d", len(stmts))
 }
}

func TestParseSchemaFS_FiltersBlankLines(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "schema.gql": {Data: []byte("\n\n\nCREATE NODE TYPE A\n\n\n")},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 1 {
  t.Fatalf("expected 1 statement, got %d", len(stmts))
 }
}

func TestParseSchemaFS_MultipleFiles(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "01_types.gql": {Data: []byte("CREATE NODE TYPE A\nCREATE NODE TYPE B")},
  "02_edges.gql": {Data: []byte("CREATE EDGE TYPE E FROM A TO B")},
  "notes.txt":    {Data: []byte("not a schema file")},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 3 {
  t.Fatalf("expected 3 statements from 2 .gql files, got %d", len(stmts))
 }
}

func TestParseSchemaFS_SubDir(t *testing.T) {
 t.Parallel()
 fs := fstest.MapFS{
  "migrations/001.gql": {Data: []byte("CREATE NODE TYPE Z")},
 }
 stmts, err := ParseSchemaFS(fs, "migrations")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 1 {
  t.Fatalf("expected 1 statement from subdir, got %d", len(stmts))
 }
}

func TestParseSchemaFS_IgnoresNonGQLKeywords(t *testing.T) {
 t.Parallel()
 // Lines not starting with CREATE/MERGE/MATCH/DELETE are skipped.
 fs := fstest.MapFS{
  "schema.gql": {Data: []byte(`
CREATE (a)
USE GRAPH mydb
SET PROPERTY foo = 1
CREATE (b)
`)},
 }
 stmts, err := ParseSchemaFS(fs, ".")
 if err != nil {
  t.Fatalf("ParseSchemaFS: %v", err)
 }
 if len(stmts) != 2 {
  t.Fatalf("expected 2 statements (USE and SET skipped), got %d", len(stmts))
 }
}
```

- [ ] **Step 9.2: Run failing tests**

```bash
go test -run TestParseSchemaFS -v ./...
```

Expected: compile error `undefined: ParseSchemaFS`.

- [ ] **Step 9.3: Write `schema.go`**

```go
// schema.go
package geode

import (
 "embed"
 "fmt"
 "strings"
)

// ParseSchemaFS reads all .gql files from the embedded filesystem at dir and
// returns Statements suitable for [Pool.BatchExec] or [Pool.PipelineExec].
//
// Use "." to reference the root of the embedded FS. Files in subdirectories
// are not traversed; only direct children of dir matching "*.gql" are read.
//
// Lines are split into statements at semicolons after stripping blank lines,
// line comments (// and --), and lines that do not start with a recognised GQL
// keyword (CREATE, MERGE, MATCH, DELETE). Semicolons within a line produce
// multiple statements.
func ParseSchemaFS(fs embed.FS, dir string) ([]Statement, error) {
 entries, err := fs.ReadDir(dir)
 if err != nil {
  return nil, fmt.Errorf("geode: ParseSchemaFS: read dir %q: %w", dir, err)
 }

 var stmts []Statement
 for _, entry := range entries {
  if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".gql") {
   continue
  }

  path := entry.Name()
  if dir != "." && dir != "" {
   path = dir + "/" + entry.Name()
  }
  content, err := fs.ReadFile(path)
  if err != nil {
   return nil, fmt.Errorf("geode: ParseSchemaFS: read %s: %w", path, err)
  }

  for line := range strings.SplitSeq(string(content), "\n") {
   trimmed := strings.TrimSpace(line)
   if trimmed == "" ||
    strings.HasPrefix(trimmed, "//") ||
    strings.HasPrefix(trimmed, "--") {
    continue
   }
   upper := strings.ToUpper(trimmed)
   if !strings.HasPrefix(upper, "CREATE") &&
    !strings.HasPrefix(upper, "MERGE") &&
    !strings.HasPrefix(upper, "MATCH") &&
    !strings.HasPrefix(upper, "DELETE") {
    continue
   }
   for _, part := range strings.Split(trimmed, ";") {
    part = strings.TrimSpace(part)
    if part != "" {
     stmts = append(stmts, Statement{Query: part})
    }
   }
  }
 }

 return stmts, nil
}
```

- [ ] **Step 9.4: Run tests**

```bash
go test -run TestParseSchemaFS -v ./...
```

Expected: all `PASS`.

- [ ] **Step 9.5: Run full suite**

```bash
go test -v -short ./...
```

- [ ] **Step 9.6: Commit**

```bash
git add schema.go schema_test.go
git commit -m "feat: add ParseSchemaFS for loading embedded .gql schema files"
```

---

## Task 10: Add `FuzzParseSchemaFS` to `fuzz_test.go`

**Files:**

- Modify: `fuzz_test.go`

- [ ] **Step 10.1: Add fuzz entry**

Append to `fuzz_test.go`:

```go
func FuzzParseSchemaFS(f *testing.F) {
 // Seed with realistic GQL file content
 f.Add("CREATE NODE TYPE Person (name STRING)")
 f.Add("// comment\nCREATE (n:A)\nCREATE (n:B)")
 f.Add("CREATE (a); CREATE (b); CREATE (c)")
 f.Add("")
 f.Add("-- SQL comment\nSELECT * FROM foo")

 f.Fuzz(func(t *testing.T, content string) {
  fs := fstest.MapFS{
   "fuzz.gql": {Data: []byte(content)},
  }
  // Must not panic
  _, _ = ParseSchemaFS(fs, ".")
 })
}
```

Also add `"testing/fstest"` to the imports in `fuzz_test.go`.

- [ ] **Step 10.2: Run fuzz briefly**

```bash
go test -fuzz=FuzzParseSchemaFS -fuzztime=5s ./...
```

Expected: no panics.

- [ ] **Step 10.3: Commit**

```bash
git add fuzz_test.go
git commit -m "test: add FuzzParseSchemaFS fuzz entry"
```

---

## Task 11: Add integration tests for BatchExec and PipelineExec

**Files:**

- Modify: `integration_test.go`

Integration tests require the `integration` build tag and a running Geode container (started automatically by testcontainers-go). Consult the existing test setup in `integration_test.go` to understand the `testDB` variable and the shared container lifecycle.

- [ ] **Step 11.1: Append integration tests at the end of `integration_test.go`**

Look for the `testDB` variable and the suite setup. Add the following after the existing test functions:

```go
// ── BatchExec integration ─────────────────────────────────────────────────────

func TestIntegration_BatchExec_AllSucceed(t *testing.T) {
 t.Parallel()
 pool := mustNewIntegrationPool(t)

 stmts := []Statement{
  {Query: "CREATE (n:BatchNode {id: 1, tag: 'batch_test'})"},
  {Query: "CREATE (n:BatchNode {id: 2, tag: 'batch_test'})"},
  {Query: "CREATE (n:BatchNode {id: 3, tag: 'batch_test'})"},
 }
 if err := pool.BatchExec(context.Background(), stmts); err != nil {
  t.Fatalf("BatchExec: %v", err)
 }

 rows, err := pool.QueryContext(context.Background(),
  "MATCH (n:BatchNode {tag: 'batch_test'}) RETURN COUNT(n) AS c")
 if err != nil {
  t.Fatalf("query after BatchExec: %v", err)
 }
 defer rows.Close()

 var count int
 if rows.Next() {
  if err := rows.Scan(&count); err != nil {
   t.Fatalf("scan: %v", err)
  }
 }
 if count != 3 {
  t.Errorf("expected 3 BatchNode rows, got %d", count)
 }
}

func TestIntegration_BatchExec_Rollback(t *testing.T) {
 pool := mustNewIntegrationPool(t)

 // Second statement is intentionally malformed; the whole transaction must roll back.
 stmts := []Statement{
  {Query: "CREATE (n:RollbackNode {id: 1})"},
  {Query: "THIS IS INVALID GQL !!!"},
 }
 err := pool.BatchExec(context.Background(), stmts)
 if err == nil {
  t.Fatal("expected error from BatchExec with invalid statement, got nil")
 }

 // Confirm no RollbackNode was persisted.
 rows, err := pool.QueryContext(context.Background(),
  "MATCH (n:RollbackNode) RETURN COUNT(n) AS c")
 if err != nil {
  t.Fatalf("query after failed BatchExec: %v", err)
 }
 defer rows.Close()

 var count int
 if rows.Next() {
  _ = rows.Scan(&count)
 }
 if count != 0 {
  t.Errorf("expected 0 RollbackNode rows after rollback, got %d", count)
 }
}

// ── PipelineExec integration ──────────────────────────────────────────────────

func TestIntegration_PipelineExec_BulkCreate(t *testing.T) {
 t.Parallel()
 pool := mustNewIntegrationPool(t)

 const total = 250
 stmts := make([]Statement, total)
 for i := range stmts {
  stmts[i] = Statement{
   Query: fmt.Sprintf("CREATE (n:PipelineNode {seq: %d, run: 'pipeline_test'})", i),
  }
 }
 if err := pool.PipelineExec(context.Background(), stmts, 50, 4); err != nil {
  t.Fatalf("PipelineExec: %v", err)
 }

 rows, err := pool.QueryContext(context.Background(),
  "MATCH (n:PipelineNode {run: 'pipeline_test'}) RETURN COUNT(n) AS c")
 if err != nil {
  t.Fatalf("query: %v", err)
 }
 defer rows.Close()

 var count int
 if rows.Next() {
  _ = rows.Scan(&count)
 }
 if count != total {
  t.Errorf("expected %d PipelineNode rows, got %d", total, count)
 }
}
```

Replace `mustNewIntegrationPool(t)` with the actual helper in the file (look for `testDB` or a pool-creation helper). Adjust node labels/GQL syntax to match what the running test suite already uses if needed.

- [ ] **Step 11.2: Run integration tests**

```bash
go test -v -tags=integration -run "TestIntegration_BatchExec|TestIntegration_PipelineExec" ./...
```

Expected: all `PASS` (requires Docker).

- [ ] **Step 11.3: Commit**

```bash
git add integration_test.go
git commit -m "test(integration): add BatchExec and PipelineExec integration tests"
```

---

## Task 12: Final build, lint, and full test run

- [ ] **Step 12.1: Build all packages**

```bash
go build ./...
```

Expected: clean.

- [ ] **Step 12.2: Run all unit tests**

```bash
go test -v -short -count=1 ./...
```

Expected: all `PASS`. No skips except for known live-server tests.

- [ ] **Step 12.3: Run benchmarks (smoke check)**

```bash
go test -bench=. -benchmem -benchtime=1x ./...
```

Expected: benchmark output, no failures.

- [ ] **Step 12.4: Run linter**

```bash
golangci-lint run
```

Expected: no new lint errors. If any appear, fix them before the final commit.

- [ ] **Step 12.5: Run each new fuzz target briefly**

```bash
go test -fuzz=FuzzEncryptDecrypt -fuzztime=10s ./crypto/...
go test -fuzz=FuzzDecrypt         -fuzztime=10s ./crypto/...
go test -fuzz=FuzzScanTime        -fuzztime=10s ./...
go test -fuzz=FuzzParseSchemaFS   -fuzztime=10s ./...
```

Expected: no failures.

- [ ] **Step 12.6: Final commit (if any lint fixes were made)**

```bash
git add -p   # stage only lint fixes
git commit -m "chore: lint fixes from migration"
```

---

## Self-Review Checklist

| Requirement | Task |
|---|---|
| `crypto/cipher.go` migrated | Task 1 |
| Crypto unit tests | Task 2 |
| Crypto benchmarks | Task 3 |
| Crypto fuzz tests | Task 3 |
| `TimeScanner` / `ScanTime()` | Task 4 |
| `TimeScanner` tests | Task 4 |
| `FuzzScanTime` | Task 4 |
| Mock driver for unit tests | Task 5 |
| `Statement` type | Task 6 |
| `Pool.BatchExec` | Task 6 |
| `BatchExec` unit tests | Tasks 6–7 |
| `Pool.PipelineExec` | Task 6 |
| `PipelineExec` unit tests | Task 7 |
| `DefaultChunkSize` / `DefaultMaxWorkers` | Task 6 |
| Batch benchmarks | Task 8 |
| `ParseSchemaFS` | Task 9 |
| `ParseSchemaFS` tests | Task 9 |
| `FuzzParseSchemaFS` | Task 10 |
| Integration tests for batch/pipeline | Task 11 |
| Full build + lint pass | Task 12 |

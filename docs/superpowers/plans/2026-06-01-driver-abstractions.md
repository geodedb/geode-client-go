# Driver Abstractions: Move geode-ui Logic into the Driver

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move logic that belongs in the driver (GQL quoting, typed graph values, DSN serialization, error helpers, pool helpers) from geode-ui into geode-client-go, then update geode-ui to use the new driver exports.

**Architecture:** Phase 1 adds new exports to geode-client-go (10 min tests per task; driver builds independently); Phase 2 removes the duplicated logic from geode-ui and replaces it with calls to the driver. The `replace` directive in geode-ui's go.mod (`replace geodedb.com/geode => ../geode-client-go`) means driver changes are picked up immediately — no version bump or publish needed. Each phase produces a working, tested state on its own.

**Tech Stack:** Go 1.26+, `database/sql`, `geodedb.com/geode` (geode-client-go), `geodedb.com/geode-ui`. Both repos are in adjacent directories: `../geode-ui` relative to the driver root.

---

## Files Changed

### geode-client-go (Phase 1)

| File | Action | Responsibility |
|---|---|---|
| `quote.go` | **Create** | Exported GQL injection-safe quoting helpers: `SanitizeGraphName`, `QuoteIdent`, `QuoteString` |
| `types.go` | **Modify** | Add `Node`, `Edge`, `Path` structs + `AsNode`, `AsEdge`, `AsPath` type-assertion helpers |
| `config.go` | **Modify** | Add `Config.FormatDSN() string` — round-trip DSN serialization |
| `driver.go` | **Modify** | Add `NewConnector(cfg *Config) (*Connector, error)` — programmatic Connector construction |
| `errors.go` | **Modify** | Add `IsAuthError(err error) bool` and `IsSyntaxError(err error) bool` |
| `pool.go` | **Modify** | Add `NewPoolFromConfig`, `Pool.TX`, `RedactDSN` |
| `quote_test.go` | **Create** | Unit tests for GQL quoting helpers |
| `types_test.go` | **Modify** | Tests for `Node`/`Edge`/`Path` and `AsNode`/`AsEdge`/`AsPath` |

### geode-ui (Phase 2)

| File | Action | Responsibility |
|---|---|---|
| `internal/server/quoting.go` | **Delete** | Replaced by driver exports |
| `internal/server/handlers_auth.go` | **Modify** | Use `geode.IsAuthError`; use `Config.FormatDSN()` in `injectCreds` |
| `internal/server/errors.go` | **Modify** | Replace duck-typed `isWireFeatureMissing` with `geode.IsSyntaxError` |
| `internal/server/result_codec.go` | **Modify** | Replace `GraphNode`/`GraphEdge` with `geode.Node`/`geode.Edge` |

---

## Phase 1: geode-client-go Additions

---

### Task 1: Export GQL Quoting Helpers

**Context:** geode-ui has `internal/server/quoting.go` with `sanitizeGraphName`, `quoteIdent`, `quoteString`. The driver has an internal `gqlStringEscaper` in `conn.go:940` that does the same escaping. All three helpers should live in the driver so any application can safely inline GQL identifiers and strings without re-implementing injection prevention.

**Files:**

- Create: `quote.go`
- Create: `quote_test.go`

- [ ] **Step 1: Write failing tests**

Create `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/quote_test.go`:

```go
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
 ok := []string{"abc", "ABC", "_foo", "foo_bar", "foo-bar", "foo9"}
 for _, name := range ok {
  got, err := geode.QuoteIdent(name)
  if err != nil {
   t.Errorf("QuoteIdent(%q) unexpected error: %v", name, err)
  }
  if got != name {
   t.Errorf("QuoteIdent(%q) = %q, want %q", name, got, name)
  }
 }

 bad := []string{"", "9starts", "-starts", "has space", "has;semi", "has\x00null"}
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

 // Control characters must be rejected.
 controls := []string{"\n", "\r", "\t", "\x00", "\x1f", "\x7f"}
 for _, s := range controls {
  if _, err := geode.QuoteString(s); !errors.Is(err, geode.ErrInvalidString) {
   t.Errorf("QuoteString(%q) expected ErrInvalidString, got %v", s, err)
  }
 }
}
```

- [ ] **Step 2: Verify tests fail**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go
go test -run "TestSanitizeGraphName|TestQuoteIdent|TestQuoteString" ./...
```

Expected: compile error — `geode.SanitizeGraphName`, `geode.QuoteIdent`, `geode.QuoteString`, `geode.ErrInvalidString` undefined.

- [ ] **Step 3: Implement `quote.go`**

Create `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/quote.go`:

```go
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
//
// Geode graph names are alphanumeric/underscore/hyphen by convention.
// This function silently drops disallowed characters rather than
// returning an error, making it safe for use with user-supplied names
// in UI contexts where a partial match is preferable to an error.
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
// GQL admin statement that takes an identifier (CREATE USER, ALTER USER,
// GRANT ROLE, etc.).
//
// Geode v0.5.x does not support quoted (delimited) identifiers: the
// lexer's scanQuotedIdentifier code path is unreachable, and backtick
// is not in the dispatch table. The only safe form is a bare identifier
// matching [A-Za-z_][A-Za-z_0-9-]*.
//
// QuoteIdent validates this shape and returns the name verbatim. Any
// input outside the shape — whitespace, punctuation, control characters,
// multi-byte runes — is rejected with ErrInvalidIdent. Callers MUST
// route any user-supplied identifier through this helper before
// string-concatenating it into a GQL statement.
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
   // Digits allowed anywhere.
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

// QuoteString returns s wrapped in GQL single quotes with backslashes
// and single quotes properly escaped for safe embedding in a GQL
// single-quoted string literal.
//
// ASCII control characters (code points < 0x20 or == 0x7F) are
// rejected with ErrInvalidString because they cannot be safely included
// in a GQL literal.
//
// Escape order is load-bearing: backslash must be doubled (\→\\) BEFORE
// single quotes are doubled ('→''). Reversing the order would let a
// payload of backslash+quote escape the literal. This mirrors the
// internal gqlStringEscaper in conn.go.
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
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -v -run "TestSanitizeGraphName|TestQuoteIdent|TestQuoteString" ./...
```

Expected:

```
--- PASS: TestSanitizeGraphName (0.00s)
--- PASS: TestQuoteIdent (0.00s)
--- PASS: TestQuoteString (0.00s)
```

- [ ] **Step 5: Full suite regression check**

```bash
go test -short ./...
```

Expected: all packages `ok`.

- [ ] **Step 6: Commit**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go
git add quote.go quote_test.go
git commit -m "feat(quote): export SanitizeGraphName, QuoteIdent, QuoteString"
```

---

### Task 2: Export Typed Node / Edge / Path Structs

**Context:** When a Geode query returns a NODE, EDGE, or PATH column the driver delivers a `map[string]any` with implicit key contracts. geode-ui duplicates `GraphNode`, `GraphEdge`, and the `asNode`/`asEdge` helpers in `result_codec.go`. These belong in the driver as exported typed structs and type-assertion helpers so any application can work with graph values without re-implementing the key convention.

**Files:**

- Modify: `types.go`
- Create/Modify: `types_test.go`

- [ ] **Step 1: Write failing tests**

Create `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/types_test.go` (or add to existing):

```go
package geode_test

import (
 "testing"

 geode "geodedb.com/geode"
)

func TestAsNode(t *testing.T) {
 m := map[string]any{
  "id":         int64(42),
  "labels":     []any{"Person", "User"},
  "properties": map[string]any{"name": "Alice"},
 }

 n, ok := geode.AsNode(m)
 if !ok {
  t.Fatal("AsNode returned false for valid node map")
 }
 if n.ID != 42 {
  t.Errorf("Node.ID = %d, want 42", n.ID)
 }
 if len(n.Labels) != 2 || n.Labels[0] != "Person" {
  t.Errorf("Node.Labels = %v, want [Person User]", n.Labels)
 }
 if n.Properties["name"] != "Alice" {
  t.Errorf("Node.Properties[name] = %v, want Alice", n.Properties["name"])
 }

 // Non-node maps must return false.
 if _, ok := geode.AsNode(map[string]any{"id": int64(1), "start_node": int64(2)}); ok {
  t.Error("AsNode returned true for an edge map")
 }
 if _, ok := geode.AsNode(nil); ok {
  t.Error("AsNode returned true for nil")
 }
 if _, ok := geode.AsNode("not a map"); ok {
  t.Error("AsNode returned true for a string")
 }
}

func TestAsEdge(t *testing.T) {
 m := map[string]any{
  "id":         int64(7),
  "start_node": int64(1),
  "end_node":   int64(2),
  "type":       "KNOWS",
  "properties": map[string]any{"since": int64(2020)},
 }

 e, ok := geode.AsEdge(m)
 if !ok {
  t.Fatal("AsEdge returned false for valid edge map")
 }
 if e.ID != 7 || e.StartNode != 1 || e.EndNode != 2 {
  t.Errorf("Edge IDs = %d/%d/%d, want 7/1/2", e.ID, e.StartNode, e.EndNode)
 }
 if e.Type != "KNOWS" {
  t.Errorf("Edge.Type = %q, want KNOWS", e.Type)
 }

 // Node map must not match AsEdge.
 if _, ok := geode.AsEdge(map[string]any{"id": int64(1), "labels": []any{"N"}}); ok {
  t.Error("AsEdge returned true for a node map")
 }
}

func TestAsPath(t *testing.T) {
 m := map[string]any{
  "nodes": []any{
   map[string]any{"id": int64(1), "labels": []any{"A"}, "properties": map[string]any{}},
  },
  "edges": []any{},
 }

 p, ok := geode.AsPath(m)
 if !ok {
  t.Fatal("AsPath returned false for valid path map")
 }
 if len(p.Nodes) != 1 {
  t.Errorf("Path.Nodes len = %d, want 1", len(p.Nodes))
 }

 // A plain map without nodes/edges key must not match.
 if _, ok := geode.AsPath(map[string]any{"id": int64(1)}); ok {
  t.Error("AsPath returned true for a plain map")
 }
}
```

- [ ] **Step 2: Verify tests fail**

```bash
go test -run "TestAsNode|TestAsEdge|TestAsPath" ./...
```

Expected: compile error — `geode.Node`, `geode.Edge`, `geode.Path`, `geode.AsNode`, `geode.AsEdge`, `geode.AsPath` undefined.

- [ ] **Step 3: Add types to `types.go`**

Append to `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/types.go`:

```go
// Node is the typed representation of a Geode graph node value as
// returned by the driver when a GQL query projects a node variable
// (e.g. MATCH (n) RETURN n). The driver delivers nodes as
// map[string]any; use AsNode to convert.
type Node struct {
 ID         int64          `json:"id"`
 Labels     []string       `json:"labels"`
 Properties map[string]any `json:"properties"`
}

// Edge is the typed representation of a Geode graph edge (relationship)
// value. The driver delivers edges as map[string]any; use AsEdge to convert.
type Edge struct {
 ID         int64          `json:"id"`
 StartNode  int64          `json:"start_node"`
 EndNode    int64          `json:"end_node"`
 Type       string         `json:"type"`
 Properties map[string]any `json:"properties"`
}

// Path is the typed representation of a Geode graph path value, which
// contains the ordered sequence of nodes and edges along a traversal.
// The driver delivers paths as map[string]any; use AsPath to convert.
type Path struct {
 Nodes []Node `json:"nodes"`
 Edges []Edge `json:"edges"`
}

// AsNode converts a driver value (map[string]any with "id", "labels",
// "properties" keys as produced by a NODE column scan) into a typed Node.
// Returns (Node{}, false) if v is not a valid node map.
func AsNode(v any) (Node, bool) {
 m, ok := v.(map[string]any)
 if !ok {
  return Node{}, false
 }
 // Edges also have "id"; require "labels" to distinguish.
 if _, hasLabels := m["labels"]; !hasLabels {
  return Node{}, false
 }
 // Edges have "start_node"; nodes do not.
 if _, hasStart := m["start_node"]; hasStart {
  return Node{}, false
 }
 id, _ := toAnyInt64(m["id"])
 labels := toStringSlice(m["labels"])
 props, _ := m["properties"].(map[string]any)
 return Node{ID: id, Labels: labels, Properties: props}, true
}

// AsEdge converts a driver value (map[string]any with "id", "start_node",
// "end_node", "type", "properties" keys) into a typed Edge.
// Returns (Edge{}, false) if v is not a valid edge map.
func AsEdge(v any) (Edge, bool) {
 m, ok := v.(map[string]any)
 if !ok {
  return Edge{}, false
 }
 _, hasStart := m["start_node"]
 _, hasEnd := m["end_node"]
 if !hasStart || !hasEnd {
  return Edge{}, false
 }
 id, _ := toAnyInt64(m["id"])
 s, _ := toAnyInt64(m["start_node"])
 e, _ := toAnyInt64(m["end_node"])
 typ, _ := m["type"].(string)
 props, _ := m["properties"].(map[string]any)
 return Edge{ID: id, StartNode: s, EndNode: e, Type: typ, Properties: props}, true
}

// AsPath converts a driver value (map[string]any with "nodes" and "edges"
// keys) into a typed Path. Returns (Path{}, false) if v is not a valid path map.
func AsPath(v any) (Path, bool) {
 m, ok := v.(map[string]any)
 if !ok {
  return Path{}, false
 }
 nodesRaw, hasNodes := m["nodes"]
 _, hasEdges := m["edges"]
 if !hasNodes || !hasEdges {
  return Path{}, false
 }
 var nodes []Node
 if ns, ok := nodesRaw.([]any); ok {
  for _, n := range ns {
   if node, ok := AsNode(n); ok {
    nodes = append(nodes, node)
   }
  }
 }
 var edges []Edge
 if es, ok := m["edges"].([]any); ok {
  for _, e := range es {
   if edge, ok := AsEdge(e); ok {
    edges = append(edges, edge)
   }
  }
 }
 return Path{Nodes: nodes, Edges: edges}, true
}

// toAnyInt64 is a best-effort int64 coercion for driver values.
// The driver always returns int64 for integer types, but tolerate
// int and float64 for JSON-decoded paths.
func toAnyInt64(v any) (int64, bool) {
 switch x := v.(type) {
 case int64:
  return x, true
 case int:
  return int64(x), true
 case float64:
  if x == float64(int64(x)) {
   return int64(x), true
  }
 }
 return 0, false
}

// toStringSlice coerces a driver value into a []string,
// tolerating both []string and []any (JSON-decoded label arrays).
func toStringSlice(v any) []string {
 switch x := v.(type) {
 case []string:
  return x
 case []any:
  out := make([]string, 0, len(x))
  for _, item := range x {
   if s, ok := item.(string); ok {
    out = append(out, s)
   }
  }
  return out
 }
 return nil
}
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -v -run "TestAsNode|TestAsEdge|TestAsPath" ./...
```

Expected: all three tests PASS.

- [ ] **Step 5: Full suite regression check**

```bash
go test -short ./...
```

- [ ] **Step 6: Commit**

```bash
git add types.go types_test.go
git commit -m "feat(types): export Node, Edge, Path structs with AsNode/AsEdge/AsPath"
```

---

### Task 3: Add `Config.FormatDSN()`

**Context:** geode-ui's `injectCreds` function manually parses and rewrites the DSN string to embed credentials. The driver can own this via a `FormatDSN` method that serializes a `Config` back to a DSN, enabling programmatic credential injection without string manipulation.

**Files:**

- Modify: `config.go`
- Modify: `config_test.go` (already exists)

- [ ] **Step 1: Write failing test**

Add to `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/config_test.go`:

```go
func TestConfig_FormatDSN_RoundTrip(t *testing.T) {
 tests := []struct {
  dsn  string
 }{
  {"quic://localhost:3141"},
  {"grpc://db.example.com:50051?tls=0"},
  {"quic://localhost:3141?page_size=500&graph=mydb"},
 }
 for _, tc := range tests {
  cfg, err := ParseDSN(tc.dsn)
  if err != nil {
   t.Fatalf("ParseDSN(%q): %v", tc.dsn, err)
  }
  formatted := cfg.FormatDSN()
  cfg2, err := ParseDSN(formatted)
  if err != nil {
   t.Fatalf("ParseDSN(FormatDSN(%q)) failed: %v — got: %q", tc.dsn, err, formatted)
  }
  if cfg.Host != cfg2.Host || cfg.Port != cfg2.Port || cfg.Transport != cfg2.Transport {
   t.Errorf("FormatDSN round-trip mismatch for %q: got %q", tc.dsn, formatted)
  }
 }
}

func TestConfig_FormatDSN_Credentials(t *testing.T) {
 cfg := &Config{
  Transport: TransportQUIC,
  Host:      "localhost",
  Port:      DefaultPort,
  Username:  "alice",
  Password:  "secret",
 }
 dsn := cfg.FormatDSN()
 if !strings.Contains(dsn, "alice") || !strings.Contains(dsn, "secret") {
  t.Errorf("FormatDSN did not include credentials: %q", dsn)
 }
 // Parsed back, credentials must survive.
 cfg2, err := ParseDSN(dsn)
 if err != nil {
  t.Fatalf("ParseDSN(FormatDSN): %v", err)
 }
 if cfg2.Username != "alice" || cfg2.Password != "secret" {
  t.Errorf("credential round-trip failed: got user=%q pass=%q", cfg2.Username, cfg2.Password)
 }
}
```

(Add `"strings"` to the imports of config_test.go if not already present.)

- [ ] **Step 2: Verify tests fail**

```bash
go test -run "TestConfig_FormatDSN" ./...
```

Expected: compile error — `cfg.FormatDSN` undefined.

- [ ] **Step 3: Implement `Config.FormatDSN()`**

Add to `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/config.go` (after the `Clone()` method):

```go
// FormatDSN serialises the Config back into a DSN string that ParseDSN
// can round-trip. It is the inverse of ParseDSN.
//
// Only non-default values are emitted as query parameters so the
// resulting DSN is minimal. Credentials are embedded in the userinfo
// segment (scheme://user:pass@host:port) which ParseDSN already handles.
func (c *Config) FormatDSN() string {
 scheme := "quic"
 if c.Transport == TransportGRPC {
  scheme = "grpc"
 }

 host := net.JoinHostPort(c.Host, c.Port)

 var userinfo string
 if c.Username != "" && c.Password != "" {
  userinfo = url.UserPassword(c.Username, c.Password).String() + "@"
 } else if c.Username != "" {
  userinfo = url.User(c.Username).String() + "@"
 }

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

 dsn := scheme + "://" + userinfo + host
 if len(q) > 0 {
  dsn += "?" + q.Encode()
 }
 return dsn
}
```

Ensure `config.go` imports `"net"`, `"net/url"`, and `"strconv"` — check existing imports first and add only what is missing.

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -v -run "TestConfig_FormatDSN" ./...
```

- [ ] **Step 5: Full suite regression check**

```bash
go test -short ./...
```

- [ ] **Step 6: Commit**

```bash
git add config.go config_test.go
git commit -m "feat(config): add Config.FormatDSN() for round-trip DSN serialization"
```

---

### Task 4: Add `NewConnector(cfg *Config)`

**Context:** The `Connector` type is exported but has no public constructor. Apps holding a programmatically assembled `*Config` cannot call `sql.OpenDB(connector)` (the preferred `database/sql` pattern) without first serializing to a DSN string. `NewConnector` closes this gap.

**Files:**

- Modify: `driver.go`
- Modify (or create): `driver_test.go`

- [ ] **Step 1: Write failing test**

Add to `driver_test.go` (or create if absent):

```go
package geode_test

import (
 "testing"

 geode "geodedb.com/geode"
)

func TestNewConnector_ValidConfig(t *testing.T) {
 cfg := &geode.Config{
  Transport: geode.TransportQUIC,
  Host:      "localhost",
  Port:      geode.DefaultPort,
 }
 c, err := geode.NewConnector(cfg)
 if err != nil {
  t.Fatalf("NewConnector: %v", err)
 }
 if c == nil {
  t.Fatal("NewConnector returned nil")
 }
 got := c.Config()
 if got.Host != "localhost" {
  t.Errorf("Connector.Config().Host = %q, want localhost", got.Host)
 }
}

func TestNewConnector_InvalidConfig(t *testing.T) {
 // Empty host must fail validation.
 cfg := &geode.Config{Transport: geode.TransportQUIC, Port: geode.DefaultPort}
 if _, err := geode.NewConnector(cfg); err == nil {
  t.Fatal("NewConnector with empty host expected error, got nil")
 }
}
```

- [ ] **Step 2: Verify tests fail**

```bash
go test -run "TestNewConnector" ./...
```

Expected: compile error — `geode.NewConnector` undefined.

- [ ] **Step 3: Implement `NewConnector`**

Add to `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/driver.go` (after the `OpenConnector` method):

```go
// NewConnector creates a Connector from a programmatically assembled Config
// without requiring a DSN string. This is the preferred constructor when
// connection parameters are known at compile time or assembled from
// structured config (environment variables, config files, UI inputs).
//
// The returned Connector can be passed to sql.OpenDB to obtain a *sql.DB:
//
// c, err := geode.NewConnector(&geode.Config{Host: "localhost", Port: "3141", ...})
// db := sql.OpenDB(c)
func NewConnector(cfg *Config) (*Connector, error) {
 if err := cfg.Validate(); err != nil {
  return nil, err
 }
 c := &Connector{
  driver: &defaultDriver,
  cfg:    cfg.Clone(),
 }
 if cfg.Transport == TransportQUIC {
  size := cfg.PoolSize
  if size <= 0 {
   size = DefaultQUICPoolSize
  }
  c.quicPool = NewQUICPool(cfg, size)
 }
 return c, nil
}
```

Find the registered `defaultDriver` variable name in `driver.go` — it is referenced in `driver.go:init()` as `sql.Register("geode", &Driver{})`. The `Connector.driver` field must point to the same `Driver` instance. Check how `OpenConnector` sets `c.driver` and use the same pattern. If the driver uses a package-level `var defaultDriver Driver` then reference `&defaultDriver`; if it creates a new `&Driver{}` in `OpenConnector`, do the same here.

**Correction after reading driver.go:106-134:** `OpenConnector` sets `c.driver = d` (the receiver `*Driver`). The `init()` function registers `&Driver{}`. There is no named `defaultDriver` variable; the registered driver is heap-allocated and there is no exported reference to it. The safest approach for `NewConnector` is to create a fresh `&Driver{}`:

```go
c := &Connector{
    driver: &Driver{},
    cfg:    cfg.Clone(),
}
```

This is consistent because `Connector.Driver()` returns the driver only for `sql.OpenDB`'s internal use; callers never compare driver identity.

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -v -run "TestNewConnector" ./...
```

- [ ] **Step 5: Full suite regression check**

```bash
go test -short ./...
```

- [ ] **Step 6: Commit**

```bash
git add driver.go driver_test.go
git commit -m "feat(driver): export NewConnector(cfg) for programmatic connector construction"
```

---

### Task 5: Add `IsAuthError` and `IsSyntaxError` Helpers

**Context:** geode-ui duck-types the driver's `DriverError` through an unexported `statusClasser` interface in two places: `isAuthError` (handlers_auth.go) and `isWireFeatureMissing` (errors.go). Both are driver-specific classifications that belong in the driver alongside the existing `IsDriverError` and `IsRetryableError` helpers.

`StatusAuth = "28000"` and `StatusSyntax = "42000"` are already exported from the driver.

**Files:**

- Modify: `errors.go`
- Modify: `errors_test.go`

- [ ] **Step 1: Write failing tests**

Add to `errors_test.go`:

```go
func TestIsAuthError(t *testing.T) {
 authErr := &geode.DriverError{Code: "28000: invalid_authorization_specification"}
 // Set the class field via Code prefix — note: DriverError.Class is the
 // first 5 chars of Code if Code matches "NNNNN: ..." format.
 // Actually construct using StatusClass matching:
 _ = authErr

 // Use a DriverError whose StatusClass() returns "28000".
 de := &geode.DriverError{}
 // DriverError.StatusClass() returns the first 5 chars of Code.
 // Set Code so StatusClass() == "28000".
 de2 := &geode.DriverError{}
 _ = de
 _ = de2

 // Simpler: verify via errors.As that IsAuthError recognises the driver type.
 // Wrap in a fmt.Errorf to test unwrapping.
 var wrapped error = fmt.Errorf("probe: %w", makeDriverError("28000"))
 if !geode.IsAuthError(wrapped) {
  t.Error("IsAuthError(28000 wrapped) = false, want true")
 }
 if geode.IsAuthError(fmt.Errorf("probe: %w", makeDriverError("42000"))) {
  t.Error("IsAuthError(42000) = true, want false")
 }
 if geode.IsAuthError(nil) {
  t.Error("IsAuthError(nil) = true, want false")
 }
 if geode.IsAuthError(errors.New("not a driver error")) {
  t.Error("IsAuthError(generic error) = true, want false")
 }
}

func TestIsSyntaxError(t *testing.T) {
 if !geode.IsSyntaxError(fmt.Errorf("wrap: %w", makeDriverError("42000"))) {
  t.Error("IsSyntaxError(42000 wrapped) = false, want true")
 }
 if geode.IsSyntaxError(fmt.Errorf("wrap: %w", makeDriverError("28000"))) {
  t.Error("IsSyntaxError(28000) = true, want false")
 }
 if geode.IsSyntaxError(nil) {
  t.Error("IsSyntaxError(nil) = true, want false")
 }
}

// makeDriverError creates a *DriverError whose StatusClass() returns the
// given class string. DriverError.StatusClass() returns e.Code[:5] when
// len(Code) >= 5; set Code accordingly.
func makeDriverError(class string) *geode.DriverError {
 return &geode.DriverError{Code: class + ": test error"}
}
```

Add `"errors"` and `"fmt"` to the import block of `errors_test.go` if not present, and make the test file `package geode_test`.

**Verify DriverError.Code field:** Check `errors.go` to confirm `DriverError` has an exported `Code string` field, and that `StatusClass()` returns `e.Code[:5]` (or the first five characters). If `DriverError` uses `Class` and `Subclass` fields instead, adjust `makeDriverError` to set `Class: class`.

From the driver's errors.go (lines 14-17 and 73-76):

```go
// StatusAuth = "28000"
// StatusSyntax = "42000"
func (e *DriverError) StatusClass() string { return e.Code[:5] } // approximate
```

Read the actual implementation before writing `makeDriverError`. The exact fields are: look at `errors.go` in the driver.

- [ ] **Step 2: Read DriverError definition**

```bash
grep -n "type DriverError\|Code\s\|Class\s\|StatusClass" \
  /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go/errors.go | head -20
```

Use the actual fields to write `makeDriverError` correctly. If `DriverError.StatusClass()` returns `e.Class`, set `Class: class` in `makeDriverError`. If it reads from `Code`, set `Code: class + ": test error"`.

- [ ] **Step 3: Verify tests fail**

```bash
go test -run "TestIsAuthError|TestIsSyntaxError" ./...
```

Expected: compile error — `geode.IsAuthError`, `geode.IsSyntaxError` undefined.

- [ ] **Step 4: Implement helpers in `errors.go`**

Add after the existing `IsRetryableError` function in `errors.go`:

```go
// IsAuthError reports whether err is a driver-level authorization failure
// (GQL status class 28000 — invalid_authorization_specification, or the
// Geode-specific 08P01 password-reset-required status). Returns false for
// nil and for errors that do not carry a driver status class.
func IsAuthError(err error) bool {
 if err == nil {
  return false
 }
 var de *DriverError
 if !errors.As(err, &de) {
  return false
 }
 switch de.StatusClass() {
 case StatusAuth, // "28000"
  "08P01": // password reset required (Geode-specific extension)
  return true
 }
 return false
}

// IsSyntaxError reports whether err is a GQL parse/syntax error (status
// class 42000). The Geode engine returns 42000 for unrecognised statements,
// including admin DDL verbs that haven't shipped on the connected server
// build yet. Handlers can use this to degrade gracefully rather than
// surfacing a raw error.
func IsSyntaxError(err error) bool {
 if err == nil {
  return false
 }
 var de *DriverError
 if !errors.As(err, &de) {
  return false
 }
 return de.StatusClass() == StatusSyntax // "42000"
}
```

- [ ] **Step 5: Fix `makeDriverError` if needed**

After reading the DriverError field structure (step 2), adjust `makeDriverError` in the test if necessary. For example, if `StatusClass()` returns `e.Class` (not a slice of `Code`):

```go
func makeDriverError(class string) *geode.DriverError {
 return &geode.DriverError{Class: class, Message: "test error"}
}
```

- [ ] **Step 6: Run tests — expect PASS**

```bash
go test -v -run "TestIsAuthError|TestIsSyntaxError" ./...
```

- [ ] **Step 7: Full suite regression check**

```bash
go test -short ./...
```

- [ ] **Step 8: Commit**

```bash
git add errors.go errors_test.go
git commit -m "feat(errors): export IsAuthError and IsSyntaxError helpers"
```

---

### Task 6: Add `NewPoolFromConfig`, `Pool.TX`, and `RedactDSN`

**Context:**

- `NewPool` only accepts a DSN string; apps with a programmatic `*Config` must serialize first.
- `Pool` has `BeginTx` but no functional transaction wrapper; the `graph.Conn.TX` pattern should also live on `Pool`.
- `internal/redact.DSN` is unexported; any app logging DSNs must re-implement redaction.

**Files:**

- Modify: `pool.go`
- Modify: `pool_test.go` (or create)

- [ ] **Step 1: Write failing tests**

Add to `pool_test.go` (or create as `package geode_test`):

```go
func TestNewPoolFromConfig_InvalidConfig(t *testing.T) {
 cfg := &geode.Config{Transport: geode.TransportQUIC} // empty host
 if _, err := geode.NewPoolFromConfig(cfg); err == nil {
  t.Fatal("NewPoolFromConfig with invalid config: expected error, got nil")
 }
}

func TestRedactDSN(t *testing.T) {
 tests := []struct {
  in   string
  want string // must NOT contain actual credentials
 }{
  {
   "quic://alice:secret@localhost:3141",
   // credentials replaced with ***
  },
  {
   "quic://localhost:3141?user=alice&pass=secret&graph=mydb",
   // query param creds masked
  },
  {"quic://localhost:3141", "quic://localhost:3141"},
  {"", ""},
 }
 for _, tc := range tests {
  got := geode.RedactDSN(tc.in)
  if strings.Contains(got, "alice") || strings.Contains(got, "secret") {
   t.Errorf("RedactDSN(%q) still contains credentials: %q", tc.in, got)
  }
  if tc.in != "" && tc.want != "" && got != tc.want {
   t.Errorf("RedactDSN(%q) = %q, want %q", tc.in, got, tc.want)
  }
 }
}
```

- [ ] **Step 2: Verify tests fail**

```bash
go test -run "TestNewPoolFromConfig_InvalidConfig|TestRedactDSN" ./...
```

Expected: compile error — `geode.NewPoolFromConfig`, `geode.RedactDSN` undefined.

- [ ] **Step 3: Implement in `pool.go`**

Add to `pool.go`:

```go
// NewPoolFromConfig creates a connection pool from a programmatically
// assembled Config, without requiring a DSN string.
// The pool is immediately validated with a ping.
func NewPoolFromConfig(cfg *Config, opts ...PoolOption) (*Pool, error) {
 c, err := NewConnector(cfg)
 if err != nil {
  return nil, err
 }

 pc := defaultPoolConfig()
 for _, opt := range opts {
  opt(&pc)
 }

 db := sql.OpenDB(c)
 db.SetMaxOpenConns(pc.maxOpenConns)
 db.SetMaxIdleConns(pc.maxIdleConns)
 db.SetConnMaxLifetime(pc.connMaxLifetime)
 db.SetConnMaxIdleTime(pc.connMaxIdleTime)

 ctx, cancel := context.WithTimeout(context.Background(), DefaultPingTimeout)
 defer cancel()
 if err := db.PingContext(ctx); err != nil {
  db.Close()
  return nil, err
 }

 return &Pool{db: db, cfg: cfg.Clone()}, nil
}

// TX executes fn inside a transaction with the given options.
// If fn returns nil, the transaction is committed. If fn returns a non-nil
// error or panics, the transaction is rolled back and the error (or a
// recovered panic) is returned.
//
// tx.Rollback is always called on the deferred path; database/sql silently
// ignores Rollback after a successful Commit, so it is always safe.
func (p *Pool) TX(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) (retErr error) {
 tx, err := p.db.BeginTx(ctx, opts)
 if err != nil {
  return err
 }
 defer func() {
  if rec := recover(); rec != nil {
   _ = tx.Rollback()
   panic(rec)
  }
  if retErr != nil {
   _ = tx.Rollback()
  }
 }()
 if err := fn(tx); err != nil {
  return err
 }
 return tx.Commit()
}

// RedactDSN returns the DSN with any credentials masked, suitable for
// safe logging. It handles URI userinfo (user:pass@host) and query
// parameter credentials (?user=x&pass=y).
func RedactDSN(dsn string) string {
 return redact.DSN(dsn)
}
```

Add the import `"geodedb.com/geode/internal/redact"` to `pool.go`'s import block.

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -v -run "TestNewPoolFromConfig_InvalidConfig|TestRedactDSN" ./...
```

- [ ] **Step 5: Full suite regression check**

```bash
go test -short ./...
```

- [ ] **Step 6: Build check**

```bash
go build ./...
```

- [ ] **Step 7: Commit**

```bash
git add pool.go pool_test.go
git commit -m "feat(pool): add NewPoolFromConfig, Pool.TX, and RedactDSN"
```

---

## Phase 2: geode-ui Updates

All commands in Phase 2 run from `/home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-ui`.

---

### Task 7: Replace `quoting.go` with Driver Exports

**Context:** `internal/server/quoting.go` re-implements what the driver now exports: `sanitizeGraphName` → `geode.SanitizeGraphName`, `quoteIdent` → `geode.QuoteIdent`, `quoteString` → `geode.QuoteString`. The file can be deleted and callers updated to use driver imports.

**Files:**

- Delete: `internal/server/quoting.go`
- Modify: all callers of `sanitizeGraphName`, `quoteIdent`, `quoteString`, `errInvalidIdent`, `errInvalidString`

- [ ] **Step 1: Find all callers**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-ui
grep -rn "sanitizeGraphName\|quoteIdent\|quoteString\|errInvalidIdent\|errInvalidString" \
  internal/server/ --include="*.go"
```

Record every file and line number. Expected callers include:

- `handlers_query.go` — `sanitizeGraphName`
- `handlers_tx.go` — `sanitizeGraphName`
- `ws_query.go` — `sanitizeGraphName`
- `handlers_schema.go` — `sanitizeGraphName`, `quoteIdent`
- Multiple `handlers_*.go` — `quoteIdent`, `quoteString`, `errInvalidIdent`, `errInvalidString`

- [ ] **Step 2: Update each caller**

For each file identified in Step 1, replace the local helper call with the driver equivalent:

| Old call | New call |
|---|---|
| `sanitizeGraphName(x)` | `geode.SanitizeGraphName(x)` |
| `quoteIdent(x)` | `geode.QuoteIdent(x)` |
| `quoteString(x)` | `geode.QuoteString(x)` |
| `errInvalidIdent` | `geode.ErrInvalidIdent` |
| `errInvalidString` | `geode.ErrInvalidString` |

Add `geode "geodedb.com/geode"` to each file's import block (use a named import to avoid collision with package-local names).

- [ ] **Step 3: Delete `quoting.go`**

```bash
git rm internal/server/quoting.go
```

- [ ] **Step 4: Build check**

```bash
go build ./...
```

Expected: no errors. If there are "undefined" errors, Step 1 missed a caller — fix them.

- [ ] **Step 5: Run unit tests**

```bash
go test -short ./...
```

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "refactor(server): replace local quoting helpers with geode.QuoteIdent/QuoteString/SanitizeGraphName"
```

---

### Task 8: Replace Duck-Typed `isAuthError` / `isWireFeatureMissing` with Driver Helpers

**Context:** Both `isAuthError` (in `handlers_auth.go`) and `isWireFeatureMissing` (in `errors.go`) duck-type `DriverError.StatusClass()` through an unexported `statusClasser` interface, plus have fragile substring fallbacks. The driver now exports `IsAuthError` and `IsSyntaxError`; the UI functions become thin wrappers.

**Files:**

- Modify: `internal/server/handlers_auth.go`
- Modify: `internal/server/errors.go`

- [ ] **Step 1: Update `isAuthError` in `handlers_auth.go`**

Find `isAuthError` at approximately line 468 of `handlers_auth.go`. Replace the entire function body with a delegation to the driver:

```go
// isAuthError reports whether err is a Geode authentication failure.
// Delegates to geode.IsAuthError which checks for status class 28000
// (invalid_authorization_specification) and 08P01 (password reset required).
func isAuthError(err error) bool {
 return geode.IsAuthError(err)
}
```

Remove the `strings` import from `handlers_auth.go` if it is no longer used after this change (check whether `strings` is referenced elsewhere in the file first).

Add `geode "geodedb.com/geode"` to the import block of `handlers_auth.go` if not already present.

- [ ] **Step 2: Update `isWireFeatureMissing` in `errors.go`**

The `isWireFeatureMissing` function (approx line 130) has three detection tiers: explicit sentinel, typed StatusClass, and substring fallback. The typed-StatusClass tier is now `geode.IsSyntaxError`. Update tier 2 to use it while preserving tier 1 (explicit `errWireFeatureMissing` sentinel) and tier 3 (substring fallback for drivers without typed errors):

```go
func isWireFeatureMissing(err error) bool {
 if err == nil {
  return false
 }
 // Tier 1: explicit sentinel wrap.
 if errors.Is(err, errWireFeatureMissing) {
  return true
 }
 // Tier 2: driver-typed syntax error (status class 42000).
 if geode.IsSyntaxError(err) {
  return true
 }
 // Tier 3: substring fallback for pre-typed driver versions.
 msg := strings.ToLower(err.Error())
 if !strings.Contains(msg, "42000") {
  return false
 }
 for _, sub := range wireFeatureMissingSubstrings {
  if strings.Contains(msg, sub) {
   return true
  }
 }
 return false
}
```

Add `geode "geodedb.com/geode"` to `errors.go`'s imports.

- [ ] **Step 3: Build check**

```bash
go build ./...
```

- [ ] **Step 4: Run tests**

```bash
go test -short ./...
```

- [ ] **Step 5: Commit**

```bash
git add internal/server/handlers_auth.go internal/server/errors.go
git commit -m "refactor(server): use geode.IsAuthError and geode.IsSyntaxError instead of duck-typed interface checks"
```

---

### Task 9: Use `geode.Node` / `geode.Edge` in `result_codec.go`

**Context:** `result_codec.go` defines `GraphNode`, `GraphEdge`, `GraphSummary`, plus `asNode`, `asEdge`, `nodeID`, `edgeID`, `toInt64`, `asStringSlice` helpers. The `GraphNode` and `GraphEdge` struct shapes are identical to the driver's new `Node` and `Edge` types (same field names and JSON tags). Replace the local types and helpers with the driver's exports; keep `GraphSummary` local since it's a UI-level aggregation concept (not a GQL path).

**Files:**

- Modify: `internal/server/result_codec.go`

- [ ] **Step 1: Audit the change surface**

```bash
grep -n "GraphNode\|GraphEdge\|asNode\|asEdge\|nodeID\|edgeID\|toInt64\|asStringSlice" \
  /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-ui/internal/server/result_codec.go
```

Record all occurrences. The helpers `toInt64` and `asStringSlice` are used by `asNode` and `asEdge`; the driver's `AsNode`/`AsEdge` helpers encapsulate them so they do not need to be called directly.

- [ ] **Step 2: Replace type definitions and helpers**

In `result_codec.go`, make the following changes:

**a. Change `GraphSummary` to use driver types:**

```go
// GraphSummary is the deduplicated set of nodes and edges the UI needs to
// render a graph view alongside the tabular result.
type GraphSummary struct {
 Nodes []geode.Node `json:"nodes"`
 Edges []geode.Edge `json:"edges"`
}
```

**b. Delete the local `GraphNode` and `GraphEdge` struct definitions** (they're replaced by `geode.Node` and `geode.Edge`).

**c. Update `graphSummaryAccum`:**

```go
type graphSummaryAccum struct {
 seenNode map[int64]struct{}
 seenEdge map[int64]struct{}
 out      GraphSummary
}
```

(No change needed here since `GraphSummary` now uses `geode.Node`/`geode.Edge`.)

**d. Update `releaseGraphSummaryAccum` to use driver types:**

The zero-clearing loop currently does:

```go
a.out.Nodes[i] = GraphNode{}
a.out.Edges[i] = GraphEdge{}
```

Change to:

```go
a.out.Nodes[i] = geode.Node{}
a.out.Edges[i] = geode.Edge{}
```

**e. Replace `asNode`, `asEdge`, `nodeID`, `edgeID` with driver calls in `visit`:**

Current `visit` body (approx lines 666-694):

```go
if id, ok := nodeID(t); ok {
    if _, dup := a.seenNode[id]; !dup {
        a.out.Nodes = append(a.out.Nodes, asNode(t))
        a.seenNode[id] = struct{}{}
    }
}
if id, ok := edgeID(t); ok {
    if _, dup := a.seenEdge[id]; !dup {
        a.out.Edges = append(a.out.Edges, asEdge(t))
        a.seenEdge[id] = struct{}{}
    }
}
```

Replace with:

```go
if n, ok := geode.AsNode(t); ok {
    if _, dup := a.seenNode[n.ID]; !dup {
        a.out.Nodes = append(a.out.Nodes, n)
        a.seenNode[n.ID] = struct{}{}
    }
}
if e, ok := geode.AsEdge(t); ok {
    if _, dup := a.seenEdge[e.ID]; !dup {
        a.out.Edges = append(a.out.Edges, e)
        a.seenEdge[e.ID] = struct{}{}
    }
}
```

**f. Delete the now-unused local functions** `asNode`, `asEdge`, `nodeID`, `edgeID`. Also delete `toInt64` and `asStringSlice` if they are no longer referenced after this change (check with `grep` first; they may be used elsewhere in the file for non-graph purposes).

**g. Add driver import** to `result_codec.go`:

```go
geode "geodedb.com/geode"
```

- [ ] **Step 3: Build check**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-ui
go build ./...
```

Fix any remaining references to the deleted local types/functions.

- [ ] **Step 4: Run tests**

```bash
go test -short ./...
```

- [ ] **Step 5: Commit**

```bash
git add internal/server/result_codec.go
git commit -m "refactor(result_codec): use geode.Node/Edge from driver; remove duplicated graph type helpers"
```

---

### Task 10: Replace `injectCreds` String Manipulation with `Config.FormatDSN()`

**Context:** `handlers_auth.go:injectCreds` manually rewrites the DSN URL to inject credentials. With `geode.ParseDSN` + setting `Config.Username`/`Config.Password` + `cfg.FormatDSN()`, this becomes trivial and removes manual string parsing from the UI.

**Files:**

- Modify: `internal/server/handlers_auth.go`

- [ ] **Step 1: Replace `injectCreds`**

Find `injectCreds` at approximately line 547 of `handlers_auth.go`. Replace the entire function:

```go
// injectCreds rewrites a Geode DSN to carry the supplied username and
// password. Delegates to geode.ParseDSN and Config.FormatDSN for
// correctness.
func injectCreds(dsn, username, password string) string {
 cfg, err := geode.ParseDSN(dsn)
 if err != nil {
  // If DSN can't be parsed (test stub DSN without ://), return as-is.
  return dsn
 }
 cfg.Username = username
 cfg.Password = password
 return cfg.FormatDSN()
}
```

Remove the `"net/url"` and `"strings"` imports from `handlers_auth.go` if they are no longer used after this and the Task 8 changes (check with `grep` first).

- [ ] **Step 2: Build check**

```bash
go build ./...
```

- [ ] **Step 3: Run tests**

```bash
go test -short ./...
```

- [ ] **Step 4: Commit**

```bash
git add internal/server/handlers_auth.go
git commit -m "refactor(auth): replace injectCreds string manipulation with Config.FormatDSN()"
```

---

## Final Validation

After all tasks:

- [ ] **Driver final check:**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go
go build ./...
go test -short ./...
golangci-lint run
```

- [ ] **geode-ui final check:**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-ui
go build ./...
go test -short ./...
golangci-lint run
```

- [ ] **Integration smoke test (if Docker available):**

```bash
cd /home/benji/src/gl/gitlab.com/devnw/codepros/geode/geode-client-go
go test -v -tags=integration -run "TestDocker_NoMatch|TestDocker_NullValues|TestDocker_NonExistentProperty" ./...
```

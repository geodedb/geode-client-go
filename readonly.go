package geode

import (
	"errors"
	"fmt"
	"strings"
)

// blockCommentOpen and blockCommentClose delimit a GQL block comment.
const (
	blockCommentOpen  = "/*"
	blockCommentClose = "*/"
)

// ErrReadOnly is returned when a mutating statement is submitted on a
// connection configured with read_only=true. It mirrors the server's
// GQLSTATE 42501 refusal (ExecuteRequest.read_only, proto field 6) but fires
// before any bytes reach the wire.
var ErrReadOnly = errors.New("geode: connection is read-only")

// guardReadOnly refuses a mutating statement on a read-only connection before
// any bytes reach the wire.
//
// This is the client half of a two-layer gate. The other half is unconditional:
// every ExecuteRequest this connection sends carries
// ExecuteRequest.read_only = Config.ReadOnly, so a statement this classifier
// lets through is still refused by the engine with GQLSTATE 42501, and that
// refusal holds regardless of the principal's role
// (geode 0.8.15, src/server/quic_connection_handler.zig:6493-6496).
//
// The refusal emits nothing, so it is safe inside an open transaction: the
// stream stays in sync, the server-side transaction is untouched, and the
// caller may keep reading on it and then Commit or Rollback as normal.
//
// Callers must hold c.mu.
func (c *Conn) guardReadOnly(query string) error {
	if c.cfg == nil || !c.cfg.ReadOnly {
		return nil
	}
	if !IsMutatingStatement(query) {
		return nil
	}
	return fmt.Errorf("%w: mutating statement refused before the wire", ErrReadOnly)
}

// readOnlyLeadingKeywords are the statement-leading keywords that can never
// mutate state. Anything not on this list is treated as mutating.
//
// Fail-closed by construction: an unparseable, empty, or unrecognised
// statement is mutating. A read-only retrieval path must never fail open, so
// this list only ever shrinks under review, never grows speculatively.
//
// CALL is deliberately absent: it is classified by procedure target
// (readOnlyProcedures) rather than by keyword.
//
//nolint:gochecknoglobals // package-level lookup table avoids per-call allocation
var readOnlyLeadingKeywords = map[string]bool{
	"MATCH":        true,
	"RETURN":       true,
	"SHOW":         true,
	keywordExplain: true,
	keywordProfile: true,
	"FOR":          true,
	"LET":          true,
	"FILTER":       true,
	"ORDER":        true,
	"SKIP":         true,
	"LIMIT":        true,
	"OPTIONAL":     true,
}

// mutatingKeywords are keywords that make a statement mutating no matter
// where they appear (a read-only-looking MATCH prefix does not launder a
// trailing SET/DELETE, and WITH ... CREATE is still a write).
//
//nolint:gochecknoglobals // package-level lookup table avoids per-call allocation
var mutatingKeywords = []string{
	"CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE", "DROP", "ALTER",
	"INSERT", "UPDATE", "GRANT", "REVOKE", "USE", "TRUNCATE", "BACKUP",
	"RESTORE", "REBUILD", "ANALYZE",
}

// readOnlyProcedures is the closed allow-list of CALL targets that cannot
// write, keyed by the procedure's uppercased dotted name.
//
// It mirrors the engine's own read_only_procedure_names table
// (geode 0.8.15, src/execution/procedure_access.zig:20-129), which
// classifyCallProcedure consults with the same "unlisted means .mutating"
// default. Wildcards are deliberately NOT used: `geode.fts.search` is a read
// but `geode.fts.rebuild` drops and rebuilds a fulltext engine, and
// `geode.ts.range` is a read but `geode.ts.insert` is a write, so a
// `geode.fts.*` or `geode.ts.*` prefix rule would fail open on exactly the
// entries the engine is careful to exclude.
//
// TestIsMutatingStatement_CallAllowList pins every entry here against that
// table.
//
//nolint:gochecknoglobals // package-level lookup table avoids per-call allocation
var readOnlyProcedures = map[string]bool{
	// Graph algorithms (streaming readers) and their short aliases.
	"ALGO.DEGREE.STREAM":                true,
	"DEGREE":                            true,
	"ALGO.BETWEENNESS.STREAM":           true,
	"BETWEENNESS":                       true,
	"ALGO.CLOSENESS.STREAM":             true,
	"CLOSENESS":                         true,
	"ALGO.EIGENVECTOR.STREAM":           true,
	"EIGENVECTOR":                       true,
	"ALGO.PAGERANK.STREAM":              true,
	"PAGERANK":                          true,
	"ALGO.LOUVAIN.STREAM":               true,
	"LOUVAIN":                           true,
	"ALGO.LABELPROPAGATION.STREAM":      true,
	"LABELPROPAGATION":                  true,
	"ALGO.UNIONFIND.STREAM":             true,
	"UNIONFIND":                         true,
	"ALGO.SCC.STREAM":                   true,
	"SCC":                               true,
	"ALGO.HARMONIC.STREAM":              true,
	"HARMONIC":                          true,
	"ALGO.BRIDGELEVERAGE.STREAM":        true,
	"BRIDGELEVERAGE":                    true,
	"ALGO.GREYCARDINAL.STREAM":          true,
	"GREYCARDINAL":                      true,
	"ALGO.LEIDEN.STREAM":                true,
	"LEIDEN":                            true,
	"ALGO.ARTICULATIONPOINTS.STREAM":    true,
	"ARTICULATIONPOINTS":                true,
	"ALGO.BRIDGES.STREAM":               true,
	"BRIDGES":                           true,
	"ALGO.BICONNECTEDCOMPONENTS.STREAM": true,
	"BICONNECTED":                       true,
	"ALGO.BFS.STREAM":                   true,
	"BFS":                               true,
	"ALGO.DFS.STREAM":                   true,
	"DFS":                               true,
	"ALGO.DIJKSTRA.STREAM":              true,
	"ALGO.SHORTESTPATH.DIJKSTRA":        true,
	"DIJKSTRA":                          true,

	// Storage / FLE / auth introspection.
	"GEODE.STORAGE.STATS":           true,
	"GEODE.STORAGE.INTEGRITY":       true,
	"GEODE.FLE.GETFIELDCONFIG":      true,
	"GEODE.FLE.LISTENCRYPTEDFIELDS": true,
	"GEODE.AUTH.USER_LIST":          true,
	"GEODE.AUTH.USER_SHOW":          true,
	"GEODE.AUTH.USER_PERMISSIONS":   true,
	"GEODE.AUTH.ROLE_LIST":          true,
	"GEODE.AUTH.POLICY_LIST":        true,
	"GEODE.AUTH.APIKEY_LIST":        true,
	"GEODE.AUTH.APIKEY_CHECK_SCOPE": true,
	"GEODE.RLS.LIST_DENY_POLICIES":  true,
	"GEODE.RLS.POLICY_COUNT":        true,

	// Fulltext readers. geode.fts.rebuild is excluded: it is a write.
	"GEODE.FTS.SEARCH":    true,
	"GEODE.FTS.PHRASE":    true,
	"GEODE.FTS.PROXIMITY": true,

	// Timeseries readers. geode.ts.insert / insert_many are excluded.
	"GEODE.TS.RANGE":      true,
	"GEODE.TS.LAST":       true,
	"GEODE.TS.BUCKET_AVG": true,
	"GEODE.TS.SAMPLE_BY":  true,

	// CDC / audit / TDE diagnostics.
	"CDC.STATUS":                          true,
	"CDC.EXPORT_DIAGNOSTICS":              true,
	"CDC.GET_EVENTS":                      true,
	"CDC.GET_GUARDRAIL_STATUS":            true,
	"CDC.GET_WATERMARK":                   true,
	"CDC.GET_PENDING_EVENTS":              true,
	"CDC.GET_FLUSH_METRICS":               true,
	"CDC.GET_MALFORMED_RECORDS":           true,
	"CDC.GET_CORRUPTION_ANALYSIS":         true,
	"CDC.GET_ORDERED_EVENTS":              true,
	"CDC.CHECK_DUPLICATES":                true,
	"CDC.GET_CHANGE_STATUS":               true,
	"CDC.GET_TELEMETRY_METRICS":           true,
	"CDC.GET_PERFORMANCE_ALERTS":          true,
	"AUDIT.GET_RECENT_EVENTS":             true,
	"AUDIT.VERIFY_HASH_CHAIN":             true,
	"AUDIT.GET_MERKLE_DIGESTS":            true,
	"AUDIT.COMPREHENSIVE_INTEGRITY_CHECK": true,
	"TDE.GET_ENCRYPTION_STATUS":           true,
	"TDE.VERIFY_ENCRYPTED_STATE":          true,
	"TDE.GET_ROTATION_STATE":              true,
	"TDE.MEASURE_OVERHEAD":                true,
	"TDE.AUDIT_PLAINTEXT_KEYS":            true,

	// db.* / dbms.* catalogue introspection.
	"DB.LABELS":            true,
	"DB.RELATIONSHIPTYPES": true,
	"DB.PROPERTYKEYS":      true,
	"DB.INDEXES":           true,
	"DB.CONSTRAINTS":       true,
	"DBMS.PROCEDURES":      true,
	"DBMS.FUNCTIONS":       true,
}

// IsMutatingStatement reports whether a GQL program may change durable state.
// It is deliberately conservative: unknown shapes are mutating.
//
// The program is split on `;` (outside literals and comments) and is
// read-only only when EVERY statement in it is read-only — the shape
// graph/batch.go produces when it concatenates a pipeline into a single
// ExecuteRequest is classified as a whole, so one write anywhere in a batch
// makes the batch a write.
//
// Comments are stripped before the leading keyword is read, so a mutation
// cannot hide behind `/* MATCH */ CREATE ...` or `// x\nDELETE ...`; string
// literals are stripped so a keyword inside data stays data. An unterminated
// literal or block comment is unclassifiable and therefore mutating, matching
// the engine's own MCP read-only classifier, which refuses anything that does
// not parse (geode 0.8.15, src/mcp/tools.zig:428-452).
//
// This is defence in depth in front of the server's own read-only refusal,
// not a replacement for it. The wire flag (ExecuteRequest.read_only) is
// always set too, so a classifier miss still fails at the engine with
// GQLSTATE 42501 (src/server/quic_connection_handler.zig:6493-6496).
func IsMutatingStatement(query string) bool {
	stripped, ok := stripGQLLiterals(query)
	if !ok {
		return true // unterminated literal or comment: fail closed
	}

	seen := false
	for _, stmt := range strings.Split(stripped, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue // empty segment between/after separators
		}
		seen = true
		if statementMutates(stmt) {
			return true
		}
	}
	return !seen // nothing classifiable at all: fail closed
}

// statementMutates classifies one already-stripped, already-uppercased
// statement of a program.
func statementMutates(stmt string) bool {
	head, _ := leadingKeyword(stmt)
	switch {
	case head == "":
		return true // no leading identifier: fail closed
	case head == "CALL":
		// Classified by procedure target in callsMutate below, together
		// with every other CALL in the statement.
	case !readOnlyLeadingKeywords[head]:
		return true
	}

	// Every CALL is classified by its target, wherever it sits.
	if callsMutate(stmt) {
		return true
	}

	// A read-only head does not launder a trailing write:
	// MATCH ... SET, FOR ... CREATE, CALL db.labels() ... CREATE.
	for _, kw := range mutatingKeywords {
		if containsWord(stmt, kw) {
			return true
		}
	}
	return false
}

// callsMutate reports whether any CALL in stmt targets a procedure that is not
// on the closed read-only allow-list.
//
// Every whole-word CALL is classified, not only a leading one. GQL admits an
// inline CALL clause (`MATCH (n) CALL geode.auth.user_create('bob') RETURN n`)
// and a CALL subquery block (`... CALL { ... }`), and the mutating procedure
// names the engine excludes — geode.auth.user_create, geode.rls.add_*,
// geode.ts.insert_many, cdc.start — carry no whole-word mutating keyword
// (`_` is an identifier byte, so USER_CREATE never matches CREATE). Without
// this scan those shapes would classify read-only client-side and rely purely
// on the wire flag, which contradicts the file's "unlisted means write"
// contract.
//
// The engine classifies the same shapes through the same table
// (geode 0.8.15, src/query/query_traits.zig:460-480 —
// `q.call_clause` and `q.initial_call_subquery` both routed through
// procedure_access.classifyCallProcedure, fail-closed on unlisted names).
//
// Fail-closed: an empty target (bare CALL, or the `CALL {` subquery form,
// anywhere in the statement) is unknown and therefore mutating.
func callsMutate(stmt string) bool {
	for off := 0; off < len(stmt); {
		i := indexWord(stmt[off:], "CALL")
		if i < 0 {
			return false
		}
		after := off + i + len("CALL")
		if !readOnlyProcedures[callTarget(stmt[after:])] {
			return true
		}
		off = after
	}
	return false
}

// leadingKeyword returns the first identifier in s and the remainder of s
// after it. Leading punctuation and whitespace are skipped so `MATCH(n)` and
// `(MATCH ...)` classify like `MATCH (n)`. A byte that is neither an
// identifier byte nor skippable yields an empty keyword, which the caller
// treats as mutating.
func leadingKeyword(s string) (string, string) {
	i := 0
	for i < len(s) && !isIdentByte(s[i]) {
		i++
	}
	start := i
	for i < len(s) && isIdentByte(s[i]) {
		i++
	}
	return s[start:i], s[i:]
}

// callTarget extracts the dotted procedure name following a CALL keyword.
// It returns "" for shapes with no name-classifiable target — a bare CALL, or
// the `CALL { ... }` subquery form — which the caller treats as mutating.
func callTarget(rest string) string {
	i := 0
	for i < len(rest) && (rest[i] == ' ' || rest[i] == '\t' || rest[i] == '\n' || rest[i] == '\r') {
		i++
	}
	start := i
	for i < len(rest) && (isIdentByte(rest[i]) || rest[i] == '.') {
		i++
	}
	return strings.Trim(rest[start:i], ".")
}

// stripGQLLiterals replaces string literals and comments with a single space
// and ASCII-uppercases the remainder, so keyword classification sees only
// statement structure. It reports false when the input ends inside a literal
// or a block comment, which makes the statement unclassifiable.
//
// Unlike params.go's placeholder scanner — whose documented blind spots are
// double-quoted strings, backslash escapes, and `//` / `/* */` comments —
// this scanner recognises all of them, because those are exactly the shapes
// an attacker would use to smuggle a write past a read-only gate. Each
// removed region becomes one space so the tokens around it can never fuse
// into a keyword (`MAT/* x */CH` stays `MAT CH`, not `MATCH`).
func stripGQLLiterals(s string) (string, bool) {
	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\'' || s[i] == '"':
			end, ok := skipLiteral(s, i)
			if !ok {
				return "", false
			}
			b.WriteByte(' ')
			i = end
		// `--` is stripped for classification only. The engine refuses any
		// program containing a `--` comment outright with GQLSTATE 42000
		// (GB02CommentDisabled, geode 0.8.15
		// src/server/quic_connection_handler.zig:6431-6433), so no `--`
		// statement ever executes. Stripping it here just stops the text
		// behind it from laundering a write past this classifier
		// (`-- MATCH (n) RETURN n\nCREATE (n:X)`); the refusal a caller
		// ultimately sees for such a program comes from the engine.
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '/',
			s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			b.WriteByte(' ')
			for i < len(s) && s[i] != '\n' {
				i++
			}
			// Keep the newline: it is statement whitespace, not comment text.
			if i < len(s) {
				b.WriteByte('\n')
			}
		case strings.HasPrefix(s[i:], blockCommentOpen):
			end := strings.Index(s[i+len(blockCommentOpen):], blockCommentClose)
			if end < 0 {
				return "", false // unterminated block comment
			}
			b.WriteByte(' ')
			// Land on the last byte of the closing delimiter; the loop's
			// i++ steps past it.
			i += len(blockCommentOpen) + end + len(blockCommentClose) - 1
		default:
			b.WriteByte(asciiUpper(s[i]))
		}
	}
	return b.String(), true
}

// skipLiteral returns the index of the closing quote of the literal starting
// at s[start], honouring both the doubled-quote (two quotes) and backslash
// escapes. It reports false when the literal is never closed.
func skipLiteral(s string, start int) (int, bool) {
	quote := s[start]
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++ // skip the escaped byte
		case quote:
			if i+1 < len(s) && s[i+1] == quote {
				i++ // doubled quote is an escaped quote, not a terminator
				continue
			}
			return i, true
		}
	}
	return 0, false
}

// containsWord reports whether word appears in s delimited by non-identifier
// characters, so "CREATED_AT" does not match "CREATE".
func containsWord(s, word string) bool {
	return indexWord(s, word) >= 0
}

// indexWord returns the index of the first occurrence of word in s delimited
// by non-identifier characters, or -1.
//
// The candidate search is delegated to strings.Index (SIMD-accelerated in the
// runtime) rather than a byte-at-a-time scan: statementMutates probes the same
// statement with 18 mutating keywords plus every CALL, so the naive scan cost
// 18 passes of per-byte comparison over the whole statement. Only the
// word-boundary check stays hand-written. s is already ASCII-uppercased by
// stripGQLLiterals, so a byte-exact search is correct here and no case folding
// (which could rewrite invalid UTF-8) is introduced.
func indexWord(s, word string) int {
	for off := 0; off+len(word) <= len(s); {
		i := strings.Index(s[off:], word)
		if i < 0 {
			return -1
		}
		i += off
		end := i + len(word)
		if (i == 0 || !isIdentByte(s[i-1])) && (end == len(s) || !isIdentByte(s[end])) {
			return i
		}
		off = i + 1
	}
	return -1
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// asciiUpper uppercases ASCII letters and leaves every other byte — including
// invalid UTF-8 — untouched. strings.ToUpper is deliberately not used: it
// rewrites invalid UTF-8 to U+FFFD and case-folds non-ASCII letters into
// ASCII ones, either of which could turn an unclassifiable statement into one
// that matches a read-only keyword.
func asciiUpper(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - ('a' - 'A')
	}
	return c
}

package geode

import "testing"

func TestIsMutatingStatement(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{"MATCH (n:X) RETURN n", false},
		{"  match (n) return n  ", false},
		{"RETURN 1", false},
		{"SHOW CAPABILITIES", false},
		{"EXPLAIN MATCH (n) RETURN n", false},
		{"PROFILE MATCH (n) RETURN n", false},
		{"CALL geode.fts.search('i','q')", false},
		{"CALL algo.bfs.stream(1, {maxDepth: 2})", false},
		{"CREATE (n:X)", true},
		{"create (n:X)", true},
		{"MERGE (n:X {k: $k})", true},
		{"MATCH (n:X) SET n.a = 1", true},
		{"MATCH (n:X) DELETE n", true},
		{"MATCH (n:X) DETACH DELETE n", true},
		{"MATCH (n:X) REMOVE n.a", true},
		{"CREATE INDEX i ON X (p) USING vector", true},
		{"DROP INDEX i", true},
		{"CREATE GRAPH g", true},
		{"USE GRAPH g", true},
		{"CREATE USER bob SET PASSWORD 'x'", true},
		{"GRANT READ TO bob", true},
		{"CALL geode.tenant.create('t')", true},
		{"CALL geode.fts.rebuild('i')", true},
		// Unparseable / unknown shapes must be treated as mutating:
		// fail closed, never fail open.
		{"", true},
		{"\x00\x01", true},
		{"WITH 1 AS x CREATE (n:X)", true},
	}
	for _, c := range cases {
		if got := IsMutatingStatement(c.query); got != c.want {
			t.Errorf("IsMutatingStatement(%q) = %v, want %v", c.query, got, c.want)
		}
	}
}

// TestIsMutatingStatement_ScannerBlindSpots covers the lexical shapes that
// params.go's placeholder scanner documents as blind spots (double-quoted
// strings, backslash escapes, `//` and `/* */` comments). The classifier must
// not inherit them: a mutation hidden behind a leading comment is still a
// mutation, and a keyword hidden inside a literal is still data.
func TestIsMutatingStatement_ScannerBlindSpots(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  bool
	}{
		// Leading comments are skipped before the keyword is read, so a
		// mutation cannot hide behind one.
		{"block comment hides CREATE", "/* MATCH */ CREATE (n:X)", true},
		{"line comment hides DELETE", "// x\nMATCH (n) DELETE n", true},
		{"double-solidus hides CREATE", "// MATCH (n) RETURN n\nCREATE (n:X)", true},
		{"double-dash hides CREATE", "-- MATCH (n) RETURN n\nCREATE (n:X)", true},
		{"nested-looking block then CREATE", "/* a */ /* b */ CREATE (n:X)", true},
		{"unterminated block comment", "/* MATCH (n) RETURN n", true},

		// Comments in the middle never launder a mutation either.
		{"comment before SET", "MATCH (n) /* r */ SET n.a = 1", true},
		{"comment cannot split a keyword", "MAT/* x */CH (n) RETURN n", true},

		// ... and a comment must not make a read-only statement mutating.
		{"trailing line comment", "MATCH (n) RETURN n // CREATE", false},
		{"trailing block comment", "MATCH (n) RETURN n /* CREATE (n:X) */", false},
		{"leading comment then MATCH", "/* audit */ MATCH (n) RETURN n", false},

		// Literal content is data, never a keyword — single quoted (with the
		// doubled-quote escape), double quoted, and backslash escaped.
		{"single-quoted CREATE", "MATCH (n) WHERE n.a = 'CREATE' RETURN n", false},
		{"doubled-quote escape", "MATCH (n) WHERE n.a = 'it''s CREATE' RETURN n", false},
		{"double-quoted DELETE", `MATCH (n) WHERE n.a = "DELETE" RETURN n`, false},
		{"backslash escaped quote", `MATCH (n) WHERE n.a = 'a\' DROP GRAPH g' RETURN n`, false},
		{"backslash in double quotes", `MATCH (n) WHERE n.a = "a\" SET n.b = 1" RETURN n`, false},
		// Dropping a literal must not fuse the tokens around it.
		{"literal adjacent to keyword", "MATCH (n) WHERE n.a='x'RETURN n", false},
		// ... and an unterminated literal is unclassifiable.
		{"unterminated single quote", "MATCH (n) WHERE n.a = 'oops RETURN n", true},
		{"unterminated double quote", `MATCH (n) WHERE n.a = "oops RETURN n`, true},

		// Identifiers that merely start with a keyword are not keywords.
		{"CREATED_AT property", "MATCH (n) RETURN n.CREATED_AT", false},
		{"n.deleted property", "MATCH (n) WHERE n.deleted RETURN n", false},
	}
	for _, c := range cases {
		if got := IsMutatingStatement(c.query); got != c.want {
			t.Errorf("%s: IsMutatingStatement(%q) = %v, want %v", c.name, c.query, got, c.want)
		}
	}
}

// TestIsMutatingStatement_MultiStatement covers `;`-separated programs,
// including the shape graph/batch.go builds when it concatenates a pipeline
// into a single ExecuteRequest. A program is read-only only when EVERY
// statement in it is read-only.
func TestIsMutatingStatement_MultiStatement(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  bool
	}{
		{"all reads", "MATCH (n) RETURN n;\nRETURN 1;\n", false},
		{"trailing semicolon only", "MATCH (n) RETURN n;", false},
		{"write last", "MATCH (n) RETURN n;\nCREATE (m:X);\n", true},
		{"write first", "CREATE (m:X);\nMATCH (n) RETURN n;\n", true},
		{"write in the middle", "RETURN 1;\nMERGE (m:X);\nRETURN 2;\n", true},
		{"semicolon inside a literal is data", "MATCH (n) WHERE n.a = 'a;CREATE (m:X)' RETURN n", false},
		{"semicolon inside a comment is data", "MATCH (n) RETURN n /* ; CREATE (m:X) */", false},
		{"only separators", ";;;", true},
		{"blank statement between reads", "RETURN 1;;RETURN 2", false},
		// graph/batch.go's execConcurrentBatch prefixes the concatenated
		// program with the /*+BATCH*/ hint. The hint is a block comment, so
		// it must not shield the statements behind it from classification.
		{"batch hint then writes", "/*+BATCH*/ MERGE (n:X {k: $a});\nMERGE (m:Y {k: $b});\n", true},
		{"batch hint then reads", "/*+BATCH*/ MATCH (n:X) RETURN n;\nRETURN 1;\n", false},
		{"batch hint then mixed", "/*+BATCH*/ MATCH (n:X) RETURN n;\nSHOW CAPABILITIES;\nCREATE (m:Y);\n", true},
	}
	for _, c := range cases {
		if got := IsMutatingStatement(c.query); got != c.want {
			t.Errorf("%s: IsMutatingStatement(%q) = %v, want %v", c.name, c.query, got, c.want)
		}
	}
}

// TestIsMutatingStatement_CallAllowList pins the closed CALL allow-list to the
// engine's own read-only procedure table
// (geode 0.8.15, src/execution/procedure_access.zig:20-129). Every name the
// engine classifies .read_only must classify read-only here, and anything not
// on the list — including unknown and near-miss names — must be mutating.
func TestIsMutatingStatement_CallAllowList(t *testing.T) {
	for name := range readOnlyProcedures {
		q := "CALL " + name + "()"
		if IsMutatingStatement(q) {
			t.Errorf("IsMutatingStatement(%q) = true, want false (engine classifies it read_only)", q)
		}
		// The same name inline, which the engine parses as q.call_clause
		// (geode 0.8.15 src/query/query_traits.zig:467-475), must classify
		// the same way.
		inline := "MATCH (n) CALL " + name + "() RETURN n"
		if IsMutatingStatement(inline) {
			t.Errorf("IsMutatingStatement(%q) = true, want false (engine classifies it read_only)", inline)
		}
	}

	// An inline CALL to a listed read procedure is admitted, so the fix does
	// not simply refuse every non-leading CALL.
	readInline := "MATCH (n) CALL geode.fts.search('i','q') YIELD node RETURN node"
	if IsMutatingStatement(readInline) {
		t.Errorf("IsMutatingStatement(%q) = true, want false", readInline)
	}

	mutating := []string{
		"CALL geode.fts.rebuild('i')",
		"CALL geode.ts.insert('m', 1, 2.0)",
		"CALL geode.ts.insert_many('m', [])",
		"CALL geode.tenant.create('t')",
		"CALL geode.auth.user_create('bob')",
		"CALL geode.rls.add_deny_policy('p')",
		"CALL cdc.start()",
		"CALL db.awaitIndex('i')",
		"CALL totally.unknown.procedure()",
		"CALL unknown()",
		// A read-only CALL followed by a write is still a write.
		"CALL db.labels() YIELD label CREATE (n:X {l: label})",
		// The CALL-subquery form is not name-classifiable here: fail closed.
		"CALL { MATCH (n) RETURN n }",
		// A bare CALL with no target is unclassifiable.
		"CALL",

		// Non-leading CALL. The engine classifies an inline call clause and a
		// CALL subquery through the same procedure table
		// (geode 0.8.15 src/query/query_traits.zig:460-480), and none of these
		// procedure names carries a whole-word mutating keyword — `_` is an
		// identifier byte, so USER_CREATE never matches CREATE — which is why
		// only the per-CALL target scan catches them.
		"MATCH (n) CALL geode.auth.user_create('bob') RETURN n",
		"MATCH (n) CALL geode.rls.add_deny_policy('p') RETURN n",
		"MATCH (n) CALL geode.ts.insert_many('m', []) RETURN n",
		"MATCH (n) CALL cdc.start() RETURN n",
		"CALL geode.fts.search('i','q') CALL geode.auth.user_create('bob')",
		// A CALL subquery block anywhere in the statement is not
		// name-classifiable either: fail closed.
		"MATCH (n) CALL { MATCH (m) RETURN m } RETURN n",
	}
	for _, q := range mutating {
		if !IsMutatingStatement(q) {
			t.Errorf("IsMutatingStatement(%q) = false, want true", q)
		}
	}
}

func TestParseDSN_ReadOnly(t *testing.T) {
	for _, dsn := range []string{
		"quic://h:3141?read_only=true",
		"quic://h:3141?readonly=1",
		"quic://h:3141?read-only=yes",
	} {
		cfg, err := ParseDSN(dsn)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", dsn, err)
		}
		if !cfg.ReadOnly {
			t.Errorf("ParseDSN(%q).ReadOnly = false, want true", dsn)
		}
	}
	cfg, err := ParseDSN("quic://h:3141")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if cfg.ReadOnly {
		t.Error("ReadOnly defaults to true, want false")
	}
}

func TestFormatDSN_RoundTripsReadOnly(t *testing.T) {
	cfg, err := ParseDSN("quic://h:3141?read_only=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	round, err := ParseDSN(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("ParseDSN(FormatDSN()): %v", err)
	}
	if !round.ReadOnly {
		t.Errorf("FormatDSN() = %q lost read_only", cfg.FormatDSN())
	}
}

// BenchmarkIsMutatingStatement makes the per-statement classification cost
// visible: guardReadOnly runs on every Exec/Query of a read-only connection,
// so this sits directly in the hot path. The shapes cover a short read, a
// realistic read with literals and comments, an inline CALL, and the
// concatenated batch program graph/batch.go emits.
func BenchmarkIsMutatingStatement(b *testing.B) {
	cases := []struct {
		name  string
		query string
	}{
		{"short read", "MATCH (n:X) RETURN n"},
		{
			"read with literals and comment",
			"/* audit */ MATCH (n:Person) WHERE n.name = 'alice' AND n.city = \"berlin\" " +
				"RETURN n.name, n.created_at ORDER BY n.created_at DESC LIMIT 25",
		},
		{"inline call", "MATCH (n) CALL geode.fts.search('idx','term') YIELD node RETURN node"},
		{
			"batch program",
			"/*+BATCH*/ MATCH (n:X) RETURN n;\nMATCH (m:Y) RETURN m;\nSHOW CAPABILITIES;\n",
		},
		{"trailing write", "MATCH (n:X) WHERE n.a = 'x' SET n.b = 1"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.query)))
			for i := 0; i < b.N; i++ {
				sinkIsMutating = IsMutatingStatement(c.query)
			}
		})
	}
}

// sinkIsMutating keeps the benchmark's result live so the call is not
// optimised away.
var sinkIsMutating bool

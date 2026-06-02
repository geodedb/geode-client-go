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
		"01_nodes.gql": {Data: []byte("CREATE NODE TYPE Person (name STRING, age INT)\nCREATE NODE TYPE Company (name STRING)\n")},
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
		"schema.gql": {Data: []byte("// This is a comment\n-- This is also a comment\nCREATE NODE TYPE Widget (id INT)\n")},
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
		"schema.gql": {Data: []byte("CREATE (a)\nUSE GRAPH mydb\nSET PROPERTY foo = 1\nCREATE (b)\n")},
	}
	stmts, err := ParseSchemaFS(fs, ".")
	if err != nil {
		t.Fatalf("ParseSchemaFS: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements (USE and SET skipped), got %d", len(stmts))
	}
}

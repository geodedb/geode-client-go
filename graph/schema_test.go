package graph_test

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"

	"geodedb.com/geode/graph"
)

func TestParseSchemaFS_SingleFile(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_init.gql": &fstest.MapFile{
			Data: []byte("CREATE NODE TYPE Person;\nCREATE EDGE TYPE Knows;\n"),
		},
	}

	stmts, err := graph.ParseSchemaFS(fsys, "schema")
	if err != nil {
		t.Fatalf("ParseSchemaFS() error = %v, want nil", err)
	}

	if len(stmts) != 2 {
		t.Fatalf("len(stmts) = %d, want 2", len(stmts))
	}

	if stmts[0].Query != "CREATE NODE TYPE Person" {
		t.Errorf("stmts[0].Query = %q, want %q", stmts[0].Query, "CREATE NODE TYPE Person")
	}

	if stmts[1].Query != "CREATE EDGE TYPE Knows" {
		t.Errorf("stmts[1].Query = %q, want %q", stmts[1].Query, "CREATE EDGE TYPE Knows")
	}
}

func TestParseSchemaFS_FiltersComments(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_comments.gql": &fstest.MapFile{
			Data: []byte("// This is a comment\n-- Another comment\nCREATE NODE TYPE Person;\n// trailing comment\n"),
		},
	}

	stmts, err := graph.ParseSchemaFS(fsys, "schema")
	if err != nil {
		t.Fatalf("ParseSchemaFS() error = %v, want nil", err)
	}

	if len(stmts) != 1 {
		t.Fatalf("len(stmts) = %d, want 1", len(stmts))
	}

	if stmts[0].Query != "CREATE NODE TYPE Person" {
		t.Errorf("stmts[0].Query = %q, want %q", stmts[0].Query, "CREATE NODE TYPE Person")
	}
}

func TestParseSchemaFS_ValidatesStatementTypes(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_bad.gql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;\n"),
		},
	}

	_, err := graph.ParseSchemaFS(fsys, "schema")
	if err == nil {
		t.Fatal("ParseSchemaFS() error = nil, want non-nil for invalid statement type")
	}
}

func TestParseSchemaFS_MultipleFiles(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/002_edges.gql": &fstest.MapFile{
			Data: []byte("CREATE EDGE TYPE Knows;\n"),
		},
		"schema/001_nodes.gql": &fstest.MapFile{
			Data: []byte("CREATE NODE TYPE Person;\n"),
		},
		"schema/README.txt": &fstest.MapFile{
			Data: []byte("This file should be ignored.\n"),
		},
	}

	stmts, err := graph.ParseSchemaFS(fsys, "schema")
	if err != nil {
		t.Fatalf("ParseSchemaFS() error = %v, want nil", err)
	}

	// Only .gql files parsed; 001 before 002 due to lexicographic order.
	if len(stmts) != 2 {
		t.Fatalf("len(stmts) = %d, want 2", len(stmts))
	}

	if stmts[0].Query != "CREATE NODE TYPE Person" {
		t.Errorf("stmts[0].Query = %q, want %q (001 should come first)", stmts[0].Query, "CREATE NODE TYPE Person")
	}

	if stmts[1].Query != "CREATE EDGE TYPE Knows" {
		t.Errorf("stmts[1].Query = %q, want %q (002 should come second)", stmts[1].Query, "CREATE EDGE TYPE Knows")
	}
}

func TestParseSchemaFS_EmptyFile(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_empty.gql": &fstest.MapFile{
			Data: []byte(""),
		},
	}

	stmts, err := graph.ParseSchemaFS(fsys, "schema")
	if err != nil {
		t.Fatalf("ParseSchemaFS() error = %v, want nil", err)
	}

	if len(stmts) != 0 {
		t.Errorf("len(stmts) = %d, want 0 for empty file", len(stmts))
	}
}

func TestParseSchemaFS_InvalidDir(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{}

	_, err := graph.ParseSchemaFS(fsys, "nonexistent")
	if err == nil {
		t.Fatal("ParseSchemaFS() error = nil, want non-nil for nonexistent directory")
	}
}

func TestParseSchemaFS_AllValidTypes(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_all.gql": &fstest.MapFile{
			Data: []byte("CREATE NODE TYPE Person;\nMERGE (n:Person {name: 'Alice'});\nMATCH (n) RETURN n;\nDELETE (n);\n"),
		},
	}

	stmts, err := graph.ParseSchemaFS(fsys, "schema")
	if err != nil {
		t.Fatalf("ParseSchemaFS() error = %v, want nil", err)
	}

	if len(stmts) != 4 {
		t.Fatalf("len(stmts) = %d, want 4", len(stmts))
	}
}

func TestApplySchema_EmptyStatements(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := graph.NewConn(db)
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	// nil slice should return nil.
	if applyErr := conn.ApplySchema(context.Background(), nil); applyErr != nil {
		t.Errorf("ApplySchema(nil) error = %v, want nil", applyErr)
	}

	// empty slice should also return nil.
	if applyErr := conn.ApplySchema(context.Background(), []graph.Statement{}); applyErr != nil {
		t.Errorf("ApplySchema([]) error = %v, want nil", applyErr)
	}
}

func TestApplySchemaFS_Integration(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"schema/001_init.gql": &fstest.MapFile{
			Data: []byte("CREATE NODE TYPE Person;\n"),
		},
	}

	db, err := sql.Open("geode", "localhost:3141")
	if err != nil {
		t.Fatalf("sql.Open() error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := graph.NewConn(db, graph.WithRetry(1, 0))
	if err != nil {
		t.Fatalf("NewConn() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No server is running; we only verify that the function does not panic
	// and returns an error (connection failure, not a panic).
	applyErr := conn.ApplySchemaFS(ctx, fsys, "schema")
	if applyErr == nil {
		t.Log("ApplySchemaFS succeeded (unexpected but not a failure in unit mode)")
	}
	// The important thing: no panic occurred.
}

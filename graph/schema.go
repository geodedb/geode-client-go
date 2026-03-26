package graph

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// maxTruncateLen is the maximum number of characters shown in error messages
// for truncated statement text.
const maxTruncateLen = 40

// ParseSchemaFS reads all .gql files from dir in fsys, parses their contents
// into Statements, and returns them in lexicographic file order. Files that are
// not regular .gql files (directories, non-.gql extensions) are silently
// skipped. An error is returned if the directory cannot be read, a file cannot
// be read, or a statement uses an unsupported keyword.
func ParseSchemaFS(fsys fs.FS, dir string) ([]Statement, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("graph: read schema dir %q: %w", dir, err)
	}

	// Collect .gql filenames and sort lexicographically (entries from ReadDir
	// are already sorted per the fs.ReadDir contract, but we sort explicitly to
	// be safe and to document intent).
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".gql" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	var stmts []Statement
	for _, name := range names {
		path := dir + "/" + name
		data, readErr := fs.ReadFile(fsys, path)
		if readErr != nil {
			return nil, fmt.Errorf("graph: read schema file %q: %w", path, readErr)
		}

		parsed, parseErr := parseSchemaContent(string(data))
		if parseErr != nil {
			return nil, fmt.Errorf("graph: parse schema file %q: %w", path, parseErr)
		}

		stmts = append(stmts, parsed...)
	}

	return stmts, nil
}

// parseSchemaContent converts raw GQL file content into Statements. Lines
// starting with // or -- (after trimming whitespace) are treated as comments
// and dropped. Empty lines are skipped. The remaining text is joined and split
// on semicolons to produce individual statements, each of which must begin with
// a recognised keyword.
func parseSchemaContent(content string) ([]Statement, error) {
	lines := strings.Split(content, "\n")

	var kept []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "--") {
			continue
		}
		kept = append(kept, trimmed)
	}

	if len(kept) == 0 {
		return nil, nil
	}

	joined := strings.Join(kept, " ")
	parts := strings.Split(joined, ";")

	var stmts []Statement
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}

		if err := validateStatementPrefix(trimmed); err != nil {
			return nil, err
		}

		stmts = append(stmts, Statement{Query: trimmed})
	}

	return stmts, nil
}

// validateStatementPrefix checks that stmt starts with one of the valid GQL
// keywords (case-insensitive). It returns a descriptive error if not.
func validateStatementPrefix(stmt string) error {
	upper := strings.ToUpper(stmt)
	for _, prefix := range []string{"CREATE", "MERGE", "MATCH", "DELETE"} {
		if strings.HasPrefix(upper, prefix) {
			return nil
		}
	}
	return fmt.Errorf("graph: unsupported statement type: %q", truncate(stmt, maxTruncateLen))
}

// truncate returns s if it is no longer than maxLen characters, otherwise it
// returns s[:maxLen] followed by "...".
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ApplySchema executes stmts sequentially against the database. Execution stops
// at the first error. An empty or nil slice returns a nil error. The context is
// checked between statements.
func (c *Conn) ApplySchema(ctx context.Context, stmts []Statement) error {
	if len(stmts) == 0 {
		return nil
	}

	for i, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("graph: apply schema statement %d: %w", i, err)
		}

		if _, err := c.db.ExecContext(ctx, stmt.Query, stmt.Args...); err != nil {
			return fmt.Errorf("graph: apply schema statement %d: %w", i, err)
		}
	}

	return nil
}

// ApplySchemaFS is a convenience method that parses all .gql files from dir in
// fsys and then applies them via ApplySchema.
func (c *Conn) ApplySchemaFS(ctx context.Context, fsys fs.FS, dir string) error {
	stmts, err := ParseSchemaFS(fsys, dir)
	if err != nil {
		return err
	}
	return c.ApplySchema(ctx, stmts)
}

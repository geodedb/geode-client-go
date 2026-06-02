// schema.go
package geode

import (
	"fmt"
	"io/fs"
	"strings"
)

// ParseSchemaFS reads all .gql files from the filesystem fsys at dir and
// returns Statements suitable for [Pool.BatchExec] or [Pool.PipelineExec].
//
// Use "." to reference the root of the FS. Files in subdirectories are not
// traversed; only direct children of dir matching "*.gql" are read.
//
// Lines are split into statements at semicolons after stripping blank lines,
// line comments (// and --), and lines that do not start with a recognised GQL
// keyword (CREATE, MERGE, MATCH, DELETE). Semicolons within a line produce
// multiple statements.
func ParseSchemaFS(fsys fs.FS, dir string) ([]Statement, error) {
	entries, err := fs.ReadDir(fsys, dir)
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
		content, err := fs.ReadFile(fsys, path)
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

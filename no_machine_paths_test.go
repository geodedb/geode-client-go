// no_machine_paths_test.go — SONNY-806, Lane D Task 16d.
//
// Carries NO build tag on purpose: the file it guards is behind
// `//go:build integration`, so a guard behind the same tag would only run in
// the lane that already fails. This one runs in `make test-unit`, which is what
// a developer runs before pushing.

package geode

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// machinePathPrefixes are absolute paths that only exist on one person's
// machine. A string literal containing one is never portable.
var machinePathPrefixes = []string{
	`"/home/`,
	`"/Users/`,
	`"/root/`,
	`"C:\\Users\\`,
}

// TestNoDeveloperMachinePathsInSources fails on any Go source in this package
// that hard-codes an absolute path into somebody's home directory.
//
// 2134501 committed probeGeodeBinary =
// "/home/benji/src/devnw/.../zig-out/bin/geode" and three probe tests went on
// passing on the machine that wrote it while failing at 0.00s everywhere else,
// including CI — where it was one of the two reasons the main pipeline was RED
// (SONNY-806). Locate such a path via an environment variable and skip when it
// is absent (resolveProbeGeodeBinary), never by naming a home directory.
func TestNoDeveloperMachinePathsInSources(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		// This file names the prefixes it forbids.
		if entry.Name() == "no_machine_paths_test.go" {
			continue
		}

		source, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		for lineNo, line := range strings.Split(string(source), "\n") {
			for _, prefix := range machinePathPrefixes {
				if strings.Contains(line, prefix) {
					t.Errorf("%s:%d hard-codes a developer-machine path (%s...): "+
						"resolve it from an environment variable and skip when unset, "+
						"so the test can run somewhere other than the machine that wrote it\n\t%s",
						entry.Name(), lineNo+1, prefix, strings.TrimSpace(line))
				}
			}
		}
	}
}

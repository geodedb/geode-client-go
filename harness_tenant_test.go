// harness_tenant_test.go declares the harness tenant id and pins it against
// its other two copies. It intentionally carries NO build tag (unlike
// integration_main_test.go and friends) so:
//
//   - harnessTenant is reachable from the Docker-driving integration suite
//     (built with `-tags=integration`) AND from a plain `go test` run.
//   - TestHarnessTenant_MatchesMakefileAndCI below can run in the ordinary
//     unit-test lane, no Docker required, to catch the three copies of this
//     id drifting apart.
package geode

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// harnessTenant is the tenant the shared integration DSN binds.
//
// The harness runs the engine with GEODE_STRICT_TENANCY=1 (see the container
// Env in integration_main_test.go), under which a tenant-scoped statement
// from a session whose resolved tenant is unset, empty, or the reserved
// "default" sentinel is refused with GQLSTATE 42501 "Tenant scope required"
// (geode src/execution/tenancy.zig:174-183 -> src/server/proto_util.zig:240).
// Every data statement in that suite is tenant-scoped, so the shared DSN has
// to carry a concrete scope or the whole lane is red.
//
// It is a compile-time constant, never interpolated from input:
// CALL geode.tenant.create reads its arguments with literalString
// (geode src/execution/tenant_dispatch.zig:191-196), so a $param raises
// TypeMismatch and the id must be rendered into the statement text — keeping
// it constant is what makes that safe. Same rule as tenantA/tenantB in
// integration_tenancy_test.go and cancelTenant in integration_cancel_test.go.
//
// This id is duplicated in two other places that cannot import a Go
// constant — the Makefile's GEODE_TEST_TENANT default and .gitlab-ci.yml's
// GEODE_TEST_DSN — so TestHarnessTenant_MatchesMakefileAndCI below is the
// single point that fails loudly if any of the three drifts from the others.
const harnessTenant = "it-harness"

// TestHarnessTenant_MatchesMakefileAndCI pins harnessTenant against its two
// other copies: the Makefile's GEODE_TEST_TENANT default and .gitlab-ci.yml's
// GEODE_TEST_DSN. Nothing enforces these three stay in sync except this test —
// a drift silently reintroduces GQLSTATE 42501 "Tenant scope required" in
// whichever lane still points at the stale id. Plain `go test`, no Docker: it
// only reads the two files from the repo root, which is also this package's
// module root (go.mod, Makefile and .gitlab-ci.yml are all siblings of this
// file).
func TestHarnessTenant_MatchesMakefileAndCI(t *testing.T) {
	const (
		makefilePath = "Makefile"
		ciPath       = ".gitlab-ci.yml"
	)

	makefileWant := "GEODE_TEST_TENANT ?= " + harnessTenant
	makefileBody, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("reading %s: %v", makefilePath, err)
	}
	if !containsLine(string(makefileBody), makefileWant) {
		t.Errorf("harnessTenant (%q, defined in harness_tenant_test.go) is out of sync "+
			"with %s: no line %q found. Keep harnessTenant, %s, and %s in sync.",
			harnessTenant, makefilePath, makefileWant, makefilePath, ciPath)
	}

	// The .gitlab-ci.yml occurrence lives inside a quoted DSN value, not on a
	// line of its own, so this is a substring check rather than containsLine's
	// exact-line match.
	ciWant := "tenant=" + harnessTenant
	ciBody, err := os.ReadFile(ciPath)
	if err != nil {
		t.Fatalf("reading %s: %v", ciPath, err)
	}
	if !strings.Contains(string(ciBody), ciWant) {
		t.Errorf("harnessTenant (%q, defined in harness_tenant_test.go) is out of sync "+
			"with %s: no occurrence of %q found. Keep harnessTenant, %s, and %s in sync.",
			harnessTenant, ciPath, ciWant, "Makefile", ciPath)
	}
}

// containsLine reports whether body has a line equal to want once leading and
// trailing whitespace on each line is trimmed.
func containsLine(body, want string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// tenantAlreadyExistsCode and tenantAlreadyExistsMessage identify the
// engine's benign duplicate-tenant refusal from `CALL geode.tenant.create`.
//
// The registry rejects a second create with error.TenantAlreadyExists (geode
// src/security/tenant_registry.zig:64,167). That error has no explicit case
// in the engine's GQLSTATE mapping table (geode src/server/proto_util.zig),
// so it falls through to the table's default arm: GQLSTATE 58000, message set
// to the Zig error name itself — @errorName(err) == "TenantAlreadyExists"
// (src/server/proto_util.zig:319-320: `else => .{ .code = "58000", .message =
// @errorName(err) }`). The message, not the code alone, is what identifies
// it: 58000 is the engine's generic "no specific mapping" class and is used
// for unrelated errors too.
const (
	tenantAlreadyExistsCode    = StatusSystem
	tenantAlreadyExistsMessage = "TenantAlreadyExists"
)

// isTenantAlreadyExists reports whether err is the engine's benign
// duplicate-tenant refusal (see tenantAlreadyExistsCode/Message) as opposed to
// any other failure from `CALL geode.tenant.create` — a permission error, a
// malformed statement, a connection failure, etc., all of which must still
// fail ensureHarnessTenant and, in turn, TestMain.
func isTenantAlreadyExists(err error) bool {
	var de *DriverError
	if !errors.As(err, &de) {
		return false
	}
	return de.Code == tenantAlreadyExistsCode && de.Message == tenantAlreadyExistsMessage
}

// TestIsTenantAlreadyExists pins isTenantAlreadyExists's classification: it
// must recognise exactly the engine's duplicate-tenant shape and reject
// everything else, including other 58000-class errors and non-DriverError
// failures. No Docker needed — DriverError is a plain value type.
func TestIsTenantAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "exact duplicate-tenant shape",
			err:  &DriverError{Code: tenantAlreadyExistsCode, Message: tenantAlreadyExistsMessage},
			want: true,
		},
		{
			name: "duplicate-tenant shape wrapped with fmt.Errorf %w",
			err: fmt.Errorf("CALL geode.tenant.create: %w",
				&DriverError{Code: tenantAlreadyExistsCode, Message: tenantAlreadyExistsMessage}),
			want: true, // errors.As must see through the wrap
		},
		{
			name: "same code, different message",
			err:  &DriverError{Code: StatusSystem, Message: "OutOfMemory"},
			want: false,
		},
		{
			name: "same message, different code",
			err:  &DriverError{Code: StatusPermissionDenied, Message: tenantAlreadyExistsMessage},
			want: false,
		},
		{
			name: "unrelated permission error",
			err:  &DriverError{Code: StatusPermissionDenied, Message: "Permission denied"},
			want: false,
		},
		{
			name: "non-DriverError failure",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTenantAlreadyExists(tt.err); got != tt.want {
				t.Errorf("isTenantAlreadyExists(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

//go:build integration

// integration_tenancy_test.go proves the engine ENFORCES the tenant and role
// the DSN binds at HELLO (conn.go carries tenant/tenant_id/role into
// HelloRequest). Both tests need a strict-tenancy engine: the integration lane
// sets GEODE_STRICT_TENANCY=1 in .gitlab-ci.yml, in the testcontainers Env of
// integration_main_test.go, and in the Makefile docker-start recipe. Without
// it the engine runs permissive and an unscoped session reads every tenant's
// rows, so the cross-tenant assertion would be vacuous — hence the explicit
// mode probe before the leak check.
package geode

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"testing"
	"time"
)

// Tenant ids used by the cross-tenant test. They are compile-time constants,
// never interpolated from input: CALL geode.tenant.create reads its arguments
// with literalString (geode src/execution/tenant_dispatch.zig:191-196, called
// at :336-337), so a $param raises TypeMismatch and the id has to be rendered
// into the statement text. Keeping them constant is what makes that safe.
const (
	tenantA = "tenant-a"
	tenantB = "tenant-b"
)

// tenancyGraph is the graph both tenant sessions bind.
const tenancyGraph = "tenancytest"

// TestIntegration_CrossTenantReadIsDenied is the negative cross-tenant case
// SONNY-806 requires. Two sessions bind different tenants at HELLO; data
// written by one must be invisible to the other.
func TestIntegration_CrossTenantReadIsDenied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	admin, err := sql.Open("geode", sharedDSN+"&graph="+tenancyGraph)
	if err != nil {
		t.Fatalf("sql.Open admin: %v", err)
	}
	defer admin.Close()

	// Register both tenants. geode.tenant.create is the engine's only tenant
	// DDL surface — there is no CREATE TENANT statement and the 0.8.15
	// SHOW CAPABILITIES row tenant_ddl=false says exactly that
	// (geode src/execution/admin_ddl.zig:201, and
	// docs/security/MULTI_TENANT_HARDENING.md "CALL-only administrative
	// scope"). Registering a non-default tenant also satisfies the GAP-0848
	// auto-enable signal, so strict tenancy holds even if the env flag were
	// dropped.
	for _, tenant := range []string{tenantA, tenantB} {
		if _, err := admin.ExecContext(ctx,
			"CALL geode.tenant.create('"+tenant+"')"); err != nil {
			t.Logf("CALL geode.tenant.create(%q): %v (may already exist)", tenant, err)
		}
	}

	// Mode probe. The admin session carries no tenant, so its resolved scope is
	// the reserved "default" sentinel. Under strict tenancy a tenant-scoped
	// read from that session is refused with GQLSTATE 42501 "Tenant scope
	// required" (geode src/execution/tenancy.zig:170-183 ->
	// src/server/proto_util.zig:240). If it succeeds, the engine is permissive
	// and the leak assertion below would prove nothing.
	var unscoped int64
	err = admin.QueryRowContext(ctx,
		"MATCH (n:TenantProbe) RETURN count(n)").Scan(&unscoped)
	if err == nil {
		t.Fatalf("unscoped read succeeded (count=%d): strict tenancy is NOT active, "+
			"so this test cannot prove cross-tenant isolation. Set GEODE_STRICT_TENANCY=1 "+
			"on the engine (see .gitlab-ci.yml and the Makefile docker-start recipe).", unscoped)
	}
	if !errors.Is(err, ErrTenantScopeRequired) {
		t.Fatalf("unscoped read err = %v, want ErrTenantScopeRequired (GQLSTATE 42501 "+
			"\"Tenant scope required\")", err)
	}
	t.Logf("strict tenancy confirmed; unscoped read refused as: %v", err)

	open := func(tenant string) *sql.DB {
		t.Helper()
		db, err := sql.Open("geode",
			sharedDSN+"&graph="+tenancyGraph+"&tenant="+url.QueryEscape(tenant))
		if err != nil {
			t.Fatalf("sql.Open %s: %v", tenant, err)
		}
		return db
	}

	// SONNY-1227: never close and reopen on a shared pool inside a test; each
	// tenant gets its own sql.DB for its own DSN.
	a := open(tenantA)
	defer a.Close()
	b := open(tenantB)
	defer b.Close()

	if _, err := a.ExecContext(ctx,
		"MERGE (n:TenantProbe {k: ?}) ON CREATE SET n.secret = ?", "k-a", "a-only"); err != nil {
		t.Fatalf("tenant-a write: %v", err)
	}

	var count int64
	if err := b.QueryRowContext(ctx,
		"MATCH (n:TenantProbe {k: ?}) RETURN count(n)", "k-a").Scan(&count); err != nil {
		t.Fatalf("tenant-b read: %v", err)
	}
	if count != 0 {
		t.Fatalf("tenant-b saw %d of tenant-a's nodes, want 0 — CROSS-TENANT LEAK", count)
	}

	if err := a.QueryRowContext(ctx,
		"MATCH (n:TenantProbe {k: ?}) RETURN count(n)", "k-a").Scan(&count); err != nil {
		t.Fatalf("tenant-a readback: %v", err)
	}
	if count != 1 {
		t.Fatalf("tenant-a saw %d of its own nodes, want 1", count)
	}
}

// TestIntegration_ReadOnlyRole proves the DSN `role` reaches HELLO and that the
// ENGINE — not the driver's client-side read-only classifier (Task 6) — refuses
// a write from a readonly-role session. The read-only DSN option is
// deliberately NOT set, so the client gate is off and the only thing that can
// refuse the CREATE is the server.
func TestIntegration_ReadOnlyRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const roleGraph = "roletest"

	admin, err := sql.Open("geode", sharedDSN+"&graph="+roleGraph)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer admin.Close()

	// Dev-only credential for the integration lane. CREATE USER <name>
	// SET PASSWORD '<pw>' [EMAIL '<e>'] [ROLE <role>] is the 0.8.15 auth DDL
	// grammar (geode src/gql/parser.zig:6706-6726, ast.CreateUserStatement at
	// src/gql/ast.zig:695-700); "readonly" is a recognised role name
	// (Role.fromString("readonly") == .ReadOnly, pinned by
	// src/security/test_auth_enforcement.zig:114). SHOW CAPABILITIES reports
	// role_ddl=true at 0.8.15 (testdata/capabilities_0.8.15.golden).
	// The engine enforces a password policy (min 8 chars, letters AND numbers)
	// — a letters-only passphrase is refused with GQLSTATE 22000.
	const roUser = "ro_probe"
	const roPass = "Correct horse battery staple ro9" // pragma: allowlist secret // gitleaks:allow
	if _, err := admin.ExecContext(ctx,
		"CREATE USER "+roUser+" SET PASSWORD '"+roPass+"' ROLE readonly"); err != nil {
		t.Logf("CREATE USER: %v (may already exist)", err)
	}

	// The readonly user must be able to BIND the graph, or HELLO fails with
	// "Access denied: insufficient permissions for graph" before any statement
	// runs and the test would prove nothing about writes. Binding a non-default
	// graph goes through the GPS bind_graph decision
	// (geode src/server/quic_connection_handler.zig:6137-6155). READ is the
	// narrowest grant that permits the bind — deliberately NOT WRITE or ALL, so
	// the CREATE below is still the thing under test.
	if _, err := admin.ExecContext(ctx,
		"GRANT READ ON GRAPH "+roleGraph+" TO "+roUser); err != nil {
		t.Logf("GRANT READ ON GRAPH: %v (may already be granted)", err)
	}

	// No read_only=true here on purpose: the client-side gate must be OFF so
	// the refusal can only come from the engine.
	roDSN := hostPortFromDSN(sharedDSN) +
		"?insecure=true&user=" + url.QueryEscape(roUser) +
		"&pass=" + url.QueryEscape(roPass) +
		"&graph=" + roleGraph +
		"&role=readonly"

	ro, err := sql.Open("geode", roDSN)
	if err != nil {
		t.Fatalf("sql.Open readonly: %v", err)
	}
	defer ro.Close()

	_, err = ro.ExecContext(ctx, "CREATE (n:RoleProbe {k: ?})", "x")
	if err == nil {
		t.Fatal("readonly role wrote successfully, want a 42501 refusal from the engine")
	}

	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("readonly role write err = %T %v, want a *DriverError", err, err)
	}
	if errors.Is(err, ErrReadOnly) {
		t.Fatalf("write was refused by the CLIENT gate (%v); this test must exercise "+
			"the engine, so the DSN must not set read_only=true", err)
	}
	if de.Code != StatusPermissionDenied && de.Class != StatusPermissionDenied {
		t.Fatalf("readonly role write err = %v (code=%q class=%q), want GQLSTATE %s",
			err, de.Code, de.Class, StatusPermissionDenied)
	}
	t.Logf("engine refused the readonly-role write as: %v", err)
}

//go:build integration

// integration_stream_binding_test.go — GAP-SONNY806-08 regression (SONNY-806,
// SONNY-1219).
//
// The SONNY-1219 anchor stream (05b4653) moved HELLO onto a dedicated stream
// opened at dial time and made every REQUEST stream skip it. HELLO is what
// carries the session's tenant and bound graph, so after that change a
// tenant-bound session read ZERO rows — including rows it had just written
// itself. That is a SILENT WRONG ANSWER: the query succeeds and returns an
// empty result set, so nothing in the call stack raises.
//
// This file pins the invariant the anchor broke: EVERY request stream a
// tenant-bound session opens must carry that session's tenant and graph
// binding. It deliberately spreads one session's work across several request
// streams (database/sql Conns held open simultaneously, so they cannot be
// recycled onto one stream) because a defect that binds only the FIRST stream
// is invisible to a single-statement test.
package geode

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"
)

// Tenants and graph used by the stream-binding regression. Like the ids in
// integration_tenancy_test.go these are compile-time constants, never
// interpolated from input: CALL geode.tenant.create reads its arguments with
// literalString, so the id has to be rendered into the statement text and
// keeping it constant is what makes that safe.
const (
	streamBindTenantA = "streambind-a"
	streamBindTenantB = "streambind-b"
	streamBindGraph   = "streambindtest"
)

// streamBindConns is how many request streams one tenant-bound session holds
// open at once. It is below DefaultMaxStreamsPerConn (8) so every one of them
// is admitted onto the same pooled QUIC connection concurrently — which is the
// condition under which a first-stream-only binding shows up.
const streamBindConns = 4

// TestIntegration_TenantBoundSessionReadsItsOwnWritesOnEveryStream is the
// GAP-SONNY806-08 regression.
//
// It fails RED on the anchor build (05b4653..3a9c88e) with tenant-a seeing 0 of
// its own rows, and PASSES once every request stream carries the tenant/graph
// binding.
func TestIntegration_TenantBoundSessionReadsItsOwnWritesOnEveryStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Unscoped admin session, used only for tenant DDL (gated on admin_tenant,
	// not on tenant scope). sharedDSN binds harnessTenant, so it is stripped.
	admin, err := sql.Open("geode", dsnWithoutTenant(sharedDSN)+"&graph="+streamBindGraph)
	if err != nil {
		t.Fatalf("sql.Open admin: %v", err)
	}
	defer admin.Close()

	for _, tenant := range []string{streamBindTenantA, streamBindTenantB} {
		if _, err := admin.ExecContext(ctx,
			"CALL geode.tenant.create('"+tenant+"')"); err != nil {
			t.Logf("CALL geode.tenant.create(%q): %v (may already exist)", tenant, err)
		}
	}

	open := func(tenant string) *sql.DB {
		t.Helper()
		db, err := sql.Open("geode",
			sharedDSN+"&graph="+streamBindGraph+"&tenant="+url.QueryEscape(tenant))
		if err != nil {
			t.Fatalf("sql.Open %s: %v", tenant, err)
		}
		// SONNY-1227: one sql.DB per DSN, never closed and reopened mid-test.
		return db
	}

	a := open(streamBindTenantA)
	defer a.Close()
	b := open(streamBindTenantB)
	defer b.Close()

	// A run-unique key prefix. The engine keeps its store across tests in a
	// suite, so a fixed key would make a re-run read the previous run's rows
	// and hide exactly the defect this test exists to catch.
	prefix := fmt.Sprintf("sb-%d", time.Now().UnixNano())

	// ── Phase 1: write and read back on ONE request stream ───────────────────
	//
	// This is the weakest form of the invariant and the control for phase 2:
	// if it fails, the binding is missing from every stream, not just the
	// siblings.
	c0, err := a.Conn(ctx)
	if err != nil {
		t.Fatalf("tenant-a Conn: %v", err)
	}
	sameKey := prefix + "-same-stream"
	if _, err := c0.ExecContext(ctx,
		"MERGE (n:StreamBindProbe {k: ?}) ON CREATE SET n.v = ?", sameKey, "v"); err != nil {
		_ = c0.Close()
		t.Fatalf("tenant-a write on its own stream: %v", err)
	}
	var sameCount int64
	if err := c0.QueryRowContext(ctx,
		"MATCH (n:StreamBindProbe {k: ?}) RETURN count(n)", sameKey).Scan(&sameCount); err != nil {
		_ = c0.Close()
		t.Fatalf("tenant-a readback on its own stream: %v", err)
	}
	if err := c0.Close(); err != nil {
		t.Fatalf("close tenant-a stream: %v", err)
	}
	if sameCount != 1 {
		t.Errorf("same-stream readback saw %d rows, want 1 — the request stream that "+
			"wrote the row cannot see it, so it is not carrying the session's "+
			"tenant/graph binding (GAP-SONNY806-08)", sameCount)
	}

	// ── Phase 2: N request streams held open at once ─────────────────────────
	//
	// Every Conn is acquired BEFORE any of them is used, so database/sql cannot
	// hand the same underlying stream out twice and all N are live siblings on
	// the pooled QUIC connection.
	conns := make([]*sql.Conn, 0, streamBindConns)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < streamBindConns; i++ {
		c, err := a.Conn(ctx)
		if err != nil {
			t.Fatalf("tenant-a Conn %d: %v", i, err)
		}
		conns = append(conns, c)
	}

	keys := make([]string, streamBindConns)
	for i, c := range conns {
		keys[i] = fmt.Sprintf("%s-stream-%d", prefix, i)
		if _, err := c.ExecContext(ctx,
			"MERGE (n:StreamBindProbe {k: ?}) ON CREATE SET n.v = ?", keys[i], "v"); err != nil {
			t.Fatalf("tenant-a write on stream %d: %v", i, err)
		}
	}

	// Every stream must see every row the session wrote, including the ones
	// written by its siblings. A stream that reads 0 is reading in a different
	// scope from the one that wrote.
	for i, c := range conns {
		for j, key := range keys {
			var n int64
			if err := c.QueryRowContext(ctx,
				"MATCH (n:StreamBindProbe {k: ?}) RETURN count(n)", key).Scan(&n); err != nil {
				t.Fatalf("tenant-a read of key %d on stream %d: %v", j, i, err)
			}
			if n != 1 {
				t.Errorf("stream %d saw %d rows for the row written by stream %d, want 1 — "+
					"this request stream is not carrying the session's tenant/graph "+
					"binding (GAP-SONNY806-08)", i, n, j)
			}
		}
	}

	// ── Phase 3: the isolation half ──────────────────────────────────────────
	//
	// Restoring the binding must not restore the old cross-tenant leak
	// (SONNY-1229): a second tenant must see none of these rows, on a fresh
	// stream and on a sibling of a stream it already used.
	bConns := make([]*sql.Conn, 0, 2)
	defer func() {
		for _, c := range bConns {
			_ = c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		c, err := b.Conn(ctx)
		if err != nil {
			t.Fatalf("tenant-b Conn %d: %v", i, err)
		}
		bConns = append(bConns, c)
	}
	for i, c := range bConns {
		for j, key := range append([]string{sameKey}, keys...) {
			var n int64
			if err := c.QueryRowContext(ctx,
				"MATCH (n:StreamBindProbe {k: ?}) RETURN count(n)", key).Scan(&n); err != nil {
				t.Fatalf("tenant-b read of key %d on stream %d: %v", j, i, err)
			}
			if n != 0 {
				t.Fatalf("tenant-b stream %d saw %d of tenant-a's rows for key %d, want 0 "+
					"— CROSS-TENANT LEAK", i, n, j)
			}
		}
	}
}

//go:build integration

// integration_retryclass_test.go proves the retry classification against the
// pinned engine (geodedb/geode:0.8.15 — SONNY-806 / GEO-GO-UP-001, Lane D
// Task 12).
//
// Two failures a GraphRAG ingest worker actually meets, one at each end of the
// classification:
//
//   - a retrieval the caller cancelled leaves its Session riding a dead
//     connection. The cancellation itself is RetryNever (the caller asked for
//     it), but the retirement that follows is RetryNewConn, and
//     Pool.RetrySession must recover by discarding the connection and pinning a
//     fresh one.
//   - a write refused because the session's ROLE is read-only is RetryNever,
//     and must be attempted exactly once however many attempts the policy
//     allows. Retrying an authorization refusal is the hot loop this whole
//     classification exists to prevent.
package geode

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"testing"
	"time"
)

// TestIntegration_CancelledRetrievalClassifiesNewConnAndRecovers walks one
// cancelled retrieval through the full class contract against a live engine.
func TestIntegration_CancelledRetrievalClassifiesNewConnAndRecovers(t *testing.T) {
	base, cancelBase := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancelBase()

	seedDB, err := sql.Open("geode", cancelDSN("&page_size=10"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer seedDB.Close()
	ensureCancelTenant(base, t)
	seedCancelRows(t, seedDB, cancelProbeRows)

	pool, err := NewPool(cancelDSN("&page_size=10&pool_size=4"),
		WithMaxOpenConns(4), WithMaxIdleConns(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	policy := DefaultRetryPolicy()
	// The default 1s initial backoff is a real wait this test does not need;
	// the classes and the re-pin are what is under test, not the clock.
	policy.InitialBackoff = 50 * time.Millisecond
	policy.MaxBackoff = 200 * time.Millisecond

	var (
		attempts  int
		connIDs   []uint64
		cancelCls RetryClass
		retireCls RetryClass
	)

	err = pool.RetrySession(base, policy, func(ctx context.Context, s *Session) error {
		attempts++
		connIDs = append(connIDs, s.ConnID())

		// Later attempts ride a fresh connection and must simply succeed.
		if attempts > 1 {
			// Session exposes no QueryRow; the Rows are drained explicitly so
			// the pinned connection is released before this attempt returns.
			rows, qerr := s.QueryContext(ctx, "MATCH (n:CancelProbe) RETURN count(n)")
			if qerr != nil {
				return qerr
			}
			defer rows.Close() //nolint:errcheck // rows.Err below is the verdict
			for rows.Next() {
				var n int64
				if serr := rows.Scan(&n); serr != nil {
					return serr
				}
			}
			return rows.Err()
		}

		// Attempt 1: cancel a retrieval mid-stream, exactly as Task 9 proved.
		qctx, cancelQuery := context.WithCancel(ctx)
		defer cancelQuery()

		rows, qerr := s.QueryContext(qctx, "MATCH (n:CancelProbe) RETURN n.i")
		if qerr != nil {
			return qerr
		}
		read := 0
		for rows.Next() {
			read++
			if read == 5 {
				cancelQuery()
			}
		}
		rowsErr := rows.Err()
		_ = rows.Close()

		if !errors.Is(rowsErr, context.Canceled) {
			t.Fatalf("Rows.Err after cancel = %v, want context.Canceled "+
				"(read %d of %d rows; the result must span pages)",
				rowsErr, read, cancelProbeRows)
		}
		// The CALLER's own cancellation is never retried: the caller asked for
		// it, and asking again is not this driver's decision to make.
		cancelCls = Classify(rowsErr)

		// The retirement it left behind is the retryable half. The next
		// statement on this Session reports it, and THAT is what must classify
		// as "retry on a different connection".
		_, execErr := s.ExecContext(ctx, "MERGE (n:RetryClassProbe {k: ?})", "x")
		if execErr == nil {
			t.Fatal("a statement on a Session retired by a cancelled retrieval succeeded")
		}
		retireCls = Classify(execErr)
		return execErr
	})
	if err != nil {
		t.Fatalf("RetrySession: %v (attempts=%d)", err, attempts)
	}

	if cancelCls != RetryNever {
		t.Errorf("Classify(cancelled Rows.Err) = %v, want %v — a cancellation the "+
			"caller asked for must never be retried", cancelCls, RetryNever)
	}
	if retireCls != RetryNewConn {
		t.Errorf("Classify(post-cancel statement err) = %v, want %v",
			retireCls, RetryNewConn)
	}
	if attempts < 2 {
		t.Fatalf("attempts = %d, want at least 2 — the retry never ran", attempts)
	}

	// The recovery is the point: the successful attempt rode a DIFFERENT
	// connection than the one the cancellation killed. Without the discard,
	// RetrySession would have re-run on the same dead connection.
	if connIDs[0] == connIDs[len(connIDs)-1] {
		t.Fatalf("the retry reused the retired connection %d (conn ids: %v)",
			connIDs[0], connIDs)
	}
	t.Logf("recovered after %d attempt(s) across connections %v", attempts, connIDs)

	// A pin taken straight after a retirement can itself fail on its first
	// write with a driver.ErrBadConn-matching error (SONNY-1227: the engine
	// tears down the pooled QUIC connection whose handshake stream went away,
	// and the driver only learns of it on the next write). That failure is
	// RetryNewConn too, so RetrySession absorbs it rather than surfacing the
	// "close the Session and pin again" dance to the caller — which is why
	// more than two attempts here is a pass, not a surprise.
	if !errors.Is(ErrSessionRetired, driver.ErrBadConn) {
		t.Error("ErrSessionRetired no longer matches driver.ErrBadConn; " +
			"the RetryNewConn classification above rests on it")
	}
}

// TestIntegration_ReadOnlyRoleRefusalIsNeverRetried proves the other end: an
// authorization refusal from the ENGINE is attempted exactly once.
//
// The client-side read-only gate is deliberately off (no read_only=true in the
// DSN), so the refusal can only come from the engine's role enforcement — the
// same GQLSTATE 42501 TestIntegration_ReadOnlyRole pins. Retrying it would
// hammer a permanently refusing engine, which is the failure mode Task 12
// exists to prevent.
func TestIntegration_ReadOnlyRoleRefusalIsNeverRetried(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const roleGraph = "retryclassrole"

	admin, err := sql.Open("geode", sharedDSN+"&graph="+roleGraph)
	if err != nil {
		t.Fatalf("sql.Open admin: %v", err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping admin (creates the graph): %v", err)
	}

	// Dev-only credential for the integration lane, created the same way
	// TestIntegration_ReadOnlyRole does (0.8.15 auth DDL; "readonly" is a
	// recognised role name). READ on the graph is the narrowest grant that
	// still lets HELLO bind it, so the CREATE below is refused for being a
	// WRITE and not for being unable to reach the graph at all.
	const roUser = "retryclass_ro"
	const roPass = "Correct horse battery staple rc9" // pragma: allowlist secret // gitleaks:allow
	if _, err := admin.ExecContext(ctx,
		"CREATE USER "+roUser+" SET PASSWORD '"+roPass+"' ROLE readonly"); err != nil {
		t.Logf("CREATE USER: %v (may already exist)", err)
	}
	if _, err := admin.ExecContext(ctx,
		"GRANT READ ON GRAPH "+roleGraph+" TO "+roUser); err != nil {
		t.Logf("GRANT READ ON GRAPH: %v (may already be granted)", err)
	}

	// hostPortFromDSN drops the query string, so the tenant has to be re-bound:
	// under strict tenancy an unscoped statement is refused with the SAME 42501
	// class as the role refusal, and the assertion would pass for the wrong
	// reason.
	roDSN := hostPortFromDSN(sharedDSN) +
		"?insecure=true&user=" + url.QueryEscape(roUser) +
		"&pass=" + url.QueryEscape(roPass) +
		"&graph=" + roleGraph +
		"&tenant=" + harnessTenant +
		"&role=readonly"

	ro, err := sql.Open("geode", roDSN)
	if err != nil {
		t.Fatalf("sql.Open readonly: %v", err)
	}
	defer ro.Close()

	policy := DefaultRetryPolicy()
	policy.MaxAttempts = 5 // generous on purpose: the CLASS must stop it, not the count
	policy.InitialBackoff = 10 * time.Millisecond

	attempts := 0
	start := time.Now()
	err = Retry(ctx, policy, func(rctx context.Context) error {
		attempts++
		_, execErr := ro.ExecContext(rctx, "CREATE (n:RetryClassRole {k: ?})", "x")
		return execErr
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a readonly-role write succeeded, want a 42501 refusal from the engine")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want exactly 1 — an authorization refusal was retried", attempts)
	}

	// It must be the ENGINE's refusal, not the client gate: this DSN sets no
	// read_only=true, so a client-gate error would mean the test proved nothing
	// about classifying a server refusal.
	if errors.Is(err, ErrReadOnly) {
		t.Fatalf("the write was refused by the CLIENT gate (%v); this test must "+
			"exercise the engine", err)
	}
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("readonly-role write err = %T %v, want a *DriverError", err, err)
	}
	if de.Code != StatusPermissionDenied && de.Class != StatusPermissionDenied {
		t.Fatalf("readonly-role write err = %v (code=%q class=%q), want GQLSTATE %s",
			err, de.Code, de.Class, StatusPermissionDenied)
	}
	if got := Classify(de); got != RetryNever {
		t.Fatalf("Classify(engine 42501 refusal) = %v, want %v", got, RetryNever)
	}

	// No backoff was paid, because no retry was scheduled. The bound is loose
	// — this asserts "did not sleep through four backoffs", not a latency SLO.
	if elapsed > 5*time.Second {
		t.Errorf("a single non-retried attempt took %v; it appears to have backed off", elapsed)
	}

	// The wrapper names the class that stopped the retry, and the cause is
	// still reachable through it.
	t.Logf("engine refusal surfaced as: %v", err)
	if !errors.Is(err, de) {
		t.Errorf("errors.Is lost the DriverError through Retry's class wrapper: %v", err)
	}
}

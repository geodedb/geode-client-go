// conn_abandoned_statement_test.go — SONNY-806, Lane D Task 16e.
//
// A context deadline on a statement whose bytes are already on the wire does
// not mean the statement did not happen, and on QUIC it does not even mean it
// stopped. The wire protocol carries no cancel message, so the engine runs the
// statement to completion and every statement after it on that graph waits
// behind it. Callers that read the bare `context deadline exceeded` as "it did
// not run" write cleanup helpers that throw the error away — which is how one
// over-budget delete cascaded through forty later tests in pipeline 2855806396.
package geode

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestStillRunningOnTheEngine_KeepsTheSentinel(t *testing.T) {
	for _, base := range []error{context.DeadlineExceeded, context.Canceled} {
		got := stillRunningOnTheEngine(base)
		if !errors.Is(got, base) {
			t.Fatalf("errors.Is(%v, %v) = false: the annotation must not break "+
				"classification built on the sentinel", got, base)
		}
		if !strings.Contains(got.Error(), "abandoned") {
			t.Fatalf("message %q does not say the statement was abandoned rather "+
				"than cancelled", got.Error())
		}
	}
}

func TestStillRunningOnTheEngine_LeavesOtherErrorsAlone(t *testing.T) {
	if got := stillRunningOnTheEngine(io.EOF); got != io.EOF {
		t.Fatalf("stillRunningOnTheEngine(io.EOF) = %v, want io.EOF unchanged: only "+
			"a caller-side context error means the engine is still working", got)
	}
}

// TestConn_ExecContextDeadline_SaysTheEngineIsStillRunningIt drives the real
// path: a statement whose response never arrives before the caller's deadline.
func TestConn_ExecContextDeadline_SaysTheEngineIsStillRunningIt(t *testing.T) {
	mt := newMockTransport()
	c := newConn(&Config{PageSize: 100}, mt)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := c.ExecContext(ctx, "MATCH (n) DETACH DELETE n", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ExecContext = %v, want a context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "still executing") {
		t.Fatalf("ExecContext error %q reads like a statement that did not run; it "+
			"must say the engine is still executing it", err.Error())
	}
}

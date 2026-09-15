// driver_badconn_test.go — SONNY-1219 / Lane D (SONNY-806).
//
// database/sql re-dials only for driver.ErrBadConn. Connector.Connect used to
// return the raw *TransportError, so a caller whose pooled connection had been
// torn down under it saw the transport error instead of a retry on a fresh
// connection — 56 of the 64 callers in the burst. These tests pin what
// markBadConn marks, what it leaves alone, and that marking never costs the
// driver its own classification.

package geode

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestMarkBadConn_ConnectionLevelFailureIsRetriable(t *testing.T) {
	te := &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}
	err := markBadConn(te)

	if !errors.Is(err, driver.ErrBadConn) {
		t.Error("a connection-level failure must be driver.ErrBadConn so database/sql re-dials")
	}
	var got *TransportError
	if !errors.As(err, &got) || got != te {
		t.Error("the classified *TransportError must survive the marking")
	}
	if cls := Classify(err); cls != RetryNewConn {
		t.Errorf("Classify = %v, want RetryNewConn (not RetryBackoff: this is not resource pressure)", cls)
	}
	if err.Error() != te.Error() {
		t.Errorf("the marking changed the message: %q vs %q", err.Error(), te.Error())
	}
}

func TestMarkBadConn_WrappedHelloFailureIsRetriable(t *testing.T) {
	te := &TransportError{Op: "read_length", Addr: "127.0.0.1:3141", cause: io.EOF}
	err := markBadConn(wrapHelloRecv(te))

	if !errors.Is(err, driver.ErrBadConn) {
		t.Error("a HELLO failure wrapped by conn.hello must still be marked")
	}
	if cls := Classify(err); cls != RetryNewConn {
		t.Errorf("Classify = %v, want RetryNewConn", cls)
	}
}

func TestMarkBadConn_LeavesNonConnectionErrorsAlone(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryClass
	}{
		{"nil", nil, RetryNever},
		{"dial failure", &TransportError{Op: constOpDial, Addr: "a", cause: io.EOF}, RetryNewConn},
		// grpc.NewClient is lazy, so an unreachable gRPC server never fails at
		// the dial — it fails at the HELLO. Marking that would make
		// database/sql pay the connect timeout three times over against an
		// endpoint that was never reachable.
		{
			"grpc handshake against an unreachable server",
			&TransportError{Op: constOpHandshake, Addr: "a", cause: io.EOF},
			RetryNewConn,
		},
		{"auth refusal", &DriverError{Code: "AUTH", Message: "Authentication required"}, RetryNever},
		{"pool draining", ErrPoolDraining, RetryNever},
		{"pool exhausted", ErrPoolExhausted, RetryBackoff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := markBadConn(tc.err)
			if tc.err == nil {
				if err != nil {
					t.Fatalf("markBadConn(nil) = %v, want nil", err)
				}
				return
			}
			if errors.Is(err, driver.ErrBadConn) {
				t.Errorf("%v must not be reported as driver.ErrBadConn", tc.err)
			}
			if cls := Classify(err); cls != tc.want {
				t.Errorf("Classify = %v, want %v", cls, tc.want)
			}
		})
	}
}

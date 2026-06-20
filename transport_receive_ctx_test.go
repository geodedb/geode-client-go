package geode

import (
	"bufio"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeDeadlineStream models a quic.Stream's read side where data never
// arrives: a Read returns only when a read deadline elapses. This reproduces
// the half-open / server-reaped stream condition (active_streams=0) from the
// receive-path deadlock report — the server closed its handler without
// delivering a terminator frame, so the client's Read would otherwise block
// forever. Honoring deadline timing (future vs past) lets the same fake drive
// both the cancellation and deadline tests.
type fakeDeadlineStream struct {
	mu       sync.Mutex
	deadline time.Time
	rearm    chan struct{} // signaled whenever the deadline changes
}

func newFakeDeadlineStream() *fakeDeadlineStream {
	return &fakeDeadlineStream{rearm: make(chan struct{}, 1)}
}

func (f *fakeDeadlineStream) SetReadDeadline(t time.Time) error {
	f.mu.Lock()
	f.deadline = t
	f.mu.Unlock()
	select {
	case f.rearm <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeDeadlineStream) Read(_ []byte) (int, error) {
	for {
		f.mu.Lock()
		dl := f.deadline
		f.mu.Unlock()

		if dl.IsZero() {
			// No deadline set: block until a deadline is armed. Without the
			// fix, a cancellable (but deadline-less) context never arms one,
			// so this Read — and ReceiveProto — would block forever.
			<-f.rearm
			continue
		}

		if d := time.Until(dl); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
				timer.Stop()
				return 0, os.ErrDeadlineExceeded
			case <-f.rearm:
				timer.Stop()
				// Deadline changed; re-evaluate.
				continue
			}
		}
		return 0, os.ErrDeadlineExceeded
	}
}

func newFakeTransport(fs *fakeDeadlineStream) *QUICTransport {
	return &QUICTransport{
		rdStream: fs,
		br:       bufio.NewReaderSize(fs, quicReadBufferSize),
		addr:     "fake",
	}
}

// TestReceiveProto_HonorsContextCancellation is the regression test for the
// receive-path deadlock: a query whose context is cancelled mid-read must not
// hang. Before the fix, ReceiveProto only checked ctx.Done() once before the
// blocking read and only acted on a context *deadline*, so a manual cancel
// during io.ReadFull was never observed and the call blocked indefinitely.
func TestReceiveProto_HonorsContextCancellation(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport(newFakeDeadlineStream())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, err := tr.ReceiveProto(ctx)
		done <- result{err: err}
	}()

	// Let ReceiveProto reach the blocking read, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReceiveProto did not return after context cancellation — deadlock")
	}
}

// TestReceiveProto_HonorsContextDeadline verifies a deadline-bounded receive
// fails at the caller's deadline rather than blocking forever on a stream the
// server has reaped without a terminator.
func TestReceiveProto_HonorsContextDeadline(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport(newFakeDeadlineStream())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := tr.ReceiveProto(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReceiveProto did not return at the context deadline — deadlock")
	}
}

// TestReceiveProto_AlreadyCancelled keeps the fast-path guard honest: a context
// already cancelled before the call returns immediately without touching the
// stream.
func TestReceiveProto_AlreadyCancelled(t *testing.T) {
	t.Parallel()

	tr := newFakeTransport(newFakeDeadlineStream())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tr.ReceiveProto(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

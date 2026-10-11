package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"geodedb.com/geode/proto"
)

func pullFrame(response *proto.ExecutionResponse) *proto.QuicServerMessage {
	return &proto.QuicServerMessage{Msg: &proto.QuicServerMessage_Pull{
		Pull: &proto.PullResponse{Response: response},
	}}
}

func pullPage(values []string, final bool) *proto.QuicServerMessage {
	return pullFrame(mockPageMsgFinal(values, final).GetExecute())
}

func noActiveResultPull() *proto.QuicServerMessage {
	return pullFrame(mockErrorMsg("02000", "no active result set").GetExecute())
}

type pendingReadTransport struct {
	*mockTransport
	waiting chan struct{}
	once    sync.Once
}

func (m *pendingReadTransport) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	if len(m.recv) == 0 {
		m.once.Do(func() { close(m.waiting) })
	}
	return m.mockTransport.ReceiveProto(ctx)
}

func TestRows_PullDrivenPagesCompleteEachRequest(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, rows, m := openMidStreamRows(ctx, t, []string{"a"})
	m.push(pullPage([]string{"b"}, false))
	m.push(pullPage([]string{"c"}, true))
	if got := drainStrings(t, rows); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("rows = %v", got)
	}
	if got := countPullMessages(m); got != 2 {
		t.Fatalf("PULL requests = %d, want 2", got)
	}
	if err := rows.Close(); err != nil || !c.IsValid() {
		t.Fatalf("completed stream is not reusable: %v", err)
	}
}

func TestRows_InlinePagesWaitForKnownPullBeforeEOF(t *testing.T) {
	fastDrainTimeout(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, rows, m := openMidStreamRows(ctx, t, []string{"a"})
	wire := &pendingReadTransport{mockTransport: m, waiting: make(chan struct{})}
	c.transport = wire
	m.push(mockPageMsgFinal([]string{"b"}, false))
	m.push(mockPageMsgFinal([]string{"c"}, true))
	dest := make([]driver.Value, 1)
	for _, want := range []string{"a", "b", "c"} {
		if err := rows.Next(dest); err != nil || dest[0] != want {
			t.Fatalf("row = %v, err = %v, want %q", dest, err, want)
		}
	}
	result := make(chan error, 1)
	go func() { result <- rows.Next(dest) }()
	select {
	case <-wire.waiting:
	case err := <-result:
		t.Fatalf("EOF returned before the outstanding PULL reply: %v", err)
	case <-ctx.Done():
		t.Fatal("never waited for the outstanding PULL reply")
	}
	m.push(mockMetricsMsg())
	m.push(noActiveResultPull())
	if err := <-result; !errors.Is(err, io.EOF) {
		t.Fatalf("completed rows: %v", err)
	}
	if got := countPullMessages(m); got != 1 {
		t.Fatalf("sent %d PULLs; inline pages must stop further PULLs", got)
	}
	if err := rows.Close(); err != nil || !c.IsValid() {
		t.Fatalf("completed stream is not reusable: %v", err)
	}
}

func TestRows_InlineCompletionRejectsUnexpectedFrames(t *testing.T) {
	cases := []struct {
		name  string
		frame *proto.QuicServerMessage
	}{
		{"unsolicited execute", mockPageMsgFinal(nil, true)},
		{"missing payload", pullFrame(nil)},
		{"rows after final", pullPage([]string{"extra"}, true)},
		{"non-final reply", pullPage(nil, false)},
		{"revoked authorization", pullFrame(mockErrorMsg("28000", "revoked").GetExecute())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c, rows, m := openMidStreamRows(ctx, t, nil)
			m.push(mockPageMsgFinal(nil, true))
			m.push(tc.frame)
			if err := rows.Next(make([]driver.Value, 1)); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("invalid completion was accepted: %v", err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if c.IsValid() {
				t.Fatal("invalid completion returned a reusable connection")
			}
		})
	}
}

func TestRows_InlineCompletionCancellationRetiresStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, rows, m := openMidStreamRows(ctx, t, nil)
	wire := &pendingReadTransport{mockTransport: m, waiting: make(chan struct{})}
	c.transport = wire
	m.push(mockPageMsgFinal(nil, true))
	result := make(chan error, 1)
	go func() { result <- rows.Next(make([]driver.Value, 1)) }()
	select {
	case <-wire.waiting:
	case err := <-result:
		t.Fatalf("returned before the pending response: %v", err)
	case <-time.After(time.Second):
		t.Fatal("did not wait for pending response")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if err := rows.Close(); err != nil || c.IsValid() {
		t.Fatalf("cancelled stream remained reusable: %v", err)
	}
}

func TestRows_CloseWithPendingPullRetiresWithoutConsumingFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, rows, m := openMidStreamRows(ctx, t, nil)
	m.push(mockPageMsgFinal([]string{"last"}, true))
	m.push(noActiveResultPull())
	if err := rows.Next(make([]driver.Value, 1)); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil || c.IsValid() {
		t.Fatalf("stream with pending reply remained reusable: %v", err)
	}
	if len(m.recv) != 1 {
		t.Fatal("Close speculatively consumed the pending response")
	}
}

func TestRows_InlineCompletionBoundsMetadataFrames(t *testing.T) {
	for _, metadataFrames := range []int{0, 63, 64} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c, rows, m := openMidStreamRows(ctx, t, nil)
		m.recv = make(chan *proto.QuicServerMessage, metadataFrames+2)
		m.push(mockPageMsgFinal(nil, true))
		for range metadataFrames {
			m.push(mockMetricsMsg())
		}
		m.push(pullPage(nil, true))
		err := rows.Next(make([]driver.Value, 1))
		wantSuccess := metadataFrames < 64
		if errors.Is(err, io.EOF) != wantSuccess || err == nil {
			t.Errorf("%d metadata frames: EOF=%v, want %v (error %v)", metadataFrames, errors.Is(err, io.EOF), wantSuccess, err)
		}
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
		if c.IsValid() != wantSuccess {
			t.Errorf("%d metadata frames: connection validity does not match completion", metadataFrames)
		}
		cancel()
	}
}

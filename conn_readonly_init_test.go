package geode

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"geodedb.com/geode/proto"
)

// initRecordingTransport is a Transport that records every ExecuteRequest sent
// during connection initialisation and answers each one with an empty (no
// schema, no page, no error) ExecutionResponse, so execInit completes without
// a server.
type initRecordingTransport struct {
	mu       sync.Mutex
	queries  []string
	readOnly []bool
	replies  chan *proto.QuicServerMessage
	closed   bool
}

func newInitRecordingTransport() *initRecordingTransport {
	return &initRecordingTransport{replies: make(chan *proto.QuicServerMessage, 8)}
}

func (t *initRecordingTransport) SendProto(_ context.Context, msg *proto.QuicClientMessage) error {
	exec := msg.GetExecute()
	if exec == nil {
		return errors.New("initRecordingTransport: expected an Execute message")
	}
	t.mu.Lock()
	t.queries = append(t.queries, exec.GetQuery())
	t.readOnly = append(t.readOnly, exec.GetReadOnly())
	t.mu.Unlock()

	// Empty ExecutionResponse: success with no schema and no page, so
	// execInit returns without draining a stream.
	t.replies <- &proto.QuicServerMessage{
		Msg: &proto.QuicServerMessage_Execute{Execute: &proto.ExecutionResponse{}},
	}
	return nil
}

func (t *initRecordingTransport) ReceiveProto(ctx context.Context) (*proto.QuicServerMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-t.replies:
		return msg, nil
	}
}

func (t *initRecordingTransport) TryReceiveProtoBuffered() (*proto.QuicServerMessage, bool, error) {
	return nil, false, nil
}

func (t *initRecordingTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *initRecordingTransport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *initRecordingTransport) RemoteClosed() bool { return false }

func (t *initRecordingTransport) Addr() string { return "recording" }

func (t *initRecordingTransport) sent() ([]string, []bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.queries...), append([]bool(nil), t.readOnly...)
}

// TestPooledGraphInit_ReadOnlySkipsCreateGraph pins the DEFAULT pooled path
// (Connector.Connect → connectPooled → pooledGraphInit, the path every
// sql.Open("geode", …) takes) against the same rule the standalone connect()
// path follows: a read-only connection must not emit CREATE GRAPH during
// setup.
//
// The graph is deliberately NOT bound by HELLO here (pooledConn.graphBound is
// false), which is the only case that reaches the CREATE/USE GRAPH pair. Since
// execInit carries ExecuteRequest.read_only, an unguarded CREATE GRAPH would
// be refused by the engine with GQLSTATE 42501 and connection setup would fail
// outright.
func TestPooledGraphInit_ReadOnlySkipsCreateGraph(t *testing.T) {
	tests := []struct {
		name     string
		readOnly bool
		want     []string
	}{
		{
			name:     "read-only issues USE GRAPH only",
			readOnly: true,
			want:     []string{"USE GRAPH g"},
		},
		{
			name:     "writable still creates then uses",
			readOnly: false,
			want:     []string{"CREATE GRAPH IF NOT EXISTS g", "USE GRAPH g"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Graph: "g", ReadOnly: tc.readOnly}
			rec := newInitRecordingTransport()
			conn := newConn(cfg, rec)

			// An unbound graph is the precondition: HELLO did not bind it,
			// so pooledGraphInit must do the graph selection itself.
			transport := &QUICTransport{pc: &pooledConn{}}
			if transport.pc.isGraphBound() {
				t.Fatal("precondition: graph must not be bound by HELLO")
			}

			if err := pooledGraphInit(context.Background(), conn, cfg, transport); err != nil {
				t.Fatalf("pooledGraphInit: %v", err)
			}

			got, readOnlyFlags := rec.sent()
			if len(got) != len(tc.want) {
				t.Fatalf("statements = %q, want %q", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("statement %d = %q, want %q", i, got[i], tc.want[i])
				}
				if readOnlyFlags[i] != tc.readOnly {
					t.Errorf("statement %d read_only = %v, want %v", i, readOnlyFlags[i], tc.readOnly)
				}
			}
			for _, q := range got {
				if strings.Contains(q, "CREATE GRAPH") && tc.readOnly {
					t.Errorf("read-only setup emitted a write: %q", q)
				}
			}
		})
	}
}

// TestPooledGraphInit_BoundGraphSkipsBoth covers the other pooled branch: when
// HELLO bound the graph, neither statement is emitted on either mode.
func TestPooledGraphInit_BoundGraphSkipsBoth(t *testing.T) {
	for _, readOnly := range []bool{true, false} {
		cfg := &Config{Graph: "g", ReadOnly: readOnly}
		rec := newInitRecordingTransport()
		conn := newConn(cfg, rec)

		transport := &QUICTransport{pc: &pooledConn{}}
		transport.pc.markAuthed("g")

		if err := pooledGraphInit(context.Background(), conn, cfg, transport); err != nil {
			t.Fatalf("read_only=%v: pooledGraphInit: %v", readOnly, err)
		}
		if got, _ := rec.sent(); len(got) != 0 {
			t.Errorf("read_only=%v: bound graph emitted %q, want nothing", readOnly, got)
		}
	}
}

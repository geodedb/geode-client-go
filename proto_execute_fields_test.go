package geode

import (
	"testing"

	"geodedb.com/geode/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestExecuteRequest_HasCanonicalFields guards against vendored-proto drift.
// The canonical engine proto (geode/src/proto/geode.proto:100-107) defines
// six fields on ExecuteRequest. Field 6 (read_only) is the wire mechanism for
// read-only mode and field 5 (idempotency_key) is the write-dedup mechanism;
// neither can be used while the vendored copy stops at field 3.
func TestExecuteRequest_HasCanonicalFields(t *testing.T) {
	//nolint:exhaustive // This table lists message fields; reserved wire numbers are not fields.
	want := map[protoreflect.FieldNumber]string{
		1: "session_id",
		2: "query",
		3: "params",
		4: "principal_envelope_bytes",
		5: "idempotency_key",
		6: "read_only",
	}

	fields := (&proto.ExecuteRequest{}).ProtoReflect().Descriptor().Fields()
	got := make(map[protoreflect.FieldNumber]string, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		got[f.Number()] = string(f.Name())
	}

	for num, name := range want {
		if got[num] != name {
			t.Errorf("field %d = %q, want %q", num, got[num], name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("field count = %d, want %d (got %v)", len(got), len(want), got)
	}
}

// TestExecuteRequest_ReadOnlySettable proves the generated Go struct exposes
// the read-only flag as a settable bool, which Task 6 depends on.
func TestExecuteRequest_ReadOnlySettable(t *testing.T) {
	req := &proto.ExecuteRequest{Query: "RETURN 1", ReadOnly: true, IdempotencyKey: "k1"}
	if !req.GetReadOnly() {
		t.Error("GetReadOnly() = false, want true")
	}
	if req.GetIdempotencyKey() != "k1" {
		t.Errorf("GetIdempotencyKey() = %q, want %q", req.GetIdempotencyKey(), "k1")
	}
}

package geode

import (
	"testing"

	"geodedb.com/geode/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestCdcBackupRestoreUploadFields guards against vendored-proto drift for the
// fields added by the regeneration recorded in .superpowers/sdd/task-2-report.md
// (SONNY-806): session_id on CdcDiagnosticsRequest, CdcControlRequest,
// BackupRequest, RestoreRequest, and UploadBackupRequest, plus payload on
// UploadBackupRequest. Keep this table in sync with the canonical engine proto
// (geode/src/proto/geode.proto) messages mirrored at proto/geode.proto:489-593.
// A future regeneration that drops or renumbers any of these fields fails here.
func TestCdcBackupRestoreUploadFields(t *testing.T) {
	tests := []struct {
		name string
		msg  interface{ ProtoReflect() protoreflect.Message }
		want map[protoreflect.FieldNumber]string
	}{
		{
			name: "CdcDiagnosticsRequest",
			msg:  &proto.CdcDiagnosticsRequest{},
			want: map[protoreflect.FieldNumber]string{
				1: "session_id",
			},
		},
		{
			name: "CdcControlRequest",
			msg:  &proto.CdcControlRequest{},
			want: map[protoreflect.FieldNumber]string{
				1: "action",
				2: "session_id",
			},
		},
		{
			name: "BackupRequest",
			msg:  &proto.BackupRequest{},
			want: map[protoreflect.FieldNumber]string{
				1: "backup_type",
				2: "session_id",
				3: "compress",
			},
		},
		{
			name: "RestoreRequest",
			msg:  &proto.RestoreRequest{},
			want: map[protoreflect.FieldNumber]string{
				1: "target_time",
				2: "confirm",
				3: "session_id",
			},
		},
		{
			name: "UploadBackupRequest",
			msg:  &proto.UploadBackupRequest{},
			want: map[protoreflect.FieldNumber]string{
				1: "size_bytes",
				2: "checksum",
				3: "session_id",
				4: "payload",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := tt.msg.ProtoReflect().Descriptor().Fields()
			got := make(map[protoreflect.FieldNumber]string, fields.Len())
			for i := 0; i < fields.Len(); i++ {
				f := fields.Get(i)
				got[f.Number()] = string(f.Name())
			}

			for num, name := range tt.want {
				if got[num] != name {
					t.Errorf("%s field %d = %q, want %q", tt.name, num, got[num], name)
				}
			}
			if len(got) != len(tt.want) {
				t.Errorf("%s field count = %d, want %d (got %v)", tt.name, len(got), len(tt.want), got)
			}
		})
	}
}

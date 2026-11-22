package geode

import (
	"math"
	"testing"

	"geodedb.com/geode/proto"
)

// TestRowsAffectedFromCounts pins the rule that turns the engine's per-kind
// update counts (DataPage.update_counts, SONNY-2822) into one RowsAffected
// number. The end-to-end contract against a real engine is
// TestIntegration_RowsAffectedReportsWhatTheStatementChanged.
func TestRowsAffectedFromCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		counts *proto.UpdateCounts
		want   int64
	}{
		{"absent (engine predates update counts)", nil, 0},
		{"read: all zeros", &proto.UpdateCounts{}, 0},
		{"CREATE of 3 nodes with 2 properties each counts nodes", &proto.UpdateCounts{
			NodesCreated: 3, PropertiesSet: 6, LabelsAdded: 3,
		}, 3},
		{"CREATE of a relationship", &proto.UpdateCounts{
			RelationshipsCreated: 1, PropertiesSet: 1,
		}, 1},
		{"DETACH DELETE of 2 nodes and 1 relationship", &proto.UpdateCounts{
			NodesDeleted: 2, RelationshipsDeleted: 1,
		}, 3},
		{"MATCH ... SET of one property on 4 rows", &proto.UpdateCounts{PropertiesSet: 4}, 4},
		{"MATCH ... SET matching nothing", &proto.UpdateCounts{}, 0},
		{"SET of a label on 2 rows", &proto.UpdateCounts{LabelsAdded: 2}, 2},
		{"REMOVE of a label and a property on 1 row", &proto.UpdateCounts{
			LabelsRemoved: 1, PropertiesSet: 1,
		}, 2},
		{"a count past int64 saturates", &proto.UpdateCounts{NodesCreated: math.MaxUint64}, math.MaxInt64},
		{"a sum past int64 saturates", &proto.UpdateCounts{
			NodesCreated: math.MaxInt64, NodesDeleted: math.MaxInt64,
		}, math.MaxInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			res := newExecResult(tt.counts)
			got, err := res.RowsAffected()
			if err != nil {
				t.Fatalf("RowsAffected: %v", err)
			}
			if got != tt.want {
				t.Errorf("RowsAffected() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestResultUpdateCounts checks the exact per-kind counts stay reachable for a
// caller that needs more than one number, and that a server which sent none
// is told apart from one that changed nothing.
func TestResultUpdateCounts(t *testing.T) {
	t.Parallel()

	if _, ok := newExecResult(nil).UpdateCounts(); ok {
		t.Error("UpdateCounts() ok = true for a result with no counts on the wire")
	}

	got, ok := newExecResult(&proto.UpdateCounts{}).UpdateCounts()
	if !ok || got != (UpdateCounts{}) {
		t.Errorf("UpdateCounts() = %+v, %v for an all-zero report, want zero value, true", got, ok)
	}

	got, ok = newExecResult(&proto.UpdateCounts{
		NodesCreated: 1, NodesDeleted: 2, RelationshipsCreated: 3, RelationshipsDeleted: 4,
		PropertiesSet: 5, LabelsAdded: 6, LabelsRemoved: 7,
	}).UpdateCounts()
	want := UpdateCounts{
		NodesCreated: 1, NodesDeleted: 2, RelationshipsCreated: 3, RelationshipsDeleted: 4,
		PropertiesSet: 5, LabelsAdded: 6, LabelsRemoved: 7,
	}
	if !ok || got != want {
		t.Errorf("UpdateCounts() = %+v, %v, want %+v, true", got, ok, want)
	}
}

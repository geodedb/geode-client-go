package geode

import (
	"database/sql/driver"
	"math"

	"geodedb.com/geode/proto"
)

// Result represents the result of an Exec query.
type Result struct {
	lastInsertID int64
	rowsAffected int64
	counts       UpdateCounts
	hasCounts    bool
}

// Compile-time interface check.
var _ driver.Result = (*Result)(nil)

// UpdateCounts is what a statement changed, as the engine reports it on the
// final page of the result (DataPage.update_counts). The semantics are the
// openCypher / Bolt query statistics; see the engine's docs/BINARY_PROTOCOL.md
// "Update counts". A MATCH ... SET over N rows assigning k properties reports
// PropertiesSet = N*k.
type UpdateCounts struct {
	NodesCreated         int64
	NodesDeleted         int64
	RelationshipsCreated int64
	RelationshipsDeleted int64
	PropertiesSet        int64
	LabelsAdded          int64
	LabelsRemoved        int64
}

// newExecResult builds the Result of an Exec from the update counts on the
// final page, or from none when the engine predates them.
func newExecResult(pc *proto.UpdateCounts) *Result {
	if pc == nil {
		return &Result{}
	}
	counts := UpdateCounts{
		NodesCreated:         saturatingInt64(pc.GetNodesCreated()),
		NodesDeleted:         saturatingInt64(pc.GetNodesDeleted()),
		RelationshipsCreated: saturatingInt64(pc.GetRelationshipsCreated()),
		RelationshipsDeleted: saturatingInt64(pc.GetRelationshipsDeleted()),
		PropertiesSet:        saturatingInt64(pc.GetPropertiesSet()),
		LabelsAdded:          saturatingInt64(pc.GetLabelsAdded()),
		LabelsRemoved:        saturatingInt64(pc.GetLabelsRemoved()),
	}
	return &Result{
		rowsAffected: counts.rowsAffected(),
		counts:       counts,
		hasCounts:    true,
	}
}

// rowsAffected collapses the per-kind counts into the one number
// database/sql asks for.
//
// The engine reports no "rows matched" count, only what changed, so the rule
// follows the Neo4j JDBC driver's update count (StatementImpl.countUpdates):
// a statement that creates or deletes graph elements reports how many nodes
// and relationships it created and deleted; a statement that only changes
// properties or labels (MATCH ... SET / REMOVE) reports how many property
// assignments and label changes it made. Neo4j JDBC reports 1 for the latter;
// this driver reports the count, so a MATCH ... SET of one property over N
// rows reports N and over no row reports 0. A SET of k properties over N rows
// reports N*k; callers that need the exact kinds read UpdateCounts.
func (c UpdateCounts) rowsAffected() int64 {
	if structural := saturatingSum(c.NodesCreated, c.NodesDeleted,
		c.RelationshipsCreated, c.RelationshipsDeleted); structural > 0 {
		return structural
	}
	return saturatingSum(c.PropertiesSet, c.LabelsAdded, c.LabelsRemoved)
}

func saturatingInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// saturatingSum adds non-negative counts, capping at math.MaxInt64.
func saturatingSum(vs ...int64) int64 {
	var sum int64
	for _, v := range vs {
		if v > math.MaxInt64-sum {
			return math.MaxInt64
		}
		sum += v
	}
	return sum
}

// LastInsertId returns the last insert ID.
// Note: Geode doesn't support auto-increment IDs in the traditional sense,
// so this always returns 0.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns how many graph elements the statement changed, by the
// rule documented on UpdateCounts.rowsAffected: nodes and relationships
// created plus deleted when there are any, otherwise property assignments plus
// label changes. A MATCH ... SET of one property reports the number of rows it
// matched, and 0 when it matched none, so a conditional write can test it.
//
// An engine older than SONNY-2822 sends no update counts; RowsAffected then
// reports 0, as before, and UpdateCounts reports false.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// UpdateCounts returns the per-kind counts the engine reported for the
// statement, and false when the engine sent none (a release that predates
// them). database/sql wraps the driver's Result, so this is reachable through
// sql.Conn.Raw and a direct (*Conn).ExecContext, not through sql.Result.
func (r *Result) UpdateCounts() (UpdateCounts, bool) {
	return r.counts, r.hasCounts
}

package geode

import "database/sql/driver"

// Result represents the result of an Exec query.
type Result struct {
	lastInsertID int64
	rowsAffected int64
}

// Compile-time interface check.
var _ driver.Result = (*Result)(nil)

// LastInsertId returns the last insert ID.
// Note: Geode doesn't support auto-increment IDs in the traditional sense,
// so this always returns 0.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected.
// Note: Geode doesn't always provide this information,
// so this may return 0 even when rows were affected.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

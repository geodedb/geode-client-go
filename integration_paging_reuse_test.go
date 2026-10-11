//go:build integration

package geode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A fully consumed result must finish every request it put on the wire before
// the same physical connection executes a mutation. Pushed pages cannot leave
// their redundant PULL response for the next statement to mistake for its own.
func TestIntegration_MultipageReadThenWriteKeepsStreamSynchronized(t *testing.T) {
	const label = "PagingReuse1324"
	const count = 2500
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "MATCH (n:"+label+") DETACH DELETE n"); err != nil {
			t.Errorf("cleanup paging fixture: %v", err)
		}
	})
	createBulkNodes(t, db, label, count)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "MATCH (n:"+label+") RETURN n.id ORDER BY n.id")
	require.NoError(t, err)
	defer rows.Close()
	var seen int
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		seen++
		require.EqualValues(t, seen, id, "ordered row %d", seen)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, count, seen)

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer writeCancel()
	_, err = conn.ExecContext(writeCtx, fmt.Sprintf("CREATE (n:%s {id: %d})", label, count+1))
	require.NoError(t, err, "mutation after the fully consumed result must receive its own response")
	var remaining int64
	err = conn.QueryRowContext(writeCtx, "MATCH (n:"+label+") RETURN count(n)").Scan(&remaining)
	require.NoError(t, err)
	require.EqualValues(t, count+1, remaining, "same connection must see every original row and exactly one new row")
}

//go:build integration

// load_test.go exercises the driver under high parallelism and large data
// volumes to catch race conditions, connection pool exhaustion, QUIC stream
// desync, and multi-page result handling.
//
// All tests clean up their own data using distinct label prefixes so they can
// run safely alongside the rest of the integration suite.
package geode

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// openLoadDB opens a dedicated pool for load tests.
func openLoadDB(t *testing.T, maxOpen, maxIdle int) *sql.DB {
	t.Helper()
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set")
	}
	db, err := sql.Open("geode", sharedDSN)
	require.NoError(t, err)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(2 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx), "ping")
	return db
}

// openLoadDBWithPageSize opens a pool whose DSN has an explicit page_size.
// This is used to force multi-page results without needing huge datasets.
func openLoadDBWithPageSize(t *testing.T, maxOpen, maxIdle, pageSize int) *sql.DB {
	t.Helper()
	if sharedDSN == "" {
		t.Fatal("sharedDSN not set")
	}
	sep := "?"
	if strings.Contains(sharedDSN, "?") {
		sep = "&"
	}
	dsn := fmt.Sprintf("%s%spage_size=%d", sharedDSN, sep, pageSize)

	db, err := sql.Open("geode", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(2 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx), "ping with page_size=%d", pageSize)
	return db
}

// warmPool opens maxOpen connections concurrently so the QUIC handshakes
// complete before any load goroutine starts.  This prevents the first batch of
// workers from all racing to establish connections simultaneously.
func warmPool(t *testing.T, db *sql.DB, maxOpen int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < maxOpen; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			db.PingContext(ctx) //nolint:errcheck
		}()
	}
	wg.Wait()
}

// cleanupLabel deletes all nodes with the given GQL label.
func cleanupLabel(t *testing.T, db *sql.DB, label string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("MATCH (n:%s) DETACH DELETE n", label))
}

// loadCtx returns a context with a 30-second deadline.
func loadCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// createBulkNodes inserts count nodes with the given label in batches.
// Each node has id (int), value (int=id*10), and category (int=id%5) properties.
func createBulkNodes(t *testing.T, db *sql.DB, label string, count int) {
	t.Helper()
	batchSize := 100
	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		var parts []string
		for i := start; i < end; i++ {
			parts = append(parts, fmt.Sprintf(
				"(:%s {id: %d, value: %d, category: %d})",
				label, i+1, (i+1)*10, (i+1)%5))
		}
		ctx, cancel := context.WithTimeout(context.Background(), loadTestTimeout)
		_, err := db.ExecContext(ctx, "CREATE "+strings.Join(parts, ", "))
		cancel()
		require.NoError(t, err, "create batch starting at %d", start)
	}
}

// ----------------------------------------------------------------------------
// TestLoad_HighParallelismReads
//
// 50 goroutines each execute 20 RETURN queries concurrently through a 20-conn
// pool, totalling 1 000 round-trips.  The pool is pre-warmed to prevent the
// initial QUIC connection burst from timing out.
// ----------------------------------------------------------------------------

func TestLoad_HighParallelismReads(t *testing.T) {
	const (
		workers        = 50
		queriesPerWork = 20
		poolSize       = 20
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()

	warmPool(t, db, poolSize)

	var (
		wg      sync.WaitGroup
		errCnt  atomic.Int64
		queryCt atomic.Int64
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for q := 0; q < queriesPerWork; q++ {
				// Inner closure: cancel() is deferred so the context lives until
				// rows.Close(), preventing spurious context.Canceled on rows.Err().
				func() {
					ctx, cancel := loadCtx()
					defer cancel()

					rows, err := db.QueryContext(ctx, "RETURN ? AS id, ? AS val", id, q)
					if err != nil {
						errCnt.Add(1)
						t.Logf("worker %d query %d: %v", id, q, err)
						return
					}
					defer rows.Close()

					for rows.Next() {
						var gotID, gotVal int64
						rows.Scan(&gotID, &gotVal) //nolint:errcheck
					}
					if err := rows.Err(); err != nil {
						errCnt.Add(1)
						t.Logf("worker %d rows.Err: %v", id, err)
						return
					}
					queryCt.Add(1)
				}()
			}
		}(w)
	}

	wg.Wait()

	t.Logf("completed %d/%d queries, errors=%d",
		queryCt.Load(), workers*queriesPerWork, errCnt.Load())
	assert.Zero(t, errCnt.Load(), "expected zero errors in high-parallelism read load")
}

// ----------------------------------------------------------------------------
// TestLoad_HighParallelismWrites
//
// 30 goroutines each create 20 nodes concurrently through a 15-conn pool,
// totalling 600 writes.  We verify no write is lost by counting after.
// ----------------------------------------------------------------------------

func TestLoad_HighParallelismWrites(t *testing.T) {
	const (
		workers       = 30
		writesPerWork = 20
		poolSize      = 15
		label         = "LoadWrite"
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	warmPool(t, db, poolSize)

	var (
		wg     sync.WaitGroup
		errCnt atomic.Int64
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWork; i++ {
				func() {
					ctx, cancel := loadCtx()
					defer cancel()
					_, err := db.ExecContext(ctx,
						fmt.Sprintf("CREATE (n:%s {worker: ?, seq: ?})", label),
						workerID, i,
					)
					if err != nil {
						errCnt.Add(1)
						t.Logf("worker %d write %d: %v", workerID, i, err)
					}
				}()
			}
		}(w)
	}

	wg.Wait()

	ctx, cancel := loadCtx()
	defer cancel()
	var count int64
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("MATCH (n:%s) RETURN count(n)", label),
	).Scan(&count)
	require.NoError(t, err, "count query")

	t.Logf("wrote %d/%d nodes, errors=%d",
		count, workers*writesPerWork, errCnt.Load())
	assert.Zero(t, errCnt.Load(), "expected zero write errors")
	assert.EqualValues(t, workers*writesPerWork, count, "node count mismatch")
}

// ----------------------------------------------------------------------------
// TestLoad_LargeDataset
//
// Creates 300 nodes and verifies three query patterns:
//   - Full scan (no ORDER BY to avoid server-side sort pressure)
//   - Filtered scan (WHERE category = 5)
//   - Aggregation (COUNT + SUM)
// ----------------------------------------------------------------------------

func TestLoad_LargeDataset(t *testing.T) {
	const (
		nodeCount = 300
		label     = "LoadDataset"
		poolSize  = 10
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	t.Log("inserting dataset...")
	for i := 0; i < nodeCount; i++ {
		func() {
			ctx, cancel := loadCtx()
			defer cancel()
			_, err := db.ExecContext(ctx,
				fmt.Sprintf("CREATE (n:%s {id: ?, value: ?, category: ?})", label),
				i, i*2, i%10,
			)
			require.NoError(t, err, "insert node %d", i)
		}()
	}

	// Full scan — count all rows (no ORDER BY to avoid sort pressure).
	t.Log("full scan...")
	func() {
		ctx, cancel := loadCtx()
		defer cancel()
		rows, err := db.QueryContext(ctx,
			fmt.Sprintf("MATCH (n:%s) RETURN n.id, n.value", label))
		require.NoError(t, err)
		defer rows.Close()

		var got int
		for rows.Next() {
			var id, val int64
			require.NoError(t, rows.Scan(&id, &val))
			got++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, nodeCount, got, "full scan row count")
	}()

	// Filtered scan — category = 5 → nodeCount/10 rows.
	t.Log("filtered scan...")
	func() {
		ctx, cancel := loadCtx()
		defer cancel()
		rows, err := db.QueryContext(ctx,
			fmt.Sprintf("MATCH (n:%s) WHERE n.category = 5 RETURN n.id", label))
		require.NoError(t, err)
		defer rows.Close()

		var got int
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			got++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, nodeCount/10, got, "filtered scan row count")
	}()

	// Aggregation.
	t.Log("aggregation...")
	func() {
		ctx, cancel := loadCtx()
		defer cancel()
		var cnt, sum int64
		err := db.QueryRowContext(ctx,
			fmt.Sprintf("MATCH (n:%s) RETURN count(n), sum(n.value)", label),
		).Scan(&cnt, &sum)
		require.NoError(t, err)
		assert.EqualValues(t, nodeCount, cnt)
		// sum of 2*i for i in [0, nodeCount) = nodeCount*(nodeCount-1)
		assert.EqualValues(t, nodeCount*(nodeCount-1), sum)
	}()
}

// ----------------------------------------------------------------------------
// TestLoad_ConnectionPoolContention
//
// 80 goroutines compete for 5 connections.  Most goroutines queue waiting for
// a free connection.  Verifies the driver correctly serializes requests and
// returns correct results under pool exhaustion.
// ----------------------------------------------------------------------------

func TestLoad_ConnectionPoolContention(t *testing.T) {
	const (
		workers  = 80
		poolSize = 5
	)

	db := openLoadDB(t, poolSize, poolSize)
	defer db.Close()
	db.SetConnMaxIdleTime(30 * time.Second)

	warmPool(t, db, poolSize)

	var (
		wg     sync.WaitGroup
		errCnt atomic.Int64
		okCnt  atomic.Int64
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				var result int64
				err := db.QueryRowContext(ctx, "RETURN ? AS x", id).Scan(&result)
				if err != nil {
					errCnt.Add(1)
					t.Logf("worker %d: %v", id, err)
					return
				}
				if result != int64(id) {
					errCnt.Add(1)
					t.Logf("worker %d: got %d, want %d", id, result, id)
					return
				}
				okCnt.Add(1)
			}()
		}(w)
	}

	wg.Wait()

	t.Logf("pool contention: ok=%d errors=%d pool=%v",
		okCnt.Load(), errCnt.Load(), db.Stats())
	assert.Zero(t, errCnt.Load(), "expected zero errors under pool contention")
	assert.EqualValues(t, workers, okCnt.Load())
}

// ----------------------------------------------------------------------------
// TestLoad_SequentialQueryStreamSync
//
// Regression test for BUG-0001 (QUIC stream desync) at scale.
//
// Uses a single connection (MaxOpenConns=1) to force every query through the
// same QUIC stream.  Runs 500 rapid sequential queries across three patterns:
//   - count+fetch pairs (mirrors GetFollowers double-query pattern)
//   - rapid-fire single-row queries
//   - create+match pairs on the same stream
// ----------------------------------------------------------------------------

func TestLoad_SequentialQueryStreamSync(t *testing.T) {
	const (
		iterations = 500
		label      = "LoadSeqSync"
	)

	// Single connection — all queries share one QUIC stream.
	db := openLoadDB(t, 1, 1)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	// Seed some data.
	for i := 0; i < 10; i++ {
		ctx, cancel := loadCtx()
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE (n:%s {id: ?, val: ?})", label), i, i*3)
		cancel()
		require.NoError(t, err, "seed node %d", i)
	}

	// Pattern 1: 200 × (QueryRow count + Query rows) — mirrors GetFollowers.
	t.Log("pattern 1: count+fetch pairs...")
	for i := 0; i < 200; i++ {
		func() {
			ctx, cancel := loadCtx()
			defer cancel()

			// Query 1: count
			var cnt int64
			err := db.QueryRowContext(ctx,
				fmt.Sprintf("MATCH (n:%s) RETURN count(n)", label),
			).Scan(&cnt)
			require.NoError(t, err, "count iter %d", i)
			assert.EqualValues(t, 10, cnt)
		}()

		// Query 2: fetch — immediately after count, same QUIC stream.
		func() {
			ctx, cancel := loadCtx()
			defer cancel()

			rows, err := db.QueryContext(ctx,
				fmt.Sprintf("MATCH (n:%s) RETURN n.id, n.val LIMIT 10", label))
			require.NoError(t, err, "fetch iter %d", i)
			defer rows.Close()

			var got int
			for rows.Next() {
				var id, val int64
				require.NoError(t, rows.Scan(&id, &val), "scan iter %d", i)
				got++
			}
			require.NoError(t, rows.Err(), "rows.Err iter %d", i)
			assert.Equal(t, 10, got, "fetch row count iter %d", i)
		}()
	}

	// Pattern 2: 200 × single-row query (rapid-fire).
	t.Log("pattern 2: rapid-fire single-row queries...")
	for i := 0; i < 200; i++ {
		func() {
			ctx, cancel := loadCtx()
			defer cancel()
			var v int64
			err := db.QueryRowContext(ctx, "RETURN ? AS x", i).Scan(&v)
			require.NoError(t, err, "rapid iter %d", i)
			assert.EqualValues(t, i, v)
		}()
	}

	// Pattern 3: 100 × create-then-match on same stream.
	t.Log("pattern 3: create+match pairs...")
	for i := 0; i < 100; i++ {
		suffix := 1000 + i

		func() {
			ctx, cancel := loadCtx()
			defer cancel()
			_, err := db.ExecContext(ctx,
				fmt.Sprintf("CREATE (n:%s {id: ?, val: ?})", label), suffix, suffix)
			require.NoError(t, err, "create iter %d", i)
		}()

		func() {
			ctx, cancel := loadCtx()
			defer cancel()
			var val int64
			err := db.QueryRowContext(ctx,
				fmt.Sprintf("MATCH (n:%s {id: ?}) RETURN n.val", label), suffix,
			).Scan(&val)
			require.NoError(t, err, "match iter %d", i)
			assert.EqualValues(t, suffix, val)
		}()
	}

	t.Logf("all %d sequential queries completed without stream desync", iterations)
}

// ----------------------------------------------------------------------------
// TestLoad_MixedReadWrite
//
// Two goroutine groups run concurrently: readers scan existing nodes while
// writers create new ones.  Exercises the driver under mixed concurrent
// workload with a realistic pool size.
// ----------------------------------------------------------------------------

func TestLoad_MixedReadWrite(t *testing.T) {
	const (
		readers   = 20
		writers   = 10
		readIter  = 30
		writeIter = 20
		label     = "LoadMixed"
		poolSize  = 15
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	warmPool(t, db, poolSize)

	// Seed initial nodes.
	for i := 0; i < 20; i++ {
		ctx, cancel := loadCtx()
		_, _ = db.ExecContext(ctx,
			fmt.Sprintf("CREATE (n:%s {id: ?, src: 'seed'})", label), i)
		cancel()
	}

	var (
		wg     sync.WaitGroup
		errCnt atomic.Int64
	)

	// Readers
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < readIter; i++ {
				func() {
					ctx, cancel := loadCtx()
					defer cancel()
					rows, err := db.QueryContext(ctx,
						fmt.Sprintf("MATCH (n:%s) RETURN n.id LIMIT 50", label))
					if err != nil {
						errCnt.Add(1)
						t.Logf("reader %d iter %d: %v", id, i, err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						var v int64
						rows.Scan(&v) //nolint:errcheck
					}
					if err := rows.Err(); err != nil {
						errCnt.Add(1)
						t.Logf("reader %d iter %d rows.Err: %v", id, i, err)
					}
				}()
			}
		}(r)
	}

	// Writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writeIter; i++ {
				func() {
					ctx, cancel := loadCtx()
					defer cancel()
					_, err := db.ExecContext(ctx,
						fmt.Sprintf("CREATE (n:%s {id: ?, src: 'writer'})", label),
						id*1000+i,
					)
					if err != nil {
						errCnt.Add(1)
						t.Logf("writer %d iter %d: %v", id, i, err)
					}
				}()
			}
		}(w)
	}

	wg.Wait()

	t.Logf("mixed read/write: errors=%d pool=%v", errCnt.Load(), db.Stats())
	assert.Zero(t, errCnt.Load(), "expected zero errors in mixed read/write load")
}

// ----------------------------------------------------------------------------
// TestLoad_ConcurrentTransactions
//
// 20 goroutines each run a multi-operation transaction concurrently.  Verifies
// that all transactions commit successfully and the final node count matches
// committed workers × ops-per-worker.
// ----------------------------------------------------------------------------

func TestLoad_ConcurrentTransactions(t *testing.T) {
	const (
		workers    = 20
		opsPerWork = 5
		label      = "LoadTx"
		poolSize   = 10
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	warmPool(t, db, poolSize)

	var (
		wg        sync.WaitGroup
		committed atomic.Int64
		errCnt    atomic.Int64
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCnt.Add(1)
					t.Logf("worker %d BeginTx: %v", id, err)
					return
				}

				for i := 0; i < opsPerWork; i++ {
					_, err := tx.ExecContext(ctx,
						fmt.Sprintf("CREATE (n:%s {worker: ?, seq: ?})", label),
						id, i,
					)
					if err != nil {
						tx.Rollback() //nolint:errcheck
						errCnt.Add(1)
						t.Logf("worker %d op %d: %v", id, i, err)
						return
					}
				}

				if err := tx.Commit(); err != nil {
					errCnt.Add(1)
					t.Logf("worker %d commit: %v", id, err)
					return
				}
				committed.Add(1)
			}()
		}(w)
	}

	wg.Wait()

	func() {
		ctx, cancel := loadCtx()
		defer cancel()
		var total int64
		err := db.QueryRowContext(ctx,
			fmt.Sprintf("MATCH (n:%s) RETURN count(n)", label),
		).Scan(&total)
		require.NoError(t, err)

		t.Logf("transactions: committed=%d errors=%d nodes=%d",
			committed.Load(), errCnt.Load(), total)
		assert.Zero(t, errCnt.Load(), "expected zero transaction errors")
		assert.EqualValues(t, committed.Load()*opsPerWork, total,
			"node count should equal committed workers × ops")
	}()
}

// ----------------------------------------------------------------------------
// TestLoad_PreparedStatementStorm
//
// 40 goroutines share one prepared statement and execute it 25 times each
// through a 10-conn pool (1 000 total executions).  Verifies the driver
// handles concurrent stmt.QueryContext calls correctly.
// ----------------------------------------------------------------------------

func TestLoad_PreparedStatementStorm(t *testing.T) {
	const (
		workers        = 40
		queriesPerWork = 25
		poolSize       = 10
	)

	db := openLoadDB(t, poolSize, poolSize/2)
	defer db.Close()

	warmPool(t, db, poolSize)

	ctx0, cancel0 := loadCtx()
	stmt, err := db.PrepareContext(ctx0, "RETURN ? AS x, ? AS y")
	cancel0()
	require.NoError(t, err)
	defer stmt.Close()

	var (
		wg     sync.WaitGroup
		errCnt atomic.Int64
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < queriesPerWork; i++ {
				func() {
					ctx, cancel := loadCtx()
					defer cancel()
					rows, err := stmt.QueryContext(ctx, id, i)
					if err != nil {
						errCnt.Add(1)
						t.Logf("worker %d iter %d: %v", id, i, err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						var x, y int64
						rows.Scan(&x, &y) //nolint:errcheck
					}
					if err := rows.Err(); err != nil {
						errCnt.Add(1)
						t.Logf("worker %d iter %d rows.Err: %v", id, i, err)
					}
				}()
			}
		}(w)
	}

	wg.Wait()

	t.Logf("prepared statement storm: errors=%d pool=%v", errCnt.Load(), db.Stats())
	assert.Zero(t, errCnt.Load(), "expected zero errors in prepared statement storm")
}

// ----------------------------------------------------------------------------
// TestLoad_MultiPageResults
//
// Creates 300 nodes and reads them back with page_size=50, forcing 6 pages.
// Exercises the PULL pagination loop to ensure no frames are skipped or
// duplicated across page boundaries.
// ----------------------------------------------------------------------------

func TestLoad_MultiPageResults(t *testing.T) {
	const (
		nodeCount = 300
		pageSize  = 50 // forces 6 pages
		label     = "LoadMultiPage"
		poolSize  = 5
	)

	db := openLoadDBWithPageSize(t, poolSize, poolSize/2, pageSize)
	defer db.Close()
	t.Cleanup(func() { cleanupLabel(t, db, label) })
	cleanupLabel(t, db, label)

	t.Logf("inserting %d nodes (page_size=%d → %d pages)...",
		nodeCount, pageSize, nodeCount/pageSize)

	for i := 0; i < nodeCount; i++ {
		func() {
			ctx, cancel := loadCtx()
			defer cancel()
			_, err := db.ExecContext(ctx,
				fmt.Sprintf("CREATE (n:%s {id: ?})", label), i)
			require.NoError(t, err, "insert %d", i)
		}()
	}

	// Full scan — read all rows across all pages without ORDER BY.
	t.Log("full multi-page scan...")
	func() {
		ctx, cancel := loadCtx()
		defer cancel()
		rows, err := db.QueryContext(ctx,
			fmt.Sprintf("MATCH (n:%s) RETURN n.id", label))
		require.NoError(t, err)
		defer rows.Close()

		var got int
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			got++
		}
		require.NoError(t, rows.Err())

		t.Logf("multi-page result: got %d/%d rows", got, nodeCount)
		assert.Equal(t, nodeCount, got, "row count across %d pages", nodeCount/pageSize)
	}()
}

// =============================================================================
// GAP-0257: ORDER BY on large result sets regression tests
// =============================================================================

// TestLoad_GAP0257_OrderByLarge verifies ORDER BY works on 1000+ row result
// sets without returning "58000: Internal Server Error".
func TestLoad_GAP0257_OrderByLarge(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const label = "GAP0257OrderBy"
	const nodeCount = 1000

	cleanupLabel(t, db, label)
	defer cleanupLabel(t, db, label)

	t.Log("creating 1000 nodes...")
	createBulkNodes(t, db, label, nodeCount)

	t.Log("ORDER BY ASC on 1000 rows...")
	ctx, cancel := context.WithTimeout(context.Background(), loadTestTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"MATCH (n:%s) RETURN n.id AS id, n.value AS value ORDER BY n.id", label))
	require.NoError(t, err,
		"GAP-0257: MATCH+ORDER BY on 1000 rows returned error")

	count := 0
	prevID := int64(0)
	for rows.Next() {
		var id, value int64
		require.NoError(t, rows.Scan(&id, &value))
		count++
		assert.Greater(t, id, prevID, "row %d: id should be ascending", count)
		prevID = id
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, nodeCount, count, "GAP-0257: expected %d sorted rows", nodeCount)
}

// TestLoad_GAP0257_OrderByDesc verifies ORDER BY DESC on 1000 nodes.
func TestLoad_GAP0257_OrderByDesc(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const label = "GAP0257Desc"
	const nodeCount = 1000

	cleanupLabel(t, db, label)
	defer cleanupLabel(t, db, label)

	createBulkNodes(t, db, label, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), loadTestTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"MATCH (n:%s) RETURN n.id AS id ORDER BY n.id DESC", label))
	require.NoError(t, err, "ORDER BY DESC on 1000 rows failed")

	count := 0
	prevID := int64(nodeCount + 1)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		count++
		assert.Less(t, id, prevID, "row %d: id should be descending", count)
		prevID = id
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, nodeCount, count)
}

// TestLoad_GAP0257_MultiPageOrderBy verifies ORDER BY on 2500 nodes spanning
// multiple pages (default page_size=1000, so 3 pages).
func TestLoad_GAP0257_MultiPageOrderBy(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const label = "GAP0257MultiPage"
	const nodeCount = 2500

	cleanupLabel(t, db, label)
	defer cleanupLabel(t, db, label)

	t.Logf("creating %d nodes...", nodeCount)
	createBulkNodes(t, db, label, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), loadTestTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"MATCH (n:%s) RETURN n.id AS id ORDER BY n.id", label))
	require.NoError(t, err,
		"GAP-0257: multi-page ORDER BY on %d rows returned error", nodeCount)

	count := 0
	prevID := int64(0)
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		count++
		assert.Greater(t, id, prevID, "row %d: ascending order broken", count)
		prevID = id
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, nodeCount, count,
		"GAP-0257: expected %d sorted rows across multiple pages", nodeCount)
}

// TestLoad_GAP0257_OrderByMultiKey verifies multi-key ORDER BY on large data.
func TestLoad_GAP0257_OrderByMultiKey(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	const label = "GAP0257MultiKey"
	const nodeCount = 1000

	cleanupLabel(t, db, label)
	defer cleanupLabel(t, db, label)

	createBulkNodes(t, db, label, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), loadTestTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"MATCH (n:%s) RETURN n.category AS cat, n.id AS id ORDER BY n.category, n.id",
		label))
	require.NoError(t, err, "ORDER BY multi-key on 1000 rows failed")

	count := 0
	prevCat := int64(-1)
	prevID := int64(-1)
	for rows.Next() {
		var cat, id int64
		require.NoError(t, rows.Scan(&cat, &id))
		count++
		if cat == prevCat {
			assert.Greater(t, id, prevID, "within category %d, id should ascend", cat)
		} else {
			assert.GreaterOrEqual(t, cat, prevCat, "categories should ascend")
		}
		prevCat = cat
		prevID = id
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, nodeCount, count)
}

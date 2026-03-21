# BUG: Stream Desynchronization on Sequential Queries

**Reported:** 2026-03-21
**Resolved:** 2026-03-21
**Severity:** Medium (intermittent, data-dependent)
**Affects:** QUIC transport, single-stream multiplexing
**Downstream impact:** divelife-api `GET /v1/feed` returns 500 intermittently (GAP-0046)

> **Status: FIXED** — Phase 2 blocking drain added to `releaseRows()` in `conn.go`.
> Verified by 8 regression tests in `conn_stream_desync_test.go`.

---

## Summary

After several sequential queries on the same `database/sql` connection, the QUIC
transport stream can become desynchronized. The server's trailing frames (metrics,
heartbeat) from one query are not fully drained before the next query begins,
causing the next `ReceiveProto()` call to decode a trailing frame as the response
to the new query — returning garbage data or an error.

The bug is intermittent because:

1. Trailing frames are only sent for some queries (server-dependent)
2. `TryReceiveProtoBuffered()` only drains frames already in the `bufio.Reader`
   buffer — frames still in the QUIC socket are invisible
3. Frame arrival timing varies with network conditions and server load

---

## Root Cause

### The drain gap in `releaseRows()`

When `Rows.Close()` is called, `releaseRows()` drains buffered trailing frames:

```go
// conn.go:720-747
func (c *Conn) releaseRows(r *Rows) {
    if c.activeRows == r {
        // Drain any remaining frames (metrics, heartbeat)
        for {
            msg, ok, err := c.transport.TryReceiveProtoBuffered()
            if err != nil || !ok {
                break  // ← ONLY drains what's already in bufio buffer
            }
            // ... check for metrics/heartbeat ...
        }
        c.activeRows = nil
        c.state = StateIdle
    }
    c.mu.Unlock()
}
```

`TryReceiveProtoBuffered()` (transport.go:277-337) checks `t.br.Buffered()` —
the number of bytes already read into the 64KB `bufio.Reader`. If the server's
trailing metrics frame hasn't arrived at the socket yet (or arrived but hasn't
been read into the buffer), the drain loop exits immediately, leaving the frame
in the stream.

### The desync sequence

```
Timeline:
─────────────────────────────────────────────────────────────────
Query 1: EXECUTE → Schema → Page(final=true)  → [Metrics frame in-flight]
                                                       ↑
                              Rows.Close() drains here ┘ (nothing buffered yet)

Query 2: EXECUTE → ... waiting for response ...
                   ↓
         Receives Query 1's Metrics frame instead of Query 2's Schema
         → decode failure or unexpected response type → error
─────────────────────────────────────────────────────────────────
```

### Contrast with `ExecContext()`

`ExecContext()` (conn.go:308-374) handles this correctly because it uses a
**blocking** `drainExecStream()` that calls `ReceiveProto()` (not the
non-blocking `TryReceiveProtoBuffered`) to read until the final page. However,
it then falls through to the same non-blocking `TryReceiveProtoBuffered()` loop
for metrics — so even `ExecContext` can leave trailing frames if they arrive
after the drain.

---

## Reproduction

### Minimal reproduction (driver-level)

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    _ "geodedb.com/geode" // register driver
)

func main() {
    // Use a single connection (no pool) to demonstrate the issue.
    db, _ := sql.Open("geode", "geode://localhost:7687?tls=false")
    db.SetMaxOpenConns(1)
    db.SetMaxIdleConns(1)

    ctx := context.Background()

    // Seed some data
    db.ExecContext(ctx, "CREATE (p:Post {id: 'p1', text: 'hello'})")
    db.ExecContext(ctx, "CREATE (p:Post {id: 'p2', text: 'world'})")

    // Rapid sequential queries — the pattern that triggers the bug.
    // With a single connection, each query reuses the same QUIC stream.
    for i := 0; i < 50; i++ {
        rows, err := db.QueryContext(ctx,
            "MATCH (p:Post) RETURN p.id, p.text LIMIT 10",
        )
        if err != nil {
            log.Fatalf("query %d failed: %v", i, err)
        }

        count := 0
        for rows.Next() {
            var id, text string
            if err := rows.Scan(&id, &text); err != nil {
                log.Fatalf("scan %d failed: %v", i, err)
            }
            count++
        }
        // Standard library checks rows.Err() here
        if err := rows.Err(); err != nil {
            log.Fatalf("rows.Err() on query %d: %v", i, err)
        }
        rows.Close()

        fmt.Printf("query %d: %d rows\n", i, count)
    }
}
```

**Expected:** All 50 queries succeed.
**Actual:** After N queries (varies, typically 5–20), one fails with a transport
error or unexpected response type.

### divelife-api reproduction path

The bug manifests on `GET /v1/feed` in the divelife-api because the feed
endpoint query (`social/repository.go:159`) runs after several preceding
queries from other endpoints in the same session (profile hydration, unread
counts, settings). With connection pooling, the same `*Conn` may serve all of
them.

```go
// internal/social/repository.go:147-169
func (r *GeodeRepository) GetFeed(ctx context.Context, userID, cursor string, limit int) ([]FeedItem, string, error) {
    query := "MATCH (p:Post) WHERE p.visibility = 'public' RETURN ..."
    rows, err := r.conn.Query(ctx, query, limit+1)
    if err != nil {
        return nil, "", fmt.Errorf("get feed: %w", err) // ← 500 occurs here
    }
    defer rows.Close()

    items, nextCursor := scanFeedItems(rows, limit)
    return items, nextCursor, nil
}
```

The `conn.Query()` call fails because the underlying `QueryContext()` receives
a stale metrics frame from a previous query instead of the expected schema
response for the feed query.

### Sequential query pattern (higher risk)

Methods that perform **two queries in one request** are most likely to trigger
the bug because the second query runs immediately after the first, before
trailing frames arrive:

```go
// internal/social/repository.go:315-345 — GetFollowers
func (r *GeodeRepository) GetFollowers(ctx context.Context, userID string, limit, offset int) ([]FollowUser, int64, error) {
    // Query 1: count
    var total int64
    row := r.conn.QueryRow(ctx,
        "MATCH (:User)-[:FOLLOWS]->(:User {id: ?}) RETURN count(*)",
        userID,
    )
    if err := row.Scan(&total); err != nil {
        return nil, 0, fmt.Errorf("count followers: %w", err)
    }

    // Query 2: fetch — runs immediately, may hit stale frame from Query 1
    rows, err := r.conn.Query(ctx,
        "MATCH (u:User)-[:FOLLOWS]->(:User {id: ?}) RETURN u.id, u.name, u.handle, u.avatar_url ...",
        userID, offset, limit,
    )
    if err != nil {
        return nil, 0, fmt.Errorf("query followers: %w", err) // ← desync failure
    }
    defer rows.Close()

    users := scanFollowUsers(rows)
    return users, total, nil
}
```

---

## Affected Code Paths

| File | Method | Risk | Reason |
|------|--------|------|--------|
| `conn.go:720-747` | `releaseRows()` | **Root cause** | Non-blocking drain misses in-flight frames |
| `transport.go:277-337` | `TryReceiveProtoBuffered()` | **Root cause** | Only checks bufio buffer, not socket |
| `rows.go:56-68` | `Rows.Close()` | Propagation | Calls `releaseRows()`, inherits drain gap |
| `conn.go:49-101` | `drainExecStream()` | Same pattern | Falls through to non-blocking drain for metrics |

---

## Proposed Fixes

### Fix 1: Blocking drain in `releaseRows()` (recommended)

After reading the final page (`r.final == true`), perform a **blocking** read
with a short deadline to consume any trailing frames:

```go
func (c *Conn) releaseRows(r *Rows) {
    if c.activeRows == r {
        // If the result set wasn't fully consumed, drain remaining data pages.
        if !r.final {
            ctx, cancel := context.WithTimeout(r.ctx, 500*time.Millisecond)
            defer cancel()
            for {
                resp, err := c.transport.ReceiveProto(ctx)
                if err != nil {
                    break
                }
                execResp := resp.GetExecute()
                if execResp == nil {
                    break
                }
                if execResp.GetPage() != nil && execResp.GetPage().GetFinal() {
                    break
                }
            }
        }

        // Drain buffered trailing frames (metrics, heartbeat).
        for {
            msg, ok, err := c.transport.TryReceiveProtoBuffered()
            if err != nil || !ok {
                break
            }
            execResp := msg.GetExecute()
            if execResp == nil {
                break
            }
            if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
                continue
            }
            break
        }

        // Short blocking read to catch metrics frames still in-flight.
        drainCtx, drainCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
        defer drainCancel()
        for {
            resp, err := c.transport.ReceiveProto(drainCtx)
            if err != nil {
                break // timeout or error — stream is clean
            }
            execResp := resp.GetExecute()
            if execResp == nil {
                break
            }
            if execResp.GetMetrics() != nil || execResp.GetHeartbeat() != nil {
                continue
            }
            break
        }

        c.activeRows = nil
        if c.inTx {
            c.state = StateInTransaction
        } else {
            c.state = StateIdle
        }
    }
    c.mu.Unlock()
}
```

**Trade-off:** Adds up to 50ms latency per query close in the worst case. In
practice, trailing frames arrive within microseconds if they exist at all, so
the timeout fires instantly when the stream is clean.

### Fix 2: Validate response message type in `QueryContext()`

Add a defensive check after `ReceiveProto()` to verify the response is actually
a schema or execute response (not a stale metrics frame from a previous query):

```go
// In QueryContext(), after receiving the first response:
resp, err := c.transport.ReceiveProto(ctx)
if err != nil {
    c.mu.Unlock()
    return nil, transportErrBadConn(err)
}

// Drain stale trailing frames from previous queries.
for resp.GetExecute() != nil &&
    (resp.GetExecute().GetMetrics() != nil || resp.GetExecute().GetHeartbeat() != nil) {
    resp, err = c.transport.ReceiveProto(ctx)
    if err != nil {
        c.mu.Unlock()
        return nil, transportErrBadConn(err)
    }
}
```

### Fix 3: Per-query stream (long-term)

Open a new QUIC bidirectional stream per query instead of multiplexing all
queries on a single stream. QUIC supports thousands of concurrent streams with
minimal overhead. This eliminates the synchronization problem entirely.

```go
// In QueryContext():
stream, err := c.quicConn.OpenStreamSync(ctx)
if err != nil {
    return nil, transportErrBadConn(err)
}
// Use stream for this query only; close stream in Rows.Close()
```

**Trade-off:** Requires refactoring the transport layer. Most correct long-term
solution.

---

## Downstream Mitigation (divelife-api)

The divelife-api `scanFeedItems()` helper also has a secondary issue: it does
not call `rows.Err()` after iteration, which means driver-level errors during
iteration are silently swallowed. This should be fixed regardless of the driver
fix:

```go
// internal/social/repository.go — current (buggy)
func scanFeedItems(rows *sql.Rows, limit int) ([]FeedItem, string) {
    // ... iteration loop ...
    return items, nextCursor  // ← missing rows.Err() check
}

// Recommended fix:
func scanFeedItems(rows *sql.Rows, limit int) ([]FeedItem, string, error) {
    // ... iteration loop ...
    if err := rows.Err(); err != nil {
        return nil, "", fmt.Errorf("iterate feed rows: %w", err)
    }
    return items, nextCursor, nil
}
```

The same issue exists in `scanFollowUsers()` and should also be fixed.

---

## Test Plan

Add an integration test that performs rapid sequential queries on a single
connection to reproduce the desync:

```go
func TestSequentialQueryStreamSync(t *testing.T) {
    db := openTestDB(t) // single connection
    db.SetMaxOpenConns(1)

    ctx := context.Background()

    // Seed data
    _, err := db.ExecContext(ctx, "CREATE (p:TestPost {id: 'seq_1', text: 'hello'})")
    require.NoError(t, err)

    // Rapid sequential queries — 100 iterations
    for i := 0; i < 100; i++ {
        rows, err := db.QueryContext(ctx,
            "MATCH (p:TestPost) RETURN p.id, p.text",
        )
        require.NoError(t, err, "query %d", i)

        for rows.Next() {
            var id, text string
            require.NoError(t, rows.Scan(&id, &text), "scan %d", i)
        }
        require.NoError(t, rows.Err(), "rows.Err() %d", i)
        rows.Close()
    }

    // Cleanup
    db.ExecContext(ctx, "MATCH (p:TestPost) DELETE p")
}

func TestSequentialQueryWithCount(t *testing.T) {
    // Reproduces the GetFollowers pattern: QueryRow (count) then Query (fetch)
    db := openTestDB(t)
    db.SetMaxOpenConns(1)

    ctx := context.Background()

    for i := 0; i < 50; i++ {
        // Query 1: count
        var count int64
        err := db.QueryRowContext(ctx, "MATCH (n:TestNode) RETURN count(n)").Scan(&count)
        require.NoError(t, err, "count query %d", i)

        // Query 2: fetch (immediately after count)
        rows, err := db.QueryContext(ctx, "MATCH (n:TestNode) RETURN n.id LIMIT 10")
        require.NoError(t, err, "fetch query %d", i)

        for rows.Next() {
            var id string
            require.NoError(t, rows.Scan(&id))
        }
        require.NoError(t, rows.Err())
        rows.Close()
    }
}
```

---

## References

- **divelife-api GAP-0046:** [GitLab #61](https://gitlab.com/devnw/sealife/divelife-api/-/issues/61)
- **geode-client-go Issue #4:** Lock management in QueryContext (closed, but related)
- **Commit 68fc471:** Prior fix for multi-page PULL deadlock (addressed different failure mode)
- **Go database/sql docs:** ["Rows should always be closed"](https://pkg.go.dev/database/sql#Rows.Close) and `rows.Err()` contract

# Multiplex driver SDD progress

Branch: multiplex-driver (off b5e9d17)
Plan: docs/superpowers/plans/2026-06-21-multiplex-driver.md

## Task 0 — De-risk gate: PASSED (commit 1348f03)

Proved the geode server multiplexes N concurrent streams on ONE QUIC connection
under find+create (MATCH+MERGE) load: 8 streams -> 23696 op pairs, final count
exact, no wedge / desync / deadline race.

ROOT CAUSE of the earlier false WEDGE (3 layered bugs, all now understood):

1. Probe did a per-stream HELLO -- wrong. Auth is per QUIC *connection*
   (conn.go pooledHello); secondary streams skip HELLO and query directly.
2. Probe CLOSED the HELLO stream before workers ran -- the FIN tore down the
   connection auth; server cleanly closed sibling streams (NO_ERROR).
3. Server flag: GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING defaults FALSE
   (server_init.zig:54). Without it the server only services the first stream
   per conn; additional streams hang. With it, the streamAcceptorLoop services
   them. Server side is correct + tested (REQ-QUIC-HC-030).

DEPLOYMENT REQUIREMENT carried forward: the multiplex client only works against
a server started with GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1. Every Task 1-6
test harness MUST set it.

## Next: Task 1 -- config max_streams_per_conn (default 8)

## Lane D (Geode Go driver, SONNY-806 / GEO-GO-UP-001) SDD progress

Branch: eng-4622/geode-go (worktree .worktrees/eng-4622-geode-go, base cd6e4fe = origin/main 2026-09-13)
Plan: sonny-upstream docs/superpowers/plans/2026-09-06-epic3-geode-go.md (Tasks 1–16 in this repo; 17–36 in go/geode)
Engine pin correction: geodedb/geode:0.8.15 (index digest sha256:96a194cfce01e59ee1dbe1504aa0ed79c0141f7e2e8b23389da18e73e6adc8ee), not 0.8.13.
Task 1 dispatched 2026-09-13 14:05 EDT from BASE cd6e4fe.
Task 1: complete (commits cd6e4fe..b298068: b7e747d pin+race lane+CI, b298068 Go default pin/engine_pin_test/auth fail-fast/reset-flow test; review Approved after I1-I3 fixes). SHOW VERSION returns "v0.8.15". 12 pre-existing read-after-write integration failures against 0.8.15 under separate triage; golangci-lint pre-commit pin bumped v2.11.4->v2.13.1 (blocked every commit). Minor: go_version 1.25.8 component inputs vs go 1.27 module.
Task 2: complete (commits ed45cea..98dc938: 2162133 proto regen from engine ebfd8812 + proto/CANONICAL, 650e860 CDC/backup/restore/upload drift guards, 98dc938 --data-dir SONNY-1218 workaround; review Approved). Minor open: protoc binary unpinned; CANONICAL_PROTO default path fails under worktree layout; new session_id/payload fields unused by call sites.

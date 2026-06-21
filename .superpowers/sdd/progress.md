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

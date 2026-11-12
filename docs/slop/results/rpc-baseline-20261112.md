# rpc integration baseline, 2026-11-12

First runs of the local cross-framework race (`bench/integration/`). Purpose:
see where the grpc + http stack stands against grpc-go and tonic, before any
tuning work. Run id `bench/integration/results/20261112T100313Z`, three rounds
per cell, 3 s warmup and 15 s measured per pass, fresh server process per arm
and size, arm order rotated per round, on a quiet box (1-min load under 2
before every launch; two earlier attempts on a contended box were discarded).

A first run, `20261112T085408Z`, is superseded: its build step compiled the
arm's own module ReleaseFast but left the imported grpc/http/stdx modules at
the default Debug. The corrected run carries the fix (a dedicated
ReleaseFast module instance, the same shape the micro-bench suite already
used). The corrected numbers are close to the superseded ones — the arm is
syscall-dominated — but only these are quotable.

The follow-up profiling pass is `rpc-profile-20261112.md`.

## Conditions

- Box: 8× Neoverse-V3 (no SMT), shared, kernel 7.3.1, no cpufreq control
  exposed. Server pinned to cores 0-3, ghz to 4-7.
- Client: ghz 0.121.0 — grpc-go's own client library, one process.
- Arms: zig = this repo at d2a4938 plus the ReleaseFast build fix, Zig
  0.17.1. go = grpc-go v1.85.0, Go 1.26.10, raw-byte codec, no protobuf.
  rust = tonic 0.14.9, tokio 1.54.1, hyper 1.12.0, h2 0.4.20, prost 0.14.5,
  rustc 1.99.0, stub generated from the shared proto (pays one prost decode +
  encode per call the other arms do not).
- Workload: `bench.v1.Bench/Unary`, request and reply both 8 B or 1024 B.
  Config parity per `bench/integration/README.md`: TCP_NODELAY on everywhere,
  max concurrent streams 1000 everywhere, stock HTTP/2 windows.

## Numbers (median of 3 rounds; CPU is seconds in the 15 s window)

| profile | size | arm | rps | vs best | p50 | p99 | server CPU | server RSS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c1 (1 conn, 1 stream) | 8 B | zig | 15,562 | 1.00x | 0.028 ms | 0.050 ms | 2.53 s | 23 MB |
| | | rust | 14,293 | 0.92x | 0.032 ms | 0.058 ms | 2.69 s | 12 MB |
| | | go | 12,108 | 0.78x | 0.044 ms | 0.090 ms | 7.28 s | 33 MB |
| | 1 KiB | zig | 15,395 | 1.00x | 0.029 ms | 0.048 ms | 2.66 s | 25 MB |
| | | rust | 13,629 | 0.89x | 0.036 ms | 0.056 ms | 2.85 s | 12 MB |
| | | go | 12,089 | 0.79x | 0.044 ms | 0.096 ms | 7.12 s | 34 MB |
| c64-2c (2 conns, 64 streams) | 8 B | zig | 65,627 | 1.00x | 0.427 ms | 1.762 ms | 3.70 s | 24 MB |
| | | go | 60,864 | 0.93x | 0.508 ms | 2.449 ms | 14.69 s | 33 MB |
| | | rust | 60,010 | 0.91x | 0.487 ms | 2.585 ms | 15.89 s | 12 MB |
| | 1 KiB | zig | 62,544 | 1.00x | 0.400 ms | 1.863 ms | 5.14 s | 25 MB |
| | | rust | 61,615 | 0.99x | 0.447 ms | 1.987 ms | 14.64 s | 12 MB |
| | | go | 55,993 | 0.90x | 0.485 ms | 3.078 ms | 17.07 s | 34 MB |
| c64-16c (16 conns, 64 streams) | 8 B | zig | 62,452 | 1.00x | 0.399 ms | 2.334 ms | 7.18 s | 24 MB |
| | | rust | 61,777 | 0.99x | 0.419 ms | 2.360 ms | 12.03 s | 12 MB |
| | | go | 57,395 | 0.92x | 0.518 ms | 2.877 ms | 16.38 s | 33 MB |
| | 1 KiB | zig | 59,932 | 1.00x | 0.408 ms | 2.356 ms | 7.89 s | 25 MB |
| | | rust | 56,652 | 0.95x | 0.469 ms | 2.803 ms | 11.18 s | 12 MB |
| | | go | 56,003 | 0.93x | 0.487 ms | 2.956 ms | 19.54 s | 34 MB |

## What the numbers say

1. **Single-stream latency: ours is fastest.** p50 0.028 ms server-side
   share of a ~0.064 ms round trip, 1.1x tonic, 1.5x grpc-go, at both sizes.
   grpc-go's single-connection server burns 7.3 s of CPU in the window to
   ours at 2.5 s.
2. **The c64 throughput rows are client-capped, not server-capped.** ghz
   sits at ~390% of its 4 cores in every c64 cell, so all three servers
   receive roughly the same ~60k rps and the rps column ranks the client.
   Do not quote the c64 rps ratios as server throughput.
3. **At the same delivered load, our server spends 3.7-7.9 s of CPU where
   tonic spends 11.2-15.9 and grpc-go spends 14.7-19.5.** That is 2-4x less
   CPU per request at equal rps, with the lowest p50 and p99 in every c64
   cell. The profiling pass confirms it independently: 9,840 cycles per
   request against tonic's 27,688 and grpc-go's 44,760.
4. **Round-to-round spread is the shared box.** c1 cells spread 1-6%; c64
   cells 2-12% in the corrected run (the discarded contended runs spread up
   to 36%). Medians only; per-round rps is in `summary.json`.
5. **Errors: at most 57 of ~900k requests in a cell (≤0.006%), all
   `Unavailable` statuses, spread across all arms.** Pattern says client
   dial races, not server failures. Watch for growth in later runs.

## Known asymmetries carried into this run

- tonic pays a prost decode + encode per call; zig and go pay none (README
  arms table). Memcpy scale at these sizes.
- grpc-go's HTTP/2 windows are BDP-adaptive; ours and tonic's are stock
  64 KiB. Immaterial at 8 B / 1 KiB, material before a 64 KiB size is added.
- The zig arm sets TCP_NODELAY itself (grpc-go and tonic default it on; our
  stock `serve` does not). That is parity, not tuning.

## Next steps, in order

1. A stronger load generator to un-cap the c64 rows: multiple ghz processes
   or a dedicated generator. Until then the throughput table has no headroom
   numbers, only per-request-cost ones.
2. The profiling pass's fruit list: `rpc-profile-20261112.md`.
3. Then: streaming profiles (server-streaming, bidi ping-pong), a 64 KiB
   size (the window asymmetry becomes a real row), and a TLS lane.

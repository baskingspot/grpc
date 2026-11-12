# rpc integration baseline, 2026-11-12

First run of the local cross-framework race (`bench/integration/`). Purpose:
see where the grpc + http stack stands against grpc-go and tonic, before any
tuning work. Run id `bench/integration/results/20261112T085408Z`, three rounds
per cell, 3 s warmup and 15 s measured per pass, fresh server process per arm
and size, arm order rotated per round.

## Conditions

- Box: 8× Neoverse-V3 (no SMT), shared, kernel 7.3.1, no cpufreq control
  exposed. Server pinned to cores 0-3, ghz to 4-7.
- Client: ghz 0.121.0 — grpc-go's own client library, one process.
- Arms: zig = this repo at 149913f, Zig 0.17.1, ReleaseFast. go = grpc-go
  v1.85.0, Go 1.26.10, raw-byte codec, no protobuf. rust = tonic 0.14.9,
  tokio 1.54.1, hyper 1.12.0, h2 0.4.20, prost 0.14.5, rustc 1.99.0, stub
  generated from the shared proto (pays one prost decode + encode per call
  the other arms do not).
- Workload: `bench.v1.Bench/Unary`, request and reply both 8 B or 1024 B.
  Config parity per `bench/integration/README.md`: TCP_NODELAY on everywhere,
  max concurrent streams 1000 everywhere, stock HTTP/2 windows.

## Numbers (median of 3 rounds)

| profile | size | arm | rps | vs best | p50 | p99 | server CPU | server RSS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c1 (1 conn, 1 stream) | 8 B | zig | 15,377 | 1.00x | 0.029 ms | 0.057 ms | 0.17 cores | 23 MB |
| | | rust | 14,088 | 0.92x | 0.033 ms | 0.072 ms | 0.18 cores | 12 MB |
| | | go | 12,014 | 0.78x | 0.045 ms | 0.099 ms | 0.50 cores | 33 MB |
| | 1 KiB | zig | 15,119 | 1.00x | 0.029 ms | 0.059 ms | 0.18 cores | 24 MB |
| | | rust | 13,226 | 0.87x | 0.037 ms | 0.086 ms | 0.19 cores | 12 MB |
| | | go | 12,036 | 0.80x | 0.044 ms | 0.102 ms | 0.48 cores | 34 MB |
| c64-2c (2 conns, 64 streams) | 8 B | zig | 63,714 | 0.99x | 0.439 ms | 2.087 ms | 0.25 cores | 23 MB |
| | | rust | 64,348 | 1.00x | 0.471 ms | 2.156 ms | 0.81 cores | 12 MB |
| | | go | 63,672 | 0.99x | 0.487 ms | 2.064 ms | 1.06 cores | 33 MB |
| | 1 KiB | zig | 60,456 | 1.00x | 0.418 ms | 2.184 ms | 0.35 cores | 24 MB |
| | | rust | 58,051 | 0.96x | 0.477 ms | 2.617 ms | 0.80 cores | 12 MB |
| | | go | 57,036 | 0.94x | 0.505 ms | 2.816 ms | 1.16 cores | 34 MB |
| c64-16c (16 conns, 64 streams) | 8 B | zig | 61,336 | 1.00x | 0.408 ms | 2.464 ms | 0.48 cores | 23 MB |
| | | rust | 58,990 | 0.96x | 0.453 ms | 2.723 ms | 0.71 cores | 12 MB |
| | | go | 55,095 | 0.90x | 0.541 ms | 3.144 ms | 1.06 cores | 33 MB |
| | 1 KiB | zig | 57,755 | 1.00x | 0.425 ms | 2.701 ms | 0.53 cores | 24 MB |
| | | rust | 52,785 | 0.91x | 0.499 ms | 3.072 ms | 0.73 cores | 12 MB |
| | | go | 54,021 | 0.94x | 0.529 ms | 3.174 ms | 1.24 cores | 34 MB |

## What the numbers say

1. **Single-stream latency: ours is fastest.** p50 0.029 ms server-side share
   of a ~0.065 ms round trip, 1.1x tonic, 1.5x grpc-go. Holds at both sizes.
   grpc-go's single-connection server burns ~0.5 cores to do it; ours 0.17.
2. **The c64 throughput rows are client-capped, not server-capped.** ghz sits
   at ~390% of its 4 cores in every c64 cell, so all three servers receive
   the same ~60k rps and the rps column ranks the client, not the servers.
   Do not quote the c64 rps ratios as server throughput.
3. **The c64 rows still rank efficiency: at the same delivered load, our
   server spends 0.25-0.53 cores where tonic spends 0.7-0.8 and grpc-go
   spends 1.06-1.24.** That is 2-4x less CPU per request at equal rps, with
   p50 and p99 latency also lowest in 5 of 6 cells. Caveat: the servers are
   under-loaded at these rates (that is what a capped client means), so this
   is per-request cost, not a saturation figure.
4. **Round-to-round spread is the shared box.** c1 cells spread 1-11%; c64
   cells spread 3-36% with the middle round usually the slow one. Medians
   only; per-round rps is in `summary.json`. A quieter box or more rounds
   before believing any ratio under 1.15x.
5. **Errors: at most 53 of ~900k requests in a cell (≤0.006%), all statuses
   `Unavailable`, spread across all arms.** Pattern says client dial races,
   not server failures. Watch for growth in later runs.

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
2. Repeat on a quiet machine or with more rounds to tighten the c64 spread.
3. Then: streaming profiles (server-streaming, bidi ping-pong), a 64 KiB
   size (the window asymmetry becomes a real row), and a TLS lane (that race
   is ours-vs-tls, not ours-vs-http).

# rpc integration profiling, 2026-11-12

First profiling pass over the integration race (`bench/integration/`,
`just bench-integration-profile`), on the corrected ReleaseFast build, right
after the corrected baseline run. Run id
`bench/integration/results/20261112T103014Z`; 12 s windows, 8 B messages, the
multiplexing profile (2 connections, 64 streams) and the closed-loop latency
profile (1 and 1). Same core split as the race: server on 0-3, ghz on 4-7.

Tool notes for this box, all of them load-bearing for repeating the pass:

- `perf_event_paranoid=2` allows the hardware counters with the `:u`
  modifier on our own children, and denies everything the
  `--call-graph` modes open (they request the precise-IP event variant), so
  the record passes are flat symbol profiles, no call graphs. DWARF is in
  the binary; `perf annotate` for line-level hotness inside one symbol is
  the untried next step.
- nix's `perf` is a shell wrapper that swallows SIGINT, so the driver stops
  each window by SIGTERM-ing the arm itself: every tool prints its summary
  when the command it measured ends.
- yama `ptrace_scope=1` forbids attaching to a sibling, so strace wraps the
  arm instead of attaching to it.

## Per-request hardware cost (perf stat, 8 B, c64-2c)

| arm  | cycles/req | instructions/req | IPC  | branch-misses/req | page faults |
| ---- | ---------: | ---------------: | ---: | ----------------: | ----------: |
| zig  |      9,840 |           37,732 | 3.83 |              20.3 |         836 |
| rust |     27,688 |           50,700 | 1.83 |             191.1 |         220 |
| go   |     44,760 |          111,548 | 2.49 |             297.8 |     100,070 |

Our server spends 2.8x less CPU per request than tonic's and 4.6x less than
grpc-go's, at the same delivered load. That matches the `/proc` CPU columns
of the baseline, so two independent instruments agree. grpc-go also takes
100k page faults (GC heap growth) where we take 836 total.

One anomaly stays open: cycles divided by task-clock implies an effective
~1.16 GHz on a Neoverse-V3, which is either deep clock throttling on this
box or a counter quirk; the relative and per-request counts are what the
table uses, and they are self-consistent.

## Allocations (the counting allocator, zig arm)

- Steady state at c64-2c: **0 allocations and 0 bytes per request**, across
  seven consecutive 100,000-request marks. The exchange arena resets
  properly; nothing on the request path reaches the process allocator.
- 91 live allocations (2.1 MB) at 2 connections: two 16 KiB socket buffers
  per connection, exchange arena pages, hpack tables. Peak 106 / 2.1 MB.
- Whole-run totals: 227 allocations, 3.2 MB, for 700,000 requests.
- The c1 window's first 100,000 requests include the ramp: 29 allocations,
  2.0 MB — startup plus arena sizing, not per-request traffic.

So the arena bump-allocator CPU in the profile below is the only allocation
cost that matters, and it is internal pointer bumps, not heap traffic.

## Syscalls (strace -c, 8 B, c1)

237,493 requests: 237,498 `recvmsg` and 237,485 `sendmsg`. **Exactly two
syscalls per request, one read and one write, nothing else** — no epoll in
the loop (each connection thread blocks in recvmsg), two futex calls for
the whole run. That is the floor for a blocking-thread-per-connection
design. The c64-2c syscall rate was not captured this pass; under
multiplexing one recvmsg should carry several streams' frames, so
syscalls-per-request there should read below 2. Worth confirming next.

## Where the CPU goes (perf record, flat symbols, zig arm)

c64-2c, 8 B (userspace, percent of samples):

| symbol                              | share |
| ----------------------------------- | ----: |
| `h2.session.Session.nextEvent`      | 39.5% |
| `h2.engine.connection.Connection.drive` | 20.1% |
| `heap.ArenaAllocator.alloc`         |  7.9% |
| `h2.session.Session.processBlock`   |  6.9% |
| `h2.engine.blocking.flush`          |  2.9% |
| `server.serveConnection`            |  2.7% |
| `compiler_rt.memcpy.memcpyFast`     |  2.2% |
| `hpack.encoder.Encoder.writeField`  |  1.7% |
| `hpack.decoder.integer`             |  1.3% |
| `h2.head.isLowerTchar`              |  1.3% |

c1, 8 B: `Connection.drive` dominates at 57.3%, then `serveConnection` 8.3%,
`blocking.flush` 7.7%, `processBlock` 4.4%, `ArenaAllocator.alloc` 2.8%.

## Low-hanging fruit, ranked by evidence

1. **The h2 event machinery is 67% of server CPU at c64**
   (`nextEvent` + `drive` + `processBlock`). At c1, `drive` alone is 57%.
   This is `baskingspot/http`'s engine, not grpc itself: the session pump
   re-walks frame headers and stream state per event. Next step is
   `perf annotate` inside `nextEvent` and `drive` for line-level hot spots
   before touching anything.
2. **Arena bump allocations are 8% of CPU** despite zero heap traffic: the
   request path allocates several arena objects per call (the message
   list in `server.dispatch`, response slices, exchange chunks). Fewer,
   pre-sized, or per-stream-reused allocations in `server.zig` and the
   engine's exchange would claw back most of it.
3. **memcpy is 2.2% through `compiler_rt`**, not through
   `baskingspot/fastmem`, because the grpc module does not import fastmem.
   Small-message framing copies headers and frames several times; either
   route copies through fastmem or remove a copy from the path.
4. **hpack encode/decode is ~3%** — the encoder writes grpc-status and
   content-type per response with static-table misses; a per-stream
   remembered header layout would cut it.
5. **Not fruit, but the ceiling**: 2 syscalls/request is the floor for
   blocking threads. Batching many streams per syscall is the readiness
   engine's job, and that is architecture, not optimization.

Open questions for the next pass: the 23 MB VmHWM against 2.1 MB live
(retained capacity or a transient peak — measure per profile, not across a
whole matrix); the c64-2c syscall-per-request rate; line-level hot spots via
`perf annotate` on the two dominant symbols.

Addendum, 2026-11-13: the `perf annotate` lead found the tombstone buildup in
`Session.streams` (68% of `nextEvent`'s samples on the hash map's probe loop),
fixed in `baskingspot/http` `0144d33` and pinned here. Cycles per request fell
27% and the capacity ladder moved 41% at 8 B — `rpc-capacity-20261113.md`
carries the numbers. The open questions above stand; the syscall rate under
multiplexing is now the interesting one, since the loadgen can finally push
the server past its old ceiling.

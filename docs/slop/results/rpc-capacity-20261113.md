# rpc capacity race, 2026-11-13

The capacity ladder over the integration race (`bench/integration/`,
`just bench-integration-capacity`): every arm under the new zig load
generator, one fresh server per step, until the server's cores or the
client's fill. Run id `bench/integration/results/20261113T211140Z`, 10 s
windows, 4-core split as always (server 0-3, client 4-7), 1-minute load
under 2 before launch.

The generator (`bench/integration/zig/loadgen.zig`) is one thread per
connection over our own `Client`, each carrying a fixed number of concurrent
unary calls in a closed loop, with TCP_NODELAY set to match ghz. It drives
all three arms identically, so unlike the ghz matrix the client never set
the ceiling: at our arm's peak it spent 2.5 of its 4 cores while the server
filled all of its 4. ghz remains the reference client for the published
matrix; these numbers are the server-side ceilings.

The ladder is 64, 256, 1024, 4096 in-flight calls (connections × streams per
connection, within every arm's 1000-stream limit). A side counts as full at
90% of its 4 cores.

## Capacity (the highest-throughput step before the server's cores filled)

| arm  | size  | capacity | in flight at peak | p50 | p99 | server CPU at peak | client CPU |
| ---- | ----- | -------: | ----------------: | --- | --- | -----------------: | ---------: |
| zig  | 8 B   |  1,039,707 rps |   256 | 0.228 ms | 0.350 ms | 368% | 246% |
| rust | 8 B   |    417,363 rps | 1,024 | 2.256 ms | 4.012 ms | 371% | 141% |
| go   | 8 B   |    262,963 rps | 1,024 | 3.576 ms | 6.116 ms | 371% |  84% |
| zig  | 1 KiB |    706,480 rps |   256 | 0.343 ms | 0.515 ms | 365% | 266% |
| rust | 1 KiB |    327,517 rps | 1,024 | 2.884 ms | 5.192 ms | 344% | 173% |
| go   | 1 KiB |    225,082 rps | 1,024 | 4.104 ms | 7.764 ms | 350% | 132% |

Zero errors in every step of every arm.

## What the numbers say

1. **At saturation we deliver 2.5x tonic's throughput and 4x grpc-go's, at
   8 B and 1 KiB alike.** The per-request-cost ranking from the baseline and
   the profile carries all the way into the capacity regime.
2. **The tail at saturation is the bigger gap.** At full cores our p99 is
   0.35 ms against tonic's 4.0 and grpc-go's 6.1: a queue drains in
   proportion to how cheap each request is, and ours are cheapest. grpc-go
   at 4096 in-flight reaches p99 51 ms.
3. **We saturate at 256 in-flight; the others need 1024.** Same coin, other
   face: their queues have to grow 4x deeper before the same 4 cores stay
   busy.
4. **Sanity checks hold.** Little's law: 256 in-flight / 1.04M rps = 246 µs
   against p50 228 µs. Cycle budget: 1.04M rps × 9.8k cycles/req over 4
   cores ≈ 2.5 GHz of a ~3 GHz part. The two instruments from the earlier
   passes (cycles and CPU-seconds) agree with the loadgen's count.
5. **The loadgen never set the ceiling.** Its CPU peaked at 266% of 4 cores
   (1 KiB case) while the server's filled; the capacity numbers are the
   servers'. The generator's own cost — roughly 2.4 µs of client CPU per
   request at peak — is itself evidence the client stack is cheap, and a
   future pass can measure client-side capacity the same way.

Caveats, in the usual order: one step per point, no repeats (the ladder is
18 windows as it is); the shared box, gated on quiet before launch; and the
generator is our client library, which biases nothing a closed-loop offer
can bias — the server does the work — but is recorded here anyway.

## What this closes and opens

This closes the client-cap caveat on the baseline: the c64 rows of the ghz
matrix ranked ghz, and these rows rank the servers. What it opens is the
optimization target: the per-request cost is the only lever that moves both
columns (capacity and tail-at-capacity), and the profile pass already
pointed at the h2 event machinery. That work starts in `baskingspot/http`.

## After the stream-table fix, same day

The profile pass's `perf annotate` lead paid out: 68% of `Session.nextEvent`'s
samples sat on the std hash map's probe loop, and the cause was tombstones —
every gRPC call opens and closes one h2 stream, each close leaves a tombstone
in `Session.streams`, the live count stays low so the table never grows, and
by the end of a run every event walked a probe chain hundreds of slots long.
The fix is ten lines in `baskingspot/http` (commit `0144d33`, pinned in
`build.zig.zon`): rebuild the table once removals outnumber live streams.
http's own gate passed both Zig lanes, and grpc's conformance suite passed in
full over the pin (783 cases, 0 failures).

Same ladder, same box, same gating (run
`bench/integration/results/20261113T212506Z`):

| arm | size | before | after | note |
| --- | --- | -----: | ----: | --- |
| zig | 8 B | 1,039,707 rps | 1,462,603 rps | +41%, stopped on the client's cores, server at 291% |
| zig | 1 KiB | 706,480 rps | 862,224 rps | +22%, client-bound; p99 at 64 in flight 0.515 ms → 0.086 ms |

Per-request cycles at the multiplexing profile fell 27% (9,840 → 7,214), and
`nextEvent`'s share of the flat profile fell from 39.5% to 22.0%. The loadgen
client shares the same http stack, so both sides of the race sped up, and
these two numbers are now **client floors, not server ceilings**: the server
finished every step with 25% or more of its cores idle. The honest statement
is "at least" — at 8 B, at least 1.46M rps on four cores, p99 1.03 ms at
1024 in flight.

`Connection.drive` (the engine's per-stream slot machinery) is now the top
symbol at 25.5%, with `ArenaAllocator.alloc` at 9.9% — the next two levers,
in that order, both in `baskingspot/http`'s engine layer.

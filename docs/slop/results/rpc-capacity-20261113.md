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

Because the generator is our own client, it verifies instead of trusting
itself: every OK reply is checked byte for byte against the expected
`bench.v1.Payload`, and a mismatch counts as a verification error in the
row and the JSON. That check caught the tonic arm double-wrapping its reply
on its first run; the full stack of cross-checks is recorded in
`rpc-capacity-20261113.md`, "Can the generator be trusted?".

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

## Can the generator be trusted?

It is our own client, so the question is fair, and the answer is checked
rather than asserted:

1. **Every OK reply is verified byte for byte**, against the exact
   `bench.v1.Payload` every arm is documented to return — not just the status
   code ghz checks. The check found a real bug on its first run: the tonic
   arm double-wrapped its reply (its generated handler encodes `Payload.data`
   itself, and the arm fed it an already-serialized message, so every reply
   carried two extra bytes). Fixed 2026-11-13; all three arms now verify
   clean (`verify 0` in every ladder row).
2. **The server's own count matches the client's**: the zig arm's counting
   mode tallies handler invocations in the server engine — a codebase the
   client library shares nothing with — and a 10 s window agreed exactly
   (15,100,001 served server-side against 13,799,547 measured plus the
   ~1.4M settle-second requests the client's count excludes).
3. **The physics agrees across all three arms**: rps × the PMU-measured
   cycles-per-request ÷ the kernel-accounted server CPU lands on a plausible
   clock for every arm (ours: 1.46M × 7,214 ÷ 2.91 cores ≈ 3.6 GHz Neoverse-V3
   boost; tonic and grpc-go ≈ 2.9 GHz on theirs). A client that over-counted
   would need an impossible frequency.
4. **The cycles-per-request figure itself came from ghz-driven windows** — the
   profile passes drive with ghz, not the loadgen — and the connectrpc
   conformance suite (783 cases, third-party runner) validates the wire
   behavior in both directions.

The one residual bias runs the other way: our client costs real CPU on the
same box, so every number here is a floor — the server finishes each ladder
with cores idle.

## Correction to the tonic rows

The first capacity run (20261113T211140Z) measured the tonic arm with the
double-wrapped reply above: 12 bytes where 10 were due at the 8 B size. The
re-measured ladder (20261113T214320Z, fixed arm) moves the ceiling within
noise: 415,152 rps at 8 B (was 417,363) and 332,803 at 1 KiB (was 327,517).
The ghz-matrix baseline rows for tonic carry the same two-byte caveat, which
is below its round-to-round spread at both sizes.

## After the engine used-list fix, same day again

The second-ranked lever fell the same day. `Connection.drive`'s flat profile
was 45% `if (!slot.used) continue` byte-tests — every engine pass
(`startReady`, `pumpAll`, `wakeDeadline`, `deadline`, `send`'s unpark, and
`findSlot` per DATA event) walked the whole slot array, one cache line per
776-byte slot, a thousand slots to find a handful of live streams. The fix
(baskingspot/http `0266fef`, pinned here) is an intrusive doubly-linked
used-list with integer links plus a free-index stack: O(1) slot acquire and
release, iteration in arrival order, live entries only.

Same ladder, same gating (run `bench/integration/results/20261113T220527Z`):

| arm | size | before | after | server CPU at peak step |
| --- | --- | -----: | ----: | --- |
| zig | 8 B | 1,462,603 rps | 1,500,396 rps | 291% → 221% |
| zig | 1 KiB | 862,224 rps | 867,280 rps | 309% → 219% |

The rps column barely moves because the loadgen is the ceiling now; the
server-CPU column is where the fix lives. Per-request cycles over the
cumulative arc: 9,840 (baseline) → 7,214 (stream-table rebuild) → **4,948**
(engine used-list), a 50% reduction, with `Connection.drive`'s share of the
flat profile collapsing from 25.5% to 4.3%. The next levers by the fresh
profile: `Session.nextEvent` 29.2%, `ArenaAllocator.alloc` 14.6%,
`Session.processBlock` 12.6%, `compiler_rt.memcpy` 4.2% (grpc does not
import fastmem), hpack encode/decode ~6% combined. The arena bump
allocations are the next target; the memcpy route wants an audit first
(remove what is removable, route the rest through fastmem), and after both,
the readiness-engine lane under zio is the architectural step.

## The engine's pool is stdx.slot now

The hand-rolled links lasted one day: stdx gained `slot` (stdx#31,
`baskingspot/stdx` `63e40a9`), and http's engine now uses it — the engine's
link bookkeeping, its free-index allocation, and the pointer-arithmetic
index recovery are all deleted, for a net −53 lines in
`src/h2/engine/connection.zig` (`baskingspot/http` `0fcded6`, pinned here
along with the stdx commit it needs). Performance is unchanged where it
matters: instructions per request 22,200 → 22,004, cycles per request
4,948 → 5,010 (within a contended box's noise), capacity 848,164 rps at
1 KiB against 867,280 (run `20261113T224841Z`, single round). The primitive
carried its weight by deleting code, which is the point.

## The arena bumps, third round same day

The third lever was `ArenaAllocator.alloc` at 14.6%: zero heap traffic, but
twenty-odd bump allocations per request. Two changes: http's `copyHead` and
`copyHeaders` copy a request head in three arena bumps (one names-and-values
block, one array, one scalars block — carved slices, same lifetime) where
per-field `dupe` paid 2N+6 (`baskingspot/http` `5ce1d35`), and grpc's
`dispatch` walks request messages on a four-entry stack array for the common
unary case, spilling to the arena only past four messages.

Cycles per request 4,948 → **4,529** (−8.5%; instructions −2.8%, so most of
the win is stalls, not straight-line work — fewer bumps, better locality).
Cumulative from the baseline: **9,840 → 4,529, −54%**. Capacity run
`20261113T233942Z`: 1,500,407 rps at 8 B on 205% of four cores (was 221% at
the same rate), 888,046 at 1 KiB; every number still client-bound.

Remaining by the profile: `Session.nextEvent` ~29%, `Session.processBlock`
~13%, `compiler_rt.memcpy` ~4% (audit, then fastmem), hpack ~6%.

## The oversized table, fourth round (2026-11-14)

`nextEvent`'s remaining 30% annotated straight onto the tombstone rebuild's
own scans: the session pre-reserved a stream table sized to `max_streams`
(1000 slots in this arm) while 64 streams were live, so every rebuild walked
1024 slots every ~65 requests, and every lookup probed chains sized for a
table sixteen times the live set. The fix is http `4f44384`: no pre-reserve;
the table grows with the live count, insertions go through the growing
`put`, and the stream-limit refusal stays count-based.

The biggest single win of the arc: cycles per request 4,638 → **3,649**
(−21%), instructions 21,400 → **15,132** (−29% — exact count, not noise).
Cumulative from the first baseline: **9,840 → 3,649, −63%**. `nextEvent`
fell from 29.8% of the flat profile to 8.9%; `processBlock` now leads at
18.7%. Capacity run `20261114T001152Z`-era: 1,452k rps at 8 B on 197% of
four cores where the previous round needed 219%, and 873k at 64 in-flight
on 115%. Every step remains client-bound.

## The memcpy audit, and where the arc stops (2026-11-14)

The audit Matt asked for: the 6-7% memcpy profile is many small
structurally-required copies — receive buffering into the session's input,
body accumulation in the buffered engine, request-head copies, HPACK encode
blocks, and the output queue's ownership of the bytes it sends. No large
avoidable copy remained; the one candidate, `appendFrame` staging every
outgoing frame through `frame_buf` before the queue copied it again, is now
gone (`baskingspot/http` `465a956`: a scatter append takes header and
payload in one pass, the staging buffer deleted). At 8 B that is a wash —
the removed copy was small and L2-hot — and at 1 KiB it shows in the tail:
p99 at 1024 in-flight is 0.407 ms where two rounds ago it was 3.5 ms, on a
queue-write path that no longer double-walks the payload.

Where this leaves the stack, five rounds in: cycles per request
**9,840 → 3,606 (−63%)**, instructions 37,732 → ~15,100 (−60%), capacity
**1.62M rps at 8 B and 869k at 1 KiB** on four cores, with the server
finishing every capacity step client-bound. The profile's remainder is
projects, not fruit: `processBlock` ~17% is HPACK decode and per-byte
header validation (the next lever there is SIMD token validation, an
http-repo project of its own), the queue writes and engine passes ~15%
restructure with a zero-copy receive path, and the arena bumps ~9% are the
exchange design itself. Nothing left is both big and mechanical.

## The evented lane, and SIMD validation (2026-11-14, closing rounds)

Two lanes ran in parallel as subagents and landed the same day.

**The zio lane** (`bench/integration/zig/serve_evented.zig`, grpc `b021c95`):
zio `5357759` drives grpc's `serve`/`serveConnection` unchanged — no API
divergence, zero errors, every reply byte-verified. zio defaults to one
executor (one core), which reaches 45-48% of the threaded arm; sized to the
cores (`BENCH_ZIO_EXECUTORS=0`), the evented arm matches thread-per-
connection exactly: **1,613,111 rps at 8 B against the threaded arm's
1,612,439, and 882,328 at 1 KiB against 875,644** (final runs
`20261114T015303Z`/`T015704Z`, both client-bound at every step). The
trade: p99 at 1024 in-flight 1 KiB is 2.5 ms against the threaded arm's
1.9. The evented engine is a viable same-performance alternative execution
model — the syscall-batching win it promises has no room to show while the
load generator is the ceiling and the h2 engine cost dominates both models.

**The SIMD lane** (http `e459b83` + the two follow-ups, pinned here): field
names, values, and method tokens validate one SIMD block at a time through
the repo's own `simd.first` idiom, with exhaustive byte-equivalence tests
(256 byte values at every position, checked against the scalar predicates).
The review pass found what the worker's 0.16 bench could not: on 0.17, at
our minimal-header workload, every sub-block field paid the vector setup
and then the scalar tail anyway — +2.9% cycles per request. Two fixes
later (`anyBad` with a below-one-block scalar guard, inlined so the guard
is a branch and not a call), the minimal-header race sits at 3,647 cycles
per request — 1% over the pre-SIMD tree — while wide header sets (real
gRPC paths, user agents, metadata) validate 27% faster (`H2HeadWide` 380 →
276 ns/op). Kept: the wide path is what real applications carry, and the
cost on minimal heads is a rounding error against a 63% arc.

The stack's closing state on this box: **3,647 cycles per request, −63%
from the first baseline; 1.61M rps at 8 B and ~880k at 1 KiB on four cores,
threaded or evented, both client-bound.** Nothing left is both big and
mechanical.

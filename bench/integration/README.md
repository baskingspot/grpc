# integration bench

The local, cross-framework race: our gRPC server against grpc-go and tonic,
driven by one load generator. `just bench-integration` builds every arm and
runs the matrix; the numbers land under `bench/integration/results/` and the
quotable ones in `docs/slop/results/`.

The numbers rank the three stacks under this machine's conditions. They are
not publishable claims: the box is a shared 8-core machine, not a dedicated
benchmark host, and the client (ghz) is grpc-go's own client library. Use them
to see where the stack stands and where it moves.

## Arms

Every arm serves `bench.v1.Bench/Unary` from `bench.proto` with a constant
reply and never looks at the request, so a pass measures framing, HTTP/2, and
the socket, and nothing else. A fresh server process runs per arm and size,
with a warmup pass discarded before the measured passes.

| Arm    | Server                                                                                                             | Request path                              | Reply path                          |
| ------ | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------- | ----------------------------------- |
| `zig`  | `grpc.server.serveConnection` over a hand accept loop, one thread per connection, `Io.Threaded`                    | buffered by the `server.Handler` exchange | a constant slice, no encode         |
| `go`   | grpc-go with `ForceServerCodec` (raw bytes), hand-registered `ServiceDesc`, no generated code                      | passed through undecoded by the raw codec | a constant `[]byte`, no encode      |
| `rust` | tonic 0.14, stub generated from `bench.proto` at build time (protox, no protoc binary), tokio multi-thread runtime | decoded to `Payload` by prost             | `Payload` clone plus a prost encode |

The codec is the one known asymmetry: the rust arm pays a prost decode of the
request and a prost encode of the reply per call; the other two pay neither.
At 8 B and 1 KiB that is memcpy scale, well under the transport cost, and the
README records it so nobody quotes the ratio past a decimal place.

`bench/zig/serve.zig` sets TCP_NODELAY on accepted sockets, which the stock
`server.serve` loop does not, because grpc-go and tonic both turn it on by
default. That is configuration parity, not a tuning choice.

## Configuration parity

| Setting                | zig                       | go                                 | rust                                      |
| ---------------------- | ------------------------- | ---------------------------------- | ----------------------------------------- |
| Max concurrent streams | 1000 (`max_streams`)      | 1000 (`grpc.MaxConcurrentStreams`) | 1000 (`concurrency_limit_per_connection`) |
| TCP_NODELAY            | set on accept             | on by default                      | on by default                             |
| HTTP/2 windows         | engine default (64 KiB)   | defaults, BDP-adaptive             | hyper/h2 defaults                         |
| Worker parallelism     | one thread per connection | GOMAXPROCS (affinity)              | tokio workers (affinity)                  |
| Stack versions         | this repo's HEAD          | pinned in `go.mod`                 | pinned in `Cargo.lock`                    |

The window row is the honest gap at larger sizes: grpc-go grows its windows
with bandwidth estimation and the other two do not. At 8 B and 1 KiB every
window in play holds dozens of in-flight messages, so the row does not move
the matrix. It will matter before a 64 KiB size is added.

## Method

ghz 0.121.0 (from the flake) drives every arm: same binary, same flags, same
request bytes. The server is pinned to cores 0-3 and ghz to 4-7 with taskset;
arm order rotates each round so drift in the box hits the arms evenly.
Each pass reads its numbers from ghz's JSON (rps, percentiles), the server's
`/proc` stat (CPU seconds inside the measured window), and `VmHWM` (peak RSS).
Canceled calls are the in-flight requests ghz drops at duration end and do not
count as errors.

| Profile   | Concurrency | Connections | What it isolates                            |
| --------- | ----------- | ----------- | ------------------------------------------- |
| `c1`      | 1           | 1           | closed-loop single-stream round trip        |
| `c64-2c`  | 64          | 2           | multiplexing: 32 streams per connection     |
| `c64-16c` | 64          | 16          | thread-per-connection scale: 16 connections |

Default matrix: sizes 8 B and 1 KiB, all profiles, three rounds, 3 s warmup
and 15 s measured per pass. Roughly twenty minutes. Flags pass through:
`just bench-integration --rounds=5 --sizes=8,1024,65536 --profiles=c1`.

## Capacity

`just bench-integration-capacity` runs the ladder instead of the matrix: every
arm under the zig load generator (`zig/loadgen.zig`, one thread per
connection, a fixed count of concurrent unary calls each, closed-loop), one
fresh server per step, at 64, 256, 1024, and 4096 in-flight calls, until the
server's cores or the client's fill. The generator sets TCP_NODELAY like ghz,
speaks plain gRPC so it drives all three arms, and prints one JSON line the
driver records. It is our own client, so it verifies instead of trusting
itself: every OK reply is checked byte for byte against the expected payload
(`verify_errors` in the row and the JSON), a check that caught the tonic arm
double-wrapping its reply on its first run. The rest of the cross-check
stack is in `docs/slop/results/rpc-capacity-20261113.md`. ghz stays the
reference client for the published matrix; the capacity numbers belong to
whichever side fills its cores first — with this generator, so far, always
the server.

## Profiling

`just bench-integration-profile` runs the profiling passes instead of the
matrix: a perf stat window per arm (cycles, instructions, branches,
branch-misses, page-faults, normalized per request), flat perf record symbol
profiles for the zig arm at the multiplexing and latency profiles, one
strace -c window at c1 for syscalls per request, and one counting-allocator
window per profile. Artifacts land beside the race's, under
`bench/integration/results/`; the quotable findings go in
`docs/slop/results/`.

The zig arm takes a third argument, `count`: it wraps the allocator in a
counter and logs one line of traffic per 100,000 requests, so a window reads
steady-state allocations per request off stderr. The wrapper's spin lock and
atomics are heavier than the stock path, which is why the flag is opt-in and
never used by the race itself.

This box's limits shape the passes (`perf_event_paranoid=2`, yama
`ptrace_scope=1`): the hardware counters work on our own children, call
graphs do not (they open the precise-IP event, which is denied), and strace
must wrap the arm rather than attach to it. nix's perf is a shell wrapper
that swallows SIGINT, so the driver ends every window by SIGTERM-ing the
arm itself — each tool reports its summary when the command it measured
ends. `driver.py` carries these as comments where they bite.

## Files

- `bench.proto` — the one service every arm serves and ghz drives.
- `zig/` — our arm; built by `zig build bench-integration-serve` (ReleaseFast
  fixed, like the micro bench suite).
- `go/` — the grpc-go arm; `go.mod` pins the version, `go build` makes
  `bin/arm-go`.
- `rust/` — the tonic arm; `Cargo.lock` pins the versions, `cargo build
--release` generates the stub via protox and tonic-prost-build.
- `driver.py` — builds the arms, runs the rounds, writes `rounds.jsonl` and
  `summary.json` per run directory.
- `results/` — run outputs, gitignored. `summary.json` carries the medians,
  the per-round rps, and the version pins that produced them.

# grpc

gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no code
generation.

Zig has no gRPC. The usual answer is a code generator that emits stubs and drags
in a foreign toolchain, which also drags in the whole HTTP/2 stack underneath it.
This library is that layer and nothing else. It carries messages, maps status
codes, runs calls on `baskingspot/http`'s HTTP/2 client, and answers them on the
engine next to it. Your own types stay your own; a message is `[]const u8` until
you decide otherwise.

```zig
const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:50051");
const stream = try address.connect(io, .{ .mode = .stream });
defer stream.close(io);

var reply: [4 * 1024]u8 = undefined;
const response = try grpc.blocking.unary(io, stream, .{
    .authority = "127.0.0.1:50051",
    .service = "helloworld.Greeter",
    .method = "SayHello",
    .body = request,
}, .{ .allocator = gpa, .message = &reply });
```

`examples/unary.zig` is that program complete, and `just example` compiles it.

This project is incubating. It is unstable, its API will change, and it is not
ready to depend on.

## What is in it

- `message`: the length-prefixed frame. `parseHeader` and `write` handle one
  message, and `Iterator` walks complete messages out of one buffer under a
  receive limit.
- `status`: the seventeen gRPC codes, `parse` and `codeFromInt` for the decimal
  wire form, and `decode` for the percent-escaped `grpc-message` field.
- `timeout`: `parse` and `encode` for `grpc-timeout`, with the coarsest exact
  unit and no truncation.
- `head`: the `:path`, the `content-type`, `te: trailers`, and
  `grpc-accept-encoding` fields, the encoded deadline, and metadata validation.
- `Call`: one call over one HTTP/2 stream. It frames the request, reassembles
  response messages across DATA frames, and ends on `grpc-status`, a trailers-only
  head, an HTTP error status, a reset, or a GOAWAY.
- `blocking`: `unary` for one request and one response, and `ServerStream` for a
  stream you pull message by message. `unaryIo` takes a reader and a writer
  instead of a stream, so a TLS transport calls the same code.
- `server`: `serve` and `serveConnection` answer unary calls on http's blocking
  engine, and `ReadinessServer` answers them on the readiness engine (epoll,
  kqueue, or poll) with the same handler. The handler fills a message and a status, and the status goes out in
  the trailing HEADERS section, never in `:status`.
- `blocking.Client`: one connection across calls. `unary` sends them in
  sequence, while `open` and `step` run several at once: every queued event goes
  to every call, and a call ignores the streams that are not its own.
- Streaming requests. `Call.writeMessage` queues a message under flow control and
  `Call.closeSend` ends the request, so client and bidirectional streams are the
  same primitives with a different order of calls.

Not implemented:

- Compression in the library. A call carries the `grpc-encoding` and
  `grpc-accept-encoding` fields and flags a compressed message, and the codec
  above it owns the bytes. `conformance/main.zig` wires gzip through
  `baskingspot/compress` to prove the seam works.
- Deadlines that bite. `grpc-timeout` goes on the wire, and nothing enforces it.
- A convenience for client and bidirectional streams. `Call` writes them and
  `conformance/main.zig` drives all five stream types, but `blocking` has no
  named helper and you drive the loop yourself.
- TLS. `baskingspot/tls` has no nonblocking client, and `baskingspot/http`'s TLS
  module is a server engine.
- Streaming on the server. The engine buffers a whole request body before the
  handler runs, so a server call carries one message in and one out.

## Install

Before the first tag, fetch the repository:

```sh
zig fetch --save git+https://github.com/baskingspot/grpc
```

Then in `build.zig`:

```zig
const grpc = b.dependency("grpc", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("grpc", grpc.module("grpc"));
```

This module builds with Zig 0.17.1.

## Documentation

- The API reference is the doc comments in `src/`. There is no generated site
  yet.
- `examples/` holds working programs: `unary.zig` calls `greeter.zig` (run both
  with `just demo-greeter`), and `etcd.zig` is a real etcd KV client. `examples/etcd.zig` is a
  working etcd KV client: `just demo-etcd` starts a throwaway etcd, puts two
  keys, lists them, deletes them, and stops the server.
- `CHANGELOG.md` records what changed, including work that is not released.

## Work on it

`nix develop` provides the toolchain, and `just ci` runs what CI runs. `just
test` runs the unit tests through the testingx runner with a two-minute timeout,
which is what turns a blocked socket into a failure instead of a stall. `just
bench` runs the frame and field benchmarks.

## License

MIT. See [LICENSE](LICENSE). The name and the logo files are not covered.

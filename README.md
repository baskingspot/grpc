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
var client: grpc.blocking.Client = undefined;
try client.init(gpa, io, socket);
defer client.deinit();

var reply: [4 * 1024]u8 = undefined;
const response = try client.unary(.{
    .authority = "127.0.0.1:50051",
    .service = "helloworld.Greeter",
    .method = "SayHello",
    .message = encoded,
    .timeout = .fromSecondsComptime(2),
}, &reply);
```

`Client.unary` accepts one serialized message and adds the gRPC frame through
`Call.writeMessageFlagged`. It returns the response bytes without their prefix.
`Client.init` takes ownership of the connected socket only on success.
`examples/unary.zig` is the complete program, and `just example` compiles it.

This project is incubating. It is unstable, its API will change, and it is not
ready to depend on.

## What is in it

- `message`: the length-prefixed frame. `parseHeader` and `write` handle one
  message, and `Iterator` walks complete messages out of one buffer under a
  receive limit.
- `status`: the seventeen gRPC codes, `parse` for the decimal wire form,
  `Code.fromInt` for an integer, and `decode` for the percent-escaped
  `grpc-message` field.
- `timeout`: `parse` and `encode` for `grpc-timeout`, with the coarsest exact
  unit and no truncation.
- `head`: the `:path`, the `content-type`, `te: trailers`, and
  `grpc-accept-encoding` fields, the encoded deadline, and metadata validation.
- `Call`: one call over one HTTP/2 stream. It frames the request, reassembles
  response messages across DATA frames, and ends on `grpc-status`, a trailers-only
  head, an HTTP error status, a reset, or a GOAWAY.
- `client` (`grpc.Client`): one connection across calls. `unary` sends calls in
  sequence and requires no other active call. `open` and `step` run several
  calls at once, and each call receives every queued event and ignores other
  streams. `unaryIo` takes a reader and a writer instead of a socket, so a TLS
  transport calls the same code. `ServerStream` pulls a server-streaming
  response one message at a time.
- `grpc_tls`: the opt-in TLS module. `grpc_tls.Connection` completes the
  handshake and presents a client over the plaintext side. `grpc_tls.serve` and
  `serveStreaming` accept on the caller's `Io`, handshake each connection, and
  run the same handlers as `grpc.server.serve`. The core module links no
  crypto.
- `server.Stream`: incremental requests and responses, including full-duplex.
  `next` reads one message. `begin`, `write`, and `finish` send the response
  head, framed bytes, and gRPC status trailers. The caller retains unsent bytes
  across backpressure.
- `server.serve` and `server.serveStreaming`: accept connections and run
  buffered or streaming handlers on the `Io` you pass. A handler is an ordinary
  function: it can block, take locks, and call back into the client, because
  each connection runs on its own task.
- `server.Handler`: the buffered convenience for unary, client-stream,
  server-stream, and half-duplex calls. `serve` and `serveConnection` use the
  `Io` engine.
- `server.ReadinessServer` and `grpc_tls.ReadinessServer`: handlers on epoll,
  kqueue, or poll, with no thread per connection. Experimental: these surfaces
  will be removed; `serve` and `grpc_tls.serve` carry TLS now, and the engines
  stay in `baskingspot/http`. Until then a callback retains its state in
  `Stream.context` and yields with `park` instead of blocking the other calls.
  `body_bytes` defaults to 64 KiB of unread input, including message prefixes.
  The HTTP engine reuses body buffers, so a streaming upload can exceed that
  limit as the handler reads it. `Stream.message_limit` bounds message
  assembly. The caller controls application storage.
- `readiness`: one connection, several calls in flight, no threads.
  Experimental: this surface will be removed; it multiplexes what `grpc.Client`
  does one call at a time. Each call keeps its own deadline.
- `Stream`: one byte-oriented call facade for either client driver. It admits
  one queued request message at a time, closes the send side, receives messages,
  and preserves status, metadata and cancellation. Raw `Call` remains available.

  `IoOptions.read_deadline` binds a plaintext reader to the same absolute call
  deadline and preserves its read-error cause. The TLS connection supplies these
  hooks for every client path.

- Streaming requests. `Call.writeMessage` queues a message under flow control and
  `Call.closeSend` ends the request, so client and bidirectional streams are the
  same primitives with a different order of calls.

Not implemented:

- Compression in the library. A call carries the `grpc-encoding` and
  `grpc-accept-encoding` fields and flags a compressed message, and the codec
  above it owns the bytes. `conformance/main.zig` wires gzip through
  `baskingspot/compress` to prove the seam works.
- A convenience for client and bidirectional streams. `Call` writes them and
  `conformance/main.zig` drives all five stream types, but `Client` has no
  named helper and you drive the loop yourself.

## Which `std.Io`

Every entry point takes an `Io` value, so the concurrency model is yours to
pick. The API does not change between implementations.

- A client on `Io.Threaded` is usually right: it parks one thread on the
  socket, and a client process has threads to spare.
- A server on `Io.Threaded` spends one thread per connection. HTTP/2
  multiplexes many calls over each of them, which serves a long way, but it is
  not the shape for thousands of connections in one process.
- For that, pass an evented `Io` instead: the same client and the same
  `serve` loop suspend instead of parking a thread. [zio](https://github.com/lalinsky/zio)
  is a complete third-party `std.Io` implementation today, and `std.Io.Evented`
  is the upstream answer when it lands.

The readiness surfaces (`server.ReadinessServer`, `grpc_tls.ReadinessServer`,
`readiness`) were the hand-rolled substitute for an evented `Io`: epoll,
kqueue, or poll with the state machine written out by hand. They are
experimental and will be removed once `serve` carries TLS; the engines remain
in `baskingspot/http`.

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

For a TLS client or server, set `.tls = true` on the dependency:

```zig
const grpc = b.dependency("grpc", .{
    .target = target,
    .optimize = optimize,
    .tls = true,
});
exe.root_module.addImport("grpc_tls", grpc.module("grpc_tls"));
```

A core-only consumer fetches HTTP and stdx, without TLS, CLI, or the developer
packages. The root build enables TLS by default. `zig build test -Dtls=false`
runs the core tests without the TLS module.

This module builds with Zig 0.17.1.

## Documentation

- The API reference is the doc comments in `src/`. There is no generated site
  yet.
- `examples/` holds complete programs. `just demo-greeter` runs `unary.zig`
  against `greeter.zig`. `just demo-etcd` starts a temporary etcd, puts two keys,
  lists them, deletes them, and then holds a watch stream open on a second
  connection: create, a put event, a delete event, cancel, and teardown.
- `just demo-kitchen` runs both variants of `kitchen.zig`. The threaded program
  uses `Io.Group` and buffered handlers. The readiness program uses epoll,
  kqueue, or poll and streams messages incrementally; it is the experimental
  lane from the list above.
- [Conformance](conformance/README.md) describes the client and server suites over h2c and TLS.
- `CHANGELOG.md` records what changed, including work that is not released.

## Work on it

`nix develop` provides the toolchain, and `just ci` runs what CI runs. `just
test` runs the unit tests through the testingx runner with a two-minute timeout,
which is what turns a blocked socket into a failure instead of a stall. `just
bench` runs the frame and field benchmarks.

## License

MIT. See [LICENSE](LICENSE). The name and the logo files are not covered.

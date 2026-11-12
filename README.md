<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/static/logo-dark.svg">
    <img src="docs/static/logo.svg" alt="grpc" height="48">
  </picture>
</h1>

gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no code
generation.

grpc lets a Zig program make and serve gRPC calls without adopting a stub
generator or a C runtime. A message is `[]const u8` to the library: you keep
your own types and choose your own codec, and the call handles the framing,
the headers, and the status over `baskingspot/http`'s HTTP/2, as a client or
as a server.

Every entry point takes an `Io` value, so the concurrency model is yours.
Under `Io.Threaded`, a client parks one thread on its socket and `serve`
spends one thread per connection; HTTP/2 multiplexes many calls over each of
those threads. For thousands of connections in one process, pass an evented
`Io` instead: [zio](https://github.com/lalinsky/zio) is a complete third-party
`std.Io` implementation, and the same calls suspend there instead of parking
a thread.

The connectrpc conformance suite passes in full: 2,025 cases over the client
and the server, cleartext and TLS, identity and gzip, and all five stream
types.

```zig
var client: grpc.Client = undefined;
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

`Client.unary` adds the five-byte gRPC frame and returns the response bytes
without their prefix. `Client.init` takes ownership of the connected socket
only on success. `examples/unary.zig` is the complete program, and `just
example` compiles it.

The library is incubating and unreleased: the API will change, so pin a
commit.

## What is in it

- `message`: the length-prefixed frame. `parseHeader` and `write` handle one
  message, and `Iterator` walks complete messages out of one buffer under a
  receive limit.
- `status`: the seventeen gRPC codes, `parse` for the decimal wire form,
  `Code.fromInt` for an integer, and `decode` for the percent-escaped
  `grpc-message` field.
- `timeout`: `parse` and `encode` for `grpc-timeout`, in the coarsest unit
  that stays exact.
- `head`: the `:path`, `content-type`, `te: trailers`, and
  `grpc-accept-encoding` fields, the encoded deadline, and metadata
  validation.
- `Call`: one call over one HTTP/2 stream. It frames the request, reassembles
  response messages across DATA frames, and ends on `grpc-status`, a
  trailers-only head, an HTTP error status, a reset, or a GOAWAY.
- `Client`: one connection across calls. `unary` sends calls in sequence and
  returns; `open` and `step` run several calls at once. `unaryIo` takes a
  reader and a writer instead of a socket, so a TLS transport calls the same
  code.
- `ServerStream`: a server-streaming response pulled one message at a time.
- `Stream`: one byte-oriented facade over a call, for either driver. It
  admits one queued request message, closes the send side, receives
  messages, and preserves status, metadata, and cancellation. Raw `Call`
  stays available below it.
- `grpc_tls`: the opt-in TLS module. `grpc_tls.Connection` completes the
  handshake and presents a client over the plaintext side; `grpc_tls.serve`
  and `serveStreaming` accept and run the same handlers as `grpc.server.serve`
  over TLS. The core module links no crypto.
- `server.serve` and `server.serveStreaming`: accept on the `Io` you pass,
  one task per connection. A handler is an ordinary function: it can block,
  take locks, and call back into a client.
- `server.Handler`: the buffered convenience for unary, client-stream,
  server-stream, and half-duplex calls. `serveConnection` serves one
  already-accepted socket.
- `server.Stream`: incremental requests and responses, including full-duplex.
  `next` reads one message; `begin`, `write`, and `finish` send the response
  head, framed bytes, and status trailers. The caller retains unsent bytes
  across backpressure.
- `server.ReadinessServer`, `grpc_tls.ReadinessServer`, and `readiness`:
  handlers on epoll, kqueue, or poll, with no thread per connection.
  Experimental, and they will be removed; the engines stay in
  `baskingspot/http`.

Not implemented:

- Compression in the library. A call carries the `grpc-encoding` and
  `grpc-accept-encoding` fields and flags a compressed message, and the codec
  above it owns the bytes. `conformance/main.zig` wires gzip through
  `baskingspot/compress` to prove the seam.
- A convenience for client and bidirectional streams. `Call.writeMessage`
  queues a message under flow control and `Call.closeSend` ends the request;
  `conformance/main.zig` drives all five stream types with them.

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

For TLS, set `.tls = true` on the dependency and import `grpc_tls`. A
core-only consumer fetches `http` and `stdx`, and nothing that links crypto.
`zig build test -Dtls=false` runs the core tests without the TLS module.

grpc builds with Zig 0.17.1.

## Documentation

- The API reference is the doc comments in `src/`. There is no generated
  site yet.
- `examples/` holds complete programs. `just demo-greeter` runs `unary.zig`
  against `greeter.zig`. `just demo-etcd` starts a temporary etcd, puts two
  keys, lists them, deletes them, and holds a watch stream open on a second
  connection: create, a put event, a delete event, cancel, and teardown.
  `just demo-kitchen` runs both variants of `kitchen.zig`, one on threads
  and one on the readiness engines.
- [Conformance](conformance/README.md) describes the client and server
  suites over h2c and TLS.

## Work on it

`nix develop` provides the toolchain, including the conformance runner. `just
ci` runs what CI runs. `just test` runs the unit tests through testingx with
a two-minute timeout, which turns a blocked socket into a failure instead of
a stall. `just conformance-server-tls` runs the combined server suite, and
`just bench` runs the frame and field benchmarks.

## License

MIT. See `LICENSE`.

## How it's made

Built with LLM assistance. [How, in detail](https://github.com/baskingspot/.github/blob/main/docs/AI.md).

<a href="https://github.com/baskingspot"><img src="https://raw.githubusercontent.com/baskingspot/.github/main/brand/assets/badge.svg" alt="baskingspot" height="24"></a>

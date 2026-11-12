+++
title = "grpc"
+++

gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no code generation.

grpc lets a Zig program make and serve gRPC calls without adopting a stub
generator or a C runtime. A message is `[]const u8` to the library: you keep
your own types and choose your own codec, and the call handles the framing,
the headers, and the status over `baskingspot/http`'s HTTP/2, as a client or
as a server.

- [The frame](#the-frame): `message.parseHeader` and `write` handle one length-prefixed frame with no allocator, and `Iterator` walks complete messages out of one buffer under a receive limit.
- [Status codes](#status-codes): the seventeen gRPC codes, `parse` for the decimal wire form, `Code.fromInt` for an integer, and `decode` for the percent-escaped `grpc-message`.
- [Timeouts](#timeouts): `parse` and `encode` for `grpc-timeout`, in the coarsest unit that stays exact.
- [The request head](#the-request-head): the `:path`, `content-type`, `te: trailers`, and `grpc-accept-encoding` fields, the encoded deadline, and metadata validation.
- [One call](#one-call): `Call` frames the request, reassembles response messages across DATA frames, and ends on `grpc-status`, a trailers-only head, an HTTP error status, a reset, or a GOAWAY.
- [The client](#the-client): `grpc.Client` holds one connection across calls. `unary` and `unaryCaptured` return the response, status, and metadata; `open` and `step` run several calls at once; `ServerStream` pulls a server-streaming response.
- [The stream facade](#the-stream-facade): `Stream` is one byte-oriented interface over a call, shared by the client and the readiness session.
- [TLS](#tls): `grpc_tls.Connection` presents a client over the plaintext side, and `grpc_tls.serve` runs the same handlers as `grpc.server.serve` over TLS.
- [Serving](#serving): `serve` and `serveStreaming` accept on the `Io` you pass, one task per connection, so a handler can block and take locks.
- [The streaming handler](#the-streaming-handler): `server.Stream` answers incrementally, full-duplex included, and retains unsent bytes across backpressure.
- [Readiness](#readiness): the experimental epoll, kqueue, and poll surfaces, with removal intended.
- [Limits](#limits): compression, client-certificate authentication, and the rest of what is not here.

The library is incubating and the API is not frozen; a consumer pins a
version. The [API reference](api/) documents every public symbol.

# Getting started

## Install

```sh
zig fetch --save https://baskingspot.dev/grpc/v0.1.0.tar.gz
```

Add the module in `build.zig`:

```zig
const grpc = b.dependency("grpc", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("grpc", grpc.module("grpc"));
```

For TLS, set `.tls = true` on the dependency and import `grpc_tls`:

```zig
const grpc = b.dependency("grpc", .{
    .target = target,
    .optimize = optimize,
    .tls = true,
});
exe.root_module.addImport("grpc_tls", grpc.module("grpc_tls"));
```

A core-only consumer fetches `http` and `stdx`, and nothing that links
crypto. `zig build test -Dtls=false` runs the core tests without the TLS
module.

grpc builds with Zig 0.17.1.

## Import it

```zig
const grpc = @import("grpc");
```

The module is `grpc`, the TLS module is `grpc_tls`. Neither collides with a
std namespace.

## Read the examples

`examples/` holds complete programs, each wired to a recipe:

```sh
just demo-greeter   # unary.zig calls greeter.zig over h2c
just demo-etcd      # the KV calls and a watch stream against a real etcd
just demo-kitchen   # eight call shapes, served and called two ways
```

The etcd example is the one to read for a real client: a unary session for
KV calls, a second connection holding a watch stream, and cancel as the way
a watch ends.

# The frame

A gRPC message is five bytes of prefix, one compressed flag and a big-endian
`u32` length, and then the payload. `message.parseHeader` reads the prefix,
`message.write` writes it, and neither allocates: the payload bytes are
yours.

```zig
const frame = try message.write(&buffer, payload, false);
```

`message.Iterator` walks complete messages out of one buffer, so a caller
holding a chunk of body bytes can hand out the messages that finished inside
it. A receive limit caps the largest message it will assemble, and an
oversized one is `error.MessageTooLarge` rather than a grown buffer.

# Status codes

`status.Code` is the seventeen gRPC codes as an enum, and an unknown integer
is still a value, `.unknown`. `Code.fromInt` maps the wire number,
`status.parse` reads the decimal text of `grpc-status`, and `status.decode`
un-escapes `grpc-message`, whose bytes arrive percent-encoded.

```zig
const code = try status.parse("14");
const text = try status.decode(&text_buffer, "no%20such%20key");
```

A status is returned, not thrown: every call answers with its code, and the
client surfaces the peer's code unchanged.

# Timeouts

`grpc-timeout` carries a relative deadline in the largest unit that stays
exact: hours, minutes, seconds, milliseconds, microseconds, or nanoseconds.
`timeout.parse` reads it into a duration, `timeout.encode` writes it back
out, and neither truncates: a timeout that does not fit the unit it selects
is `error.OutOfRange`, not a shorter deadline.

A call's `timeout` field is a `stdx.time.OptionalDuration`:

```zig
.timeout = .fromSecondsComptime(2),
```

The client turns it into an absolute deadline and reads no message past it.

# The request head

`head` builds the fields a gRPC call sends: `:path` from the service and
method, `content-type`, the required `te: trailers`, `grpc-encoding` and
`grpc-accept-encoding`, and the encoded `grpc-timeout`. It also validates
application metadata: names are lowercase, and reserved prefixes are
rejected before they reach the wire.

```zig
const request: head.Request = .{
    .service = "helloworld.Greeter",
    .method = "SayHello",
    .timeout = .fromSecondsComptime(2),
};
var field_buffer: [12]http.Header = undefined;
var scratch: [256]u8 = undefined;
var timeout_buffer: [timeout.max_digits + 1]u8 = undefined;
const fields = try head.fields(&field_buffer, &scratch, &timeout_buffer, request);
```

# One call

`Call` is one call over one HTTP/2 stream. It writes the request head,
queues request messages with `writeMessage` under the stream's flow control,
and ends the request with `closeSend`. On the way back it reassembles
response messages across as many DATA frames as they span, under the
message limit.

A call ends exactly once, on whichever comes first: `grpc-status` in
trailers, a trailers-only response head, an HTTP error status, a stream
reset, or a GOAWAY. The status it ends with is the answer; a peer status
survives, and a transport that dies first answers `unavailable`.

```zig
try call.writeMessage(request_bytes);
call.closeSend();
```

This is the layer a streaming caller drives by hand: `writeMessage` and
`closeSend` are the same primitives for client-stream and bidirectional
calls, with a different order of calls.

# The client

`grpc.Client` holds one connection across calls.

```zig
var client: grpc.Client = undefined;
try client.init(gpa, io, socket);
defer client.deinit();
```

`init` takes a pointer, not a value: the client points its transport reader
and writer at its own fields, and every `Call` it opens points into its
connection, so it must live at one address. `deinit` closes a socket the
client owns; a client built over a caller-owned reader and writer with
`initIo` closes nothing.

`unary` sends one call and returns its response bytes, status, and message:

```zig
var reply: [4 * 1024]u8 = undefined;
const response = try client.unary(.{
    .authority = "127.0.0.1:50051",
    .service = "helloworld.Greeter",
    .method = "SayHello",
    .message = encoded,
    .timeout = .fromSecondsComptime(2),
}, &reply);
```

`unaryCaptured` returns a `Captured` with the same answer plus the response
headers, the trailers, and raw `error_details` bytes. The metadata lives in
the capture's arena, so call `Captured.deinit`.

`open` starts a call without waiting for it, and `step` advances every open
call one turn: flush output, deliver events, read input. HTTP/2 multiplexes
the streams, so several calls share the one connection and its one thread.

`ServerStream` is the one-shot pull iterator for a server-streaming
response: hand it a connected socket and a request, and `next` returns one
message at a time until the call's end.

# The stream facade

`grpc.Stream` is one byte-oriented interface over a call, for the client and
for the readiness session alike:

```zig
var stream = client.stream(&calls[0], &calls);
defer stream.deinit();
```

`send` copies request bytes and returns false while the previous message is
still queued; advance and send again. `nextMessage` returns the next
response message. `closeSend` ends the request side. `result` is the
status. `deinit` releases the call, not the client or the socket it borrows.

# TLS

`grpc_tls` is the opt-in module; the core links no crypto. Build with
`.tls = true` and import `grpc_tls`.

`grpc_tls.Connection` completes the handshake and presents a `grpc.Client`
over the plaintext side. `grpc_tls.serve` and `grpc_tls.serveStreaming`
accept on the caller's `Io`, handshake each connection through the blocking
`tls_std` server, and run the same handlers as `grpc.server.serve` over the
decrypted bytes. Both offer and require ALPN `h2`.

```zig
const creds: tls.CertificateChain = try .fromPem(pem, der, &chain_storage);
var listener = try bind_address.listen(io, .{ .mode = .stream });
try grpc_tls.serveStreaming(io, &listener, handler, .{
    .allocator = gpa,
    .cert_chain = chain.slice,
    .signer = key.signer(),
});
```

The chain, the signer, and the listener stay caller-owned; `serve` returns
when the listener does.

# Serving

`serve` accepts connections until the listener stops and hands each one to
the `Io` executor as its own task. A handler is an ordinary function: it can
block, take locks, and call back into a client, because nothing else shares
its thread.

```zig
var listener = try bind_address.listen(io, .{ .mode = .stream });
try grpc.server.serve(io, &listener, handler, .{
    .allocator = gpa,
    .body_bytes = 64 * 1024,
});
```

`serveConnection` serves one already-accepted socket. `serveStreaming` runs
the same loop with a `StreamingHandler`.

`server.Handler` is the buffered convenience: the whole request is in
memory before the handler runs, which fits unary, client-stream,
server-stream, and half-duplex calls.

# The streaming handler

`server.Stream` is the incremental surface, for a response that interleaves
with a request still uploading. `next` reads one request message;
`begin` sends the response head, `write` sends framed bytes, and `finish`
sends the status trailers and waits for queued output to drain.

```zig
while (try stream.next()) |request| {
    try stream.begin(&.{});
    try stream.send(reply_bytes, false);
}
try stream.finish(.{ .code = .ok }, &.{}, "");
```

A write the peer has no window for parks the stream and retains the unsent
bytes; `unsent` says how many. Full-duplex works: a response can be under
way while the request is still arriving. `expired` reports whether the
request's `grpc-timeout` has passed, and a handler that parks with a wake
time is woken at it, with no traffic needed to deliver the wake.

# Readiness

`server.ReadinessServer`, `grpc_tls.ReadinessServer`, and the `readiness`
client session are the no-thread surfaces: epoll, kqueue, or poll, with the
handler's state machine written out by hand and `park` as the yield.

They are experimental and will be removed. `serve` and `grpc_tls.serve`
carry TLS now, and an evented `Io` gives one-thread multiplexing to the same
code, which is what these surfaces were a hand-rolled substitute for. The
readiness engines stay in `baskingspot/http`.

# Limits

Not implemented:

- Compression in the library. A call carries the `grpc-encoding` and
  `grpc-accept-encoding` fields and flags a compressed message, and the codec
  above it owns the bytes. The conformance client wires gzip through
  `baskingspot/compress` to prove the seam.
- A convenience for client and bidirectional streams. `Call.writeMessage`
  and `Call.closeSend` are the primitives; the conformance client drives all
  five stream types with them, and `Stream` is the byte-level facade.
- Client-certificate authentication. `grpc_tls.serve` does not expose the
  `tls_std` client-auth policy; a server accepts any peer the TLS handshake
  completes with.
- Handler interruption. The blocking engine runs a handler to completion;
  `Request.expired` answers between messages, and a deadline cannot stop a
  handler that is already running.

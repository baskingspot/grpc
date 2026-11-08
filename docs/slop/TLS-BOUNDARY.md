# The TLS boundary, refined

Written 2026-11-09 after the conformance client reached 644/644. The question was
whether `conformance/tls_bridge.zig` is gRPC work. It is not, and neither is most
of what sits under it.

## The evidence

A plaintext reader and writer over TLS, with a deadline, is what any HTTPS client
needs. Nothing in the bridge knows gRPC exists: it drives a handshake, moves
bytes, and answers "did the wait end". `h1.client.fetchIo` would use the same
piece unchanged.

The ecosystems put the line in the same place:

- **grpc-go** implements neither TLS nor HTTP/2. `credentials.NewTLS` wraps
  `crypto/tls`, checks that the negotiated ALPN is `h2`, and hands gRPC a
  `net.Conn`. Frames and HPACK come from `x/net/http2`. gRPC owns the five-byte
  message prefix, status, metadata, trailers, and the call state machine.
- **tonic** is the same shape: `hyper` for HTTP/2, `rustls` behind
  `ClientTlsConfig` for TLS, and gRPC owning codec, framing, status, metadata.
- **grpc-java** puts Netty's HTTP/2 handler and TLS underneath and owns the same
  upper half.

So TLS is a first-class transport dependency of a gRPC client, not an optional
extra nobody is expected to wire. What gRPC must not do is implement it.

## Where each piece belongs

**`tls` / `tls_nonblocking`** — the handshake, the record layer, and a step loop
over a socket. `Client` and `Server` symmetric. This is the analogue of
`crypto/tls.Conn` and `rustls::StreamOwned`. Already correct: `Client` landed
there as `tls_nonblocking.Client`.

**`http` / `http_tls`** — HTTP/2 in sans-I/O and engine form; the TLS transport
its engines drive (`transport/tls.zig` server, `transport/tls_client.zig`
client); **and** the blocking plaintext connection over TLS. The last one belongs
here because it is the same concern as `transport/plain.zig`: both present bytes
over a socket, one decrypts first. Any blocking HTTPS client wants it, `h1`
included.

**`grpc`** — message framing, status, metadata, the `grpc-timeout` field, `Call`,
and the server. Sockets only in the convenience layer, and TLS reached through
`http_tls` behind an opt-in module. No record layer, no crypto, no
deadline-for-TLS special case.

## What `http_tls` should expose

The shape the conformance bridge already implements, minus its gRPC-adjacent
naming:

```zig
pub const client = struct {
    pub const Options = struct {
        /// PEM bytes, as handed by a caller that already holds them.
        ca_pem: []const u8,
        /// ASCII DNS name for SNI and the DNS-ID check.
        host_name: []const u8,
        /// Current time in seconds, for certificate validity.
        now_sec: i64,
        alpn: []const u8 = "h2",
    };

    pub const Connection = struct {
        pub fn init(allocator, io, socket: Io.net.Stream, options: Options) !Connection;
        pub fn deinit(self: *Connection) void;
        pub fn reader(self: *Connection) *Io.Reader;
        pub fn writer(self: *Connection) *Io.Writer;
        /// Absolute monotonic nanoseconds, or null. Enforced by the connection
        /// because it owns the record layer; a caller that polls the fd instead
        /// reads ciphertext.
        pub fn setDeadline(self: *Connection, absolute_ns: ?i96) void;
        /// True when the last wait ended because the deadline passed. `Io.Reader`
        /// has no timeout to report.
        pub fn expired(self: *const Connection) bool;
    };
};
```

Four properties come from tls-lead and h2, and none are preferences:

- **No `canWrite`.** The write is the query. After `close_notify`, `write` succeeds
  while TX is open and returns `error.Closed` once it is not. A predicate would
  duplicate state that an intervening `next()` can change.
- **A deadline is policy, not transport.** Stopping mid-handshake and resuming
  later is legal; nothing in `Client` has a clock. On expiry, decide and `deinit`
  to release the slot. Never `endInput()` for it: that means transport EOF and
  surfaces as `error.UnexpectedEof`.
- **Do not copy plaintext.** `application_data` stays valid until it is consumed
  and `next()` hands back the remainder, so the transport is the buffer. A private
  copy doubles the plaintext that has to be wiped. Partial consume keeps the tail:
  `consumeApplication(n)` advances the pinned slice by `n` and the next `next()`
  hands back exactly the remainder, any prefix, any number of times (tls 8b8d76d).
  The blocking wrapper is therefore offset arithmetic over a slice it already
  holds.
- **An empty `receiveBuffer()` is backpressure, not idle.** It is empty while
  plaintext is unconsumed, so a driver that treats empty as "wait for readiness"
  spins. It must deliver the plaintext first, then read.
- **The expiry flag is the caller's.** The transport has no error for a local
  timeout and should not grow one.

Two more are measured, and they are the ones that cost days:

1. The deadline is owned here, not by the caller. Reading the fd under TLS feeds
   ciphertext to whatever parses next; the session reads `17 03 03` as a frame
   length and fails with FRAME_SIZE_ERROR. That is how the last eight conformance
   cases failed.
2. Bytes read from the socket go into the transport's own `receiveBuffer`, and
   the plaintext handed out is copied and consumed immediately. A private buffer
   read first and fitted afterwards loses a tail and misaligns the frame stream.

## What grpc does with it

An opt-in module, `grpc.tls`, depending on `http_tls` and on the `http` module
`grpc` already has. It exposes a blocking client over a TLS connection:

```zig
const grpc_tls = @import("grpc_tls");
var tls = try grpc_tls.Connection.init(gpa, io, socket, .{ .ca_pem = pem, .host_name = "example.com", .now_sec = now });
defer tls.deinit();
const response = try tls.unary(.{ .authority = "example.com", .service = "...", .method = "...", .body = framed }, &reply);
```

The core module stays crypto-free: `message`, `status`, `Call`, and the server
pull nothing extra, so an h2c-only build fetches no TLS crate and links no
crypto. The TLS build names the module, the way `http_tls` is named.

## Done: 2026-11-09

`http_tls.client.Connection` landed at http `38ea34f`, hardened at `02d5433` and
`db93025`. `conformance/tls_bridge.zig` is deleted. The harness dials, builds the
connection from the certificate the runner handed it, and drives `grpc.tls` over
it. 644 of 644 still passes, and 409 of 409 cleartext.

Two facts from wiring it that are worth keeping:

- **The module is the unit, not the file.** A module rooted at
  `http_tls/client.zig` does not compile: Zig anchors a module's imports to its
  root directory, so `../h2/...` is out of bounds. Depend on the `http_tls`
  module and use `http_tls.client.Connection`.
- **`grpc.tls` takes a pointer** and owns the socket. Its `deinit` closes the
  socket and releases the TLS state; `http_tls.client.Connection.deinit` does not
  close it, because the caller opened it.

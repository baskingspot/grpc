# The TLS seam

How the conformance client gets TLS, and the four rules that are easy to get
wrong. Written 2026-11-08 while blocked on the layers below.

## Status

grpc passes every gRPC conformance case that does not need TLS. TLS is excluded
by `supports_tls: false` in `conformance/config.yaml`, and the axis is roughly
another 400 cases.

The blocker is not in this repository:

1. `baskingspot/tls` has no `tls_nonblocking.Client`. `tls_nonblocking` has
   `Server` and nothing else.
2. `baskingspot/tls` has no way to add a trust anchor from bytes.
   `std.crypto.Certificate.Bundle` only loads from a path, and the conformance
   runner generates its certificate per run. `addCertsFromPemBytes` is the
   missing piece.
3. `baskingspot/http` has no client transport to wrap, and it will not add one
   until the client exists.

When they land, this repository changes in one place: an adapter in
`conformance/` that drives the `http` client transport, plus one line in the
config. The library takes no TLS dependency.

## The adapter

Mirror `http`'s `transport/plain.zig` and `transport/tls.zig`. Do not invent a
second seam.

```zig
Transport.init(allocator, io, fd, options, read_bytes, write_bytes) // allocates; grpc passes no storage
transport.write(bytes) usize     // queue plaintext; short count is backpressure
transport.send(P)                // drain to the fd, non-blocking
transport.receive(P)             // read once into the transport buffer and commit
transport.next() Event           // six arms
transport.consume(n)             // release a pinned application_data slice
transport.canWrite() bool        // our TX is still open, not a promise the peer accepts
transport.closeWrite()
```

`P` is `http.transport.platform_linux` or `platform_darwin`, chosen from
`builtin.os.tag`. Under TLS the two size parameters are unused: the record and
output buffers are fixed arrays inside the slot.

The event union has six arms. `close_notify` arrives as `.closed` with
`canWrite()` still true, `key_update` arrives as `.progress`, and 0-RTT is
`error.EarlyDataUnsupported`.

## The four rules

1. A frame does not fit one TLS record. A maximum DATA frame is 16393 bytes and
   `tls.frame.max_plaintext_len` is 16384, so `write` returns short on a full
   frame. Carry `frame_len` and `frame_off`, write from the offset, send, and
   only call `nextFrame` again once the offset reaches the length. A half-queued
   frame followed by a new one produces a stream that fails frame-length or
   HPACK checks with no obvious cause.
2. Make one attempt per step and yield. `send` is non-blocking and `write`
   returns zero while the TLS layer owes output, so a retry loop inside one step
   makes no progress and burns a core until the deadline. The retry is across
   steps, back through the readiness wait.
3. Send before every blocking read, not only on `need_output`. During the
   handshake and around a key update the transport owes the peer bytes, and a
   server waiting for our ClientHello will not send. `need_output` means "has
   output now", not "the only time it can have output". Skipping this deadlocks
   both ends.
4. An empty `receiveBuffer()` is backpressure, not EOF. It can be empty while
   plaintext is still unconsumed, and `received()` asserts when plaintext is
   pending. Only this driver's own socket read returning zero is EOF, and only
   then call `endInput()`.

`application_data` is pinned until `consume(n)`. Copy into the h2 connection and
consume before the next `next()` or `receive()`.

The fd stays with grpc: it connects the stream and hands it over, so the
deadline poll before a read is unchanged.

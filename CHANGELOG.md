# Changelog

All notable changes to grpc are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `message`: the length-prefixed gRPC frame. `parseHeader` and `write` handle
  one message, and `Iterator` walks complete messages out of one buffer under a
  receive limit.
- `status`: the seventeen gRPC codes, `parse` and `codeFromInt` for the decimal
  wire form, and `decode` for the percent-escaped `grpc-message` field.
- `timeout`: `parse` and `encode` for the `grpc-timeout` field.
- `head`: the `:path`, the `content-type`/`te`/`grpc-accept-encoding` fields, the
  encoded deadline, and metadata validation.
- `Call`: one call over an HTTP/2 stream. It frames the request, reassembles
  response messages across DATA frames, and ends on `grpc-status`, a trailers
  only head, an HTTP error status, a reset, or a GOAWAY.
- `blocking`: `unary` and `unaryIo` send one request and return one response
  over `std.Io`. `unaryIo` takes a reader and a writer, so a TLS adapter calls
  it after the handshake with no change here. `Config` is an allocator and the
  destination for the response message.
- `Call.writeMessage` and `Call.writeMessageFlagged`: queue a request message
  under flow control, so client and bidirectional streams reuse the same call.
- `Call.closeSend`: end the request after the queued messages, instead of the
  call closing it when it opens.
- The first answer wins. A head that rejects a response, or a trailer that
  carries the status, is not overwritten by a later event about the same call. A
  body that turns out unreadable after the trailers still overrides it.
- `blocking.Client.send`: the write half of `step`, for a caller that acts
  between sending and waiting, such as cancelling after a delay.
- `Call.cancel`: abort a call with `RST_STREAM` CANCEL.
- Deadlines are enforced. `blocking.unary`, `Client.unary`, `Client.step`, and
  `ServerStream` wait with `poll(2)` on the socket and end the call
  DEADLINE_EXCEEDED when the time runs out. A transport with no socket, such as
  a TLS reader, has nothing to poll and runs without a deadline.
- `Call.errorDetails`: the decoded `grpc-status-details-bin` trailer, as the raw
  `google.rpc.Status` bytes the codec above owns.
- Metadata keys are lowered on the way out, and `-bin` values are base64 encoded
  with or without padding on the way in.
- `Call` refuses a response whose content type is not gRPC, whose subformat is
  not proto, that names an encoding the caller did not accept, or that carries a
  compressed message. A response with no `grpc-status` is now UNKNOWN, which is
  what the specification says to report.
- `Call.responseHeaders` and `Call.responseTrailers`: the response field
  sections, copied into the call's arena so they stay readable after the
  connection reclaims the stream.
- `server`: `serve` and `serveConnection` answer unary calls on http's blocking
  HTTP/2 engine, with the status in the trailing HEADERS section. A handler
  allocates its response from `Request.arena`, which lives until the stream is
  complete.
- `blocking.Client.initIo`: a client over any reader and writer pair, which is
  the shape a TLS transport presents as plaintext. A caller-owned pair is not
  closed here, and a deadline needs an fd, so a transport that decrypts in
  memory must enforce the deadline itself.
- `readiness.Session`: one connection, several calls in flight, driven by
  `poll(2)` on the caller's thread. Each call carries its own deadline, and a
  deadline that passes ends that call DEADLINE_EXCEEDED without touching the
  others.
- The drivers use `Io` rather than raw syscalls: `server.serve` hands each
  connection to an `Io.Group` with `Group.concurrent` (inline when the
  implementation has no concurrency), and both the blocking client and the
  readiness session wait on a deadline with `Io.Batch.awaitConcurrent` instead of
  a hand-rolled `poll(2)`.
- `server.ReadinessServer`: the same handler on http's readiness engine, epoll
  on Linux and kqueue on macOS, so one thread carries many connections and the
  engine's per-connection deadlines apply. `serve` and `serveConnection` remain
  the blocking path.
- `server.Adapter`: the engine handler for a caller that owns its own transport
  loop, such as the TLS engines in `http_tls`.
- `status.encodeMessage`: the percent encoder for a `grpc-message` field.
- `blocking.Client`: one connection that serves several calls. `unary` runs them
  in sequence, and `open` with `step` runs several at once: each event is handed
  to every call, and a call ignores the streams that are not its own.
- `blocking.ServerStream`: a server-streaming call. `next` returns one message
  at a time and `status` is the answer after it returns null. `init` takes a
  pointer because the h2 stream inside the call points at the connection.

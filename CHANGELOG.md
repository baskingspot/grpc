# Changelog

All notable changes to grpc are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- The client is `grpc.Client`, not `grpc.blocking.Client`: the same API runs on
  any `std.Io` implementation, and only blocks a thread under `Io.Threaded`.
  `Request`, `UnaryRequest`, `Config`, `Response`, `Captured`, `unary`,
  `unaryIo`, and `ServerStream` moved to the `grpc` root with it.
- `server.ReadinessServer`, `grpc_tls.ReadinessServer`, and `readiness` are
  experimental, with removal intended once `serve` carries TLS.

### Added

- `grpc.Header` and `grpc.Headers` (the `http` field and field list) and
  `grpc.OptionalDuration` and `grpc.OptionalTimestamp` (`stdx.time`) name the
  types in public fields, so callers need not reach through `@FieldType`. The
  layouts are unchanged; no namespace is re-exported.
- `server.Request.deadline` is the request's `grpc-timeout` as an
  `OptionalTimestamp` on the awake clock, taken once when the buffered request
  is dispatched. It is `.none` when the field is absent or malformed, as before.
  `Request.expired(io)` reports whether it has passed, without restarting the
  budget. It only answers: the blocking engine still cannot interrupt a running
  handler, and `io` must read the awake clock (std's threaded `Io` on Linux and
  macOS; other backends are not checked). `timeout.deadline` is the shared
  parse, now also used by `server.Stream`.
- `Stream` lends a byte-oriented call over either `blocking.Client` or
  `readiness.Session`, with common request backpressure, send-close, receive,
  status, metadata, cancellation, and transport-advance operations. Its `send`
  returns false while one earlier message frame is still pending; `deinit`
  releases only the call, not the borrowed client, session, or socket.
- `blocking.Client.unaryCaptured` and `grpc_tls.Connection.unaryCaptured`
  return `blocking.Captured`: the status and message as `unary` does, plus the
  response `headers` (a trailers-only answer's fields are here), `trailers`, and
  raw `error_details` (`google.rpc.Status` bytes). The metadata lives in the
  capture's arena, so call `Captured.deinit`; the message and status text stay in
  your `reply` buffer. `unary` is unchanged and still returns no metadata.
- `server.Stream.send`, `flush` and `unsent`: frame one response message into
  stream-owned storage and write it through `write`, which stays the raw byte
  sink. `send` returns `error.Pending` while the previous message is unsent and
  `error.TooLarge` above `send_limit`, which defaults to `message.default_limit`.
- `server.Stream.finish` waits (`error.WouldBlock`) until a queued message is
  fully sent, is safe to call again after it succeeds, and sends the trailers
  it first chose on a retry. At an expired deadline it replaces an unstarted
  message with DEADLINE_EXCEEDED, and resets with CANCEL after a partial one.
- `status.Code.fromHttp` maps an HTTP response status to a gRPC status code.
- `message`: the length-prefixed gRPC frame. `parseHeader` and `write` handle
  one message, and `Iterator` walks complete messages out of one buffer under a
  receive limit.
- `status`: the seventeen gRPC codes, `parse` for the decimal wire form,
  `Code.fromInt` for an integer, and `decode` for the percent-escaped
  `grpc-message` field.
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
- Socket drivers enforce deadlines with `Io.Batch.awaitConcurrent`. TLS readers enforce deadlines in the record layer. `unaryIo` callers enforce their own deadlines.
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
- `server`: `serve` and `serveConnection` answer buffered calls on http's blocking
  HTTP/2 engine, with the status in the trailing HEADERS section. A handler
  allocates its response from `Request.arena`, which lives until the stream is
  complete.
- `grpc_tls.ReadinessServer` serves buffered or streaming gRPC handlers over TLS through `http_tls`, with caller-owned certificates and signer.
- `server.Stream`, `StreamingHandler`, and `ReadinessServer.initStreaming` support incremental requests and responses, full-duplex calls, output backpressure, and request-head deadlines.
- `ReadinessOptions.body_bytes` and TLS `ServerOptions.common.body_bytes` default to 64 KiB. They cap unread streaming input or the complete buffered request.
- `grpc_tls`: the opt-in TLS module. `grpc_tls.Connection` handshakes over a
  socket and exposes a blocking gRPC client over it, with the deadline enforced
  by the record layer and an expiry reported as DEADLINE_EXCEEDED. The core
  module links no crypto.
- `blocking.Client.initIo` takes `IoOptions`, which says whether a deadline is
  enforced by polling the fd or by the reader. A decrypting transport must own
  it: reading the fd under TLS hands the connection ciphertext.
- `blocking.Client.initIo`: a client over any reader and writer pair, which is
  the shape a TLS transport presents as plaintext. A caller-owned pair is not
  closed here, and a deadline needs an fd, so a transport that decrypts in
  memory must enforce the deadline itself.
- `readiness.Session`: one connection, several calls in flight, driven by
  `std.Io` on the caller's thread. Each call carries its own deadline, and a
  deadline that passes ends that call DEADLINE_EXCEEDED without touching the
  others.
- The drivers use `Io` rather than raw syscalls: `server.serve` hands each
  connection to an `Io.Group` with `Group.concurrent` (inline when the
  implementation has no concurrency), and both the blocking client and the
  readiness session wait on a deadline with `Io.Batch.awaitConcurrent` instead of
  a hand-rolled `poll(2)`.
- `server.Request.messages` and `server.Response.messages` replace the single
  `msg`. A handler sees every request message and returns every response message,
  which makes client-stream, server-stream, and half-duplex bidi calls work on
  both buffered engines.
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

### Changed

- `server.serve` declares its error set as `Io.net.Server.AcceptError`, so
  the `Io.Future` that `Io.concurrent` returns for it can be named. Existing
  callers are unaffected: a connection's own failure still ends its task
  quietly and is not part of the set.

- The optional socket descriptor in `blocking.Client` and `blocking.Client.IoOptions` uses `stdx.posix.OptionalFd`. `.none` replaces null for a caller-supplied byte pair.
- Request `timeout` is a compact `stdx.time.OptionalDuration` instead of `deadline_ns`. Call and server deadlines use `OptionalTimestamp`; use `.none` for no deadline or wake. Arithmetic and conversion are checked.
- Unary helpers accept `UnaryRequest.message` without a gRPC prefix. Replace framed `.body` values with serialized bytes in `.message`.

- Enable `grpc_tls` with `.tls = true` on the build dependency. Core consumers no longer fetch TLS, CLI, or developer packages.
- Streaming `body_bytes` limits unread input instead of the total upload. The HTTP engine reuses body buffers as the handler reads them.

### Fixed

- `Call.cancel` now finishes the call as CANCELLED and closes the send side,
  so a later `closeSend` does not write on the reset stream. A response with two
  `grpc-status` fields now ends that call as INTERNAL ("duplicate grpc-status")
  instead of returning `Call.Error.DuplicateStatus`, which is removed.
- Expiring or dropping an unfinished `Call` queues RST_STREAM(CANCEL) once.
  Blocking and readiness timeout exits flush that reset.
- Terminal writes resolve unfinished calls as UNAVAILABLE in blocking and
  readiness clients, including unary helpers. Peer and deadline statuses win.
- `server`: an application wake persists when a streaming handler returns Pending or WouldBlock.
- `Call`: `message_limit` applies to each response message, even when DATA carries several. Fragmented prefixes and bodies wait for more input.
- `Call`: the response reassembly buffer drops messages already read, moving the unread tail forward once the read prefix is half the buffer, so a long stream of messages that straddle DATA frames no longer grows it without bound.
- `blocking` unary helpers: a status text that does not fit the caller's reply buffer is truncated, after the message, instead of replacing the peer's status with `error.MessageTooLarge`. `status.code` is the peer's; a message too large for the buffer is still `error.MessageTooLarge`. A larger `Config.message` or reply buffer keeps the whole text.
- `blocking.unary` and `blocking.ServerStream.init` start their deadlines before allocation, so setup time counts toward the call budget.
- `blocking.Client.open` rejects unsupported deadlines before it creates a stream or sends request bytes.
- `blocking.Client.step` and `unary` resolve unfinished calls as UNAVAILABLE after EOF or a terminal read failure. Previously resolved statuses remain unchanged.
- `blocking.Client.isPoisoned` reports terminal transport failures and EOF. New calls cannot reuse those transports.
- `blocking.Client.step` and `readiness.Session.wait` return after output progress, so callers can continue a stream before `closeSend`.
- `grpc_tls.Connection` uses one absolute deadline through every client path. `IoOptions.read_deadline` supplies deadline hooks for other plaintext readers.
- Client drivers release DATA leases for expired calls outside the active set. A late response no longer blocks subsequent calls.
- Accepted calls continue after a peer GOAWAY.
- `blocking.ServerStream.next` returns a complete message before the server sends its final status.
- Streaming adapters retain a failed call across blocked status writes and do not rearm an observed deadline during final response backpressure.
- The conformance server resets a stream if expiry interrupts a partial frame. It sends no trailers after truncated bytes.
- `grpc_tls.ReadinessServer.init` selects buffered body mode, regardless of `ServerOptions.body_mode`.
- `server.Stream.next` repeats terminal framing errors instead of a panic on a later read.
- Client drivers expire calls before event dispatch and ignore completed deadlines when they select a wait. `ServerStream` uses one deadline across response messages.
- `server` accepts case-insensitive gRPC media types, including types with parameters.
- `server` rejects non-POST requests with HTTP 405 and unsupported media types with HTTP 415 before the handler runs.

# Changelog

All notable changes to grpc are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Long-lived connections with heavy call churn no longer slow down as the
  connection ages: each gRPC call opens and closes one HTTP/2 stream, and the
  session's stream table collected a tombstone per closed stream, making every
  event's stream lookup probe longer for the life of the connection. The fix
  rides the `http` pin (`baskingspot/http` `0144d33`): the table rebuilds once
  removals outnumber live streams. Per-request server cycles fell 27%, and
  single-connection capacity rose 41% (8-byte payloads).

## [0.1.0] - 2026-11-12

### Added

- `message`, `status`, `timeout`, and `head`: the framing layer.
  `message.parseHeader` and `write` handle one length-prefixed frame with no
  allocator; `status` carries the seventeen codes with `parse`, `Code.fromInt`,
  and `decode` for the percent-escaped `grpc-message`; `timeout` reads and
  writes `grpc-timeout` in the coarsest exact unit; `head` builds the request
  fields and validates metadata.
- `Call`: one call over one HTTP/2 stream. It frames the request, reassembles
  response messages across DATA frames, and ends on `grpc-status`, a
  trailers-only head, an HTTP error status, a reset, or a GOAWAY.
- `grpc.Client`: one connection across calls. `unary` and `unaryCaptured`
  return the response, status, and metadata; `open` and `step` run several
  calls at once; `ServerStream` pulls a server-streaming response; `Stream` is
  the byte-oriented facade over a call, shared with the readiness client.
- `server.serve` and `server.serveStreaming` accept on the caller's `Io`, one
  task per connection, so a handler can block and take locks.
  `server.Handler` buffers the request; `server.Stream` answers incrementally,
  full-duplex included, and retains unsent bytes across backpressure.
- `grpc_tls`: the opt-in TLS module. `grpc_tls.Connection` presents a client
  over the plaintext side; `grpc_tls.serve` and `serveStreaming` run the same
  handlers over TLS. The core module links no crypto.
- The experimental readiness surfaces, with removal intended:
  `server.ReadinessServer`, `grpc_tls.ReadinessServer`, and `readiness`.
- The connectrpc conformance suite passes in full: 2,025 cases over the client
  and the server, cleartext and TLS, identity and gzip, and all five stream
  types.

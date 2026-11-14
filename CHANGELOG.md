# Changelog

All notable changes to grpc are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Long-lived connections with heavy call churn no longer slow down as the
  connection ages, and busy connections carry less CPU per call. Both fixes
  ride the `http` pin (`baskingspot/http` `0144d33`, `0266fef`, and `0fcded6`,
  whose slot pool is `stdx.slot`, with `stdx` moving to the same commit): the
  session's stream table rebuilds once removals outnumber live streams (each
  gRPC call leaves one tombstone otherwise), and the engine walks live
  streams through a used list instead of scanning every configured slot.
  Per-request server cycles fell 63% (9,840 → 3,649) across the four fixes
  below, and single-connection capacity rose 44% (8-byte payloads): the
  stream-table rebuild (`0144d33`), the engine's live-only slot walk
  (`0266fef`), the one-block request-head copy (`5ce1d35`), and the stream
  table that grows with the live count instead of a pre-reserved
  `max_streams` (`4f44384`) — the pre-reserve made every lookup probe long
  chains and every rebuild walk an oversized table. grpc's own dispatch
  walks request messages on the stack for the common unary case, spilling
  to the arena only past four messages.

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

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
- `blocking.Client`: one connection that serves several calls in sequence. One
  call at a time, and `unary` returns with the connection ready for the next.
- `blocking.ServerStream`: a server-streaming call. `next` returns one message
  at a time and `status` is the answer after it returns null. `init` takes a
  pointer because the h2 stream inside the call points at the connection.

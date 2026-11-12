//! gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no
//! code generation.
//!
//! The primitive layer is `message`: one length-prefixed frame, no allocator,
//! and no stream. A caller that already has HTTP/2 and its own message type
//! needs nothing above it.

const http = @import("http");
const time = @import("stdx").time;

/// The header field and field list that `server`, `Call`, and the client use.
pub const Header = http.Header;
pub const Headers = http.Headers;
/// The compact optional durations and timestamps that carry deadlines.
pub const OptionalDuration = time.OptionalDuration;
pub const OptionalTimestamp = time.OptionalTimestamp;

/// The client: one connection across calls. `unary` and `unaryIo` send one
/// call and return, `ServerStream` pulls a server-streaming response one
/// message at a time, and `open` and `step` run several calls at once. The
/// `io` value carries the concurrency model: a call blocks the calling thread
/// under `Io.Threaded` and suspends under an evented `Io` such as zio.
pub const Client = @import("client.zig").Client;
pub const ServerStream = @import("client.zig").ServerStream;
pub const Captured = @import("client.zig").Captured;
pub const Request = @import("client.zig").Request;
pub const UnaryRequest = @import("client.zig").UnaryRequest;
pub const Config = @import("client.zig").Config;
pub const Response = @import("client.zig").Response;
pub const unary = @import("client.zig").unary;
pub const unaryIo = @import("client.zig").unaryIo;

pub const Call = @import("Call.zig");
pub const Stream = @import("Stream.zig");
pub const head = @import("head.zig");
pub const message = @import("message.zig");
pub const readiness = @import("readiness.zig");
pub const server = @import("server.zig");
pub const status = @import("status.zig");
pub const timeout = @import("timeout.zig");

test {
    _ = @import("client.zig");
    _ = Call;
    _ = Stream;
    _ = head;
    _ = message;
    _ = readiness;
    _ = server;
    _ = status;
    _ = timeout;
}

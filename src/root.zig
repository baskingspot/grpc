//! gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no
//! code generation.
//!
//! The primitive layer is `message`: one length-prefixed frame, no allocator,
//! and no stream. A caller that already has HTTP/2 and its own message type
//! needs nothing above it.

const http = @import("http");
const time = @import("stdx").time;

/// The header field and field list that `server`, `Call` and `blocking` use.
pub const Header = http.Header;
pub const Headers = http.Headers;
/// The compact optional durations and timestamps that carry deadlines.
pub const OptionalDuration = time.OptionalDuration;
pub const OptionalTimestamp = time.OptionalTimestamp;

pub const blocking = @import("blocking.zig");
pub const Call = @import("Call.zig");
pub const Stream = @import("Stream.zig");
pub const head = @import("head.zig");
pub const message = @import("message.zig");
pub const readiness = @import("readiness.zig");
pub const server = @import("server.zig");
pub const status = @import("status.zig");
pub const timeout = @import("timeout.zig");

test {
    _ = blocking;
    _ = Call;
    _ = Stream;
    _ = head;
    _ = message;
    _ = readiness;
    _ = server;
    _ = status;
    _ = timeout;
}

//! gRPC in Zig: message framing, status codes, and calls over HTTP/2, with no
//! code generation.
//!
//! The primitive layer is `message`: one length-prefixed frame, no allocator,
//! and no stream. A caller that already has HTTP/2 and its own message type
//! needs nothing above it.

pub const blocking = @import("blocking.zig");
pub const Call = @import("Call.zig");
pub const head = @import("head.zig");
pub const message = @import("message.zig");
pub const readiness = @import("readiness.zig");
pub const server = @import("server.zig");
pub const status = @import("status.zig");
pub const timeout = @import("timeout.zig");

test {
    _ = blocking;
    _ = Call;
    _ = head;
    _ = message;
    _ = readiness;
    _ = server;
    _ = status;
    _ = timeout;
}

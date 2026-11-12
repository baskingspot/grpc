//! The byte-oriented client stream shared by the blocking and readiness drivers.
//!
//! A stream borrows its `Call` and its client or session. It adds no socket or
//! scheduling policy: `advance` runs the driver that made it. Request and
//! response bytes still belong to the call, and the raw call remains available
//! for operations this small facade does not carry.

const http = @import("http");

const Call = @import("Call.zig");
const message = @import("message.zig");

const Stream = @This();

call: *Call,
driver: Driver,

/// The operations a client supplies when it lends out one of its calls.
/// Applications normally get these from `Client.stream` or
/// `readiness.Session.stream` instead of constructing them directly.
pub const Driver = struct {
    context: *anyopaque,
    /// Every live call on a blocking client. Unused by a readiness session,
    /// whose own slots already describe the complete set.
    calls: []Call = &.{},
    advance: *const fn (*anyopaque, []Call) anyerror!bool,
    close: *const fn (*anyopaque, *Call) void,
};

/// Build a borrowed stream over a call and its driver. The call, driver
/// context, and every call in `driver.calls` must stay in place until `deinit`.
/// This is the adapter used by the two client implementations.
pub fn init(call: *Call, driver: Driver) Stream {
    return .{ .call = call, .driver = driver };
}

/// Release the call through the client that owns it. This does not close the
/// client, session, or socket. The stream must not be used again.
pub fn deinit(self: *Stream) void {
    self.driver.close(self.driver.context, self.call);
    self.* = undefined;
}

/// Drive the owning client: `Client.step`, or `Session.wait` on the
/// readiness client. False has that driver's existing meaning.
pub fn advance(self: *Stream) !bool {
    return self.driver.advance(self.driver.context, self.driver.calls);
}

/// Frame and queue one message if the previous queued message has drained.
/// False queues nothing and means backpressure: keep the bytes, call
/// `advance`, and retry. At most one message frame is lent to the call's writer
/// queue at a time. The call still owns the frame after this returns.
pub fn send(self: *Stream, bytes: []const u8) Call.Error!bool {
    if (self.call.pendingBytes() != 0) return false;
    try self.call.writeMessage(bytes);
    return true;
}

/// Bytes in the call's framed request queue that HTTP/2 has not accepted yet.
pub fn pendingBytes(self: *const Stream) usize {
    return self.call.pendingBytes();
}

/// Close the send side behind queued request bytes. The next `advance` sends
/// END_STREAM once the bytes drain.
pub fn closeSend(self: *Stream) void {
    self.call.closeSend();
}

/// True when every request byte is on the wire and END_STREAM was sent.
pub fn requestSent(self: *const Stream) bool {
    return self.call.requestSent();
}

/// The next complete response message, without advancing the connection.
/// Its bytes borrow the call and are valid until the next event or message
/// read. Call `advance` when this is null and `result` is still null.
pub fn nextMessage(self: *Stream) Call.Error!?message.Message {
    return self.call.nextMessage();
}

/// The final status, or null while the call is still running.
pub fn result(self: *const Stream) ?Call.Result {
    return self.call.result();
}

/// Response headers copied into the call, valid until the stream is released.
pub fn responseHeaders(self: *const Stream) ?http.Headers {
    return self.call.responseHeaders();
}

/// Response trailers copied into the call, valid until the stream is released.
pub fn responseTrailers(self: *const Stream) ?http.Headers {
    return self.call.responseTrailers();
}

/// Decoded binary error details owned by the call.
pub fn errorDetails(self: *const Stream) []const u8 {
    return self.call.errorDetails();
}

/// Ask the transport to cancel this call.
pub fn cancel(self: *Stream) !void {
    return self.call.cancel();
}

/// The sans-I/O call, for callers that need a field or operation outside this
/// facade. Its lifetime and receive-buffer borrowing rules are unchanged.
pub fn raw(self: *Stream) *Call {
    return self.call;
}

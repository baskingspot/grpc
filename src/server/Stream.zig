//! A gRPC stream on an HTTP/2 exchange, with no socket or codec.
//!
//! `next` reassembles one message across body chunks. `write` accepts framed
//! bytes and returns the accepted prefix. The caller retains the unsent tail.
//! `send` frames one message into stream-owned storage and `flush` drives it
//! through `write`. `begin` and `finish` write the response head and the gRPC
//! status trailers.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Base64Encoder = std.base64.standard_no_pad.Encoder;

const http = @import("http");
const Exchange = http.h2.engine.Exchange;

const message = @import("../message.zig");
const status = @import("../status.zig");
const timeout = @import("../timeout.zig");
const time = @import("stdx").time;

const Stream = @This();

/// The engine replaces this pointer on each invocation. Do not retain it.
exchange: *Exchange,
io: Io,
/// Application state that survives a pending read or write. Allocate it from
/// `exchange.arena` for the call lifetime. The adapter owns `Exchange.context`.
context: ?*anyopaque = null,
/// The request-head deadline on the awake clock, or `.none`.
deadline: time.OptionalTimestamp = .none,
/// Maximum framed message bytes. Zero permits empty messages only.
message_limit: u32 = message.default_limit,
/// Maximum message body bytes `send` accepts, excluding the five-byte prefix.
send_limit: u32 = message.default_limit,
input: []const u8 = &.{},
prefix: [message.prefix_len]u8 = undefined,
prefix_used: u8 = 0,
header: ?message.Header = null,
read_error: ?message.Error = null,
body: std.ArrayList(u8) = .empty,
ready: bool = false,
deadline_notified: bool = false,
finished: bool = false,
/// One framed message accepted by `send`: `queued[queued_sent..queued_len]` is
/// unsent. The storage comes from `exchange.arena`, grows only when a larger
/// message arrives, and is reused by every later message.
queued: []u8 = &.{},
queued_len: usize = 0,
queued_sent: usize = 0,
/// The trailers `finish` committed to. Retries send these, not new arguments.
final: ?[]const http.Header = null,

pub fn init(io: Io, exchange: *Exchange) time.Timestamp.Error!Stream {
    var self: Stream = .{ .io = io, .exchange = exchange };
    self.deadline = try timeout.deadline(
        exchange.request.head.headers.get("grpc-timeout"),
        try self.now(),
    );
    return self;
}

/// A message borrows this stream until the next `next`. Pending reads retain
/// incomplete prefixes and bodies. A clean end returns null. A framing error
/// is terminal and repeats on later reads.
pub fn next(self: *Stream) !?message.Message {
    if (self.read_error) |err| return err;
    if (self.ready) {
        self.body.clearRetainingCapacity();
        self.prefix_used = 0;
        self.header = null;
        self.ready = false;
    }
    while (true) {
        if (self.header) |header| {
            if (self.body.items.len == header.length) {
                self.ready = true;
                return .{ .compressed = header.compressed, .bytes = self.body.items };
            }
        }
        if (self.input.len == 0) {
            const source = self.exchange.request_body orelse return error.NotStreaming;
            const chunk = source.next() catch |err| switch (err) {
                error.EndOfStream => {
                    if (self.prefix_used != 0) {
                        self.read_error = error.Truncated;
                        return error.Truncated;
                    }
                    return null;
                },
                else => return err,
            };
            self.input = chunk.data;
        }
        if (self.prefix_used < message.prefix_len) {
            const count = @min(self.input.len, message.prefix_len - self.prefix_used);
            @memcpy(self.prefix[self.prefix_used..][0..count], self.input[0..count]);
            self.prefix_used += @intCast(count);
            self.input = self.input[count..];
            if (self.prefix_used != message.prefix_len) continue;
            const header = message.parseHeader(&self.prefix) catch |err| {
                self.read_error = err;
                return err;
            };
            if (header.length > self.message_limit) {
                self.read_error = error.TooLarge;
                return error.TooLarge;
            }
            self.header = header;
            continue;
        }
        const count = @min(self.input.len, self.header.?.length - self.body.items.len);
        try self.body.appendSlice(self.exchange.arena, self.input[0..count]);
        self.input = self.input[count..];
    }
}

pub fn begin(self: *Stream, headers: []const http.Header) !void {
    try self.exchange.beginResponse(.{
        .status = .ok,
        .content_type = "application/grpc",
        .headers = headers,
    });
}

/// Write already-framed gRPC bytes. Zero means backpressure, not completion.
/// This is the raw byte sink: the caller keeps the unaccepted tail and no
/// message state is involved. Do not interleave it with a message that
/// `unsent` reports as incomplete, or the frames corrupt each other.
pub fn write(self: *Stream, bytes: []const u8) !usize {
    return self.exchange.write(bytes);
}

/// Frame `bytes` and queue the message, then try to write it. The framed copy
/// lives in stream-owned storage, so `bytes` may change once this returns.
///
/// A previous message that is still unsent is flushed first. If it cannot
/// finish, this returns `error.Pending` and `bytes` is not accepted: return
/// the error from the handler and call `send` again on the next turn. After a
/// successful return a rest that the transport has not taken stays queued; see
/// `unsent` and `flush`. `error.TooLarge` rejects a body above `send_limit`.
/// Any other error is a transport failure and ends the stream.
pub fn send(self: *Stream, bytes: []const u8, compressed: bool) !void {
    if (self.finished) return error.Finished;
    if (bytes.len > self.send_limit) return error.TooLarge;
    try self.flush();
    const total = message.prefix_len + bytes.len;
    if (self.queued.len < total) {
        const limit = @as(usize, self.send_limit) + message.prefix_len;
        const grown = @min(@max(total, self.queued.len * 2), limit);
        self.queued = try self.exchange.arena.alloc(u8, grown);
    }
    message.writeHeader(self.queued[0..message.prefix_len], compressed, @intCast(bytes.len));
    @memcpy(self.queued[message.prefix_len..total], bytes);
    self.queued_len = total;
    self.queued_sent = 0;
    self.flush() catch |err| switch (err) {
        error.Pending => {},
        else => return err,
    };
}

/// Write the queued message through `write` until it is complete. Zero accepted
/// bytes returns `error.Pending`; the cursor keeps every accepted byte, and the
/// next call continues from it. With nothing queued this succeeds.
pub fn flush(self: *Stream) !void {
    while (self.queued_sent < self.queued_len) {
        const accepted = try self.write(self.queued[self.queued_sent..self.queued_len]);
        if (accepted == 0) return error.Pending;
        self.queued_sent += accepted;
    }
    self.queued_len = 0;
    self.queued_sent = 0;
}

/// Bytes of the queued message the transport has not accepted.
pub fn unsent(self: *const Stream) usize {
    return self.queued_len - self.queued_sent;
}

/// Send the final status, once. A call after completion returns without effect.
/// Retry on WouldBlock, which also reports a queued message that is still
/// unsent: the trailers follow only after its last byte. The first call that
/// reaches the trailers fixes them, and a retry sends those exactly.
///
/// An expired RPC deadline ends a message that cannot finish. If no byte of it
/// was sent, it is dropped and the status becomes DEADLINE_EXCEEDED unless
/// `result` already is an error. If part was sent, the stream is reset with
/// CANCEL and no trailers follow a truncated message. Before expiry the
/// message is waited for, so a clean finish never drops accepted output.
/// A blocked, expired final status waits for output credit instead of another
/// immediate deadline wake.
pub fn finish(
    self: *Stream,
    result: status.Status,
    metadata: []const http.Header,
    details: []const u8,
) !void {
    if (self.finished) return;
    if (try self.expired()) self.deadline_notified = true;
    if (self.final == null) {
        var outcome = result;
        var kept_details = details;
        if (self.unsent() != 0) {
            self.flush() catch |err| switch (err) {
                error.Pending => {
                    if (!try self.expired()) return error.WouldBlock;
                    if (self.queued_sent != 0) {
                        try self.exchange.resetStream(.cancel);
                        self.queued_len = 0;
                        self.queued_sent = 0;
                        self.exchange.setDeadline(.none);
                        self.finished = true;
                        return;
                    }
                    self.queued_len = 0;
                    if (outcome.code == .ok) {
                        outcome = .{ .code = .deadline_exceeded, .message = "deadline exceeded" };
                        kept_details = &.{};
                    }
                },
                else => return err,
            };
        }
        if (!self.exchange.response_started) try self.begin(&.{});
        self.final = try trailers(self.exchange.arena, outcome, metadata, kept_details);
    }
    try self.exchange.endResponse(self.final.?);
    self.exchange.setDeadline(.none);
    self.finished = true;
}

/// Yield until input, output credit, or the earlier of this wake and the RPC
/// deadline. A null wake waits only for progress or the RPC deadline.
pub fn park(self: *Stream, wake: time.OptionalTimestamp) time.Timestamp.Error!void {
    var at = wake;
    if (self.deadline.get()) |deadline| {
        // An expired status write waits for credit. The timer already fired.
        if (!self.deadline_notified or deadline.compare(try self.now()) == .gt) {
            if (at.get()) |existing| {
                if (deadline.compare(existing) == .lt) at = try .init(deadline);
            } else {
                at = try .init(deadline);
            }
        }
    }
    self.exchange.setDeadline(at);
    self.exchange.waitForProgress();
}

pub fn now(self: *const Stream) time.Timestamp.Error!time.Timestamp {
    return .fromStd(Io.Clock.now(.awake, self.io));
}

pub fn expired(self: *const Stream) time.Timestamp.Error!bool {
    const deadline = self.deadline.get() orelse return false;
    return (try self.now()).compare(deadline) != .lt;
}

/// Serialize the status identically for buffered and incremental responses.
pub fn trailers(
    arena: Allocator,
    result: status.Status,
    metadata: []const http.Header,
    details: []const u8,
) ![]const http.Header {
    const encoded = try arena.alloc(u8, status.encodedLen(result.message));
    const text = try status.encodeMessage(encoded, result.message);
    const code = try arena.print("{d}", .{@backingInt(result.code)});
    const with_message: u32 = @intFromBool(text.len != 0);
    const with_details: u32 = @intFromBool(details.len != 0);
    const fields = try arena.alloc(http.Header, 1 + with_message + with_details + metadata.len);
    fields[0] = .{ .name = "grpc-status", .value = code };
    var at: u32 = 1;
    if (with_message != 0) {
        fields[at] = .{ .name = "grpc-message", .value = text };
        at += 1;
    }
    if (with_details != 0) {
        const buffer = try arena.alloc(
            u8,
            Base64Encoder.calcSize(details.len),
        );
        fields[at] = .{
            .name = "grpc-status-details-bin",
            .value = Base64Encoder.encode(buffer, details),
        };
        at += 1;
    }
    @memcpy(fields[at..], metadata);
    return fields;
}

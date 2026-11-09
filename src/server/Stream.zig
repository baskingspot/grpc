//! A gRPC stream on an HTTP/2 exchange, with no socket or codec.
//!
//! `next` reassembles one message across body chunks. `write` accepts framed
//! bytes and returns the accepted prefix. The caller retains the unsent tail.
//! `begin` and `finish` write the response head and the gRPC status trailers.

const Stream = @This();
const std = @import("std");
const http = @import("http");
const message = @import("../message.zig");
const status = @import("../status.zig");
const timeout = @import("../timeout.zig");
const Io = std.Io;
const Exchange = http.h2.engine.Exchange;

/// The engine replaces this pointer on each invocation. Do not retain it.
exchange: *Exchange,
io: Io,
/// Application state that survives a pending read or write. Allocate it from
/// `exchange.arena` for the call lifetime. The adapter owns `Exchange.context`.
context: ?*anyopaque = null,
/// The request-head deadline, in absolute awake-clock nanoseconds.
deadline_ns: ?i96 = null,
/// Maximum framed message bytes. Zero permits empty messages only.
message_limit: u32 = message.default_limit,
input: []const u8 = &.{},
prefix: [message.prefix_len]u8 = undefined,
prefix_used: u8 = 0,
header: ?message.Header = null,
read_error: ?message.Error = null,
body: std.ArrayList(u8) = .empty,
ready: bool = false,
deadline_notified: bool = false,
finished: bool = false,

pub fn init(io: Io, exchange: *Exchange) Stream {
    var self: Stream = .{ .io = io, .exchange = exchange };
    if (exchange.request.head.headers.get("grpc-timeout")) |raw| {
        if (timeout.parse(raw)) |parsed| {
            self.deadline_ns = self.now() + @as(i96, parsed.toNanos());
        } else |_| {}
    }
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
pub fn write(self: *Stream, bytes: []const u8) !usize {
    return self.exchange.write(bytes);
}

/// Send the final status. Retry on WouldBlock. A blocked, expired final status
/// waits for output credit instead of another immediate deadline wake.
/// The caller must complete each partial frame or reset the stream first.
pub fn finish(
    self: *Stream,
    result: status.Status,
    metadata: []const http.Header,
    details: []const u8,
) !void {
    if (self.expired()) self.deadline_notified = true;
    if (!self.exchange.response_started) try self.begin(&.{});
    try self.exchange.endResponse(try trailers(self.exchange.arena, result, metadata, details));
    self.exchange.setDeadline(null);
    self.finished = true;
}

/// Yield until input, output credit, or the earlier of this wake and the RPC
/// deadline. A null wake waits only for progress or the RPC deadline.
pub fn park(self: *Stream, wake_ns: ?i96) void {
    var at = wake_ns;
    if (self.deadline_ns) |deadline| {
        // An expired status write waits for credit. The timer already fired.
        if (!self.deadline_notified or deadline > self.now()) {
            if (at == null or deadline < at.?) at = deadline;
        }
    }
    self.exchange.setDeadline(at);
    self.exchange.waitForProgress();
}

pub fn now(self: *const Stream) i96 {
    return Io.Clock.awake.now(self.io).nanoseconds;
}

pub fn expired(self: *const Stream) bool {
    const deadline = self.deadline_ns orelse return false;
    return self.now() >= deadline;
}

/// Serialize the status identically for buffered and incremental responses.
pub fn trailers(
    arena: std.mem.Allocator,
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
            std.base64.standard_no_pad.Encoder.calcSize(details.len),
        );
        fields[at] = .{
            .name = "grpc-status-details-bin",
            .value = std.base64.standard_no_pad.Encoder.encode(buffer, details),
        };
        at += 1;
    }
    @memcpy(fields[at..], metadata);
    return fields;
}
